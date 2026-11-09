import argparse
import json
import math
import os
import platform
import statistics
import sys
import time

from client import KVError
from cluster import LAB_DIR, Cluster

KEY = "bench"


def p95(values):
    """Sort the values and take the one at position ceil(0.95 * n)."""
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def run_reads(client, consistency, node, iterations, max_staleness_ms):
    """Send `iterations` Get requests one after another. Latency is measured on the client."""
    latencies = []
    errors = {}
    for i in range(iterations):
        start = time.perf_counter()
        try:
            client.get(KEY, consistency, node, max_staleness_ms)
        except KVError as error:
            errors[error.code] = errors.get(error.code, 0) + 1
            continue
        latencies.append((time.perf_counter() - start) * 1000)
    result = {"attempts": iterations, "ok": len(latencies), "errors": errors,
              "availability": 100.0 * len(latencies) / iterations, "median": None, "p95": None}
    if latencies:
        result["median"] = statistics.median(latencies)
        result["p95"] = p95(latencies)
    return result


def ms(value):
    if value is None:
        return "-"
    return f"{value:.2f}"


def main():
    sys.stdout.reconfigure(errors="backslashreplace")
    parser = argparse.ArgumentParser(description="PACELC read benchmark (Task 4).")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--max-staleness-ms", type=int, default=5000)
    parser.add_argument("--link-delay-ms", type=int, default=0,
                        help="extra delay on every replica-to-replica link (simulated WAN), default 0")
    parser.add_argument("--skip-partition", action="store_true", help="only the healthy-cluster runs")
    parser.add_argument("--out", default=os.path.join(LAB_DIR, "results"), help="folder for the result files")
    args = parser.parse_args()

    n = args.iterations
    bound = args.max_staleness_ms
    lines = []

    def say(text=""):
        print(text, flush=True)
        lines.append(text)

    log_dir = os.path.join(LAB_DIR, "logs", "bench")
    cluster = Cluster(quiet=True, link_delay_ms=args.link_delay_ms, max_staleness_ms=bound, log_dir=log_dir)
    print("starting 3 replicas ...", flush=True)
    cluster.start()
    client = cluster.client()
    results = {}
    try:
        client.put(KEY, "value-1", "cp", node="A")
        cluster.wait_for(lambda: client.get(KEY, "stale", "C", bound).value == "value-1", what="C to sync")

        # Warm-up: open all connections before measuring.
        run_reads(client, "linearizable", "C", 100, bound)
        run_reads(client, "stale", "C", 100, bound)
        run_reads(client, "linearizable", "A", 100, bound)

        print(f"healthy cluster: {n} reads per scenario ...", flush=True)
        results["lin_remote"] = run_reads(client, "linearizable", "C", n, bound)
        results["follower"] = run_reads(client, "stale", "C", n, bound)
        results["lin_direct"] = run_reads(client, "linearizable", "A", n, bound)

        if not args.skip_partition:
            print("partition DC-1 {A, B} | DC-2 {C} ...", flush=True)
            cluster.partition(["A", "B"], ["C"])
            time.sleep(0.2)
            started = time.monotonic()
            results["lin_remote_partition"] = run_reads(client, "linearizable", "C", n, bound)
            results["follower_partition"] = run_reads(client, "stale", "C", n, bound)
            results["partition_run_s"] = time.monotonic() - started

            print(f"waiting until C lags more than max_staleness_ms={bound} ...", flush=True)
            cluster.wait_for(lambda: client.status("C").lag_ms > bound, timeout=bound / 1000 + 10,
                             what="C to exceed the staleness bound")
            results["lag_after_wait_ms"] = client.status("C").lag_ms
            results["follower_partition_late"] = run_reads(client, "stale", "C", n, bound)
            cluster.heal()
    finally:
        cluster.stop()

    lin = results["lin_remote"]
    fol = results["follower"]
    direct = results["lin_direct"]
    say("PACELC read benchmark (Task 4)")
    say(f"3 replicas on localhost, {n} sequential Get requests per scenario, client next to follower C.")
    say(f"Leaseholder A is the remote node. FOLLOWER_STALE uses max_staleness_ms = {bound}.")
    if args.link_delay_ms:
        say(f"Simulated WAN: every replica-to-replica link adds {args.link_delay_ms} ms per direction.")
    say(f"Machine: {platform.platform()}, Python {platform.python_version()}")
    say()

    if args.skip_partition:
        say("| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |")
        say("|---|---|---|---|---|")
        say(f"| Linearizable | Leaseholder (Remote) | {ms(lin['median'])} | {ms(lin['p95'])} | "
            f"{lin['availability']:.1f} |")
        say(f"| Follower Stale | Local Follower | {ms(fol['median'])} | {ms(fol['p95'])} | "
            f"{fol['availability']:.1f} |")
    else:
        lin_p = results["lin_remote_partition"]
        fol_p = results["follower_partition"]
        fol_late = results["follower_partition_late"]
        say("Table 1. Lab table. Latency: healthy cluster. Availability: healthy / during the partition.")
        say("| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |")
        say("|---|---|---|---|---|")
        say(f"| Linearizable | Leaseholder (Remote) | {ms(lin['median'])} | {ms(lin['p95'])} | "
            f"{lin['availability']:.1f} / {lin_p['availability']:.1f} |")
        say(f"| Follower Stale | Local Follower | {ms(fol['median'])} | {ms(fol['p95'])} | "
            f"{fol['availability']:.1f} / {fol_p['availability']:.1f} |")
        say()
        say("Table 2. Every scenario in detail.")
        say("| Scenario | Network | Requests | Successful | Availability (%) | Median (ms) | p95 (ms) | Errors |")
        say("|---|---|---|---|---|---|---|---|")
        early = f"partition, first {results['partition_run_s']:.1f} s"
        rows = [
            ("Linearizable via C -> leaseholder A", "healthy", lin),
            ("Follower stale on C", "healthy", fol),
            ("Linearizable via A (client next to leaseholder)", "healthy", direct),
            ("Linearizable via C -> leaseholder A", early, lin_p),
            ("Follower stale on C", early, fol_p),
            ("Follower stale on C", f"partition, C lag {results['lag_after_wait_ms']} ms > {bound} ms",
             fol_late),
        ]
        for label, network, result in rows:
            errors = ", ".join(f"{code} x{count}" for code, count in result["errors"].items()) or "-"
            say(f"| {label} | {network} | {result['attempts']} | {result['ok']} | "
                f"{result['availability']:.1f} | {ms(result['median'])} | {ms(result['p95'])} | {errors} |")
    say()
    say(f"Linearizable via C costs {lin['median'] / fol['median']:.1f}x the median latency of a follower read "
        f"(an extra hop to the leaseholder).")

    os.makedirs(args.out, exist_ok=True)
    suffix = f"_wan{args.link_delay_ms}ms" if args.link_delay_ms else ""
    with open(os.path.join(args.out, f"perf_cap_bench{suffix}.json"), "w", encoding="utf-8") as file:
        json.dump({"iterations": n, "max_staleness_ms": bound, "link_delay_ms": args.link_delay_ms,
                   "results": results}, file, indent=2)
    with open(os.path.join(args.out, f"perf_cap_bench{suffix}.txt"), "w", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")
    print(f"\nsaved to {args.out}", flush=True)


if __name__ == "__main__":
    main()
