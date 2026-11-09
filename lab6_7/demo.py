import argparse
import itertools
import math
import os
import shlex
import sys
import time

from client import KVError
from cluster import LOG_ROOT, NODE_IDS, Cluster, default_ports_if_free
from server import clock_text, vclock_text

N = len(NODE_IDS)


class LogTail:
    """Prints the new lines of the replicas' log files as one timeline."""

    def __init__(self, cluster):
        self.cluster = cluster
        self.offsets = {node: 0 for node in cluster.nodes}

    def new_lines(self):
        lines = []
        for node in self.cluster.nodes:
            path = self.cluster.log_path(node)
            if not os.path.exists(path):
                continue
            with open(path, "rb") as file:
                file.seek(self.offsets[node])
                data = file.read()
            end = data.rfind(b"\n") + 1  # only complete lines
            self.offsets[node] += end
            for line in data[:end].decode("utf-8", errors="replace").splitlines():
                if line.strip():
                    lines.append(line)
        lines.sort(key=lambda line: line[:15])  # HH:MM:SS.ffffff, all replicas share one clock
        return lines

    def show(self, wait=0.15):
        time.sleep(wait)  # the replicas log asynchronously
        for line in self.new_lines():
            print("    | " + line)


class Demo:

    def __init__(self, cluster, step_mode, show_logs):
        self.cluster = cluster
        self.client = cluster.client()
        self.step_mode = step_mode
        self.show_logs = show_logs
        self.tail = LogTail(cluster)

    def step(self, title):
        if self.step_mode:
            input("\n  [Enter] next step ")
        print()
        print("=" * 100)
        print(title)
        print("=" * 100)

    def note(self, text):
        for line in text.strip().splitlines():
            print("  " + line.strip())

    def logs(self):
        if self.show_logs:
            self.tail.show()
        else:
            self.tail.new_lines()

    def put(self, key, value, strategy, node, timestamp=0):
        label = {"cp": "QUORUM_CP", "ap": "ALWAYS_AP"}[strategy]
        print(f"\n  > put {key}={value!r} {label} via {node}")
        try:
            result = self.client.put(key, value, strategy, node, timestamp)
            print(f"    OK    success=True, acks_received={result.acks}/{N}")
        except KVError as error:
            acks = error.reply.acks_received if error.reply is not None else 0
            print(f"    FAIL  success=False, acks_received={acks}: {error}")
        self.logs()

    def get(self, key, consistency, node, max_staleness_ms=5000):
        label = {"linearizable": "LINEARIZABLE", "stale": "FOLLOWER_STALE"}[consistency]
        extra = f" max_staleness_ms={max_staleness_ms}" if consistency == "stale" else ""
        print(f"\n  > get {key} {label}{extra} via {node}")
        try:
            result = self.client.get(key, consistency, node, max_staleness_ms)
            lag = f", lag {result.lag_ms} ms" if result.lag_ms is not None else ""
            value = repr(result.value) if result.found else "not found"
            print(f"    OK    value={value}  served by {result.served_by}, is_stale={result.is_stale}{lag}, "
                  f"read_ts={clock_text(result.read_timestamp)}, {result.latency_ms:.2f} ms")
        except KVError as error:
            print(f"    FAIL  {type(error).__name__}: {error}")
        self.logs()

    def replicas(self, key):
        print(f"\n  state of {key!r} on every replica:")
        for node, version in self.client.inspect(key).items():
            if version == "down":
                text = "DOWN"
            elif version is None:
                text = "(not found)"
            else:
                text = (f"{version.value!r:<16} ts={clock_text(version.timestamp)}  "
                        f"vc={vclock_text(dict(version.vclock))}")
            print(f"    {node}  {text}")

    # ------------------------------------------------------------------------------

    def run(self, skip_union):
        cluster = self.cluster
        client = self.client
        print("CAP theorem / PACELC demo: replicated key-value store, N = 3")
        print(cluster.describe())
        print("  Replica-to-replica traffic goes through TCP proxies (netsim.py), so the network can be cut.")
        print("  Lines starting with '|' are the replicas' own logs.")
        self.logs()

        self.step("Step 1. QUORUM_CP write through follower B (Task 2: B forwards it to leaseholder A)")
        self.note("""A stages the write on the followers (Prepare), commits after a majority of acks,
                     then the commit reaches the followers through anti-entropy (Sync).""")
        self.put("cart", "milk", "cp", "B")
        cluster.wait_converged("cart", "milk")
        self.replicas("cart")

        self.step("Step 2. The two read paths (Task 2)")
        self.note("""LINEARIZABLE: C proxies the read to the remote leaseholder A (two hops).
                     FOLLOWER_STALE: C answers from its own copy (one hop), marked is_stale=True.""")
        self.get("cart", "linearizable", "C")
        self.get("cart", "stale", "C")

        self.step("Step 3. Network partition: DC-1 {A, B} | DC-2 {C}")
        cluster.partition(["A", "B"], ["C"])
        print("  " + cluster.net.describe())
        self.logs()

        self.step("Step 4. CP mode under the partition (Task 3, Part A)")
        self.note("Majority side: 2 of 3 replicas acknowledge -> committed.")
        self.put("cart", "milk,eggs", "cp", "A")
        self.note("Minority side: C cannot reach the leaseholder or a majority -> rejected, nothing written.")
        self.put("cart", "milk,bread", "cp", "C")

        self.step("Step 5. Reads during the partition (PACELC: if Partition, choose A or C)")
        self.get("cart", "linearizable", "C")
        self.note("C refuses the linearizable read (consistency), but a follower read stays available:")
        self.get("cart", "stale", "C")
        self.note("The value is old: 'milk,eggs' was committed in DC-1 and C cannot know it.")

        self.step("Step 6. AP mode under the partition (Task 3, Part B)")
        self.note("ALWAYS_AP is accepted by the one replica that can be reached (sloppy quorum).")
        self.put("cart", "milk,bread", "ap", "C")
        self.replicas("cart")
        self.note("The replicas now disagree: two concurrent versions (look at the vector clocks).")

        self.step("Step 7. Bounded staleness: C stops hearing from the leaseholder")
        bound = 1500
        cluster.wait_for(lambda: client.status("C").lag_ms > bound, timeout=10, what="C lag > bound")
        self.get("cart", "stale", "C", max_staleness_ms=bound)
        self.note("With a looser bound the same follower still answers:")
        self.get("cart", "stale", "C", max_staleness_ms=5000)

        self.step("Step 8. The partition heals: anti-entropy + last-write-wins")
        cluster.heal()
        final = cluster.wait_converged("cart")
        self.logs()
        self.replicas("cart")
        self.note(f"""All replicas converged to {final!r}: the concurrent write with the higher timestamp.
                      The other one ('milk,eggs' from DC-1) is silently lost - the price of LWW.""")
        self.get("cart", "linearizable", "C")

        self.step("Step 9. Quorum arithmetic: does a read quorum of R see a write acknowledged by W?")
        self.quorum_table()

        if not skip_union:
            self.step("Step 10. The same conflict with --merge union (Dynamo shopping cart)")
            self.union_demo()

        print("\nDone. Logs of the replicas:", os.path.dirname(cluster.log_path("A")))

    def quorum_table(self):
        cluster = self.cluster
        client = self.client
        old_show = self.show_logs
        self.show_logs = False
        rows = []
        for w in (1, 2, 3):
            for r in (1, 2, 3):
                key = f"quorum-w{w}-r{r}"
                client.put(key, "old", "cp", "A")
                cluster.wait_converged(key, "old")
                write_set = NODE_IDS[:w]
                others = NODE_IDS[w:]
                if others:
                    cluster.partition(write_set, others)
                acks = client.put(key, "new", "ap", "A").acks
                stale = []
                read_sets = list(itertools.combinations(NODE_IDS, r))
                for read_set in read_sets:
                    newest, _ = client.quorum_get(key, read_set)
                    if newest.value != "new":
                        stale.append("{" + ",".join(read_set) + "}")
                cluster.heal()
                cluster.wait_ready()
                rows.append((w, r, acks, len(read_sets), stale))
        self.logs()
        self.show_logs = old_show

        print(f"\n  N = {N}. The write reaches exactly W replicas (partition), then every read set of size R")
        print("  is asked; it returns the version with the highest timestamp.\n")
        print("    W  R  R+W  R+W>N   write acks  read sets  stale read sets  which")
        for w, r, acks, total, stale in rows:
            strong = "yes" if r + w > N else "no"
            which = " ".join(stale) if stale else "-"
            print(f"    {w}  {r}  {r + w:<3}  {strong:<6}  {acks:<10}  {total:<9}  {len(stale):<15}  {which}")
            assert len(stale) == math.comb(N - w, r)
        print("\n  Stale reads happen only when R + W <= N: then a read set can miss every replica of the write.")

    def union_demo(self):
        old = self.cluster
        old.stop()
        log_dir = os.path.join(LOG_ROOT, "demo-union")
        clean_logs(log_dir)
        cluster = Cluster(ports=old.ports, merge="union", log_dir=log_dir)
        cluster.start()
        self.cluster = cluster
        self.client = cluster.client()
        self.tail = LogTail(cluster)
        self.tail.new_lines()
        try:
            print("  New cluster started with --merge union.")
            self.put("cart", "milk", "cp", "A")
            cluster.wait_converged("cart", "milk")
            cluster.partition(["A", "B"], ["C"])
            print("  " + cluster.net.describe())
            self.put("cart", "milk,eggs", "ap", "A")
            self.put("cart", "milk,bread", "ap", "C")
            cluster.heal()
            print("  partition healed")
            final = cluster.wait_converged("cart")
            self.logs()
            self.replicas("cart")
            self.note(f"Union merge keeps both concurrent carts: {final!r} (LWW kept only one of them).")
        finally:
            cluster.stop()


def clean_logs(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    for name in os.listdir(log_dir):
        if name.endswith(".log"):
            os.remove(os.path.join(log_dir, name))


def parse_groups(words):
    """['AB', 'C'] or ['A,B', 'C'] or ['AB|C'] -> [['A', 'B'], ['C']]"""
    text = " ".join(words).replace("|", " ")
    groups = []
    for part in text.split():
        group = [node for node in part.replace(",", "").upper() if node in NODE_IDS]
        if group:
            groups.append(group)
    return groups


SHELL_HELP = """commands (nodes are A, B, C; A is the leaseholder):
  put KEY VALUE [cp|ap] [NODE] [+MS]     write; +MS sets the LWW timestamp to now+MS (ap only)
  get KEY [lin|stale] [NODE] [MAX_MS]    read; MAX_MS = max_staleness_ms for stale reads
  inspect KEY                            the version every replica holds
  quorum KEY NODES                       quorum read from e.g. A,C (newest timestamp wins)
  partition GROUP GROUP                  e.g. 'partition AB C'
  isolate NODE | heal                    cut one node off | repair the network
  crash NODE | restart NODE              kill a replica process | start it again (empty store)
  status | logs on | logs off | help | quit"""


def shell(demo):
    cluster = demo.cluster
    print(SHELL_HELP)
    print("\nThe replicas also accept client.py from another terminal, e.g.:")
    print(f"  python client.py get cart --consistency stale --node C   (ports {cluster.ports})")
    while True:
        try:
            line = input("\nkv> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not line:
            continue
        try:
            words = shlex.split(line)
        except ValueError as error:
            print("  " + str(error))
            continue
        command = words[0].lower()
        args = words[1:]
        try:
            if command in ("quit", "exit", "q"):
                return
            elif command == "help":
                print(SHELL_HELP)
            elif command == "put" and len(args) >= 2:
                strategy, node, timestamp = "cp", "A", 0
                for word in args[2:]:
                    if word.lower() in ("cp", "ap"):
                        strategy = word.lower()
                    elif word.upper() in NODE_IDS:
                        node = word.upper()
                    elif word.startswith("+") and word[1:].isdigit():
                        timestamp = time.time_ns() // 1000 + int(word[1:]) * 1000
                demo.put(args[0], args[1], strategy, node, timestamp)
            elif command == "get" and len(args) >= 1:
                consistency, node, bound = "linearizable", "A", 5000
                for word in args[1:]:
                    if word.lower() in ("lin", "linearizable"):
                        consistency = "linearizable"
                    elif word.lower() in ("stale", "follower"):
                        consistency = "stale"
                    elif word.upper() in NODE_IDS:
                        node = word.upper()
                    elif word.isdigit():
                        bound = int(word)
                demo.get(args[0], consistency, node, bound)
            elif command == "inspect" and len(args) == 1:
                demo.replicas(args[0])
            elif command == "quorum" and len(args) == 2:
                nodes = [node for node in args[1].upper() if node in NODE_IDS]
                newest, answers = demo.client.quorum_get(args[0], nodes)
                for node, version in answers.items():
                    shown = repr(version.value) + " ts=" + clock_text(version.timestamp) if version else "(not found)"
                    print(f"    {node}  {shown}")
                if newest is None:
                    print(f"  R={len(nodes)}: not found")
                else:
                    print(f"  R={len(nodes)}: newest = {newest.value!r}")
            elif command == "partition" and args:
                cluster.partition(*parse_groups(args))
                print("  " + cluster.net.describe())
                demo.logs()
            elif command == "isolate" and len(args) == 1:
                cluster.isolate(args[0].upper())
                print("  " + cluster.net.describe())
                demo.logs()
            elif command == "heal":
                cluster.heal()
                print("  " + cluster.net.describe())
                demo.tail.show(wait=0.8)
            elif command == "crash" and len(args) == 1:
                cluster.crash(args[0].upper())
                print(f"  {args[0].upper()} killed")
                demo.logs()
            elif command == "restart" and len(args) == 1:
                cluster.restart(args[0].upper())
                print(f"  {args[0].upper()} started again with an empty store")
                demo.tail.show(wait=0.8)
            elif command == "status":
                for node in cluster.nodes:
                    reply = demo.client.status(node)
                    if reply is None:
                        print(f"  {node}  DOWN")
                        continue
                    lag = "-" if reply.role == "leaseholder" else f"{reply.lag_ms} ms"
                    recovering = " RECOVERING" if reply.recovering else ""
                    print(f"  {node}  {reply.role:<11}{recovering} keys={reply.keys} lag={lag} "
                          f"reachable peers: {', '.join(reply.reachable_peers) or 'none'}")
                print("  network: " + cluster.net.describe())
            elif command == "logs" and args and args[0] in ("on", "off"):
                demo.show_logs = args[0] == "on"
            elif command == "logs":
                demo.tail.show(wait=0)
            else:
                print("  unknown command, type 'help'")
        except KVError as error:
            print(f"  FAIL {error}")
        except Exception as error:
            print(f"  error: {error}")


def main():
    sys.stdout.reconfigure(errors="backslashreplace")
    parser = argparse.ArgumentParser(description="CAP / PACELC walkthrough on a 3-replica cluster.")
    parser.add_argument("--step", action="store_true", help="wait for Enter before every step")
    parser.add_argument("--interactive", action="store_true", help="start the cluster and open a prompt")
    parser.add_argument("--no-logs", action="store_true", help="do not print the replicas' log lines")
    parser.add_argument("--skip-union", action="store_true", help="skip the union-merge step")
    args = parser.parse_args()

    ports = default_ports_if_free()
    if ports is None:
        print("Ports 50051-50053 are busy (are servers already running?), using free ports instead.")
    log_dir = os.path.join(LOG_ROOT, "interactive" if args.interactive else "demo")
    clean_logs(log_dir)
    cluster = Cluster(ports=ports, log_dir=log_dir)
    print("starting replicas A, B, C ...")
    cluster.start()
    demo = Demo(cluster, args.step, not args.no_logs)
    try:
        if args.interactive:
            print(cluster.describe())
            demo.logs()
            shell(demo)
        else:
            demo.run(args.skip_union)
    finally:
        demo.cluster.stop()
        cluster.stop()


if __name__ == "__main__":
    main()
