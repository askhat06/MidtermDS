"""Task C4: how long does one Increment take?

Run from the project folder:  python tests/perf_benchmark.py

Four configurations: 1 replica or 3 replicas, 1 client or 16 clients.
The replicas are started inside this script.
"""
import math
import os
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import CounterClient
from server import start_server

REQUESTS = 2000


def measure(addresses, number_of_clients):
    """Send REQUESTS increments in total. Return all latencies in milliseconds."""
    latencies = []
    lock = threading.Lock()
    requests_per_client = REQUESTS // number_of_clients

    def one_client():
        client = CounterClient(addresses, log_enabled=False)
        for i in range(requests_per_client):
            start = time.perf_counter()
            client.incr("bench", 1)
            milliseconds = (time.perf_counter() - start) * 1000
            with lock:
                latencies.append(milliseconds)

    threads = []
    for i in range(number_of_clients):
        thread = threading.Thread(target=one_client)
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    return latencies


def p95(latencies):
    """Sort the values and take the one at position ceil(0.95 * n)."""
    ordered = sorted(latencies)
    position = math.ceil(0.95 * len(ordered))
    return ordered[position - 1]


def main():
    servers = []
    addresses = []
    for name in ["replica-A", "replica-B", "replica-C"]:
        server, port = start_server(0, name, log_enabled=False)
        servers.append(server)
        addresses.append("127.0.0.1:" + str(port))

    one_replica = [addresses[0]]
    configurations = [
        ("Single replica, 1 client", one_replica, 1),
        ("Single replica, 16 clients", one_replica, 16),
        ("Quorum (3 replicas), 1 client", addresses, 1),
        ("Quorum (3 replicas), 16 clients", addresses, 16),
    ]

    print("| Configuration | Median latency (ms) | p95 latency (ms) | Requests |")
    print("|---|---|---|---|")
    for label, replica_addresses, number_of_clients in configurations:
        latencies = measure(replica_addresses, number_of_clients)
        median = statistics.median(latencies)
        print(f"| {label} | {median:.2f} | {p95(latencies):.2f} | {len(latencies)} |")

    for server in servers:
        server.stop(0)


if __name__ == "__main__":
    main()