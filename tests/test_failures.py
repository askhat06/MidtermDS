"""Task C3: failure-injection tests.

Run from the project folder:  pytest tests/test_failures.py -v

Route (b) from the assignment: the fault is simulated inside the server
(--fault delay --delay-ms N). A crash is simulated by stopping a server.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from client import CounterClient
from server import start_server


def start_three_replicas(fault_of_replica_c, delay_ms):
    """Start replicas A, B and C. Only replica C gets the fault."""
    servers = []
    addresses = []
    for name in ["replica-A", "replica-B", "replica-C"]:
        if name == "replica-C":
            server, port = start_server(0, name, fault_of_replica_c, delay_ms, log_enabled=False)
        else:
            server, port = start_server(0, name, log_enabled=False)
        servers.append(server)
        addresses.append("127.0.0.1:" + str(port))
    return servers, addresses


@pytest.fixture
def healthy_replicas():
    servers, addresses = start_three_replicas("none", 0)
    yield servers, addresses
    for server in servers:
        server.stop(0)


@pytest.fixture
def replicas_with_slow_c():
    """The FIRST reply of replica C is 2000 ms late."""
    servers, addresses = start_three_replicas("delay", 2000)
    yield servers, addresses
    for server in servers:
        server.stop(0)


def test_replica_crash_mid_request_still_commits(replicas_with_slow_c):
    """Scenario 1: a replica dies while a write is in flight."""
    servers, addresses = replicas_with_slow_c
    client = CounterClient(addresses, log_enabled=False)
    results = []

    def write():
        results.append(client.incr("x", 1))

    thread = threading.Thread(target=write)
    thread.start()
    time.sleep(0.5)
    servers[2].stop(0)
    thread.join()

    assert len(results) == 1
    assert results[0].committed
    assert results[0].acks == 2


def test_duplicate_request_moves_value_once(healthy_replicas):
    """Scenario 2: the same request is sent twice."""
    servers, addresses = healthy_replicas
    client = CounterClient(addresses, log_enabled=False)

    first = client.incr("x", 5, key="same-key")
    second = client.incr("x", 5, key="same-key")

    assert not first.was_duplicate
    assert second.was_duplicate
    assert second.new_value == 5
    for address in addresses:
        one_replica = CounterClient([address], log_enabled=False)
        assert one_replica.get("x").value == 5


def test_timeout_then_retry_moves_value_once(replicas_with_slow_c):
    """Scenario 3: a replica answers slower than the deadline, the client retries."""
    servers, addresses = replicas_with_slow_c
    client = CounterClient(addresses, timeout=0.3, log_enabled=False)

    result = client.incr("x", 5)

    assert result.committed
    assert result.acks == 3
    for address in addresses:
        one_replica = CounterClient([address], log_enabled=False)
        assert one_replica.get("x").value == 5