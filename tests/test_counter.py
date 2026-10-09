"""Task C2: unit and integration tests.

Run from the project folder:  pytest tests/test_counter.py -v

Every test starts its own server on a free port and stops it at the end.
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from client import CounterClient
from server import start_server


@pytest.fixture
def running_server():
    """One server. The test receives its address."""
    server, port = start_server(0, log_enabled=False)
    yield "127.0.0.1:" + str(port)
    server.stop(0)


@pytest.fixture
def slow_server():
    """One server whose FIRST reply is 1000 ms late."""
    server, port = start_server(0, fault="delay", delay_ms=1000, log_enabled=False)
    yield "127.0.0.1:" + str(port)
    server.stop(0)


@pytest.fixture
def three_replicas():
    """Three independent servers. The test receives (servers, addresses)."""
    servers = []
    addresses = []
    for name in ["replica-A", "replica-B", "replica-C"]:
        server, port = start_server(0, name, log_enabled=False)
        servers.append(server)
        addresses.append("127.0.0.1:" + str(port))
    yield servers, addresses
    for server in servers:
        server.stop(0)


def test_increment_applies_delta(running_server):
    client = CounterClient([running_server], log_enabled=False)

    result = client.incr("x", 5)

    assert result.committed
    assert result.new_value == 5
    assert client.get("x").value == 5


def test_duplicate_key_not_reapplied(running_server):
    client = CounterClient([running_server], log_enabled=False)

    r1 = client.incr("x", 5, key="k-1")
    r2 = client.incr("x", 5, key="k-1")

    assert r1.new_value == 5
    assert r2.new_value == 5
    assert r2.was_duplicate
    assert client.get("x").value == 5


def test_concurrent_increments_exact(running_server):
    def increment_1000_times():
        client = CounterClient([running_server], log_enabled=False)
        for i in range(1000):
            client.incr("x", 1)

    thread_1 = threading.Thread(target=increment_1000_times)
    thread_2 = threading.Thread(target=increment_1000_times)
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()

    client = CounterClient([running_server], log_enabled=False)
    assert client.get("x").value == 2000


def test_get_missing_counter(running_server):
    client = CounterClient([running_server], log_enabled=False)

    reply = client.get("no-such-counter")

    assert reply.found is False


def test_retry_after_timeout_is_safe(slow_server):
    client = CounterClient([slow_server], timeout=0.3, log_enabled=False)

    result = client.incr("x", 5)

    assert result.committed
    assert result.was_duplicate
    assert result.new_value == 5
    assert client.get("x").value == 5


def test_majority_commit_two_acks(three_replicas):
    servers, addresses = three_replicas
    servers[2].stop(0).wait()
    client = CounterClient(addresses, log_enabled=False)

    result = client.incr("x", 1)

    assert result.committed
    assert result.acks == 2


def test_no_commit_below_majority(three_replicas):
    servers, addresses = three_replicas
    servers[1].stop(0).wait()
    servers[2].stop(0).wait()
    client = CounterClient(addresses, log_enabled=False)

    result = client.incr("x", 1)

    assert not result.committed
    assert result.acks == 1
    assert result.new_value is None


def test_replicas_converge(three_replicas):
    servers, addresses = three_replicas
    client = CounterClient(addresses, log_enabled=False)

    for i in range(20):
        client.incr("x", 1)
        client.incr("y", 2)

    for address in addresses:
        one_replica = CounterClient([address], log_enabled=False)
        assert one_replica.get("x").value == 20
        assert one_replica.get("y").value == 40