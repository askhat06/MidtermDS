import contextlib
import itertools
import math
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from client import InvalidKeyError, ReplicaUnavailableError, StaleReadExceededError, now_us
from cluster import NODE_IDS, Cluster
from server import compare_vclocks

N = len(NODE_IDS)


class NetworkPartition:
    """Failure-injection wrapper: splits the three replicas into two data centres.

    The replicas run as separate processes (subprocess) and never talk to each other
    directly: every replica-to-replica connection goes through a TCP proxy in this test
    process, one proxy per direction (netsim.LinkProxy). Entering the wrapper cuts every
    proxy that crosses the line between DC-1 and DC-2. Open connections are closed and new
    ones are refused, which is what the replicas would see in a real partition. Leaving the
    wrapper restores the links (partition recovery). The test client still reaches every
    replica directly, as if it sat in the same data centre.
    """

    def __init__(self, cluster, dc1=("A", "B"), dc2=("C",)):
        self.cluster = cluster
        self.dc1 = list(dc1)
        self.dc2 = list(dc2)

    def __enter__(self):
        self.cluster.partition(self.dc1, self.dc2)
        return self

    def __exit__(self, *exc):
        self.cluster.heal()
        return False


def new_key(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="module")
def shared_cluster(tmp_path_factory):
    cluster = Cluster(log_dir=str(tmp_path_factory.mktemp("cluster-logs")))
    cluster.start()
    yield cluster
    cluster.stop()


@pytest.fixture
def cluster(shared_cluster):
    """The shared cluster, healthy at the start of every test and healed again after it."""
    shared_cluster.wait_ready()
    yield shared_cluster
    shared_cluster.heal()
    for node in shared_cluster.nodes:
        if not shared_cluster.is_running(node):
            shared_cluster.restart(node)


# --------------------------------------------------------------------------------------
# Task 3, Part A: CP mode (strict quorum)
# --------------------------------------------------------------------------------------

def test_cp_write_on_majority_side_succeeds(cluster):
    """QUORUM_CP sent to DC-1 (A, B) during the partition: success with acks_received = 2."""
    client = cluster.client()
    key = new_key("cp-majority")
    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        via_a = client.put(key, "via-A", "cp", node="A")
        via_b = client.put(key, "via-B", "cp", node="B")  # B forwards it to leaseholder A

        assert via_a.success and via_a.acks == 2
        assert via_b.success and via_b.acks == 2
        cluster.wait_for(lambda: cluster.local_values(key)["B"] == "via-B", what="B to apply the commit")
        values = cluster.local_values(key)
        assert values["A"] == "via-B"
        assert values["C"] is None  # DC-2 did not get the write


def test_cp_write_on_minority_side_fails(cluster):
    """QUORUM_CP sent to DC-2 (C alone): ReplicaUnavailableError, quorum lost, nothing written."""
    client = cluster.client()
    key = new_key("cp-minority")
    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        with pytest.raises(ReplicaUnavailableError) as failure:
            client.put(key, "should-not-exist", "cp", node="C")

        reply = failure.value.reply
        assert reply.success is False
        assert reply.acks_received < 2
        assert reply.error_message.startswith("ReplicaUnavailableError")
        assert "quorum lost" in reply.error_message
        time.sleep(0.3)
        assert cluster.local_values(key) == {"A": None, "B": None, "C": None}


def test_isolated_leaseholder_cannot_commit_alone(cluster):
    """The leaseholder itself is in the minority: it has only its own ack (1 of 3) and aborts.

    The majority side cannot commit either, because the lease stays on A (no lease transfer
    in this emulation), so QUORUM_CP is unavailable everywhere: consistency over availability.
    """
    client = cluster.client()
    key = new_key("cp-isolated")
    with NetworkPartition(cluster, dc1=("B", "C"), dc2=("A",)):
        with pytest.raises(ReplicaUnavailableError) as failure:
            client.put(key, "x", "cp", node="A")
        assert failure.value.reply.acks_received == 1

        with pytest.raises(ReplicaUnavailableError):
            client.put(key, "y", "cp", node="B")
    time.sleep(0.3)
    assert cluster.local_values(key) == {"A": None, "B": None, "C": None}


# --------------------------------------------------------------------------------------
# Task 3, Part B: AP mode (sloppy quorum, last-write-wins)
# --------------------------------------------------------------------------------------

def test_ap_write_on_minority_side_is_accepted(cluster):
    """ALWAYS_AP sent to DC-2 (C alone): success = true, acks_received = 1."""
    client = cluster.client()
    key = new_key("ap-minority")
    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        result = client.put(key, "only-in-dc2", "ap", node="C")

        assert result.success is True
        assert result.acks == 1
        assert cluster.local_values(key) == {"A": None, "B": None, "C": "only-in-dc2"}
    assert cluster.wait_converged(key) == "only-in-dc2"  # hinted data reaches DC-1 after the heal


def test_partition_recovery_merges_divergent_values_with_lww(cluster):
    """Both sides accept a write for the same key; after the heal every replica keeps the later one."""
    client = cluster.client()
    key = new_key("lww")
    client.put(key, "before-partition", "cp", node="A")
    cluster.wait_converged(key, "before-partition")

    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        in_dc1 = client.put(key, "written-in-dc1", "ap", node="A")
        time.sleep(0.01)
        in_dc2 = client.put(key, "written-in-dc2", "ap", node="C")
        assert (in_dc1.acks, in_dc2.acks) == (2, 1)

        # the replicas have diverged: two concurrent versions of the same key
        assert cluster.local_values(key) == {"A": "written-in-dc1", "B": "written-in-dc1",
                                             "C": "written-in-dc2"}
        version_a = client.read_local(key, "A")
        version_c = client.read_local(key, "C")
        assert compare_vclocks(dict(version_a.vclock), dict(version_c.vclock)) == "concurrent"
        assert version_c.timestamp > version_a.timestamp

    assert cluster.wait_converged(key) == "written-in-dc2"
    assert client.get(key, "linearizable", node="B").value == "written-in-dc2"


def test_lww_follows_the_timestamp_not_the_arrival_order(cluster):
    """The write with the larger PutRequest.timestamp wins, even though it was written first."""
    client = cluster.client()
    key = new_key("lww-ts")
    base = now_us()
    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        client.put(key, "first-but-newer-timestamp", "ap", node="C", timestamp=base + 2_000_000)
        client.put(key, "second-but-older-timestamp", "ap", node="A", timestamp=base + 1_000_000)

    assert cluster.wait_converged(key) == "first-but-newer-timestamp"


def test_union_merge_keeps_both_concurrent_writes(tmp_path):
    """The same split with --merge union (Dynamo shopping cart): no write is lost.

    With last-write-wins the result would be only one of the two carts.
    """
    with Cluster(merge="union", log_dir=str(tmp_path)) as union_cluster:
        client = union_cluster.client()
        client.put("cart", "milk", "cp", node="A")
        union_cluster.wait_converged("cart", "milk")

        with NetworkPartition(union_cluster, dc1=("A", "B"), dc2=("C",)):
            client.put("cart", "milk,eggs", "ap", node="A")
            client.put("cart", "milk,bread", "ap", node="C")

        assert union_cluster.wait_converged("cart") == "bread,eggs,milk"


def test_lww_with_clock_skew_keeps_the_older_write(tmp_path):
    """LWW trusts the clocks: C's clock runs 3 s behind, so its later write loses the merge."""
    with Cluster(clock_skew_ms={"C": -3000}, log_dir=str(tmp_path)) as skewed:
        client = skewed.client()
        key = new_key("skew")
        client.put(key, "before-partition", "cp", node="A")
        skewed.wait_converged(key, "before-partition")

        with NetworkPartition(skewed, dc1=("A", "B"), dc2=("C",)):
            client.put(key, "earlier-write-dc1", "ap", node="A")
            time.sleep(0.2)
            client.put(key, "later-write-dc2", "ap", node="C")

        assert skewed.wait_converged(key) == "earlier-write-dc1"


# --------------------------------------------------------------------------------------
# Task 2: linearizable and follower read paths
# --------------------------------------------------------------------------------------

def test_linearizable_read_is_answered_by_the_leaseholder(cluster):
    """Every node proxies a LINEARIZABLE read to leaseholder A; the reply is not stale."""
    client = cluster.client()
    key = new_key("lin")
    client.put(key, "v1", "cp", node="B")

    for gateway in NODE_IDS:
        result = client.get(key, "linearizable", node=gateway)
        assert result.value == "v1"
        assert result.served_by == "A"
        assert result.is_stale is False


def test_linearizable_read_fails_on_the_minority_side(cluster):
    """C has a copy of the key but must not answer a LINEARIZABLE read when A is unreachable."""
    client = cluster.client()
    key = new_key("lin-minority")
    client.put(key, "v1", "cp", node="A")
    cluster.wait_converged(key, "v1")

    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        with pytest.raises(ReplicaUnavailableError) as failure:
            client.get(key, "linearizable", node="C")
        assert failure.value.code == "LEASEHOLDER_UNAVAILABLE"
        assert client.get(key, "linearizable", node="B").value == "v1"  # majority side works


def test_follower_read_is_served_locally_with_bounded_lag(cluster):
    """FOLLOWER_STALE on a follower: answered by the follower itself, marked stale, small lag."""
    client = cluster.client()
    key = new_key("follower")
    client.put(key, "v1", "cp", node="A")
    cluster.wait_for(lambda: client.get(key, "stale", node="B", max_staleness_ms=5000).value == "v1",
                     what="follower B to apply the write")

    result = client.get(key, "stale", node="B", max_staleness_ms=5000)
    assert result.served_by == "B"
    assert result.is_stale is True
    assert 0 <= result.lag_ms < 1000
    assert 0 < result.read_timestamp <= now_us()


def test_follower_read_stays_available_but_stale_during_partition(cluster):
    """PACELC, partition branch: C keeps answering follower reads, but with the old value."""
    client = cluster.client()
    key = new_key("stale")
    client.put(key, "v1", "cp", node="A")
    cluster.wait_converged(key, "v1")

    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        client.put(key, "v2", "cp", node="A")  # committed on A and B only

        stale = client.get(key, "stale", node="C", max_staleness_ms=5000)
        assert stale.value == "v1"
        assert stale.is_stale is True
        assert stale.served_by == "C"
        assert client.get(key, "linearizable", node="B").value == "v2"


def test_follower_read_beyond_max_staleness_is_rejected(cluster):
    """When the follower lags more than max_staleness_ms it answers STALE_READ_EXCEEDED."""
    client = cluster.client()
    key = new_key("bound")
    client.put(key, "v1", "cp", node="A")
    cluster.wait_converged(key, "v1")

    with NetworkPartition(cluster, dc1=("A", "B"), dc2=("C",)):
        time.sleep(0.8)  # C hears nothing from the leaseholder any more
        with pytest.raises(StaleReadExceededError) as failure:
            client.get(key, "stale", node="C", max_staleness_ms=300)
        assert "STALE_READ_EXCEEDED" in str(failure.value)
        assert client.get(key, "stale", node="C", max_staleness_ms=5000).value == "v1"

    cluster.wait_for(lambda: client.get(key, "stale", node="C", max_staleness_ms=300).value == "v1",
                     what="C to catch up after the heal")


# --------------------------------------------------------------------------------------
# Quorum arithmetic: a read quorum sees the latest write if R + W > N
# --------------------------------------------------------------------------------------

QUORUM_CASES = [(w, r) for w in (1, 2, 3) for r in (1, 2, 3)]


@pytest.mark.parametrize("w, r", QUORUM_CASES, ids=[f"W{w}-R{r}" for w, r in QUORUM_CASES])
def test_read_quorum_sees_latest_write_only_if_r_plus_w_exceeds_n(cluster, w, r):
    """Write to exactly W replicas, then try every possible read quorum of size R.

    The write set is {A}, {A, B} or {A, B, C}; the partition cuts it off from the other
    replicas, so the ALWAYS_AP write reaches exactly W of them. A quorum read asks R replicas
    and keeps the version with the highest timestamp. It can miss the write only if all R
    replicas are outside the write set: C(N - W, R) read sets, which is zero iff R + W > N.
    """
    client = cluster.client()
    key = new_key(f"quorum-w{w}-r{r}")
    client.put(key, "old", "cp", node="A")
    cluster.wait_converged(key, "old")

    write_set = NODE_IDS[:w]
    others = NODE_IDS[w:]
    if others:
        network = NetworkPartition(cluster, dc1=write_set, dc2=others)
    else:
        network = contextlib.nullcontext()
    with network:
        result = client.put(key, "new", "ap", node="A")
        assert result.acks == w

        stale_read_sets = []
        for read_set in itertools.combinations(NODE_IDS, r):
            newest, _ = client.quorum_get(key, read_set)
            if newest.value != "new":
                stale_read_sets.append(read_set)

    assert len(stale_read_sets) == math.comb(N - w, r)
    if r + w > N:
        assert stale_read_sets == []
    else:
        assert stale_read_sets != []


# --------------------------------------------------------------------------------------
# Node crashes, retries, key ranges
# --------------------------------------------------------------------------------------

def test_follower_crash_then_restart_catches_up(cluster):
    """With one follower down a CP write still commits (2 of 3); the restarted node catches up."""
    client = cluster.client()
    key = new_key("crash-follower")
    cluster.crash("C")

    result = client.put(key, "written-while-c-was-down", "cp", node="A")
    assert result.success and result.acks == 2

    cluster.restart("C")  # comes back with an empty store
    cluster.wait_converged(key, "written-while-c-was-down")
    cluster.wait_for(lambda: client.get(key, "stale", node="C", max_staleness_ms=1000).found,
                     what="C to serve follower reads again")


def test_leaseholder_crash_blocks_cp_but_not_follower_reads_or_ap(cluster):
    """Leaseholder down: CP writes and linearizable reads stop, follower reads and AP go on."""
    client = cluster.client()
    key = new_key("crash-leaseholder")
    client.put(key, "v1", "cp", node="A")
    cluster.wait_converged(key, "v1")
    cluster.crash("A")

    with pytest.raises(ReplicaUnavailableError):
        client.put(key, "v2", "cp", node="B")
    with pytest.raises(ReplicaUnavailableError):
        client.get(key, "linearizable", node="B")
    assert client.get(key, "stale", node="B", max_staleness_ms=5000).value == "v1"
    assert client.put(key, "v3-ap", "ap", node="B").acks == 2

    cluster.restart("A")  # empty store; it rebuilds the state from B and C before serving
    cluster.wait_for(lambda: client.get(key, "linearizable", node="A").value == "v3-ap",
                     what="the restarted leaseholder to recover the data")


def test_retry_with_same_idempotency_key_is_applied_once(cluster):
    """A retried Put (same idempotency key, even through another node) creates no new version."""
    client = cluster.client()
    key = new_key("idem")
    first = client.put(key, "v1", "cp", node="B", idempotency_key="put-" + key)
    version_before = client.read_local(key, "A")
    second = client.put(key, "v1", "cp", node="C", idempotency_key="put-" + key)
    version_after = client.read_local(key, "A")

    assert first.success and second.success
    assert second.acks == first.acks
    assert version_after.timestamp == version_before.timestamp


def test_key_outside_the_leaseholder_range_is_rejected(cluster):
    """Only the range [a-z] exists; its leaseholder is A."""
    client = cluster.client()
    with pytest.raises(InvalidKeyError):
        client.put("Cart", "x", "cp", node="A")
    with pytest.raises(InvalidKeyError):
        client.get("42", "linearizable", node="B")
