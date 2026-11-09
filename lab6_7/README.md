# Lab 6-7: Consistency Models & the CAP Theorem

The replicated gRPC service from Assignment 2, extended to a key-value store with configurable
consistency: `QUORUM_CP` and `ALWAYS_AP` writes, `LINEARIZABLE` and `FOLLOWER_STALE` reads, a
leaseholder for the key range `[a-z]`, real network partitions between the replicas, and
last-write-wins or union conflict resolution.

All commands are run from the `lab6_7` folder. They are written for Windows; on Linux and macOS
use `python3` and `source ../.venv/bin/activate`.

## 1. Setup

The lab uses the virtual environment in the repository root.

```
cd lab6_7
..\.venv\Scripts\activate
pip install -r requirements.txt
```

`kvstore_pb2.py` and `kvstore_pb2_grpc.py` are generated from `kvstore.proto` and are already in
the folder. After a change to the proto file, generate them again:

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kvstore.proto
```

## 2. See it work

```
python demo.py
```

The script starts replicas A, B and C on ports 50051-50053, then walks through every behaviour of
the lab and prints the client results together with the replicas' own log lines (marked `|`):

1. a `QUORUM_CP` write through follower B, forwarded to leaseholder A;
2. a `LINEARIZABLE` read (proxied to A) and a `FOLLOWER_STALE` read (answered by C itself);
3. a network partition DC-1 {A, B} | DC-2 {C};
4. `QUORUM_CP` on the majority side succeeds with 2 acks, on the minority side it fails;
5. linearizable reads fail on C, follower reads still answer with the old value;
6. `ALWAYS_AP` on C is accepted with 1 ack, the replicas diverge;
7. C falls behind more than `max_staleness_ms` and answers `STALE_READ_EXCEEDED`;
8. the partition heals and last-write-wins merges the two versions;
9. the quorum table: for every W and R, how many read quorums miss the write;
10. the same conflict with `--merge union`, where both writes survive.

`python demo.py --step` waits for Enter before every step (useful when presenting).

To try things yourself:

```
python demo.py --interactive
```

```
kv> put cart milk cp B          QUORUM_CP write through B
kv> get cart lin C              LINEARIZABLE read through C
kv> get cart stale C 1500       FOLLOWER_STALE read on C with max_staleness_ms=1500
kv> partition AB C              DC-1 {A, B} | DC-2 {C}
kv> put cart bread ap C         ALWAYS_AP write on the minority side
kv> inspect cart                the version every replica holds
kv> heal                        repair the network, watch the merge in the logs
kv> crash A / restart A         kill a replica process / start it again with an empty store
kv> status                      role, lag and reachable peers of every replica
kv> help / quit
```

While the shell runs, `client.py` (section 6) also works from another terminal.

## 3. Run the tests

```
pytest -v
```

`tests/test_cap.py` holds 26 test cases (about 10 seconds). The tests start their own replicas on
free ports, so nothing has to be running.

| Group | Tests |
|---|---|
| Task 3, Part A (CP mode) | majority side commits with `acks_received = 2`; minority side fails with `ReplicaUnavailableError`; an isolated leaseholder cannot commit alone |
| Task 3, Part B (AP mode) | minority side accepts with `acks_received = 1`; after the heal LWW merges the divergent values; LWW follows the timestamp, not the arrival order; union merge keeps both writes; clock skew makes LWW keep the older write |
| Task 2 (read paths) | linearizable reads are answered by the leaseholder; they fail on the minority side; follower reads are local with a bounded lag; they stay available but stale in a partition; `STALE_READ_EXCEEDED` beyond `max_staleness_ms` |
| Quorum arithmetic | 9 cases W = 1..3, R = 1..3: a read quorum misses the write in exactly C(N-W, R) cases, so never when R + W > N |
| Crashes and retries | follower crash and catch-up; leaseholder crash; retry with the same idempotency key; key outside `[a-z]` |

The failure-injection wrapper is `NetworkPartition` in `tests/test_cap.py`. The replicas run as
separate processes and every replica-to-replica connection goes through a TCP proxy (`netsim.py`).
A partition closes the connections that cross the cut and refuses new ones.

## 4. Run the benchmark (Task 4)

```
python perf_cap_bench.py
```

1,000 `Get` requests per scenario on a 3-replica cluster on localhost. The client sits next to
follower C; the leaseholder A is the remote node. The script measures the healthy cluster, then
the partition DC-1 {A, B} | DC-2 {C}, then the moment C's lag passes `max_staleness_ms = 5000`.
The tables are printed and saved to `results/`.

`python perf_cap_bench.py --link-delay-ms 5` adds 5 ms to every replica-to-replica link in each
direction, as if A were in another data centre.

## 5. Manual run in four terminals

```
python server.py --id A
python server.py --id B
python server.py --id C
```

A listens on 50051 and holds the lease for `[a-z]`, B on 50052, C on 50053. Client terminal:

```
python client.py put user-1 alice --node B
python client.py get user-1 --node C
python client.py get user-1 --consistency stale --node C
python client.py put user-1 bob --strategy ap --node C
python client.py inspect user-1
python client.py quorum-get user-1 --nodes B,C
python client.py status
```

Stop a replica with Ctrl+C to simulate a crash: with C down a `QUORUM_CP` write still commits
with `acks 2/3`; with A down it fails, while `--consistency stale` reads and `--strategy ap` writes
still work. Partitions need the proxies, so use `demo.py --interactive` for them.

## How it works

```
                 client gateway (client.py)
            /             |               \
   +-------------+  +-------------+  +-------------+
   |  Replica A  |  |  Replica B  |  |  Replica C  |
   |  port 50051 |  |  port 50052 |  |  port 50053 |
   | leaseholder |  |  follower   |  |  follower   |
   +-------------+  +-------------+  +-------------+
          \______ link proxies (netsim.py) _____/
```

* **Leaseholder.** A holds the lease for keys whose first character is in `[a-z]`; other keys are
  rejected.
* **QUORUM_CP write.** A follower forwards it to A. A stamps it with its hybrid logical clock,
  sends `Prepare` to both followers in parallel and commits once a majority (2 of 3, counting
  itself) acknowledged. Without a majority the write is aborted and never becomes visible.
* **ALWAYS_AP write.** Any replica accepts it at once, stamps it with a timestamp and a vector
  clock, and pushes it to the peers it can reach. `acks_received` counts the replicas that have it.
* **LINEARIZABLE read.** Only the leaseholder answers; every other replica proxies the request
  to A. If A cannot be reached the read fails with `LEASEHOLDER_UNAVAILABLE`.
* **FOLLOWER_STALE read.** A follower answers from its own store. The leaseholder sends it a
  *closed timestamp* every 100 ms: all writes before this time are already on the follower. If
  the follower lags more than `max_staleness_ms` behind it answers `STALE_READ_EXCEEDED`
  (`max_staleness_ms = 0` means the server default, 5000 ms). `read_timestamp` is the closed
  timestamp and `is_stale = true`.
* **Anti-entropy.** Every 100 ms each replica pushes the versions that changed since the last
  push to each peer. This delivers commits to the followers, catches up a restarted replica, and
  merges divergent values after a partition.
* **Conflicts.** Two versions are concurrent if neither vector clock contains the other.
  `--merge lww` (default) keeps the one with the higher timestamp. `--merge union` keeps the
  union of the comma-separated items.

| Situation | What the client gets |
|---|---|
| `QUORUM_CP` without a majority | `PutReply(success=false, error_message="ReplicaUnavailableError: quorum lost ...")` |
| `LINEARIZABLE` read, leaseholder unreachable | gRPC `UNAVAILABLE`, code `LEASEHOLDER_UNAVAILABLE` |
| `FOLLOWER_STALE` read, follower lags too much | gRPC `FAILED_PRECONDITION`, code `STALE_READ_EXCEEDED` |
| key outside `[a-z]` | `InvalidKeyError` / gRPC `INVALID_ARGUMENT` |

The error code is also sent as trailing metadata `kv-error`; `client.py` turns it into
`ReplicaUnavailableError`, `StaleReadExceededError` or `InvalidKeyError`.

Simplifications: the lease never moves to another replica (no failover); the replicas keep their
data in memory, so a restarted replica rebuilds it from the others, and a restarted leaseholder
serves again only after every follower has sent it the full state.

## Files

| File | Purpose |
|---|---|
| `kvstore.proto` | public `KeyValueStore` API (Task 1) and the internal `Replica` protocol |
| `kvstore_pb2.py`, `kvstore_pb2_grpc.py` | generated stubs |
| `server.py` | one replica: leaseholder routing, CP / AP writes, follower reads, anti-entropy, conflict resolution |
| `client.py` | client gateway: library and command line (`put`, `get`, `inspect`, `quorum-get`, `status`) |
| `netsim.py` | TCP link proxies that can cut the network between replicas |
| `cluster.py` | starts the three replicas as processes behind the proxies; partition, crash, restart |
| `tests/test_cap.py` | test suite with the `NetworkPartition` failure-injection wrapper |
| `perf_cap_bench.py` | Task 4 benchmark |
| `demo.py` | walkthrough and interactive shell |
| `results/` | saved outputs of the tests, the benchmark and the demo |
| `Lab_Report_CAP.pdf` | lab report |
