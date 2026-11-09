import argparse
import os
import sys
import threading
import time
import uuid
from concurrent import futures

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as rpc

# Layout from the lab handout. Node 1 (A, port 50051) is the leaseholder.
DEFAULT_PORTS = {"A": 50051, "B": 50052, "C": 50053}
DEFAULT_LEASEHOLDER = "A"

# Keyspace ranges and the node holding the lease for each one. A key belongs to a range
# by its first character. There is one range, [a-z]; other keys are rejected.
KEY_RANGES = [("a", "z")]

# gRPC waits up to two minutes between reconnect attempts by default. After a partition
# heals we want the replicas to find each other again within half a second.
CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]

# Per-request log lines that --quiet switches off (the benchmark sends thousands of reads).
CHATTY_EVENTS = {"GET", "PUT", "PREPARE", "APPLY"}
PRINT_LOCK = threading.Lock()


class HybridClock:
    """Hybrid logical clock in microseconds since 1970.

    It follows the wall clock, but never goes backwards and always moves past every
    timestamp it has observed from other replicas. Writes are ordered by these values
    (last-write-wins) and the follower staleness is measured with them.
    """

    def __init__(self, skew_ms=0):
        self._last = 0
        self._skew_us = skew_ms * 1000
        self._lock = threading.Lock()

    def wall(self):
        return time.time_ns() // 1000 + self._skew_us

    def now(self):
        with self._lock:
            self._last = max(self._last + 1, self.wall())
            return self._last

    def observe(self, timestamp):
        with self._lock:
            if timestamp > self._last:
                self._last = timestamp


def clock_text(timestamp):
    """21:46:03.120 for a timestamp in microseconds."""
    if timestamp <= 0:
        return "-"
    seconds = timestamp // 1_000_000
    millis = timestamp // 1000 % 1000
    return time.strftime("%H:%M:%S", time.localtime(seconds)) + f".{millis:03d}"


def vclock_text(vclock):
    parts = [f"{node}@{clock_text(vclock[node])}" for node in sorted(vclock)]
    return "{" + ", ".join(parts) + "}"


class Entry:
    """One version of a key."""

    def __init__(self, key, value, timestamp, origin, vclock):
        self.key = key
        self.value = value
        self.timestamp = timestamp  # last-write-wins timestamp
        self.origin = origin        # node that accepted the write
        self.vclock = vclock        # node -> time of that node's last write to this key

    @staticmethod
    def from_proto(message):
        return Entry(message.key, message.value, message.timestamp, message.origin, dict(message.vclock))

    def to_proto(self):
        return pb.Version(key=self.key, value=self.value, timestamp=self.timestamp,
                          origin=self.origin, vclock=self.vclock)

    def lww_rank(self):
        """Last-write-wins order: higher timestamp wins, the node id breaks ties."""
        return (self.timestamp, self.origin)

    def same_as(self, other):
        return (self.value, self.timestamp, self.origin, self.vclock) == \
               (other.value, other.timestamp, other.origin, other.vclock)


def next_vclock(base, node, event_time):
    """Vector clock of a new write made by `node` on top of the version `base`."""
    vclock = dict(base.vclock) if base is not None else {}
    vclock[node] = event_time
    return vclock


def compare_vclocks(a, b):
    """'before' if a happened before b, 'after', 'equal' or 'concurrent'."""
    nodes = set(a) | set(b)
    a_le_b = all(a.get(node, 0) <= b.get(node, 0) for node in nodes)
    b_le_a = all(b.get(node, 0) <= a.get(node, 0) for node in nodes)
    if a_le_b and b_le_a:
        return "equal"
    if a_le_b:
        return "before"
    if b_le_a:
        return "after"
    return "concurrent"


def split_items(value):
    return [item.strip() for item in value.split(",") if item.strip() != ""]


def union_merge(a, b):
    """Union merge (Dynamo shopping cart): keep the items of both concurrent versions."""
    items = sorted(set(split_items(a.value)) | set(split_items(b.value)))
    newest = max(a, b, key=Entry.lww_rank)
    nodes = set(a.vclock) | set(b.vclock)
    vclock = {node: max(a.vclock.get(node, 0), b.vclock.get(node, 0)) for node in nodes}
    return Entry(a.key, ",".join(items), newest.timestamp, newest.origin, vclock)


def resolve(current, incoming, policy):
    """Decide which version a replica keeps. Returns (kept version, note)."""
    if current is None:
        return incoming, "new key"
    if current.same_as(incoming):
        return current, "same"
    relation = compare_vclocks(current.vclock, incoming.vclock)
    concurrent = relation in ("concurrent", "equal")

    if policy == "union":
        if relation == "before":
            return incoming, "newer"
        if relation == "after":
            return current, "older"
        return union_merge(current, incoming), "conflict, union merge"

    # Last-write-wins looks only at the timestamp. That makes the merge order-independent,
    # so all replicas converge to the same value, but one of two concurrent writes is lost.
    if incoming.lww_rank() > current.lww_rank():
        winner = incoming
    else:
        winner = current
    if concurrent:
        return winner, "conflict, last-write-wins"
    if winner is incoming:
        return winner, "newer timestamp"
    return winner, "older timestamp"


class Peer:
    """Connection to another replica. It goes through the network, so it can be cut."""

    def __init__(self, node_id, address):
        self.id = node_id
        self.address = address
        channel = grpc.insecure_channel(address, options=CHANNEL_OPTIONS)
        self.kv = rpc.KeyValueStoreStub(channel)
        self.replica = rpc.ReplicaStub(channel)
        self.reachable = None   # unknown until the first call
        self.synced_upto = 0    # our change number that this peer has acknowledged
        self.incarnation = ""   # the peer's process id from its last SyncReply
        self.sync_busy = False


class Replica:
    """State and replication logic of one node. The gRPC servicers below call into it."""

    def __init__(self, node_id, peers, leaseholder=DEFAULT_LEASEHOLDER, merge="lww",
                 sync_interval_ms=100, peer_timeout_ms=500, ack_grace_ms=50,
                 max_staleness_ms=5000, clock_skew_ms=0, recover=False, quiet=False):
        self.id = node_id
        self.leaseholder = leaseholder
        self.is_leaseholder = node_id == leaseholder
        self.merge_policy = merge
        self.sync_interval = sync_interval_ms / 1000
        self.peer_timeout = peer_timeout_ms / 1000
        self.ack_grace = ack_grace_ms / 1000
        self.default_staleness_ms = max_staleness_ms
        self.quiet = quiet
        self.incarnation = uuid.uuid4().hex[:8]
        self.clock = HybridClock(clock_skew_ms)

        self.peers = {}
        for peer_id, address in peers.items():
            self.peers[peer_id] = Peer(peer_id, address)
        self.size = len(self.peers) + 1
        self.majority = self.size // 2 + 1

        self.lock = threading.RLock()
        self.cp_lock = threading.Lock()  # the leaseholder sequences QUORUM_CP writes
        self.store = {}         # key -> Entry: the local state machine
        self.changed = {}       # key -> change number, for delta anti-entropy
        self.change_no = 0
        self.staged = {}        # follower: QUORUM_CP writes prepared but not committed yet
        self.inflight = set()   # leaseholder: timestamps of QUORUM_CP writes waiting for a quorum
        self.closed_ts = 0      # follower: all committed writes up to this time are applied here
        self.seen = {}          # idempotency key -> PutReply

        # A restarted leaseholder has lost its memory. It may serve linearizable reads again
        # only after every follower has sent it the full state.
        self.recovering = recover and self.is_leaseholder
        self.recovered_from = set()

        self.pool = futures.ThreadPoolExecutor(max_workers=16)
        self.wake_sync = threading.Event()

    # ----- helpers -------------------------------------------------------------

    def log(self, event, text):
        if self.quiet and event in CHATTY_EVENTS:
            return
        now_us = time.time_ns() // 1000
        stamp = time.strftime("%H:%M:%S", time.localtime(now_us // 1_000_000)) + f".{now_us % 1_000_000:06d}"
        line = f"{stamp} [{self.id}] {event:<8} {text}\n"
        with PRINT_LOCK:  # one write per line, so lines of different threads do not mix
            sys.stdout.write(line)
            sys.stdout.flush()

    def leaseholder_for(self, key):
        """Node holding the lease for the range of this key, or None if no range contains it."""
        first = key[:1]
        for low, high in KEY_RANGES:
            if first != "" and low <= first <= high:
                return self.leaseholder
        return None

    def lag_ms(self, closed_ts):
        """How far this follower is behind the leaseholder, or None if it never heard from it."""
        if closed_ts == 0:
            return None
        return max(0, (self.clock.wall() - closed_ts) // 1000)

    def mark(self, peer, ok):
        if peer.reachable == ok:
            return
        before = peer.reachable
        peer.reachable = ok
        if ok and before is None:
            self.log("NET", f"connected to peer {peer.id}")
        elif ok:
            self.log("NET", f"peer {peer.id} is reachable again")
        else:
            self.log("NET", f"peer {peer.id} is UNREACHABLE")

    def call(self, peer, method, request, timeout=None):
        """One RPC to a peer. Returns the reply, or None if the peer did not answer."""
        if timeout is None:
            timeout = self.peer_timeout
        try:
            reply = method(request, timeout=timeout)
        except grpc.RpcError:
            self.mark(peer, False)
            return None
        self.mark(peer, True)
        return reply

    def replicate(self, send, need):
        """Run send(peer) for all peers in parallel and count the acknowledgements.

        Waits until `need` peers have acknowledged (or every peer answered, or the timeout),
        then gives the other peers a short grace period. In a healthy cluster all of them
        answer within it, so the count is complete; an unreachable peer does not slow the
        write down once the quorum is there.
        """
        jobs = set()
        for peer in self.peers.values():
            jobs.add(self.pool.submit(send, peer))
        acks = 0
        deadline = time.monotonic() + self.peer_timeout
        grace_end = None
        while jobs:
            now = time.monotonic()
            if acks >= need:
                if grace_end is None:
                    grace_end = now + self.ack_grace
                limit = min(grace_end, deadline)
            else:
                limit = deadline
            if now >= limit:
                break
            done, jobs = futures.wait(jobs, timeout=limit - now, return_when=futures.FIRST_COMPLETED)
            for job in done:
                if job.result():
                    acks = acks + 1
        return acks

    def merge_entry(self, entry, source):
        """Merge a version into the local store. Returns True if the store changed."""
        with self.lock:
            current = self.store.get(entry.key)
            kept, note = resolve(current, entry, self.merge_policy)
            if entry.vclock:
                self.clock.observe(max(entry.vclock.values()))
            if kept is current:
                if note.startswith("conflict"):
                    self.log("MERGE", f"{entry.key}: {note} -> kept {current.value!r} "
                                      f"(ts {clock_text(current.timestamp)}), dropped {entry.value!r} "
                                      f"from {entry.origin} (ts {clock_text(entry.timestamp)}) [{source}]")
                return False
            self.store[entry.key] = kept
            self.change_no = self.change_no + 1
            self.changed[entry.key] = self.change_no
        if note.startswith("conflict"):
            self.log("MERGE", f"{entry.key}: {note} -> {kept.value!r} (ts {clock_text(kept.timestamp)}), "
                              f"local was {current.value!r} (ts {clock_text(current.timestamp)}) [{source}]")
        else:
            self.log("APPLY", f"{entry.key}={kept.value!r} ts={clock_text(kept.timestamp)} "
                              f"vc={vclock_text(kept.vclock)} [{source}]")
        return True

    # ----- writes --------------------------------------------------------------

    def write_cp(self, request):
        """QUORUM_CP on the leaseholder: the write becomes visible only after a majority acked."""
        with self.cp_lock:
            stored = self.seen.get(request.idempotency_key)
            if request.idempotency_key and stored is not None:
                self.log("PUT", f"{request.key}: idempotency key seen before -> returning the stored reply")
                return stored
            if self.recovering:
                message = (f"ReplicaUnavailableError: leaseholder {self.id} restarted and is still "
                           f"recovering the state from its followers")
                self.log("PUT", f"{request.key}={request.value!r} QUORUM_CP -> REJECTED, {message}")
                return pb.PutReply(success=False, acks_received=0, error_message=message)

            with self.lock:
                timestamp = self.clock.now()
                base = self.store.get(request.key)
                entry = Entry(request.key, request.value, timestamp, self.id,
                              next_vclock(base, self.id, timestamp))
                self.inflight.add(timestamp)

            def prepare(peer):
                message = pb.PrepareRequest(version=entry.to_proto(), leaseholder=self.id)
                reply = self.call(peer, peer.replica.Prepare, message)
                return reply is not None and reply.ok

            acks = 1 + self.replicate(prepare, need=self.majority - 1)
            committed = acks >= self.majority
            with self.lock:
                self.inflight.discard(timestamp)
                if committed:
                    self.merge_entry(entry, "commit")

            if committed:
                reply = pb.PutReply(success=True, acks_received=acks)
                if request.idempotency_key:
                    self.seen[request.idempotency_key] = reply
                self.wake_sync.set()  # push the commit to the followers right away
                self.log("PUT", f"{request.key}={request.value!r} QUORUM_CP ts={clock_text(timestamp)} "
                                f"-> COMMIT, acks {acks}/{self.size}")
            else:
                message = (f"ReplicaUnavailableError: quorum lost - {acks}/{self.size} replicas "
                           f"acknowledged, {self.majority} required")
                reply = pb.PutReply(success=False, acks_received=acks, error_message=message)
                self.log("PUT", f"{request.key}={request.value!r} QUORUM_CP -> ABORT, {message}")
            return reply

    def write_ap(self, request):
        """ALWAYS_AP: accept on this node at once, then push to every peer that can be reached."""
        stored = self.seen.get(request.idempotency_key)
        if request.idempotency_key and stored is not None:
            return stored
        with self.lock:
            event_time = self.clock.now()
            timestamp = request.timestamp if request.timestamp > 0 else event_time
            base = self.store.get(request.key)
            entry = Entry(request.key, request.value, timestamp, self.id,
                          next_vclock(base, self.id, event_time))
            applied = self.merge_entry(entry, "local write")

        def push(peer):
            message = pb.SyncRequest(sender=self.id, versions=[entry.to_proto()],
                                     expected_incarnation=peer.incarnation)
            return self.call(peer, peer.replica.Sync, message) is not None

        acks = 1 + self.replicate(push, need=0)
        reply = pb.PutReply(success=True, acks_received=acks)
        if request.idempotency_key:
            self.seen[request.idempotency_key] = reply
        note = ""
        if acks < self.size:
            note = " (sloppy quorum: the other replicas get it after the partition heals)"
        if not applied:
            note += " - but the stored version has a later timestamp, so LWW keeps that one"
        self.log("PUT", f"{request.key}={request.value!r} ALWAYS_AP ts={clock_text(timestamp)} "
                        f"vc={vclock_text(entry.vclock)} -> ACCEPTED, acks {acks}/{self.size}{note}")
        return reply

    # ----- anti-entropy --------------------------------------------------------

    def start(self):
        threading.Thread(target=self.sync_loop, daemon=True).start()

    def sync_loop(self):
        while True:
            self.wake_sync.wait(self.sync_interval)
            self.wake_sync.clear()
            for peer in self.peers.values():
                with self.lock:
                    if peer.sync_busy:
                        continue
                    peer.sync_busy = True
                self.pool.submit(self.sync_peer, peer)

    def sync_peer(self, peer):
        """Push to one peer every version that changed since its last acknowledgement.

        The leaseholder also sends its closed timestamp: every QUORUM_CP write with a
        smaller timestamp is committed and contained in what the follower has received.
        """
        try:
            with self.lock:
                full = peer.synced_upto == 0
                if full:
                    entries = list(self.store.values())
                else:
                    entries = []
                    for key, number in self.changed.items():
                        if number > peer.synced_upto:
                            entries.append(self.store[key])
                upto = self.change_no
                closed = 0
                if self.is_leaseholder and not self.recovering:
                    if self.inflight:
                        closed = min(self.inflight) - 1
                    else:
                        closed = self.clock.now()
                expected = peer.incarnation
            message = pb.SyncRequest(sender=self.id, versions=[entry.to_proto() for entry in entries],
                                     closed_timestamp=closed, expected_incarnation=expected, full=full)
            reply = self.call(peer, peer.replica.Sync, message)
            if reply is None:
                return
            with self.lock:
                if peer.incarnation != "" and reply.incarnation != peer.incarnation:
                    self.log("SYNC", f"peer {peer.id} restarted with an empty store -> sending it everything")
                    peer.synced_upto = 0
                else:
                    peer.synced_upto = upto
                peer.incarnation = reply.incarnation
        finally:
            peer.sync_busy = False

    # ----- internal RPC handlers -------------------------------------------------

    def on_prepare(self, request):
        if request.leaseholder != self.leaseholder:
            return pb.PrepareReply(ok=False, node=self.id)
        entry = Entry.from_proto(request.version)
        with self.lock:
            self.staged[entry.timestamp] = entry
        self.clock.observe(entry.timestamp)
        self.log("PREPARE", f"{entry.key}={entry.value!r} staged for leaseholder {request.leaseholder} -> ack")
        return pb.PrepareReply(ok=True, node=self.id)

    def on_sync(self, request):
        merged = 0
        for message in request.versions:
            if self.merge_entry(Entry.from_proto(message), "sync from " + request.sender):
                merged = merged + 1
        with self.lock:
            if self.recovering and request.full and request.sender in self.peers:
                self.recovered_from.add(request.sender)
                if self.recovered_from >= set(self.peers):
                    self.recovering = False
                    self.log("LEASE", "got the full state from every follower -> serving again")
            closed = request.closed_timestamp
            if closed > 0 and request.sender == self.leaseholder and closed > self.closed_ts:
                # Only trust the closed timestamp if the leaseholder knows this incarnation of
                # us; after a restart we first need the full state.
                if request.full or request.expected_incarnation == self.incarnation:
                    self.closed_ts = closed
                    for timestamp in list(self.staged):
                        if timestamp <= closed:
                            del self.staged[timestamp]
        return pb.SyncReply(node=self.id, incarnation=self.incarnation, merged=merged)

    def on_read_local(self, request):
        with self.lock:
            entry = self.store.get(request.key)
        if entry is None:
            return pb.ReadLocalReply(found=False, node=self.id)
        return pb.ReadLocalReply(found=True, version=entry.to_proto(), node=self.id)

    def on_status(self):
        with self.lock:
            closed = self.closed_ts
            keys = len(self.store)
            staged = len(self.staged)
        if self.is_leaseholder:
            role = "leaseholder"
            lag = 0
        else:
            role = "follower"
            lag = self.lag_ms(closed)
            if lag is None:
                lag = -1
        reachable = [peer.id for peer in self.peers.values() if peer.reachable]
        return pb.StatusReply(node=self.id, role=role, leaseholder=self.leaseholder,
                              closed_timestamp=closed, lag_ms=lag, keys=keys, staged=staged,
                              merge_policy=self.merge_policy, reachable_peers=reachable,
                              incarnation=self.incarnation, recovering=self.recovering)


def fail(context, status, error_code, message):
    """End the RPC with an error. The code also goes into the trailing metadata for the client."""
    context.set_trailing_metadata((("kv-error", error_code),))
    context.abort(status, f"{error_code}: {message}")


def error_code_of(error):
    """The kv-error code that a replica attached to an RpcError, or None for a network error."""
    try:
        for key, value in error.trailing_metadata() or ():
            if key == "kv-error":
                return value
    except Exception:
        pass
    return None


def was_forwarded(context):
    for key, value in context.invocation_metadata():
        if key == "forwarded-by":
            return value
    return None


def value_text(found, value):
    if not found:
        return "not found"
    return repr(value)


class KeyValueStoreServicer(rpc.KeyValueStoreServicer):
    """Public API (Task 2): every request is routed by its consistency level or write strategy."""

    def __init__(self, replica):
        self.r = replica

    def Get(self, request, context):
        r = self.r
        holder = r.leaseholder_for(request.key)
        if holder is None:
            fail(context, grpc.StatusCode.INVALID_ARGUMENT, "INVALID_KEY",
                 f"key {request.key!r} is outside the keyspace range [a-z]")

        if request.consistency == pb.LINEARIZABLE:
            # Linearizable reads are answered by the leaseholder only. Every QUORUM_CP write
            # is committed there first, so its copy is always the latest committed one.
            if holder != r.id:
                return self.forward_get(holder, request, context)
            if r.recovering:
                fail(context, grpc.StatusCode.UNAVAILABLE, "LEASEHOLDER_RECOVERING",
                     f"leaseholder {r.id} restarted and is still recovering its state")
            return self.read_on_leaseholder(request, context, "LINEARIZABLE")

        # FOLLOWER_STALE: answered from the local state machine of whichever node got it.
        if holder == r.id:
            return self.read_on_leaseholder(request, context, "FOLLOWER_STALE")
        return self.read_on_follower(request, context)

    def read_on_leaseholder(self, request, context, mode):
        r = self.r
        with r.lock:
            entry = r.store.get(request.key)
        found = entry is not None
        value = entry.value if found else ""
        context.set_trailing_metadata((("served-by", r.id),))
        r.log("GET", f"{request.key} {mode} on leaseholder -> {value_text(found, value)}")
        return pb.GetReply(value=value, found=found, read_timestamp=r.clock.now(), is_stale=False)

    def read_on_follower(self, request, context):
        r = self.r
        bound = request.max_staleness_ms
        if bound == 0:
            bound = r.default_staleness_ms
        with r.lock:
            closed = r.closed_ts
            entry = r.store.get(request.key)
        lag = r.lag_ms(closed)
        if lag is None or lag > bound:
            if lag is None:
                why = "has not heard from the leaseholder yet"
            else:
                why = f"lags {lag} ms behind the leaseholder"
            r.log("GET", f"{request.key} FOLLOWER_STALE: follower {why}, max_staleness_ms={bound} "
                         f"-> STALE_READ_EXCEEDED")
            fail(context, grpc.StatusCode.FAILED_PRECONDITION, "STALE_READ_EXCEEDED",
                 f"follower {r.id} {why}, max_staleness_ms={bound}")
        found = entry is not None
        value = entry.value if found else ""
        context.set_trailing_metadata((("served-by", r.id), ("lag-ms", str(lag))))
        r.log("GET", f"{request.key} FOLLOWER_STALE on follower (lag {lag} ms <= {bound} ms) "
                     f"-> {value_text(found, value)}")
        return pb.GetReply(value=value, found=found, read_timestamp=closed, is_stale=True)

    def forward_get(self, holder, request, context):
        r = self.r
        if was_forwarded(context):
            fail(context, grpc.StatusCode.FAILED_PRECONDITION, "ROUTING_LOOP",
                 f"{r.id} got a forwarded request but is not the leaseholder")
        peer = r.peers[holder]
        try:
            reply, call = peer.kv.Get.with_call(request, timeout=r.peer_timeout * 2,
                                                metadata=(("forwarded-by", r.id),))
        except grpc.RpcError as error:
            code = error_code_of(error)
            if code is not None:
                # The leaseholder answered with an error of its own: pass it on unchanged.
                r.mark(peer, True)
                context.set_trailing_metadata((("kv-error", code),))
                context.abort(error.code(), error.details())
            r.mark(peer, False)
            r.log("GET", f"{request.key} LINEARIZABLE -> leaseholder {holder} unreachable -> UNAVAILABLE")
            fail(context, grpc.StatusCode.UNAVAILABLE, "LEASEHOLDER_UNAVAILABLE",
                 f"leaseholder {holder} is unreachable from {r.id}, a linearizable read is not "
                 f"possible on this side of the partition")
        r.mark(peer, True)
        context.set_trailing_metadata((("served-by", holder), ("forwarded-by", r.id)))
        r.log("GET", f"{request.key} LINEARIZABLE -> proxied to leaseholder {holder} "
                     f"-> {value_text(reply.found, reply.value)}")
        return reply

    def Put(self, request, context):
        r = self.r
        holder = r.leaseholder_for(request.key)
        if holder is None:
            return pb.PutReply(success=False, acks_received=0,
                               error_message=f"InvalidKeyError: key {request.key!r} is outside the "
                                             f"keyspace range [a-z]")
        if request.strategy == pb.QUORUM_CP:
            if holder != r.id:
                return self.forward_put(holder, request, context)
            return r.write_cp(request)
        return r.write_ap(request)

    def forward_put(self, holder, request, context):
        r = self.r
        if was_forwarded(context):
            return pb.PutReply(success=False, acks_received=0,
                               error_message=f"RoutingError: {r.id} is not the leaseholder")
        peer = r.peers[holder]
        try:
            reply = peer.kv.Put(request, timeout=r.peer_timeout * 3, metadata=(("forwarded-by", r.id),))
        except grpc.RpcError:
            r.mark(peer, False)
            message = (f"ReplicaUnavailableError: quorum lost - leaseholder {holder} is unreachable "
                       f"from {r.id}, so no majority ({r.majority} of {r.size}) can acknowledge the write")
            r.log("PUT", f"{request.key}={request.value!r} QUORUM_CP -> FAILED, {message}")
            return pb.PutReply(success=False, acks_received=0, error_message=message)
        r.mark(peer, True)
        r.log("PUT", f"{request.key}={request.value!r} QUORUM_CP -> forwarded to leaseholder {holder}: "
                     f"success={reply.success}, acks {reply.acks_received}/{r.size}")
        return reply


class ReplicaServicer(rpc.ReplicaServicer):
    """Internal replica-to-replica protocol."""

    def __init__(self, replica):
        self.r = replica

    def Prepare(self, request, context):
        return self.r.on_prepare(request)

    def Sync(self, request, context):
        return self.r.on_sync(request)

    def ReadLocal(self, request, context):
        return self.r.on_read_local(request)

    def Status(self, request, context):
        return self.r.on_status()


def start_server(node_id, port, peers, **options):
    """Start one replica. Returns (server, replica)."""
    replica = Replica(node_id, peers, **options)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    rpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(replica), server)
    rpc.add_ReplicaServicer_to_server(ReplicaServicer(replica), server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    replica.start()
    return server, replica


def parse_peers(text):
    """'B=127.0.0.1:50052,C=127.0.0.1:50053' -> {'B': '127.0.0.1:50052', 'C': '127.0.0.1:50053'}"""
    peers = {}
    for item in text.split(","):
        item = item.strip()
        if item == "":
            continue
        node, address = item.split("=", 1)
        peers[node.strip()] = address.strip()
    return peers


def exit_with_parent():
    """The cluster script keeps our stdin open. End of input means it is gone, so stop too."""
    def watch():
        try:
            sys.stdin.read()
        except Exception:
            pass
        os._exit(0)
    threading.Thread(target=watch, daemon=True).start()


def main():
    sys.stdout.reconfigure(errors="backslashreplace")
    parser = argparse.ArgumentParser(description="One replica of the key-value store.")
    parser.add_argument("--id", default="A", help="node id: A, B or C")
    parser.add_argument("--port", type=int, default=None,
                        help="default: 50051 for A, 50052 for B, 50053 for C")
    parser.add_argument("--peers", default=None,
                        help="ID=host:port,... (default: the other two nodes on localhost)")
    parser.add_argument("--leaseholder", default=DEFAULT_LEASEHOLDER,
                        help="node holding the lease for the range [a-z]")
    parser.add_argument("--merge", choices=["lww", "union"], default="lww",
                        help="conflict resolution for concurrent writes")
    parser.add_argument("--sync-interval-ms", type=int, default=100)
    parser.add_argument("--peer-timeout-ms", type=int, default=500)
    parser.add_argument("--ack-grace-ms", type=int, default=50)
    parser.add_argument("--max-staleness-ms", type=int, default=5000,
                        help="bound used when a FOLLOWER_STALE request sends max_staleness_ms = 0")
    parser.add_argument("--clock-skew-ms", type=int, default=0, help="shift this node's wall clock")
    parser.add_argument("--recover", action="store_true",
                        help="restarted leaseholder: wait for the full state of all followers")
    parser.add_argument("--quiet", action="store_true", help="do not log every read and write")
    parser.add_argument("--exit-with-parent", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    port = args.port
    if port is None:
        port = DEFAULT_PORTS.get(args.id, 50051)
    if args.peers is None:
        peers = {}
        for node, node_port in DEFAULT_PORTS.items():
            if node != args.id:
                peers[node] = f"127.0.0.1:{node_port}"
    else:
        peers = parse_peers(args.peers)
    if args.exit_with_parent:
        exit_with_parent()

    try:
        server, replica = start_server(
            args.id, port, peers, leaseholder=args.leaseholder, merge=args.merge,
            sync_interval_ms=args.sync_interval_ms, peer_timeout_ms=args.peer_timeout_ms,
            ack_grace_ms=args.ack_grace_ms, max_staleness_ms=args.max_staleness_ms,
            clock_skew_ms=args.clock_skew_ms, recover=args.recover, quiet=args.quiet)
    except RuntimeError as error:
        print(f"[{args.id}] cannot listen on port {port}: {error}", flush=True)
        sys.exit(1)

    if replica.is_leaseholder:
        role = "LEASEHOLDER for range [a-z]"
    else:
        role = f"follower (leaseholder is {args.leaseholder})"
    peer_text = ", ".join(f"{node}={address}" for node, address in peers.items())
    replica.log("START", f"listening on 127.0.0.1:{port} | {role} | merge={args.merge} | peers: {peer_text}")
    if replica.recovering:
        replica.log("LEASE", "restarted leaseholder: waiting for the full state from every follower")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
