import argparse
import sys
import time
import uuid

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as rpc
from server import CHANNEL_OPTIONS, DEFAULT_PORTS, clock_text, error_code_of, vclock_text

DEFAULT_NODES = {node: f"127.0.0.1:{port}" for node, port in DEFAULT_PORTS.items()}
BACKOFF_SECONDS = [0.2, 0.4, 0.8]


class KVError(Exception):
    def __init__(self, code, message, reply=None):
        super().__init__(message)
        self.code = code
        self.reply = reply


class ReplicaUnavailableError(KVError):
    """Quorum lost, the leaseholder cannot be reached, or the node itself is down."""


class StaleReadExceededError(KVError):
    """The follower is further behind the leaseholder than max_staleness_ms allows."""


class InvalidKeyError(KVError):
    """The key is outside the keyspace range [a-z]."""


def now_us():
    return time.time_ns() // 1000


def to_strategy(strategy):
    if strategy in (pb.QUORUM_CP, pb.ALWAYS_AP):
        return strategy
    name = str(strategy).lower()
    if name in ("cp", "quorum_cp"):
        return pb.QUORUM_CP
    if name in ("ap", "always_ap"):
        return pb.ALWAYS_AP
    raise ValueError(f"unknown write strategy {strategy!r}")


def to_consistency(consistency):
    if consistency in (pb.LINEARIZABLE, pb.FOLLOWER_STALE):
        return consistency
    name = str(consistency).lower()
    if name in ("linearizable", "lin"):
        return pb.LINEARIZABLE
    if name in ("stale", "follower", "follower_stale"):
        return pb.FOLLOWER_STALE
    raise ValueError(f"unknown read consistency {consistency!r}")


class PutResult:

    def __init__(self, key, value, strategy, node, reply, latency_ms):
        self.key = key
        self.value = value
        self.strategy = pb.WriteStrategy.Name(strategy)
        self.node = node
        self.success = reply.success
        self.acks = reply.acks_received
        self.error = reply.error_message
        self.reply = reply
        self.latency_ms = latency_ms


class GetResult:

    def __init__(self, key, consistency, node, reply, served_by, lag_ms, latency_ms):
        self.key = key
        self.consistency = pb.ReadConsistency.Name(consistency)
        self.node = node
        self.value = reply.value
        self.found = reply.found
        self.read_timestamp = reply.read_timestamp
        self.is_stale = reply.is_stale
        self.served_by = served_by
        self.lag_ms = lag_ms
        self.latency_ms = latency_ms


def metadata_value(call, name):
    try:
        for key, value in call.trailing_metadata() or ():
            if key == name:
                return value
    except Exception:
        pass
    return None


class KVClient:
    """Client gateway: sends each request to one chosen node (the "gateway" replica)."""

    def __init__(self, nodes=None, timeout=3.0, retries=2):
        if nodes is None:
            nodes = DEFAULT_NODES
        self.addresses = dict(nodes)
        self.timeout = timeout
        self.retries = retries
        self.channels = {}
        self.kv = {}
        self.replica = {}
        for node, address in self.addresses.items():
            channel = grpc.insecure_channel(address, options=CHANNEL_OPTIONS)
            self.channels[node] = channel
            self.kv[node] = rpc.KeyValueStoreStub(channel)
            self.replica[node] = rpc.ReplicaStub(channel)

    def close(self):
        for channel in self.channels.values():
            channel.close()

    def size(self):
        return len(self.addresses)

    def call(self, method, request):
        """Send a request; retry only when the node itself did not answer."""
        attempt = 0
        while True:
            try:
                return method.with_call(request, timeout=self.timeout)
            except grpc.RpcError as error:
                if error_code_of(error) is not None:
                    raise  # the replica answered with an error: retrying will not help
                transient = error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
                if not transient or attempt >= self.retries:
                    raise
                time.sleep(BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)])
                attempt = attempt + 1

    def put(self, key, value, strategy="cp", node="A", timestamp=0, idempotency_key=None,
            raise_on_failure=True):
        """Write through `node`. Raises ReplicaUnavailableError when the write is not accepted."""
        strategy = to_strategy(strategy)
        if idempotency_key is None:
            idempotency_key = str(uuid.uuid4())  # one key per logical write, reused by retries
        request = pb.PutRequest(key=key, value=value, idempotency_key=idempotency_key,
                                strategy=strategy, timestamp=timestamp)
        start = time.perf_counter()
        try:
            reply, _ = self.call(self.kv[node].Put, request)
        except grpc.RpcError as error:
            raise ReplicaUnavailableError("NODE_UNAVAILABLE",
                                          f"node {node} ({self.addresses[node]}) did not answer: "
                                          f"{error.code().name}")
        result = PutResult(key, value, strategy, node, reply, (time.perf_counter() - start) * 1000)
        if not reply.success and raise_on_failure:
            if reply.error_message.startswith("InvalidKeyError"):
                raise InvalidKeyError("INVALID_KEY", reply.error_message, reply)
            raise ReplicaUnavailableError("QUORUM_LOST", reply.error_message, reply)
        return result

    def get(self, key, consistency="linearizable", node="A", max_staleness_ms=0):
        """Read through `node` with the given consistency level."""
        consistency = to_consistency(consistency)
        request = pb.GetRequest(key=key, consistency=consistency, max_staleness_ms=max_staleness_ms)
        start = time.perf_counter()
        try:
            reply, call = self.call(self.kv[node].Get, request)
        except grpc.RpcError as error:
            code = error_code_of(error)
            if code == "STALE_READ_EXCEEDED":
                raise StaleReadExceededError(code, error.details())
            if code == "INVALID_KEY":
                raise InvalidKeyError(code, error.details())
            if code is not None:
                raise ReplicaUnavailableError(code, error.details())
            raise ReplicaUnavailableError("NODE_UNAVAILABLE",
                                          f"node {node} ({self.addresses[node]}) did not answer: "
                                          f"{error.code().name}")
        latency_ms = (time.perf_counter() - start) * 1000
        served_by = metadata_value(call, "served-by") or node
        lag = metadata_value(call, "lag-ms")
        lag_ms = int(lag) if lag is not None else None
        return GetResult(key, consistency, node, reply, served_by, lag_ms, latency_ms)

    def read_local(self, key, node):
        """The version that one replica holds right now (internal API, no routing)."""
        reply = self.replica[node].ReadLocal(pb.ReadLocalRequest(key=key), timeout=self.timeout)
        if not reply.found:
            return None
        return reply.version

    def inspect(self, key):
        """node -> version (None if the key is missing there, "down" if the node did not answer)."""
        result = {}
        for node in self.addresses:
            try:
                result[node] = self.read_local(key, node)
            except grpc.RpcError:
                result[node] = "down"
        return result

    def quorum_get(self, key, nodes):
        """Dynamo-style quorum read: ask every node in `nodes`, keep the newest version.

        Returns (newest version or None, {node: version or None}).
        """
        answers = {}
        for node in nodes:
            answers[node] = self.read_local(key, node)
        versions = [version for version in answers.values() if version is not None]
        if not versions:
            return None, answers
        newest = max(versions, key=lambda version: (version.timestamp, version.origin))
        return newest, answers

    def status(self, node):
        try:
            return self.replica[node].Status(pb.StatusRequest(), timeout=self.timeout)
        except grpc.RpcError:
            return None


def version_text(version):
    if version is None:
        return "(not found)"
    if version == "down":
        return "DOWN (no answer)"
    return (f"{version.value!r:<18} ts={clock_text(version.timestamp)}  origin={version.origin}  "
            f"vc={vclock_text(dict(version.vclock))}")


def parse_nodes(text):
    nodes = {}
    for item in text.split(","):
        node, address = item.strip().split("=", 1)
        nodes[node.strip()] = address.strip()
    return nodes


def main():
    sys.stdout.reconfigure(errors="backslashreplace")
    parser = argparse.ArgumentParser(description="Client gateway for the key-value store.")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--cluster", default=None,
                        help="ID=host:port,... (default: A=127.0.0.1:50051,B=...:50052,C=...:50053)")
    common.add_argument("--retries", type=int, default=2, help="retries when the node does not answer")
    commands = parser.add_subparsers(dest="command", required=True)

    put = commands.add_parser("put", help="write a value", parents=[common])
    put.add_argument("key")
    put.add_argument("value")
    put.add_argument("--strategy", choices=["cp", "ap"], default="cp",
                     help="cp = QUORUM_CP (majority), ap = ALWAYS_AP (any node)")
    put.add_argument("--node", default="A", help="replica that receives the request")
    put.add_argument("--timestamp", type=int, default=0, help="LWW timestamp in microseconds (ALWAYS_AP)")
    put.add_argument("--ts-offset-ms", type=int, default=None,
                     help="LWW timestamp = now + this many milliseconds (ALWAYS_AP)")
    put.add_argument("--idempotency-key", default=None)

    get = commands.add_parser("get", help="read a value", parents=[common])
    get.add_argument("key")
    get.add_argument("--consistency", choices=["linearizable", "stale"], default="linearizable",
                     help="linearizable = LINEARIZABLE, stale = FOLLOWER_STALE")
    get.add_argument("--node", default="A", help="replica that receives the request")
    get.add_argument("--max-staleness-ms", type=int, default=5000)

    inspect = commands.add_parser("inspect", help="show the version every replica holds", parents=[common])
    inspect.add_argument("key")

    quorum = commands.add_parser("quorum-get", help="read from R replicas and keep the newest",
                                 parents=[common])
    quorum.add_argument("key")
    quorum.add_argument("--nodes", default="A,B", help="the read quorum, e.g. A,B")

    commands.add_parser("status", help="role, staleness and reachable peers of every replica",
                        parents=[common])
    args = parser.parse_args()

    nodes = parse_nodes(args.cluster) if args.cluster else DEFAULT_NODES
    client = KVClient(nodes, retries=args.retries)
    exit_code = 0

    if args.command == "put":
        timestamp = args.timestamp
        if args.ts_offset_ms is not None:
            timestamp = now_us() + args.ts_offset_ms * 1000
        try:
            result = client.put(args.key, args.value, args.strategy, args.node, timestamp,
                                args.idempotency_key)
            print(f"OK   put {args.key}={args.value!r} {result.strategy} via {args.node}: "
                  f"acks {result.acks}/{client.size()}")
        except KVError as error:
            print(f"FAIL put {args.key}={args.value!r} via {args.node}: {error}")
            exit_code = 1

    elif args.command == "get":
        try:
            result = client.get(args.key, args.consistency, args.node, args.max_staleness_ms)
            if result.found:
                shown = f"{args.key} = {result.value!r}"
            else:
                shown = f"{args.key}: not found"
            details = f"{result.consistency} via {args.node}, served by {result.served_by}, " \
                      f"is_stale={result.is_stale}"
            if result.lag_ms is not None:
                details += f", lag {result.lag_ms} ms"
            details += f", read_ts={clock_text(result.read_timestamp)}, {result.latency_ms:.1f} ms"
            print(f"{shown}   [{details}]")
        except KVError as error:
            print(f"FAIL get {args.key} via {args.node}: {error}")
            exit_code = 1

    elif args.command == "inspect":
        for node, version in client.inspect(args.key).items():
            print(f"{node}  {version_text(version)}")

    elif args.command == "quorum-get":
        read_set = [node.strip() for node in args.nodes.split(",") if node.strip()]
        newest, answers = client.quorum_get(args.key, read_set)
        for node, version in answers.items():
            print(f"  {node}  {version_text(version)}")
        if newest is None:
            print(f"R={len(read_set)} {read_set}: not found")
        else:
            print(f"R={len(read_set)} {read_set}: newest = {newest.value!r} from {newest.origin}")

    elif args.command == "status":
        for node in nodes:
            reply = client.status(node)
            if reply is None:
                print(f"{node}  DOWN")
                continue
            if reply.role == "leaseholder":
                lag = "-"
            elif reply.lag_ms < 0:
                lag = "never synced"
            else:
                lag = f"{reply.lag_ms} ms"
            recovering = " RECOVERING" if reply.recovering else ""
            print(f"{node}  {reply.role:<11}{recovering} keys={reply.keys:<3} staged={reply.staged:<2} "
                  f"lag={lag:<12} merge={reply.merge_policy} "
                  f"reachable peers: {', '.join(reply.reachable_peers) or 'none'}")

    client.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
