"""Counter client.

Run:  python client.py incr likes:post-42 --by 5
      python client.py get likes:post-42

With three replicas:
      python client.py incr likes:post-42 --replicas 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053
"""
import argparse
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import EventLog, LamportClock

BACKOFF_SECONDS = [0.2, 0.4, 0.8]


class IncrResult:
    """What incr() returns."""

    def __init__(self, committed, new_value, was_duplicate, acks, replicas):
        self.committed = committed
        self.new_value = new_value
        self.was_duplicate = was_duplicate
        self.acks = acks
        self.replicas = replicas


class CounterClient:

    def __init__(self, addresses, name="client-1", timeout=2.0, log_file=None, log_enabled=True):
        self._stubs = []
        for address in addresses:
            channel = grpc.insecure_channel(address)
            self._stubs.append(counter_pb2_grpc.CounterStub(channel))
        self._timeout = timeout
        self._clock = LamportClock()
        self._log = EventLog(name, log_file, log_enabled)

    def incr(self, counter_id, delta, key=None):
        """Send the increment to ALL replicas. It is committed if a majority answered."""
        if key is None:
            key = str(uuid.uuid4())

        acks = 0
        duplicates = 0
        new_value = None
        for stub in self._stubs:
            reply = self._increment_on_one_replica(stub, counter_id, delta, key)
            if reply is not None:
                acks = acks + 1
                new_value = reply.new_value
                if reply.was_duplicate:
                    duplicates = duplicates + 1

        majority = len(self._stubs) // 2 + 1
        committed = acks >= majority
        if not committed:
            new_value = None
        was_duplicate = committed and duplicates == acks
        return IncrResult(committed, new_value, was_duplicate, acks, len(self._stubs))

    def _increment_on_one_replica(self, stub, counter_id, delta, key):
        """Send the request to one replica. Return the reply, or None if it did not answer."""
        text = f"Increment(counter={counter_id}, delta={delta})"
        for attempt in range(4):
            if attempt > 0:
                time.sleep(BACKOFF_SECONDS[attempt - 1])

            now = self._clock.tick()
            self._log.write("SEND", text, now)
            request = counter_pb2.IncrementRequest(
                counter_id=counter_id, delta=delta, idempotency_key=key, lamport_time=now)
            try:
                reply = stub.Increment(request, timeout=self._timeout)
            except grpc.RpcError as error:
                code = error.code()
                if code == grpc.StatusCode.DEADLINE_EXCEEDED or code == grpc.StatusCode.UNAVAILABLE:
                    continue
                return None

            now = self._clock.update(reply.lamport_time)
            self._log.write("RECV", f"IncrementReply(new_value={reply.new_value})", now, reply.lamport_time)
            return reply
        return None

    def get(self, counter_id):
        """Read the counter from the first replica that answers."""
        text = f"Get(counter={counter_id})"
        for stub in self._stubs:
            now = self._clock.tick()
            self._log.write("SEND", text, now)
            request = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=now)
            try:
                reply = stub.Get(request, timeout=self._timeout)
            except grpc.RpcError:
                continue

            now = self._clock.update(reply.lamport_time)
            self._log.write("RECV", f"GetReply(value={reply.value}, found={reply.found})", now, reply.lamport_time)
            return reply
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["incr", "get"])
    parser.add_argument("counter_id")
    parser.add_argument("--by", type=int, default=1)
    parser.add_argument("--key", default=None)
    parser.add_argument("--replicas", default="127.0.0.1:50051")
    parser.add_argument("--name", default="client-1")
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()

    addresses = args.replicas.split(",")
    client = CounterClient(addresses, args.name, log_file=args.log_file)

    for i in range(args.repeat):
        if args.command == "incr":
            result = client.incr(args.counter_id, args.by, args.key)
            if result.committed:
                if result.was_duplicate:
                    duplicate = "yes"
                else:
                    duplicate = "no"
                print(f"OK committed value={result.new_value} "
                      f"(replicas acked: {result.acks}/{result.replicas}, duplicate: {duplicate})")
            else:
                print(f"FAILED not committed (replicas acked: {result.acks}/{result.replicas})")
        else:
            reply = client.get(args.counter_id)
            if reply is None:
                print("FAILED no replica answered")
            elif reply.found:
                print(f"value={reply.value}")
            else:
                print("not found")


if __name__ == "__main__":
    main()