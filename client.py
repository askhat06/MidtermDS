import argparse
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc

BACKOFF_SECONDS = [0.2, 0.4, 0.8]


class CounterClient:

    def __init__(self, address, timeout=2.0):
        channel = grpc.insecure_channel(address)
        self._stub = counter_pb2_grpc.CounterStub(channel)
        self._timeout = timeout

    def incr(self, counter_id, delta, key=None):
        """Send the increment. Return the reply, or None if the server did not answer."""
        if key is None:
            key = str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key)

        for attempt in range(4):
            if attempt > 0:
                time.sleep(BACKOFF_SECONDS[attempt - 1])
            try:
                reply = self._stub.Increment(request, timeout=self._timeout)
            except grpc.RpcError as error:
                code = error.code()
                if code == grpc.StatusCode.DEADLINE_EXCEEDED or code == grpc.StatusCode.UNAVAILABLE:
                    print(f"attempt {attempt + 1} failed: {code.name}")
                    continue
                return None
            return reply
        return None

    def get(self, counter_id):
        """Read the counter. Return the reply, or None if the server did not answer."""
        request = counter_pb2.GetRequest(counter_id=counter_id)
        try:
            return self._stub.Get(request, timeout=self._timeout)
        except grpc.RpcError:
            return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["incr", "get"])
    parser.add_argument("counter_id")
    parser.add_argument("--by", type=int, default=1)
    parser.add_argument("--key", default=None)
    parser.add_argument("--address", default="127.0.0.1:50051")
    args = parser.parse_args()

    client = CounterClient(args.address)

    if args.command == "incr":
        reply = client.incr(args.counter_id, args.by, args.key)
        if reply is None:
            print("FAILED the server did not answer")
        elif reply.was_duplicate:
            print(f"OK value={reply.new_value} (duplicate: yes)")
        else:
            print(f"OK value={reply.new_value} (duplicate: no)")
    else:
        reply = client.get(args.counter_id)
        if reply is None:
            print("FAILED the server did not answer")
        elif reply.found:
            print(f"value={reply.value}")
        else:
            print("not found")


if __name__ == "__main__":
    main()