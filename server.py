"""Counter server (Parts A and B).

Run:  python server.py --port 50051 --name replica-A
"""
import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import EventLog, LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):

    def __init__(self, name="replica-A", log_file=None, log_enabled=True):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self._clock = LamportClock()
        self._log = EventLog(name, log_file, log_enabled)

    def Increment(self, request, context):
        now = self._clock.update(request.lamport_time)
        text = f"Increment(counter={request.counter_id}, delta={request.delta})"
        self._log.write("RECV", text, now, request.lamport_time)

        with self._lock:
            if request.idempotency_key in self._seen:
                saved_counter_id, saved_value = self._seen[request.idempotency_key]
                new_value = saved_value
                was_duplicate = True
            else:
                new_value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = new_value
                self._seen[request.idempotency_key] = (request.counter_id, new_value)
                was_duplicate = False
                now = self._clock.tick()
                self._log.write("APPLY", f"counter={request.counter_id} -> {new_value}", now)

        now = self._clock.tick()
        self._log.write("SEND", f"IncrementReply(new_value={new_value})", now)
        return counter_pb2.IncrementReply(
            new_value=new_value, was_duplicate=was_duplicate, lamport_time=now)

    def Get(self, request, context):
        now = self._clock.update(request.lamport_time)
        self._log.write("RECV", f"Get(counter={request.counter_id})", now, request.lamport_time)

        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)

        now = self._clock.tick()
        self._log.write("SEND", f"GetReply(value={value}, found={found})", now)
        return counter_pb2.GetReply(value=value, found=found, lamport_time=now)


def start_server(port, name="replica-A", log_file=None, log_enabled=True):
    """Start the server and return (server, port).

    Port 0 means: the operating system picks a free port. The tests use this.
    """
    servicer = CounterServicer(name, log_file, log_enabled)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    real_port = server.add_insecure_port("127.0.0.1:" + str(port))
    server.start()
    return server, real_port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    parser.add_argument("--log-file", default=None)
    args = parser.parse_args()

    server, port = start_server(args.port, args.name, args.log_file)
    print(f"[{args.name}] is listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()