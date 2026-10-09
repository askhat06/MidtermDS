import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):

    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}

    def Increment(self, request, context):
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

        print(f"Increment {request.counter_id} by {request.delta} -> {new_value} "
              f"(duplicate: {was_duplicate})", flush=True)
        return counter_pb2.IncrementReply(new_value=new_value, was_duplicate=was_duplicate)

    def Get(self, request, context):
        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)
        return counter_pb2.GetReply(value=value, found=found)


def start_server(port):
    servicer = CounterServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    real_port = server.add_insecure_port("127.0.0.1:" + str(port))
    server.start()
    return server, real_port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    server, port = start_server(args.port)
    print(f"Server is listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()