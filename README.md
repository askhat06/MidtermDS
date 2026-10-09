# Replicated Counter Service

A gRPC counter service with idempotent increments (Part A), Lamport clocks (Part B)
and three replicas with majority-acknowledged writes (Part C).

All commands are run from the project folder.
The commands below are written for Windows. On Linux and macOS use `python3` instead of `python`.

## 1. Setup

```
python -m venv .venv
.venv\Scripts\activate
pip install grpcio grpcio-tools pytest
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

On Linux and macOS the second command is `source .venv/bin/activate`.
The last command generates `counter_pb2.py` and `counter_pb2_grpc.py`. They are never edited by hand.

## 2. Start one replica

Terminal 1:

```
python server.py --port 50051
```

Terminal 2:

```
python client.py incr likes:post-42 --by 5
python client.py get likes:post-42
```

The last command prints `value=5`.

## 3. Start three replicas

Three terminals, one command in each:

```
python server.py --port 50051 --name replica-A
python server.py --port 50052 --name replica-B
python server.py --port 50053 --name replica-C
```

Client terminal:

```
python client.py incr likes:post-42 --by 5 --replicas 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053
python client.py incr likes:post-42 --key demo --replicas 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053
python client.py incr likes:post-42 --key demo --replicas 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053
python client.py get likes:post-42 --replicas 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053
```

Expected last lines:

```
OK committed value=5 (replicas acked: 3/3, duplicate: no)
OK committed value=6 (replicas acked: 3/3, duplicate: no)
OK committed value=6 (replicas acked: 3/3, duplicate: yes)
value=6
```

Stop one replica with Ctrl+C: a write still commits with `2/3`.
Stop a second one: the client prints `FAILED not committed (replicas acked: 1/3)`.

## 4. Run the test suite

```
pytest -v
```

The tests start their own servers on free ports, so nothing has to be running.
`tests/test_counter.py` holds the Task C2 tests, `tests/test_failures.py` the Task C3 failure-injection tests.

## 5. Run the benchmark

```
python tests/perf_benchmark.py
```

It starts three replicas inside the script and prints the latency table for the four configurations.

## 6. Capture the Lamport trace (Task B2)

Terminal 1:

```
python server.py --port 50051 --name replica-A --log-file logs/replica-A.log
```

Terminal 2:

```
python client.py incr x --repeat 2 --name client-1 --log-file logs/client-1.log
python client.py incr y --repeat 2 --name client-2 --log-file logs/client-2.log
python client.py get x --repeat 3 --name client-3 --log-file logs/client-3.log
```

Each process writes its own log file into `logs/`. Delete the old files before a new run, because the lines are appended.

## 7. Fault injection

```
python server.py --port 50051 --fault delay --delay-ms 3000
```

With `--fault delay` the server applies the first Increment and then waits `--delay-ms` milliseconds before it answers.
The client deadline is 2 seconds, so the first attempt times out and the retry is answered from the deduplication store.

## Files

| File | Purpose |
|---|---|
| `counter.proto` | interface definition |
| `counter_pb2.py`, `counter_pb2_grpc.py` | generated stubs |
| `server.py` | one replica; flags `--port`, `--name`, `--fault`, `--delay-ms`, `--log-file` |
| `client.py` | `incr` and `get` commands, retries, quorum write |
| `clocks.py` | Lamport clock and event log |
| `tests/test_counter.py` | Task C2 tests |
| `tests/test_failures.py` | Task C3 failure-injection tests |
| `tests/perf_benchmark.py` | Task C4 measurement script |
| `logs/` | captured event logs |
| `report.pdf` | test report |