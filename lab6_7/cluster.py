import atexit
import os
import socket
import subprocess
import sys
import time

from client import KVClient
from netsim import Network
from server import DEFAULT_PORTS

LAB_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(LAB_DIR, "server.py")
NODE_IDS = ["A", "B", "C"]


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def port_in_use(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def default_ports_if_free():
    """50051-50053 as in the handout, or free ports if something already listens there."""
    if any(port_in_use(port) for port in DEFAULT_PORTS.values()):
        return None
    return dict(DEFAULT_PORTS)


class Cluster:
    """Three replicas, each in its own process (python server.py ...).

    Clients talk to the replicas directly. Replica-to-replica traffic goes through the
    link proxies of netsim.Network, so the test can cut the network between them.
    """

    def __init__(self, nodes=NODE_IDS, ports=None, leaseholder="A", merge="lww",
                 sync_interval_ms=100, max_staleness_ms=5000, link_delay_ms=0,
                 clock_skew_ms=None, log_dir=None, quiet=False):
        self.nodes = list(nodes)
        if ports is None:
            ports = {node: free_port() for node in self.nodes}
        self.ports = dict(ports)
        self.leaseholder = leaseholder
        self.merge = merge
        self.sync_interval_ms = sync_interval_ms
        self.max_staleness_ms = max_staleness_ms
        self.link_delay_ms = link_delay_ms
        self.clock_skew_ms = dict(clock_skew_ms or {})
        self.log_dir = log_dir or os.path.join(LAB_DIR, "logs", "cluster")
        self.quiet = quiet
        self.processes = {}
        self.log_files = {}
        self.net = None
        self._client = None
        self._stopped = False

    # ----- start and stop ----------------------------------------------------------

    def start(self, timeout=30):
        os.makedirs(self.log_dir, exist_ok=True)
        self.net = Network(self.ports, self.link_delay_ms)
        atexit.register(self.stop)
        for node in self.nodes:
            self.spawn(node, recover=False)
        self.wait_ready(timeout)
        return self

    def spawn(self, node, recover):
        command = [sys.executable, "-u", SERVER,
                   "--id", node,
                   "--port", str(self.ports[node]),
                   "--peers", self.net.peers_of(node),
                   "--leaseholder", self.leaseholder,
                   "--merge", self.merge,
                   "--sync-interval-ms", str(self.sync_interval_ms),
                   "--max-staleness-ms", str(self.max_staleness_ms),
                   "--exit-with-parent"]
        skew = self.clock_skew_ms.get(node, 0)
        if skew != 0:
            command += ["--clock-skew-ms", str(skew)]
        if recover:
            command.append("--recover")
        if self.quiet:
            command.append("--quiet")
        log = open(self.log_path(node), "a", encoding="utf-8")
        environment = dict(os.environ, PYTHONIOENCODING="utf-8")
        flags = 0
        if os.name == "nt":
            flags = subprocess.CREATE_NEW_PROCESS_GROUP  # Ctrl+C in the console must not kill the nodes
        self.processes[node] = subprocess.Popen(command, cwd=LAB_DIR, stdin=subprocess.PIPE, stdout=log,
                                                stderr=subprocess.STDOUT, env=environment,
                                                creationflags=flags)
        self.log_files[node] = log

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        for node in list(self.processes):
            self.kill_process(node)
        if self._client is not None:
            self._client.close()
        if self.net is not None:
            self.net.close()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()

    def kill_process(self, node):
        process = self.processes.pop(node, None)
        if process is not None:
            process.kill()
            process.wait()
            if process.stdin is not None:
                process.stdin.close()
        log = self.log_files.pop(node, None)
        if log is not None:
            log.close()

    # ----- information -----------------------------------------------------------

    def log_path(self, node):
        return os.path.join(self.log_dir, f"{node}.log")

    def addresses(self):
        return {node: f"127.0.0.1:{self.ports[node]}" for node in self.nodes}

    def client(self):
        """A client connected to every replica directly. It does not retry."""
        if self._client is None:
            self._client = KVClient(self.addresses(), timeout=3.0, retries=0)
        return self._client

    def describe(self):
        lines = []
        for node in self.nodes:
            if node == self.leaseholder:
                role = "leaseholder for [a-z]"
            else:
                role = "follower"
            lines.append(f"  {node}  127.0.0.1:{self.ports[node]:<6} {role}")
        return "\n".join(lines)

    def is_running(self, node):
        process = self.processes.get(node)
        return process is not None and process.poll() is None

    def health_problems(self):
        """Everything that is not healthy yet; an empty list means the cluster is fine."""
        problems = []
        client = self.client()
        lag_limit = 3 * self.sync_interval_ms + 300
        for node in self.nodes:
            if not self.is_running(node):
                problems.append(f"{node} is not running")
                continue
            status = client.status(node)
            if status is None:
                problems.append(f"{node} does not answer")
                continue
            missing = set(self.nodes) - {node} - set(status.reachable_peers)
            if missing:
                problems.append(f"{node} cannot reach {', '.join(sorted(missing))}")
            if status.recovering:
                problems.append(f"{node} is recovering")
            if status.role == "follower" and not 0 <= status.lag_ms <= lag_limit:
                problems.append(f"{node} lags {status.lag_ms} ms")
        return problems

    def wait_ready(self, timeout=30):
        """Wait until every replica answers, every link works and the followers are in sync."""
        deadline = time.monotonic() + timeout
        while True:
            problems = self.health_problems()
            if not problems:
                return
            if time.monotonic() > deadline:
                raise RuntimeError("cluster is not healthy: " + "; ".join(problems))
            time.sleep(0.05)

    def wait_for(self, condition, timeout=10, what="condition"):
        deadline = time.monotonic() + timeout
        while True:
            try:
                if condition():
                    return
            except Exception:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"timed out after {timeout} s waiting for {what}")
            time.sleep(0.05)

    def local_values(self, key):
        """node -> value that replica holds (None if missing, "down" if it does not answer)."""
        values = {}
        for node, version in self.client().inspect(key).items():
            if version is None or version == "down":
                values[node] = version
            else:
                values[node] = version.value
        return values

    def wait_converged(self, key, value=None, timeout=10):
        """Wait until every running replica holds the same value (and `value`, if given)."""
        def converged():
            values = [v for node, v in self.local_values(key).items() if self.is_running(node)]
            if len(set(values)) != 1 or values[0] is None:
                return False
            return value is None or values[0] == value
        self.wait_for(converged, timeout, f"all replicas to agree on {key!r}")
        return self.local_values(key)[self.nodes[0]]

    # ----- failure injection -------------------------------------------------------

    def partition(self, *groups):
        self.net.partition(*groups)

    def isolate(self, node):
        self.net.isolate(node)

    def heal(self):
        self.net.heal()

    def crash(self, node):
        """Kill the replica process. It loses everything it kept in memory."""
        self.kill_process(node)

    def restart(self, node, timeout=30):
        """Start a crashed replica again with an empty store."""
        if self.is_running(node):
            return
        with open(self.log_path(node), "a", encoding="utf-8") as log:
            log.write("----- restarted by the test harness -----\n")
        self.spawn(node, recover=(node == self.leaseholder))
        self.wait_for(lambda: self.client().status(node) is not None, timeout, f"{node} to start")
