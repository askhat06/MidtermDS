import socket
import threading
import time


def close_quietly(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


class LinkProxy:
    """One direction of the network between two replicas, e.g. A -> B.

    Replica A does not connect to B directly. It connects to this proxy, which forwards
    the bytes to B. cut() closes every open connection and refuses new ones, so A sees
    exactly what a real network partition looks like: B is unreachable. restore() lets
    traffic through again. delay_ms adds a fixed delay to every chunk of data (WAN link).
    """

    def __init__(self, name, target_port, delay_ms=0):
        self.name = name
        self.target = ("127.0.0.1", target_port)
        self.delay = delay_ms / 1000
        self.blocked = False
        self.closed = False
        self.lock = threading.Lock()
        self.connections = set()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(64)
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self.accept_loop, daemon=True).start()

    def accept_loop(self):
        while not self.closed:
            try:
                incoming, _ = self.listener.accept()
            except OSError:
                return
            if self.blocked:
                close_quietly(incoming)
                continue
            try:
                outgoing = socket.create_connection(self.target, timeout=2)
                outgoing.settimeout(None)
            except OSError:
                close_quietly(incoming)  # the target replica is down
                continue
            for sock in (incoming, outgoing):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            pair = (incoming, outgoing)
            with self.lock:
                if self.blocked:
                    close_quietly(incoming)
                    close_quietly(outgoing)
                    continue
                self.connections.add(pair)
            threading.Thread(target=self.pump, args=(incoming, outgoing, pair), daemon=True).start()
            threading.Thread(target=self.pump, args=(outgoing, incoming, pair), daemon=True).start()

    def pump(self, source, target, pair):
        try:
            while True:
                data = source.recv(65536)
                if not data:
                    break
                if self.delay > 0:
                    time.sleep(self.delay)
                if self.blocked:
                    break
                target.sendall(data)
        except OSError:
            pass
        self.drop(pair)

    def drop(self, pair):
        with self.lock:
            if pair not in self.connections:
                return
            self.connections.discard(pair)
        for sock in pair:
            close_quietly(sock)

    def cut(self):
        with self.lock:
            self.blocked = True
            pairs = list(self.connections)
            self.connections.clear()
        for pair in pairs:
            for sock in pair:
                close_quietly(sock)

    def restore(self):
        self.blocked = False

    def close(self):
        self.closed = True
        self.cut()
        close_quietly(self.listener)


class Network:
    """All links between the replicas: one LinkProxy for every ordered pair of nodes."""

    def __init__(self, node_ports, delay_ms=0):
        self.nodes = list(node_ports)
        self.links = {}
        for source in self.nodes:
            for target in self.nodes:
                if source != target:
                    name = f"{source}->{target}"
                    self.links[(source, target)] = LinkProxy(name, node_ports[target], delay_ms)
        self.groups = [list(self.nodes)]

    def address(self, source, target):
        """Address that `source` must use to reach `target`."""
        return f"127.0.0.1:{self.links[(source, target)].port}"

    def peers_of(self, source):
        """The --peers argument for the server process of `source`."""
        parts = []
        for target in self.nodes:
            if target != source:
                parts.append(f"{target}={self.address(source, target)}")
        return ",".join(parts)

    def partition(self, *groups):
        """partition(["A", "B"], ["C"]): links inside a group work, links between groups are cut."""
        groups = [list(group) for group in groups]
        listed = [node for group in groups for node in group]
        for node in self.nodes:
            if node not in listed:
                groups.append([node])  # a node that is not listed ends up alone
        for (source, target), link in self.links.items():
            together = any(source in group and target in group for group in groups)
            if together:
                link.restore()
            else:
                link.cut()
        self.groups = groups

    def isolate(self, node):
        others = [other for other in self.nodes if other != node]
        self.partition([node], others)

    def heal(self):
        for link in self.links.values():
            link.restore()
        self.groups = [list(self.nodes)]

    def describe(self):
        if len(self.groups) == 1:
            return "healthy: " + ", ".join(self.nodes) + " all connected"
        parts = ["{" + ", ".join(group) + "}" for group in self.groups]
        return "partitioned: " + " | ".join(parts)

    def close(self):
        for link in self.links.values():
            link.close()
