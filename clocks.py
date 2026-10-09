"""Lamport clock and event log (Task B1).

Every process (each client and each replica) has ONE clock and ONE log.
"""
import os
import threading

class LamportClock:

    def __init__(self):
        self.time = 0
        self.lock = threading.Lock()

    def tick(self):
        """Rule 1: before a local event (send, apply) the clock grows by 1."""
        with self.lock:
            self.time = self.time + 1
            return self.time

    def update(self, received_time):
        """Rule 2: when a message arrives: max(own time, received time) + 1."""
        with self.lock:
            self.time = max(self.time, received_time) + 1
            return self.time


class EventLog:
    """Prints one line for every event. Can also save the lines to a file."""

    def __init__(self, node, log_file=None, enabled=True):
        self.node = node
        self.log_file = log_file
        self.enabled = enabled
        self.lock = threading.Lock()
        if log_file is not None:
            folder = os.path.dirname(log_file)
            if folder != "":
                os.makedirs(folder, exist_ok=True)

    def write(self, event, text, time, received_time=None):
        if not self.enabled:
            return

        start = "[" + self.node + "] " + event
        line = start.ljust(21) + " " + text.ljust(50) + " L=" + str(time)
        if received_time is not None:
            line = line + "   (received L=" + str(received_time) + ")"
        with self.lock:
            print(line, flush=True)
            if self.log_file is not None:
                with open(self.log_file, "a", encoding="utf-8") as file:
                    file.write(line + "\n")