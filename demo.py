"""Run with python3 demo.py. The driver controls message delivery and failure timing."""

import json
import multiprocessing as mp
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

NAMES = ("A", "B", "C")


def merge(view, updates):
    for peer, report in updates.items():
        # Compare incarnation first. Within one incarnation, suspicion wins.
        if report > view.get(peer, (0, False)):
            view[peer] = report


def broker(name, directory, pipe):
    db = sqlite3.connect(directory / f"{name}.sqlite")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS boot (number INTEGER);
        CREATE TABLE IF NOT EXISTS orders (batch TEXT UNIQUE, payload TEXT);
        INSERT INTO boot SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM boot);
        UPDATE boot SET number = number + 1;
    """)
    incarnation = db.execute("SELECT number FROM boot").fetchone()[0]
    view = {name: (incarnation, False)}
    try:
        while True:
            command, *args = pipe.recv()
            if command == "append":
                batch, order = args
                with db:
                    db.execute("INSERT OR IGNORE INTO orders VALUES (?, ?)", (batch, order))
                row, payload = db.execute(
                    "SELECT rowid, payload FROM orders WHERE batch = ?", (batch,)
                ).fetchone()
                if payload != order:
                    raise ValueError("Batch ID reused for a different order")
                reply = row - 1  # Public offsets start at zero.
            elif command == "replay":
                reply = db.execute("SELECT batch, payload FROM orders ORDER BY rowid").fetchall()
            elif command == "fetch":
                topic, partition, offset = args
                row = db.execute(
                    "SELECT payload FROM orders WHERE json_valid(payload) "
                    "AND json_extract(payload, '$.topic') = ? "
                    "AND json_extract(payload, '$.partition') = ? "
                    "ORDER BY rowid LIMIT 1 OFFSET ?", (topic, partition, offset),
                ).fetchone()
                reply = json.loads(row[0]) if row else None
            elif command == "count":
                reply = db.execute("SELECT count(*) FROM orders").fetchone()[0]
            elif command == "since":
                reply = db.execute(
                    "SELECT batch, payload FROM orders WHERE rowid > ? ORDER BY rowid",
                    (args[0],),
                ).fetchall()
            elif command == "view":
                reply = view
            elif command == "merge":
                reported = args[0].get(name)
                if reported and reported >= (incarnation, True):
                    incarnation = reported[0] + 1
                    with db:
                        db.execute("UPDATE boot SET number = ?", (incarnation,))
                    view[name] = (incarnation, False)
                merge(view, args[0])
                reply = view
            else:
                raise ValueError(command)
            pipe.send(reply)
    except EOFError:
        pass
    finally:
        db.close()
        pipe.close()


class Demo:
    def __init__(self, directory):
        self.directory = directory
        self.nodes = {}

    def start(self, name):
        client, server = mp.Pipe()
        process = mp.Process(target=broker, args=(name, self.directory, server))
        process.start()
        server.close()
        self.nodes[name] = (process, client)

    def stop(self, name):
        process, pipe = self.nodes.pop(name)
        process.kill()
        process.join()
        pipe.close()

    def call(self, name, command, *args):
        _, pipe = self.nodes[name]
        pipe.send((command, *args))
        if not pipe.poll(5):
            raise TimeoutError(f"{name} timed out on {command}")
        return pipe.recv()

    def ping(self, sender, target, blocked):
        if (sender, target) in blocked or target not in self.nodes:
            return False
        return target in self.call(target, "view")

    def probe_c(self, blocked):
        if self.ping("A", "C", blocked):
            return True
        return self.ping("A", "B", blocked) and self.ping("B", "C", blocked)

    def gossip(self, left, right):
        left_view = self.call(left, "view")
        right_view = self.call(right, "view")
        self.call(left, "merge", right_view)
        self.call(right, "merge", left_view)

    def publish(self, leader, follower, batch, order, lose_reply=False):
        # The driver schedules one leader and one follower for each majority write.
        offset = self.call(leader, "append", batch, order)
        replica_offset = self.call(follower, "append", batch, order)
        assert offset == replica_offset
        if lose_reply:
            self.stop(leader)
            raise TimeoutError("C crashed after two disk writes, before the client reply")
        return offset


def run(cluster):
    for name in NAMES:
        cluster.start(name)
    cluster.gossip("A", "B")
    cluster.gossip("B", "C")
    cluster.gossip("A", "B")

    blocked = {("A", "C")}
    reachable = cluster.probe_c(blocked)
    assert reachable
    print("Probe: A cannot ping C directly. B reaches C, so A keeps C alive.")

    first = ("batch-1", "BUY 10 AAPL")
    try:
        cluster.publish("C", "A", *first, lose_reply=True)
    except TimeoutError as error:
        print(f"1. {error}.")

    # Both probe paths now fail. The router consults A's local view.
    if cluster.probe_c(blocked):
        raise RuntimeError("Expected C to be offline")
    old_report = {"C": (1, True)}
    view = cluster.call("A", "merge", old_report)
    target = next(name for name in NAMES if not view[name][1])
    offset = cluster.publish(target, "B", *first)
    assert offset == 0
    print(f"2. Router retries batch-1 through {target}. Original offset: {offset}.")

    second = ("batch-2", "SELL 3 MSFT")
    second_offset = cluster.publish("A", "B", *second)
    assert second_offset == 1
    cluster.start("C")
    cluster.gossip("B", "C")
    a = cluster.call("A", "view")["C"]
    b = cluster.call("B", "view")["C"]
    assert a == (1, True) and b == (2, False)
    print("3. A still suspects C at incarnation 1. B has heard C is alive at incarnation 2.")

    cluster.gossip("A", "B")
    latest = cluster.call("A", "merge", old_report)
    assert latest["C"] == (2, False)
    print("4. A learns from B. A delayed suspicion about incarnation 1 is ignored.")

    history = cluster.call("A", "replay")
    recovered = cluster.call("C", "replay")
    assert recovered == [first] and history == [first, second]
    for batch in history[len(recovered):]:
        cluster.call("C", "append", *batch)
    assert all(cluster.call(name, "replay") == history for name in NAMES)
    print("5. C catches up. Audit replay on all three brokers:")
    for offset, (batch, order) in enumerate(history):
        print(f"   offset {offset} | {batch} | {order}")


if __name__ == "__main__":
    with TemporaryDirectory() as temp:
        cluster = Demo(Path(temp))
        try:
            run(cluster)
        finally:
            for name in list(cluster.nodes):
                cluster.stop(name)
