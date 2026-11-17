"""Broker lab model shared by the desktop controls and presentation."""
from collections import deque
import itertools
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time

from demo import Demo, NAMES
from matchingEngine import MatchingEngine
from partitioner import partition_for

LINKS = ("AB", "AC", "BC")
CLIENTS = ("Atlas", "Birch", "Cedar")
SYMBOLS = ("AAPL", "GOOG", "TSLA")
STEPS = [
    "Submit Atlas’s order to the router FIFO.",
    "Dispatch the oldest queued order and watch it reach two brokers.",
    "Let matching engine 0 pull AAPL and rest the bid in its book.",
    "Block A–C. Ask B to check whether C is still reachable.",
    "Crash leader C after replication, just before the reply.",
    "Retry through the surviving pair and inspect the original offset.",
    "Let engine 0 pull the sell, match three shares and publish both fills.",
    "Have the client router read order-results and return both fills.",
    "Restart C. Compare what A and B know about it.",
    "Exchange views, replay the old suspicion, and recover C’s log.",
]


class Lab:
    def __init__(self):
        self.serial = 0
        self.disk = None
        self.reset("guided")

    def close(self):
        if self.disk:
            for name in list(self.cluster.nodes):
                self.cluster.stop(name)
            self.disk.cleanup()

    def reset(self, mode):
        self.close()
        self.disk = TemporaryDirectory()
        self.cluster = Demo(Path(self.disk.name))
        self.mode, self.step, self.clock = mode, 0, 0
        self.streaming = mode == "live"
        self.enabled = dict.fromkeys(CLIENTS, True)
        self.blocked, self.waiting = set(), deque()
        self.in_flight = None
        self.awaiting_ack = False
        self.events, self.recent = deque(maxlen=180), deque(maxlen=60)
        self.workers = [MatchingEngine(i) for i in range(3)]
        self.ends = [0, 0, 0]
        self.result_cursor = self.results_end = self.records = 0
        self.returns = deque(maxlen=120)
        self.requests = {}
        self.total, self.sequence, self.leader = 0, 0, "C"
        for name in NAMES:
            self.cluster.start(name)
        for pair in ("AB", "BC", "AB"):
            self.cluster.gossip(*pair)
        self.event("System", "", "", "Three broker processes are online.")

    def event(self, kind, src, dst, message, detail=""):
        self.serial += 1
        self.events.append(dict(id=self.serial, kind=kind, src=src, dst=dst,
                                message=message, detail=detail, time=time.strftime("%H:%M:%S")))

    def reachable(self, a, b):
        return (a in self.cluster.nodes and b in self.cluster.nodes
                and "".join(sorted((a, b))) not in self.blocked)

    def sync(self, src, dst):
        start = self.cluster.call(dst, "count")
        end = self.cluster.call(src, "count")
        if start < end:
            for batch in self.cluster.call(src, "since", start):
                self.cluster.call(dst, "append", *batch)
            self.event("Replica", src, dst, f"Recovered offsets {start}–{end - 1}.")

    def exchange(self, a, b):
        if self.reachable(a, b):
            self.cluster.gossip(a, b)
            self.event("Gossip", a, b, "Exchanged membership views.")
            self.sync(a, b)
            self.sync(b, a)

    def probe(self, a, b):
        if a not in self.cluster.nodes:
            return
        if self.reachable(a, b):
            self.exchange(a, b)
            return
        self.event("Fault", a, b, "Direct probe timed out.")
        helper = next(n for n in NAMES if n not in (a, b))
        if self.reachable(a, helper):
            self.event("Gossip", a, helper, f"Can you reach broker {b}?")
            if self.reachable(helper, b):
                self.event("Gossip", helper, b, "Indirect probe succeeded.")
                self.cluster.gossip(helper, b)
                self.exchange(helper, a)
                return
        version = self.cluster.call(a, "view").get(b, (0, False))[0]
        self.cluster.call(a, "merge", {b: (version, True)})
        self.event("Gossip", a, "", f"{a} suspects {b}.", "Both probe paths failed.")

    def order(self, client, side, quantity, symbol, price=10000):
        self.sequence += 1
        batch = f"ord-{self.sequence:04}"
        self.requests[batch] = dict(topic="orders", partition=partition_for(symbol), id=batch,
                                    client=client, side=side, quantity=quantity, symbol=symbol, price=price)
        self.waiting.append((batch, f"{side} {quantity} {symbol} @ {price / 100:.2f}", client))
        self.event("Order", client, "Router", f"{side} {quantity} {symbol}", batch)
        return batch

    def deliver(self, lose_reply=False, defer_ack=False):
        if self.awaiting_ack:
            self.acknowledge()
            return
        if not self.waiting and self.in_flight is None:
            return
        order = self.in_flight or self.waiting[0]
        batch, payload, client = order
        record = self.requests[batch]
        receipt = self.append_record(batch, record, "Router")
        if receipt is None:
            self.event("Fault", "Router", "", f"{batch} waiting for quorum.")
            return
        leader, fresh = receipt
        if self.in_flight is None:
            self.in_flight = self.waiting.popleft()
        part = record["partition"]
        if fresh:
            offset = self.ends[part]
            self.ends[part] += 1
            self.total += 1
            self.recent.append(dict(offset=f"P{part}:{offset}", batch=batch, payload=payload, client=client))
            record_offset = offset
        else:
            # The router has one in-flight order; a retry precedes later orders.
            record_offset = self.ends[part] - 1
        offset = f"P{part}:{record_offset}"
        if lose_reply:
            self.toggle_node(leader)
            self.event("Fault", "Router", "", "Reply lost. Retry pending.", batch)
        else:
            self.awaiting_ack = True
            self.event("Ack", leader, "Router", f"Committed offset {offset}.", batch)
            if not defer_ack:
                self.acknowledge()

    def append_record(self, batch, record, producer):
        for leader in dict.fromkeys([self.leader, *NAMES]):
            peers = [n for n in NAMES if n != leader and self.reachable(leader, n)]
            if peers and self.cluster.call(leader, "count") == self.records:
                break
        else:
            return None
        follower = peers[0]
        self.sync(leader, follower)
        index = self.cluster.publish(leader, follower, batch, json.dumps(record, sort_keys=True))
        fresh = index == self.records
        if fresh:
            self.records += 1
        self.leader = leader
        topic = "Orders" if record["topic"] == "orders" else "Results"
        self.event("Order", producer, topic, f"Publish {batch} to {record['topic']}.")
        if topic == "Orders":
            self.event("Order", topic, f"P{record['partition']}", f"Partition by {record['symbol']}.")
        self.event("Replica", leader, follower, f"Stored {batch} on {leader} and {follower}.")
        return leader, fresh

    def fetch(self, topic, partition, offset):
        for broker in self.cluster.nodes:
            if self.cluster.call(broker, "count") == self.records:
                return self.cluster.call(broker, "fetch", topic, partition, offset)
        return None

    def consume(self, worker):
        if not worker.enabled or worker.offset >= self.ends[worker.partition]:
            return
        name = f"W{worker.partition}"
        if worker.pending is None:
            record = self.fetch("orders", worker.partition, worker.offset)
            if record is None:
                return
            self.event("Gossip", name, f"P{worker.partition}", f"Fetch offset {worker.offset}.")
            self.event("Order", f"P{worker.partition}", name, f"Read {record['id']} ({record['symbol']}).")
            worker.pending = worker.match(record)
        if worker.pending:
            batch = f"fill-{worker.partition}-{worker.offset}"
            result = dict(topic="order-results", partition=0, id=batch, fills=worker.pending)
            receipt = self.append_record(batch, result, name)
            if receipt is None:
                return
            if receipt[1]:
                self.results_end += 1
        worker.pending = None
        worker.offset += 1

    def route_results(self):
        if self.result_cursor >= self.results_end:
            return
        record = self.fetch("order-results", 0, self.result_cursor)
        if record:
            self.event("Ack", "Results", "Router", f"Read results offset {self.result_cursor}.")
            for fill in record["fills"]:
                self.returns.append(fill)
                self.event("Ack", "Router", fill["client"], f"{fill['order']} filled {fill['quantity']} {fill['symbol']} @ {fill['price']/100:.2f}.")
            self.result_cursor += 1

    def acknowledge(self):
        batch, _, client = self.in_flight
        self.event("Ack", "Router", client, f"{batch} acknowledged.")
        del self.requests[batch]
        self.in_flight = None
        self.awaiting_ack = False

    def toggle_node(self, n):
        if n in self.cluster.nodes:
            self.cluster.stop(n)
            message = "Broker process stopped."
        else:
            self.cluster.start(n)
            message = "Broker restarted from its local disk."
        self.event("Fault", n, "", message)

    def toggle_link(self, link):
        self.blocked.symmetric_difference_update([link])
        self.event("Fault", *link, "Link blocked." if link in self.blocked else "Link restored.")

    def next_step(self):
        if self.step == 0:
            self.order("Atlas", "BUY", 10, "AAPL")
        elif self.step == 1:
            self.deliver()
        elif self.step == 2:
            self.consume(self.workers[0])
        elif self.step == 3:
            self.toggle_link("AC")
            self.probe("A", "C")
        elif self.step == 4:
            self.order("Birch", "SELL", 3, "AAPL")
            self.deliver(lose_reply=True)
            self.probe("A", "C")
        elif self.step == 5:
            self.deliver()
            self.order("Cedar", "BUY", 5, "TSLA")
            self.deliver()
        elif self.step == 6:
            self.consume(self.workers[0])
        elif self.step == 7:
            self.route_results()
        elif self.step == 8:
            self.toggle_node("C")
            self.cluster.gossip("B", "C")
            self.event("Gossip", "C", "B", "C announced incarnation 2.", "Inspect A and B to compare their views.")
        elif self.step == 9:
            self.exchange("A", "B")
            view = self.cluster.call("A", "merge", {"C": (1, True)})
            self.event("Gossip", "B", "A", "Delayed suspicion discarded.", f"A sees C at incarnation {view['C'][0]}.")
            self.exchange("B", "C")
            self.toggle_link("AC")
        self.step = min(len(STEPS), self.step + 1)

    def tick(self):
        if self.mode != "live":
            return
        self.clock += 1
        if self.clock % 2 == 0:
            a, b = list(itertools.permutations(NAMES, 2))[(self.clock // 2 - 1) % 6]
            self.probe(a, b)
        self.route_results()
        for worker in self.workers:
            self.consume(worker)
        self.deliver(defer_ack=True)
        if self.streaming and self.clock % 8 == 1:
            for i, client in enumerate(CLIENTS):
                if self.enabled[client] and len(self.waiting) < 60:
                    sequence = self.sequence
                    self.order(client, ("BUY", "SELL")[sequence % 2],
                               1 + sequence % 20, SYMBOLS[i])

    def state(self):
        brokers = {}
        for n in NAMES:
            brokers[n] = (dict(view=self.cluster.call(n, "view"), count=self.cluster.call(n, "count"))
                          if n in self.cluster.nodes else None)
        return dict(mode=self.mode, step=self.step, steps=STEPS, brokers=brokers, leader=self.leader,
                    clients=self.enabled, blocked=sorted(self.blocked), streaming=self.streaming,
                    total=self.total, pending=len(self.waiting), orders=list(self.recent), events=list(self.events))

    def action(self, data):
        op, value = data.get("op"), data.get("value")
        if op == "reset" and value in ("live", "guided"):
            self.reset(value)
        elif op == "step" and self.mode == "guided":
            self.next_step()
        elif self.mode != "live":
            raise ValueError("Switch to live mode to change the cluster.")
        elif op == "node" and value in NAMES:
            self.toggle_node(value)
        elif op == "link" and value in LINKS:
            self.toggle_link(value)
        elif op == "stream":
            self.streaming = not self.streaming
        elif op == "client" and value in CLIENTS:
            self.enabled[value] = not self.enabled[value]
        elif op == "send" and data.get("client") in CLIENTS and data.get("side") in ("BUY", "SELL"):
            quantity = int(data["quantity"])
            symbol = data["symbol"].strip().upper()
            if not (0 < quantity <= 10000 and symbol.isascii() and symbol.isalpha() and len(symbol) <= 8):
                raise ValueError("Use 1–10,000 shares and a symbol of 1–8 letters.")
            price = int(data.get("price", 10000))
            if not 1 <= price <= 100000000:
                raise ValueError("Enter a positive limit price in cents (up to 100,000,000).")
            self.order(data["client"], data["side"], quantity, symbol, price)
        else:
            raise ValueError("Unknown control.")
