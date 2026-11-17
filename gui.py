"""Start the native broker lab with python3 gui.py."""
import time
import tkinter as tk
from tkinter import messagebox, ttk

from lab import CLIENTS, LINKS, NAMES, STEPS, Lab

NETWORK = dict(Atlas=(.12, .19), Birch=(.12, .5), Cedar=(.12, .81),
              Router=(.37, .5), A=(.62, .19), B=(.62, .81), C=(.89, .5))
FLOW = dict(Atlas=(.20,.09), Birch=(.50,.09), Cedar=(.80,.09),
            Router=(.50,.26), Orders=(.50,.43), Results=(.50,.92))
for i, x in enumerate((.20,.50,.80)):
    FLOW[f"P{i}"] = (x,.59)
    FLOW[f"W{i}"] = (x,.76)
PARTS = ("P0","P1","P2")
WORKERS = ("W0","W1","W2")
COLORS = dict(Order="#72c9ff", Replica="#d9abff", Gossip="#80dec2",
              Ack="#e6e999", Fault="#ff978f")


class Window:
    def __init__(self, root, lab):
        self.root, self.lab = root, lab
        self.selected = "Router"
        self.view = tk.StringVar(value="architecture")
        self.latest = 0
        self.packets = []
        self.mode = tk.StringVar(value="guided")
        self.client = tk.StringVar(value=CLIENTS[0])
        self.side = tk.StringVar(value="BUY")
        self.symbol = tk.StringVar(value="AAPL")
        self.price = tk.StringVar(value="10000")
        self.quantity = tk.StringVar(value="10")
        root.title("Akfak — Broker lab")
        root.geometry("1110x830")
        root.minsize(880, 725)
        outer = ttk.Frame(root, padding=12)
        outer.pack(fill="both", expand=True)
        header = ttk.Frame(outer)
        header.pack(fill="x")
        ttk.Label(header, text="Akfak   /   Broker lab", font=("Helvetica", 23, "bold")).pack(side="left")
        for name, value in (("Live sandbox", "live"), ("Guided demo", "guided")):
            ttk.Radiobutton(header, text=name, variable=self.mode, value=value,
                            command=self.reset).pack(side="right")
        self.caption = ttk.Label(outer, wraplength=730)
        self.caption.pack(fill="x", pady=6)
        toolbar = ttk.Frame(outer)
        toolbar.pack(fill="x")
        self.advance = ttk.Button(toolbar, command=self.advance_run)
        self.advance.pack(side="left")
        for label,value in (("Broker network","network"), ("Order flow","architecture")):
            ttk.Radiobutton(toolbar, text=label, value=value, variable=self.view,
                            command=self.switch_view).pack(side="right")
        mid = ttk.Frame(outer)
        mid.pack(fill="both", expand=True, pady=8)
        self.map = tk.Canvas(mid, bg="#142523", highlightthickness=0, width=615, height=330)
        self.map.pack(side="left", fill="both", expand=True)
        self.map.bind("<Configure>", lambda _: self.paint())
        self.map.bind("<Button-1>", self.pick)
        inspector = ttk.LabelFrame(mid, text="Inspector", padding=8, width=218)
        inspector.pack(side="right", fill="y", padx=(8, 0))
        self.info = ttk.Label(inspector, width=30, justify="left")
        self.info.pack(anchor="nw")
        self.control = ttk.Button(inspector, command=self.toggle)
        self.control.pack(anchor="w", pady=6)
        self.status = ttk.Label(outer, font=("Helvetica", 13, "bold"))
        self.status.pack(anchor="w")
        self.flight = ttk.Label(outer)
        self.flight.pack(anchor="w", pady=3)
        controls = ttk.Frame(outer)
        controls.pack(fill="x", pady=5)
        for label, variable, choices in (("Client", self.client, CLIENTS), ("Side", self.side, ("BUY", "SELL"))):
            ttk.Label(controls, text=label).pack(side="left")
            ttk.Combobox(controls, textvariable=variable, values=choices, width=7,
                         state="readonly").pack(side="left", padx=(3, 10))
        for label, variable in (("Symbol", self.symbol), ("Shares", self.quantity), ("Limit ¢", self.price)):
            ttk.Label(controls, text=label).pack(side="left")
            ttk.Entry(controls, textvariable=variable, width=7).pack(side="left", padx=(3, 10))
        self.send = ttk.Button(controls, text="Queue order", command=self.submit)
        self.send.pack(side="left")
        self.tabs = ttk.Notebook(outer)
        self.tabs.pack(fill="both", expand=True)
        self.queue = self.table("Router FIFO", ("Position", "Order ID", "Client", "Order"), (70, 100, 100, 300))
        self.messages = self.table("Messages", ("Time", "Kind", "Route", "Message"), (80, 80, 110, 450))
        self.orders = self.table("Committed log", ("Offset", "Order ID", "Client", "Order"), (70, 100, 100, 300))
        self.fills = self.table("Delivered fills", ("Client", "Order ID", "Side", "Execution", "Remaining"), (85,100,55,190,100))
        self.tabs.select(self.messages.master)
        self.messages.bind("<<TreeviewSelect>>", self.detail)
        self.note = ttk.Label(outer, text="Select a node or a link on the map to inspect it.")
        self.note.pack(anchor="w", pady=(4, 0))
        self.refresh()
        root.after(900, self.tick)
        root.after(40, self.animate)

    def table(self, title, columns, widths):
        frame = ttk.Frame(self.tabs)
        self.tabs.add(frame, text=title)
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=6)
        for column, width in zip(columns, widths):
            tree.heading(column, text=column)
            tree.column(column, width=width, minwidth=45, stretch=column == columns[-1])
        scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        tree.pack(fill="both", expand=True)
        return tree

    def reset(self):
        self.lab.reset(self.mode.get())
        self.selected = "Router"
        self.view.set("architecture")
        self.packets.clear()
        self.messages.delete(*self.messages.get_children())
        self.note.config(text="Select a node or link to inspect it.")
        self.refresh()

    def advance_run(self):
        if self.lab.mode == "guided":
            if self.lab.step == len(STEPS):
                self.reset()
                return
            current = self.lab.step
            self.lab.next_step()
            network_step = current in (3, 4, 8, 9)
            self.view.set("network" if network_step else "architecture")
            self.selected = {0:"Router",1:"Orders",2:"W0",3:"AC",4:"C",5:"P0",
                             6:"W0",7:"Results",8:"B",9:"A"}[current]
            self.packets.clear()
            if current == 7:
                self.tabs.select(self.fills.master)
            elif current == 0:
                self.tabs.select(self.queue.master)
            else:
                self.tabs.select(self.messages.master)
        else:
            self.lab.streaming = not self.lab.streaming
        self.refresh()

    def submit(self):
        try:
            self.lab.action(dict(op="send", client=self.client.get(), side=self.side.get(),
                                 symbol=self.symbol.get(), quantity=self.quantity.get(), price=self.price.get()))
        except ValueError as error:
            messagebox.showerror("Order details", str(error), parent=self.root)
            return
        self.tabs.select(self.queue.master)
        self.refresh()

    def toggle(self):
        n = self.selected
        if n == "Router":
            self.tabs.select(self.queue.master)
        elif n in WORKERS:
            worker = self.lab.workers[int(n[1])]
            worker.enabled = not worker.enabled
        elif n in PARTS or n == "Orders":
            self.tabs.select(self.orders.master)
        elif n == "Results":
            self.tabs.select(self.fills.master)
        else:
            op = "link" if n in LINKS else "node" if n in NAMES else "client"
            self.lab.action(dict(op=op, value=n))
        self.refresh()

    def tick(self):
        if self.lab.mode == "live":
            self.lab.tick()
            self.refresh()
        self.root.after(900, self.tick)

    def refresh(self):
        self.state = self.lab.state()
        s = self.state
        live = self.lab.mode == "live"
        if live:
            cue = "Each active client queues one order per burst. Watch the router drain them in arrival order."
            action = "Pause automatic orders" if self.lab.streaming else "Resume automatic orders"
        elif self.lab.step < len(STEPS):
            cue = f"Step {self.lab.step + 1} of {len(STEPS)}   {STEPS[self.lab.step]}"
            action = "Run step"
        else:
            cue, action = "Walkthrough complete. Compare the replicas and inspect the committed log.", "Replay"
        self.caption.config(text=cue)
        self.advance.config(text=action)
        self.send.config(state="normal" if live else "disabled")
        self.status.config(text=f"Router FIFO   {len(self.lab.waiting)} waiting       |       {s['total']} committed")
        current = self.lab.in_flight
        stage = "reply ready" if self.lab.awaiting_ack else "retry pending"
        self.flight.config(text=f"In flight: {current[0]}   {current[1]}   ({stage})" if current else "In flight: none")
        self.queue.delete(*self.queue.get_children())
        for i, (batch, payload, client) in enumerate(self.lab.waiting, 1):
            self.queue.insert("", "end", values=(i, batch, client, payload))
        self.orders.delete(*self.orders.get_children())
        for order in reversed(s["orders"]):
            self.orders.insert("", "end", values=tuple(order[k] for k in ("offset","batch","client","payload")))
        self.fills.delete(*self.fills.get_children())
        for f in reversed(self.lab.returns):
            self.fills.insert("", "end", values=(f["client"], f["order"], f["side"],
                f"{f['quantity']} {f['symbol']} @ {f['price']/100:.2f}", f["remaining"]))
        new = [e for e in s["events"] if e["id"] > self.latest]
        for i, e in enumerate(new):
            self.latest = e["id"]
            route = " → ".join(n for n in (e["src"], e["dst"]) if n) or "Cluster"
            self.messages.insert("", 0, iid=str(e["id"]), values=(e["time"],e["kind"],route,e["message"]))
            if e["src"] in self.points and e["dst"] in self.points:
                self.packets.append((e, time.monotonic() + i*.09))
        for old in self.messages.get_children()[180:]:
            self.messages.delete(old)
        self.inspect()
        self.paint()

    def detail(self, _):
        for item in self.messages.selection():
            event = next((e for e in self.lab.events if str(e["id"]) == item), None)
            if event:
                self.note.config(text="   ".join(filter(None,(event["message"], event["detail"]))))

    def inspect(self):
        n = self.selected
        if n == "Router":
            text = ["Client router", f"{self.lab.result_cursor} result batches routed", f"{len(self.lab.waiting)} orders waiting", "Dispatch order: oldest first"]
            action = "Inspect FIFO"
        elif n == "Orders":
            text = ["Akfak / orders", "Key: security symbol", "Partitions: 3", "Replicas: brokers A, B, C"]
            action = "Inspect order log"
        elif n == "Results":
            text = ["Akfak / order-results", f"{self.lab.results_end} fill batches committed",
                    f"{self.lab.result_cursor} read by router"]
            action = "Inspect delivered fills"
        elif n in PARTS or n in WORKERS:
            i = int(n[1])
            worker = self.lab.workers[i]
            text = [f"Orders partition {i}", f"{self.lab.ends[i]} records",
                    f"Engine {i} next offset: {worker.offset}",
                    f"Consumer lag: {self.lab.ends[i] - worker.offset}"]
            if n in WORKERS:
                text += ["Order book (oldest first)"]
                text += [f"{r['side']} {r['remaining']} {r['symbol']} @ {r['price']/100:.2f}"
                         for r in worker.resting[:5]] or ["Empty"]
                action = "Pause worker" if worker.enabled else "Resume worker"
            else:
                action = "Inspect order log"
        elif n in LINKS:
            blocked = n in self.lab.blocked
            text = ["Link " + " ↔ ".join(n), "Blocked" if blocked else "Open in both directions"]
            action = "Restore link" if blocked else "Block link"
        elif n in CLIENTS:
            enabled = self.lab.enabled[n]
            text = [n, "Automatic orders enabled" if enabled else "Automatic orders paused"]
            action = "Pause client" if enabled else "Resume client"
        else:
            info = self.state["brokers"][n]
            text = ["Broker " + n, f"{info['count']} log records on disk" if info else "Offline"]
            if info:
                text.append("Local membership")
                for peer, (version,suspect) in sorted(info["view"].items()):
                    text.append(f"  {peer}    {'suspected' if suspect else 'alive'}    v{version}")
            action = "Crash broker" if info else "Restart broker"
        self.info.config(text="\n".join(text))
        self.control.config(text=action, state="normal" if self.lab.mode=="live" or n in ("Router","Orders","Results",*PARTS) else "disabled")

    @property
    def points(self):
        return FLOW if self.view.get()=="architecture" else NETWORK

    def switch_view(self):
        self.selected = "Router" if self.view.get()=="architecture" else self.lab.leader
        self.packets.clear()
        self.inspect()
        self.paint()

    def xy(self, n):
        x, y = self.points[n]
        return x*self.map.winfo_width(), y*self.map.winfo_height()

    def paint(self):
        c = self.map
        c.delete("all")
        if self.view.get() == "architecture":
            self.paint_flow()
            self.draw_packets()
            return
        for a,b in [*((n,"Router") for n in CLIENTS), *(("Router",n) for n in NAMES), *LINKS]:
            blocked = a+b in self.lab.blocked
            c.create_line(*self.xy(a), *self.xy(b), fill="#ff978f" if blocked else "#536d67",
                          dash=(6,4) if blocked else ())
        for n in [*NETWORK, *LINKS]:
            if n in LINKS:
                ax,ay=self.xy(n[0]); bx,by=self.xy(n[1])
                x,y=(ax+bx)/2,(ay+by)/2
                w,h,label=20,12,"–".join(n)
            else:
                x,y=self.xy(n)
                if n=="Router":
                    sub=f"{len(self.lab.waiting)} waiting"
                elif n in CLIENTS:
                    sub="client" if self.lab.enabled[n] else "paused"
                elif n not in self.lab.cluster.nodes:
                    sub="offline"
                else:
                    sub="leader" if n==self.lab.leader else "replica"
                w,h,label=42,22,f"{'Broker ' if n in NAMES else ''}{n}\n{sub}"
            tag="select:"+n
            c.create_rectangle(x-w,y-h,x+w,y+h,fill="#203831",
                               outline="#e5eba0" if self.selected==n else "#78978c",tags=(tag,))
            c.create_text(x,y,text=label,justify="center",fill="white",tags=(tag,))
        self.draw_packets()

    def flow_path(self, a, b):
        ax, ay = self.xy(a)
        bx, by = self.xy(b)
        if (a,b) == ("Results","Router"):
            gutter = self.map.winfo_width() * .955
            return [(ax,ay), (gutter,ay), (gutter,by), (bx,by)]
        return [(ax,ay), (bx,by)]

    def paint_flow(self):
        c = self.map
        width, height = c.winfo_width(), c.winfo_height()
        edges = [(n,"Router") for n in CLIENTS] + [("Router","Orders")]
        for p,w in zip(PARTS,WORKERS):
            edges.extend([("Orders",p), (p,w), (w,"Results")])
        edges.append(("Results","Router"))
        for a,b in edges:
            path = self.flow_path(a,b)
            # Stop arrows at the node edge so their direction stays visible.
            x,y = path[-1]
            px,py = path[-2]
            dx,dy = px-x,py-y
            scale = min(65/max(abs(dx),.001), 16/max(abs(dy),.001), 1)
            path[-1] = (x+dx*scale,y+dy*scale)
            c.create_line(*[v for point in path for v in point], fill="#86a5b0",
                          width=2, arrow=tk.LAST)
        for n in FLOW:
            x,y = self.xy(n)
            if n in CLIENTS:
                title, sub, bg = n, "Trading client", "#233e60"
            elif n == "Router":
                title, sub, bg = "Client router", f"{len(self.lab.waiting)} queued", "#233e60"
            elif n in ("Orders","Results"):
                title = "Akfak / orders" if n=="Orders" else "Akfak / order-results"
                sub = "Key: security · A/B/C" if n=="Orders" else "Fills for both sides"
                bg = "#694a2d" if n=="Orders" else "#4d3f68"
            elif n in PARTS:
                i=int(n[1])
                title, sub, bg = f"Partition {i}", f"{self.lab.ends[i]} orders", "#694a2d"
            else:
                i=int(n[1])
                worker=self.lab.workers[i]
                lag=self.lab.ends[i]-worker.offset
                title, sub, bg = f"Matching engine {i}", (f"{lag} behind" if worker.enabled else f"Paused · {lag} behind"), "#244e42"
            tag="select:"+n
            half=min(82,width*.13)
            c.create_rectangle(x-half,y-16,x+half,y+16,fill=bg,
                               outline="#f2e496" if n==self.selected else "#76918c",tags=tag)
            c.create_text(x,y-6,text=title,fill="white",font=("Helvetica",11,"bold"),tags=tag)
            c.create_text(x,y+7,text=sub,fill="#e1eee9",font=("Helvetica",10),tags=tag)

    def pick(self, event):
        for item in reversed(self.map.find_overlapping(event.x,event.y,event.x,event.y)):
            for tag in self.map.gettags(item):
                if tag.startswith("select:"):
                    self.selected = tag.split(":")[1]
                    self.inspect()
                    self.paint()
                    return

    def draw_packets(self):
        self.map.delete("packet")
        now = time.monotonic()
        self.packets = [(e,t) for e,t in self.packets if now-t < .7]
        for e,t in self.packets:
            fraction=(now-t)/.7
            if fraction < 0:
                continue
            path=self.flow_path(e["src"], e["dst"])
            progress=fraction*(len(path)-1)
            segment=int(progress)
            ax,ay=path[segment]
            bx,by=path[segment+1]
            progress-=segment
            x,y=ax+(bx-ax)*progress,ay+(by-ay)*progress
            self.map.create_oval(x-4,y-4,x+4,y+4,fill=COLORS[e["kind"]],outline="",tags="packet")

    def animate(self):
        if self.packets:
            self.draw_packets()
        self.root.after(40, self.animate)


if __name__ == "__main__":
    root = tk.Tk()
    lab = Lab()
    try:
        Window(root, lab)
        root.mainloop()
    finally:
        lab.close()
