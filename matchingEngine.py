"""Price/time limit order book for a single partition; prices are integer cents."""


class MatchingEngine:
    def __init__(self, partition):
        self.partition = partition
        self.offset = 0
        self.enabled = True
        self.resting = []
        self.pending = None

    def match(self, incoming):
        order = dict(incoming, remaining=incoming["quantity"])
        fills = []
        buy = order["side"] == "BUY"
        eligible = [r for r in self.resting
                    if r["symbol"] == order["symbol"] and r["side"] != order["side"]
                    and (r["price"] <= order["price"] if buy else r["price"] >= order["price"])]
        # Stable sorting preserves arrival order for equal prices.
        for resting in sorted(eligible, key=lambda r: r["price"], reverse=not buy):
            quantity = min(order["remaining"], resting["remaining"])
            for leg in (order, resting):
                leg["remaining"] -= quantity
                fills.append(dict(order=leg["id"], client=leg["client"], side=leg["side"],
                                  symbol=leg["symbol"], quantity=quantity, price=resting["price"],
                                  remaining=leg["remaining"]))
            if not resting["remaining"]:
                self.resting.remove(resting)
            if not order["remaining"]:
                break
        if order["remaining"]:
            self.resting.append(order)
        return fills
