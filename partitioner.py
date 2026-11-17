"""Stable assignment of demo securities to the three matching engines."""
from zlib import crc32

EXAMPLES = (("AAPL", "MSFT"), ("GOOG", "AMZN"), ("TSLA", "META"))


def partition_for(symbol):
    for partition, symbols in enumerate(EXAMPLES):
        if symbol in symbols:
            return partition
    return crc32(symbol.encode("ascii")) % len(EXAMPLES)
