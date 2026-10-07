from enum import Enum
from typing import TypedDict


# 0. Enums
class OrderType(Enum):
    SELL = 1
    BUY = 2

class OrderStatus(Enum):
    OPEN = 1
    PARTIALLY_FILLED = 2
    FILLED = 3
    REJECTED = 4


# 1. Client -> Client Router
class OrderSendRequest(TypedDict):
    clientID: int
    orderType: OrderType
    quantity: int
    price: int          # cents
    security: str       # e.g. "AAPL"


# 2. Client Router -> Kafka
class Order(TypedDict):
    correlationId: int

    clientID: int
    orderType: OrderType

    quantity: int
    remainingQuantity: int

    price: int          # limit price in cents
    security: str


# 3. Matching Engine -> Results Topic
class OrderResult(TypedDict):
    correlationId: int
    clientID: int

    security: str
    orderType: OrderType

    executionQuantity: int
    executionPrice: int

    remainingQuantity: int
    status: OrderStatus