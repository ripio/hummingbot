from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "ripio_trade"

MAX_ORDER_ID_LEN = 36

# Base URL
REST_URL = "https://api.ripiotrade.co/"
WSS_URL = "wss://ws.ripiotrade.co"

API_VERSION = "v4"

# Public API endpoints
ORDER_BOOK_PATH_URL ="/orders/level-2/"
TICKER_PRICE_PATH_URL = "/ticker/"
PAIRS_PATH_URL = "/pairs/"
SERVER_TIME_PATH_URL = "/server-time/"
CURRENCIES = "/currencies/"
# EXCHANGE_INFO_PATH_URL = "/exchangeInfo"
# PING_PATH_URL = "/ping"
# SNAPSHOT_PATH_URL = "/depth"
# SERVER_TIME_PATH_URL = "/time"

# Private API endpoints
BALANCE_PATH_URL = "/user/balances/"
CANCEL_PATH_URL = "/orders/"
ORDER_STATUS_BY_ID_URL_PATH = "/orders/{}/"
NEW_ORDER_PATH_URL = "/orders/"
TICKET_PATH_URL = "/ticket/"

# Private API endpoints or BinanceClient function
# ACCOUNTS_PATH_URL = "/account"
# MY_TRADES_PATH_URL = "/myTrades"
# ORDER_PATH_URL = "/order"
# BINANCE_USER_STREAM_PATH_URL = "/userDataStream"

WS_PRIVATE_CHANNELS = [
    "order_status",
    "balance",
]

SIDE_BUY = "buy"
SIDE_SELL = "sell"


ORDER_STATE = {
    "pending_creation": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "executed_completely": OrderState.FILLED,
    "executed_partially": OrderState.PARTIALLY_FILLED,
    "canceled": OrderState.CANCELED,
}