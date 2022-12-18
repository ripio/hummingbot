from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "ripio_trade"

# Base URL
REST_URL = "https://api.ripiotrade.co/"
WSS_URL = "wss://ws.ripiotrade.co"

API_VERSION = "v4"

# Public API endpoints
ORDER_BOOK_PATH_URL ="/orders/level-3/"
TICKER_PRICE_PATH_URL = f"/ticker/"
PAIRS_PATH_URL = f"/pairs/"
# EXCHANGE_INFO_PATH_URL = "/exchangeInfo"
# PING_PATH_URL = "/ping"
# SNAPSHOT_PATH_URL = "/depth"
# SERVER_TIME_PATH_URL = "/time"

# Private API endpoints or BinanceClient function
# ACCOUNTS_PATH_URL = "/account"
# MY_TRADES_PATH_URL = "/myTrades"
# ORDER_PATH_URL = "/order"
# BINANCE_USER_STREAM_PATH_URL = "/userDataStream"

SIDE_BUY = 'buy'
SIDE_SELL = 'sell'


ORDER_STATE = {
    "pending_creation": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "executed_completely": OrderState.FILLED,
    "executed_partially": OrderState.PARTIALLY_FILLED,
    "canceled": OrderState.CANCELED,
}