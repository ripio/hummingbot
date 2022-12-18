#!/usr/bin/env python
from decimal import Decimal
import logging
# from sqlalchemy.engine import RowProxy
import dateutil
import time
from typing import (
    Any,
    Optional,
    Dict,
    List
)
from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.connector.exchange.ripio_trade.ripio_trade_utils import convert_from_exchange_trading_pair
import hummingbot.connector.exchange.ripio_trade.ripio_trade_constants as CONSTANTS

_hob_logger = None


class RipioTradeOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _hob_logger
        if _hob_logger is None:
            _hob_logger = logging.getLogger(__name__)
        return _hob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: Optional[float] = None,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        content = {
            "trading_pair": convert_from_exchange_trading_pair(msg["trading_pair"]),
            "update_id": timestamp,
            "bids": [[d['unit_price'], d['amount']] for d in msg['data']['bids']],
            "asks": [[d['unit_price'], d['amount']] for d in msg['data']['asks']]
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        timestamp = dateutil.parser.parse(msg["date"])
        trade_type = TradeType.SELL if msg["type"] == "sell" else TradeType.BUY
        millis = int(round(time.time() * 1000))
        str_millis = str(millis)
        uniq_id = str_millis[- 10:]

        content = {
            "trading_pair": convert_from_exchange_trading_pair(msg["trading_pair"]),
            "trade_type": float(trade_type.value),
            "trade_id": uniq_id,
            "update_id": timestamp,
            "price": msg["unit_price"],
            "amount": msg["amount"],
        }
        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            content,
            timestamp=float(timestamp.strftime('%s'))
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   symbol: str = "",
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        content = {
            "trading_pair": symbol,
            "update_id": msg["updated_id"],
            "bids": [[d["unit_price"], d["amount"]] for d in msg["bids"]],
            "asks": [[d["unit_price"], d["amount"]] for d in msg["asks"]]
        }
        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp=timestamp)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        retval = RipioTradeOrderBook()
        retval.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return retval
    
    @classmethod
    def restore_from_snapshot_and_diffs(cls, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError(CONSTANTS.EXCHANGE_NAME + " order book needs to retain individual order data.")