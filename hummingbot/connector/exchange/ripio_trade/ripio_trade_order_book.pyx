#!/usr/bin/env python
from decimal import Decimal

from aiokafka import ConsumerRecord
import bz2
import logging
from sqlalchemy.engine import RowProxy
import dateutil
import time
from typing import (
    Any,
    Optional,
    Dict
)
import ujson

from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.connector.exchange.ripio_trade.ripio_trade_utils import convert_from_exchange_trading_pair

_hob_logger = None


cdef class RipioTradeOrderBook(OrderBook):
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

    # @classmethod
    # def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
    #     ts = record["timestamp"]
    #     msg = record["json"] if type(record["json"])==dict else ujson.loads(record["json"])
    #     if metadata:
    #         msg.update(metadata)

    #     return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
    #         "trading_pair": convert_from_exchange_trading_pair(msg["ch"].split(".")[1]),
    #         "update_id": int(ts),
    #         "bids": msg["tick"]["bids"],
    #         "asks": msg["tick"]["asks"]
    #     }, timestamp=ts * 1e-3)

    # @classmethod
    # def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
    #     ts = record["timestamp"]
    #     msg = record["json"] if type(record["json"])==dict else ujson.loads(record["json"])
    #     if metadata:
    #         msg.update(metadata)
    #     return OrderBookMessage(OrderBookMessageType.DIFF, {
    #         "trading_pair": convert_from_exchange_trading_pair(msg["s"]),
    #         "update_id": int(ts),
    #         "bids": msg["b"],
    #         "asks": msg["a"]
    #     }, timestamp=ts * 1e-3)

    # @classmethod
    # def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
    #     ts = record.timestamp
    #     msg = ujson.loads(record.value.decode())
    #     if metadata:
    #         msg.update(metadata)
    #     return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
    #         "trading_pair": convert_from_exchange_trading_pair(msg["ch"].split(".")[1]),
    #         "update_id": ts,
    #         "bids": msg["tick"]["bids"],
    #         "asks": msg["tick"]["asks"]
    #     }, timestamp=ts * 1e-3)

    # @classmethod
    # def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
    #     decompressed = bz2.decompress(record.value)
    #     msg = ujson.loads(decompressed)
    #     ts = record.timestamp
    #     if metadata:
    #         msg.update(metadata)
    #     return OrderBookMessage(OrderBookMessageType.DIFF, {
    #         "trading_pair": convert_from_exchange_trading_pair(msg["s"]),
    #         "update_id": ts,
    #         "bids": msg["bids"],
    #         "asks": msg["asks"]
    #     }, timestamp=ts * 1e-3)

    # @classmethod
    # def trade_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
    #     msg = record["json"]
    #     ts = record.timestamp
    #     data = msg["tick"]["data"][0]
    #     if metadata:
    #         msg.update(metadata)
    #     return OrderBookMessage(OrderBookMessageType.TRADE, {
    #         "trading_pair": convert_from_exchange_trading_pair(msg["ch"].split(".")[1]),
    #         "trade_type": float(TradeType.BUY.value) if data["direction"] == "sell" else float(TradeType.SELL.value),
    #         "trade_id": ts,
    #         "update_id": ts,
    #         "price": data["price"],
    #         "amount": data["amount"]
    #     }, timestamp=ts * 1e-3)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        retval = RipioTradeOrderBook()
        retval.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return retval
