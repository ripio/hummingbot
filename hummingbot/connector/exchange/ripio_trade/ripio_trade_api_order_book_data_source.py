#!/usr/bin/env python

import aiohttp
from aiohttp import WSMessage, WSMsgType
import asyncio
# import gzip
import json
import logging
import pandas as pd
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
    AsyncIterable,
)
# import websockets
# from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.ripio_trade.ripio_trade_order_book import RipioTradeOrderBook
from hummingbot.connector.exchange.ripio_trade.ripio_trade_utils import convert_to_exchange_trading_pair, convert_from_exchange_trading_pair
from hummingbot.connector.exchange.ripio_trade.ripio_trade_constants import TICKER_PRICE_PATH_URL, PAIRS_PATH_URL
from collections import defaultdict
import hummingbot.connector.exchange.ripio_trade.ripio_trade_constants as CONSTANTS
import hummingbot.connector.exchange.ripio_trade.ripio_trade_web_utils as web_utils
# the max depth  is 50
DEPTH = 50
# ROOT_PUBLIC_REST_URL = 'https://api.ripiotrade.co/v3/public'
# PAGE_SIZE = 1000


WAITING_TIME = 1
FAILURE_WAITING_TIME = 1


class RipioTradeAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    HEARTBEAT_PING_INTERVAL = 10.0
    
    TRADE_FILTER_ID = "trade"
    DIFF_FILTER_ID = "orderbook/level_2"

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
            self, 
            trading_pairs: List[str] = None,
            shared_client: Optional[aiohttp.ClientSession] = None,):
        super().__init__(trading_pairs)
        self._shared_client = shared_client or self._get_session_instance()
        self._trading_pairs: List[str] = trading_pairs

        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    @classmethod
    def _get_session_instance(cls) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession()
        return session

    # TODO
    @classmethod
    async def get_last_traded_prices(
        cls, trading_pairs: List[str], client: Optional[aiohttp.ClientSession] = None) -> Dict[str, float]:
        if len(trading_pair) == 0:
            return {}
        
        results = {}
        url = web_utils.public_rest_url(CONSTANTS.TICKER_PRICE_PATH_URL)
        client = client or cls._get_session_instance()
        async with client.get(url, timeout=10) as response:
            if response.status == 200:
                resp_json = await response.json()["data"]
                for trading_pair in trading_pairs:
                    results[trading_pair] = float([i["last"] for i in resp_json if i["pair"] == convert_to_exchange_trading_pair(trading_pair)][0])
        return results

    @staticmethod
    async def fetch_trading_pairs(client: Optional[aiohttp.ClientSession] = None) -> List[str]:
        client = client or RipioTradeAPIOrderBookDataSource._get_session_instance()
        url = web_utils.public_rest_url(CONSTANTS.PAIRS_PATH_URL)
        async with client.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                raw_trading_pairs = [d["symbol"] for d in data["data"] if d["enabled"]]
                trading_pair_list: List[str] = []
                for raw_trading_pair in raw_trading_pairs:
                    converted_trading_pair: Optional[str] = \
                        convert_from_exchange_trading_pair(raw_trading_pair)
                    if converted_trading_pair is not None:
                        trading_pair_list.append(converted_trading_pair)
                return trading_pair_list
        return []
    
    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        client = client or RipioTradeAPIOrderBookDataSource._get_session_instance()
        async with client.get(f'{web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_PATH_URL, convert_to_exchange_trading_pair(trading_pair))}?limit={DEPTH}') as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Ripio Trade market snapshot for {trading_pair}. HTTP status is {response.status}.")
            api_data = await response.read()
            data: Dict[str, Any] = json.loads(api_data)
            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_snapshot(self._shared_client, trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = RipioTradeOrderBook.snapshot_message_from_exchange(
            snapshot,
            metadata={"trading_pair": trading_pair},
            timestamp=snapshot_timestamp
        )
        order_book: OrderBook = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book
    
    async def _iter_messages(self, ws: aiohttp.ClientWebSocketResponse) -> AsyncIterable[str]:
        try:
            while True:
                msg: WSMessage = await ws.receive()
                if msg.type == WSMsgType.CLOSED:
                    raise ConnectionError
                yield msg.data
        except Exception:
            self.logger().error("Unexpected error occurred iterating through websocket messages.",
                                exc_info=True)
            raise
        finally:
            await ws.close()
    
    async def listen_for_subscriptions(self):
        ws = None
        while True:
            try:
                ws = await self._shared_client.ws_connect(
                    url=CONSTANTS.WSS_URL,
                    heartbeat=self.HEARTBEAT_PING_INTERVAL,
                    receive_timeout=self.MESSAGE_TIMEOUT,
                )
                await self._subscribe_to_order_book_streams(ws)
                async for raw_msg in self._iter_messages(ws):
                    msg = json.loads(raw_msg)
                    if self.TRADE_FILTER_ID in msg:
                        self._message_queue[self.TRADE_FILTER_ID].put_nowait(msg)
                    if self.DIFF_FILTER_ID in msg:
                        self._message_queue[self.DIFF_FILTER_ID].put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error occurred when listening to order book streams. "
                                    "Retrying in 5 seconds...",
                                    exc_info=True)
                await self._sleep(5.0)
            finally:
                ws and await ws.close()
                
    async def _subscribe_to_order_book_streams(self, ws: aiohttp.ClientWebSocketResponse):
        try:
            # for trading_pair in self._trading_pairs:
            params: Dict[str, Any] = {
                "method": "subscribe",
                "topics": [f"{self.TRADE_FILTER_ID}@{convert_to_exchange_trading_pair(trading_pair)}" for trading_pair in self._trading_pairs] + \
                    [f"{self.DIFF_FILTER_ID}@{convert_to_exchange_trading_pair(trading_pair)}" for trading_pair in self._trading_pairs]
                # "topics": [self.TRADE_FILTER_ID, self.DIFF_FILTER_ID]
            }
            await ws.send_json(params)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise
        
    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for trades using websocket trade channel
        """
        msg_queue = self._message_queue[self.TRADE_FILTER_ID]
        while True:
            try:
                msg = await msg_queue.get()
                msg_timestamp: int = int(time.time() * 1e3)
                
                for trade_entry in msg[self.TRADE_FILTER_ID]:
                    trade_msg: OrderBookMessage = RipioTradeOrderBook.trade_message_from_exchange(
                        msg=trade_entry,
                        timestamp=msg_timestamp,
                        metadata={"market_id": msg["market_id"]})
                    output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                
    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook diffs using websocket book channel
        """
        msg_queue = self._message_queue[self.DIFF_FILTER_ID]
        msg = None
        while True:
            try:
                msg = await msg_queue.get()
                msg_timestamp = int(time.time() * 1e3)

                diff_msg: OrderBookMessage = RipioTradeOrderBook.diff_message_from_exchange(
                    msg=msg, 
                    timestamp=msg_timestamp,
                    metadata={"market_id": msg["market_id"]}
                )
                output.put_nowait(diff_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    f"Error while parsing OrderBookMessage from ws message {msg}", exc_info=True
                )
                
    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook snapshots by fetching orderbook
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_snapshot(
                            client=self._shared_client, trading_pair=trading_pair
                        )
                        snapshot_timestamp: int = int(time.time() * 1e3)
                        snapshot_msg: OrderBookMessage = RipioTradeOrderBook.snapshot_message_from_exchange(
                            msg=snapshot,
                            timestamp=snapshot_timestamp,
                            metadata={"market_id": trading_pair}  # Manually insert trading_pair here since API response does include trading pair
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        # Be careful not to go above API rate limits.
                        await asyncio.sleep(5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().network(
                            "Unexpected error with WebSocket connection.",
                            exc_info=True,
                            app_warning_msg="Unexpected error with WebSocket connection. Retrying in 5 seconds. "
                                            "Check network connection."
                        )
                        await asyncio.sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)