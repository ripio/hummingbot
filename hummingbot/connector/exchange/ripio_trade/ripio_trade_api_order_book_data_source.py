#!/usr/bin/env python

import aiohttp
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
)
# import websockets
# from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.ripio_trade.ripio_trade_order_book import RipioTradeOrderBook
from hummingbot.connector.exchange.ripio_trade.ripio_trade_utils import convert_to_exchange_trading_pair, convert_from_exchange_trading_pair


# the max depth of the snapthot is 200
DEPTH = 200
ROOT_PUBLIC_REST_URL = 'https://api.ripiotrade.co/v3/public'
PAGE_SIZE = 1000


WAITING_TIME = 1
FAILURE_WAITING_TIME = 1


class RipioTradeAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _haobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._haobds_logger is None:
            cls._haobds_logger = logging.getLogger(__name__)
        return cls._haobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        results = dict()
        for trading_pair in trading_pairs:
            async with aiohttp.ClientSession() as client:
                while True:
                    response = await client.get(f'{ROOT_PUBLIC_REST_URL}/{convert_to_exchange_trading_pair(trading_pair)}/ticker')
                    if response.status == 429 or response.status == 502:
                        await asyncio.sleep(FAILURE_WAITING_TIME)
                        continue
                    if response.status != 200:
                        raise IOError(f"Error fetching Ripio Trade markets information. HTTP status is {response.status}.")
                    resp_json = await response.json()
                    results[trading_pair] = float(resp_json["data"]["last"])
                    break
        return results

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                url = f'{ROOT_PUBLIC_REST_URL}/pairs'
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

        except Exception as e:
            pass

        return []


    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        async with client.get(f'{ROOT_PUBLIC_REST_URL}/{convert_to_exchange_trading_pair(trading_pair)}/orders?limit={DEPTH}') as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Ripio Trade market snapshot for {trading_pair}. HTTP status is {response.status}.")
            api_data = await response.read()
            data: Dict[str, Any] = json.loads(api_data)
            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = RipioTradeOrderBook.snapshot_message_from_exchange(
                snapshot,
                metadata={"trading_pair": trading_pair},
                timestamp=snapshot_timestamp
            )
            order_book: OrderBook = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
            return order_book

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        trading_pairs: List[str] = await self.fetch_trading_pairs()
        tasks = [
            ev_loop.create_task(self._listen_trades_for_pair(pair, output))
            for pair in trading_pairs
        ]
        await asyncio.gather(*tasks)

    async def _listen_trades_for_pair(self, pair: str, output: asyncio.Queue):
        # there is no ws connection in the exchange api
        # this code emulates ws connection
        url_path = f'{ROOT_PUBLIC_REST_URL}/{convert_to_exchange_trading_pair(pair)}/trades?page_size={PAGE_SIZE}'
        last_updated_date_trade = None
        async with aiohttp.ClientSession() as client:
            while True:
                try:
                    async with client.get(url_path) as response:
                        response: aiohttp.ClientResponse = response
                        # 429 Too Many Requests
                        # 502 Bad Gateway (also sometimes appears with a large number of requests)
                        if response.status == 429 or response.status == 502:
                            await asyncio.sleep(FAILURE_WAITING_TIME)
                            continue
                        if response.status != 200:
                            raise IOError(f'Error fetching Ripio Trade market snapshot for {pair}. HTTP status is {response.status}.')
                        trades: List[str] = (await response.json())['data']['trades']
                        if not last_updated_date_trade:
                            if len(trades) > 1:
                                last_updated_date_trade = trades[0]['date']
                        else:
                            for trade in trades:
                                if trade['date'] != last_updated_date_trade:
                                    msg_book: OrderBookMessage = RipioTradeOrderBook.trade_message_from_exchange(
                                        trade,
                                        metadata={"trading_pair": f"{pair}"}
                                    )
                                    output.put_nowait(msg_book)
                                else:
                                    break
                            last_updated_date_trade = trades[0]['date']
                    await asyncio.sleep(WAITING_TIME)
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    self.logger().error(f"listen trades for pair {pair}", err)
                    self.logger().error(
                        "Unexpected error with WebSocket connection. "
                        f"Retrying after {int(self.MESSAGE_TIMEOUT)} seconds...",
                        exc_info=True)
                    await asyncio.sleep(self.MESSAGE_TIMEOUT)

    async def listen_for_order_book_diffs(self,
                                          ev_loop: asyncio.BaseEventLoop,
                                          output: asyncio.Queue):
        trading_pairs: List[str] = await self.fetch_trading_pairs()
        tasks = [
            self._listen_order_book_for_pair(pair, output)
            for pair in trading_pairs
        ]

        await asyncio.gather(*tasks)

    async def _listen_order_book_for_pair(self, pair: str, output: asyncio.Queue = None):
        # there is no ws connection in the exchange api
        # this code emulates ws connection
        url_path = f'{ROOT_PUBLIC_REST_URL}/{convert_to_exchange_trading_pair(pair)}/orders/?limit={DEPTH}'
        async with aiohttp.ClientSession() as client:
            while True:
                try:
                    async with client.get(url_path) as response:
                        response: aiohttp.ClientResponse = response
                        # 429 Too Many Requests
                        # 502 Bad Gateway (also sometimes appears with a large number of requests)
                        if response.status == 429 or response.status == 502:
                            await asyncio.sleep(FAILURE_WAITING_TIME)
                            continue
                        if response.status != 200:
                            raise IOError(f'Error fetching Ripio Trade order book diffs for {pair}. HTTP status is {response.status}.')
                        msg: List[str] = (await response.json())['data']
                        # !!! the update_id parameter is missing from the schema response
                        updated_id: float = time.time()
                        msg_book: OrderBookMessage = RipioTradeOrderBook.diff_message_from_exchange(
                            msg,
                            time.time(),
                            pair,
                            {'updated_id': updated_id}
                        )
                        output.put_nowait(msg_book)
                    await asyncio.sleep(WAITING_TIME)
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    self.logger().error(f"listen trades for pair {pair}", err)
                    self.logger().error(
                        "Unexpected error with WebSocket connection. "
                        f"Retrying after {int(self.MESSAGE_TIMEOUT)} seconds...",
                        exc_info=True)
                    await asyncio.sleep(self.MESSAGE_TIMEOUT)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.fetch_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = RipioTradeOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
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
                
    async def listen_for_subscriptions(self):
        pass