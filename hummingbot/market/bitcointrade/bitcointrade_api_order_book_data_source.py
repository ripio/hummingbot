#!/usr/bin/env python
# ---------------
import asyncio
import base64
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import re
import time
import ujson
import datetime

from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitcointrade.bitcointrade_order_book import BitcoinTradeOrderBook

TRADING_PAIRS = ['BRLBTC', 'BRLETH', 'BRLLTC', 'BRLBCH', 'BRLXRP', 'BRLEOS', 'BRLDAI']

ROOT_PUBLIC_REST_URL = 'https://api.bitcointrade.com.br/v3/public/'
PAGE_SIZE = 250
LIMIT_SIZE = 10

WAITING_TIME = 1
FAILURE_WAITING_TIME = 1


class BitcoinTradeAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 3000.0

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have trading_pair as index and include brl volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:

            trading_pairs: List[Dict[str,Any]] = TRADING_PAIRS
            market_data: List[Dict[str, Any]] = []
            for pair in trading_pairs:
                response = await client.get(ROOT_PUBLIC_REST_URL + f'{pair}/ticker')
                response: aiohttp.ClientResponse = response
                if response.status != 200:
                    raise IOError(f"Error fetching Bitcoin Trade markets information. "
                                f"HTTP status is {response.status}.")
                md = (await response.json())['data']
                md.update({'symbol':pair})
                market_data.append(md)

            market_data: List[Dict[str, Any]] = [{**item, **{'base':item['symbol'].replace('BRL', ''), 'quote':'BRL'}}
                                                 for item in market_data]

            # Build the data frame.
            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="symbol")
            brl_volume: float = [
                (quoteVolume*price)
                for  quoteVolume, price in zip(all_markets['volume'].astype("float"),
                                                all_markets['last'].astype("float"))]
            all_markets.loc[:, "BRLVolume"] = brl_volume
            all_markets.loc[:, "volume"] = all_markets.volume

            return all_markets.sort_values("BRLVolume", ascending=False)


    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            self._trading_pairs = TRADING_PAIRS
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        url_path = ROOT_PUBLIC_REST_URL + f'{trading_pair}/orders/?limit={LIMIT_SIZE}'
        async with client.get(url_path) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f'Error fetching BitcoinTrade market snapshot for {trading_pair}. '
                              f'HTTP status is {response.status}.')
            data: Dict[str, Any] = await response.json()
            bid = [[d['unit_price'], d['amount']] for d in data['data']['bids']]
            ask = [[d['unit_price'], d['amount']] for d in data['data']['asks']]
            # !!! the update_id parameter is missing from the schema response
            updated_id: float = time.time()
            bids = [OrderBookRow(i[0], i[1], updated_id) for i in bid]
            asks = [OrderBookRow(i[0], i[1], updated_id) for i in ask]

            return {
                'symbol': trading_pair,
                'bids': bids,
                'asks': asks,
                'lastUpdateId': updated_id
            }

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = BitcoinTradeOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)
            return retval

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        trading_pairs: List[str] = await self.get_trading_pairs()
        tasks = [
            ev_loop.create_task(self._listen_trades_for_pair(pair, output))
            for pair in trading_pairs
        ]
        await asyncio.gather(*tasks)

    async def _listen_trades_for_pair(self, pair: str, output: asyncio.Queue):
        url_path = ROOT_PUBLIC_REST_URL + f'{pair}/trades' + f'?page_size={PAGE_SIZE}'
        last_updated_date_trade = None
        client = aiohttp.ClientSession(raise_for_status=False)
        while True:
            try:
                async with client.get(url_path) as response:
                    response: aiohttp.ClientResponse = response
                    if response.status == 429 or response.status == 502:
                        await asyncio.sleep(FAILURE_WAITING_TIME)
                        continue
                    if response.status != 200:
                        raise IOError(f'Error fetching BitcoinTrade market snapshot for {pair}. '
                                    f'HTTP status is {response.status}.')
                    trades: List[str] = (await response.json())['data']['trades']
                    msg = []
                    if not last_updated_date_trade:
                        last_updated_date_trade = trades[0]['date']
                    else:
                        for trade in trades:
                            if trade['date'] != last_updated_date_trade:
                                msg_book: OrderBookMessage = BitcoinTradeOrderBook.trade_message_from_exchange(
                                    trade,
                                    metadata={"symbol": f"{pair}"}
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
        trading_pairs: List[str] = await self.get_trading_pairs()
        tasks = [
            self._listen_order_book_for_pair(pair, output)
            for pair in trading_pairs
        ]

        await asyncio.gather(*tasks)

    async def _listen_order_book_for_pair(self, pair: str, output: asyncio.Queue = None):
        url_path = ROOT_PUBLIC_REST_URL + f'{pair}/orders/?limit={LIMIT_SIZE}'
        client = aiohttp.ClientSession(raise_for_status=False)
        while True:
            try:
                async with client.get(url_path) as response:
                    response: aiohttp.ClientResponse = response
                    if response.status == 429 or response.status == 502:
                        await asyncio.sleep(FAILURE_WAITING_TIME)
                        continue
                    if response.status != 200:
                        raise IOError(f'Error fetching BitcoinTrade order book diffs for {pair}. '
                                    f'HTTP status is {response.status}.')
                    msg: List[str] = (await response.json())['data']
                    # !!! the update_id parameter is missing from the schema response
                    updated_id: float = time.time()
                    msg_book: OrderBookMessage = BitcoinTradeOrderBook.diff_message_from_exchange(
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
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = BitcoinTradeOrderBook.snapshot_message_from_exchange(
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
