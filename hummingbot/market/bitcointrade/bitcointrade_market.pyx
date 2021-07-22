import aiohttp
from aiohttp.test_utils import TestClient
import asyncio
from async_timeout import timeout
import conf
from datetime import datetime
from decimal import Decimal
from libc.stdint cimport int64_t
import logging
import pandas as pd
import re
import time
from typing import (
    Any,
    AsyncIterable,
    Coroutine,
    Dict,
    List,
    Optional,
    Tuple
)
import ujson

import hummingbot
from hummingbot.core.clock cimport Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.core.event.events import (
    MarketEvent,
    MarketWithdrawAssetEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketTransactionFailureEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType,
    TradeFee
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitcointrade.bitcointrade_api_order_book_data_source import BitcoinTradeAPIOrderBookDataSource
from hummingbot.market.bitcointrade.bitcointrade_auth import BitcoinTradeAuth
from hummingbot.market.bitcointrade.bitcointrade_in_flight_order import BitcoinTradeInFlightOrder
from hummingbot.market.bitcointrade.bitcointrade_order_book_tracker import BitcoinTradeOrderBookTracker
from hummingbot.market.trading_rule cimport TradingRule
from hummingbot.market.market_base import (
    MarketBase,
    NaN,
    s_decimal_NaN)
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.client.config.fee_overrides_config_map import fee_overrides_config_map
from hummingbot.core.utils.estimate_fee import estimate_fee

hm_logger = None
s_decimal_0 = Decimal(0)
# TRADING_PAIR_SPLITTER = re.compile(r"^(BRL)(\w+)$")
TRADING_PAIR_SPLITTER_FROM_EXCHANGE = re.compile(r"^(BRL)(\w+)$")
TRADING_PAIR_SPLITTER_TO_EXCHANGE = re.compile(r"^(\w+)(BRL)$")
BITCOINTRADE_ROOT_API = "https://api.bitcointrade.com.br/v3/"
TRADING_PAIRS = ['BRLBTC', 'BRLETH', 'BRLLTC', 'BRLBCH', 'BRLXRP', 'BRLEOS', 'BRLDAI']


class BitcoinTradeAPIError(IOError):
    def __init__(self, error_payload: Dict[str, Any]):
        super().__init__(str(error_payload))
        self.error_payload = error_payload


cdef class BitcoinTradeMarketTransactionTracker(TransactionTracker):
    cdef:
        BitcoinTradeMarket _owner

    def __init__(self, owner: BitcoinTradeMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)


cdef class BitcoinTradeMarket(MarketBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value
    API_CALL_TIMEOUT = 10.0
    UPDATE_ORDERS_INTERVAL = 10.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global hm_logger
        if hm_logger is None:
            hm_logger = logging.getLogger(__name__)
        return hm_logger

    def __init__(self,
                 bitcointrade_api_key: str,
                 bitcointrade_secret_key: str,
                 poll_interval: float = 5.0,
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                 OrderBookTrackerDataSourceType.EXCHANGE_API,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()
        self._account_id = "1"
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._data_source_type = order_book_tracker_data_source_type
        self._ev_loop = asyncio.get_event_loop()
        self._bitcointrade_auth = BitcoinTradeAuth(api_key=bitcointrade_api_key, secret_key=bitcointrade_secret_key)
        self._in_flight_orders = {}
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._order_book_tracker = BitcoinTradeOrderBookTracker(
            data_source_type=order_book_tracker_data_source_type,
            trading_pairs=trading_pairs
        )
        self._poll_notifier = asyncio.Event()
        self._poll_interval = poll_interval
        self._shared_client = None
        self._status_polling_task = None
        self._trading_required = trading_required
        self._trading_rules = {}
        self._trading_rules_polling_task = None
        self._tx_tracker = BitcoinTradeMarketTransactionTracker(self)

    # @staticmethod
    # def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    #     try:
    #         cut_pair = trading_pair.replace("-", "").replace("_", "")
    #         m = TRADING_PAIR_SPLITTER.match(cut_pair)
    #         if m == None:
    #             raise Exception(f"Incorrect slpit: {trading_pair} => zero")
    #         #!!! currency unit prices are quoted as currency pairs CurrencyBase
    #         return m.group(1), m.group(2)
    #     # Exceptions are now logged as warnings in trading pair fetcher 
    #     except Exception as e:
    #         raise Exception(f"Incorrect slpit: {trading_pair} => zero") 
    @staticmethod
    def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
        try:
            cut_pair = trading_pair.replace("-", "").replace("_", "")
            m = TRADING_PAIR_SPLITTER_FROM_EXCHANGE.match(cut_pair)
            if m == None:
                m = TRADING_PAIR_SPLITTER_TO_EXCHANGE.match(cut_pair)
                if m == None:
                    raise Exception(f"Incorrect slpit: {trading_pair} => zero")
            #!!! currency unit prices are quoted as currency pairs CurrencyBase
            return m.group(2), m.group(1)
        # Exceptions are now logged as warnings in trading pair fetcher 
        except Exception as e:
            raise Exception(f"Incorrect slpit: {trading_pair} => zero")           


    @staticmethod
    def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
        if BitcoinTradeMarket.split_trading_pair(exchange_trading_pair) is None:
            return None

        # split_result = BitcoinTradeMarket.split_trading_pair(exchange_trading_pair)
        base_asset, quote_asset = BitcoinTradeMarket.split_trading_pair(exchange_trading_pair)
        return f"{quote_asset.upper()}-{base_asset.upper()}"
        # return f"{base_asset.upper()}-{quote_asset.upper()}"

    @staticmethod
    def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
        base_asset, quote_asset = BitcoinTradeMarket.split_trading_pair(hb_trading_pair)
        return f"{quote_asset.upper()}{base_asset.upper()}"
        # return f"{base_asset.upper()}{quote_asset.upper()}"    

    @property
    def name(self) -> str:
        return "bitcointrade"

    @property
    def order_book_tracker(self) -> BitcoinTradeOrderBookTracker:
        return self._order_book_tracker

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, BitcoinTradeInFlightOrder]:
        return self._in_flight_orders

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, Any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        #for item in saved_states:
        #   self.logger().info(f"key:{item}   item:{saved_states[]}") 
        self._in_flight_orders.update({
            key: BitcoinTradeInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    @property
    def shared_client(self) -> str:
        return self._shared_client

    @shared_client.setter
    def shared_client(self, client: aiohttp.ClientSession):
        self._shared_client = client

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await BitcoinTradeAPIOrderBookDataSource.get_active_exchange_markets()

    cdef c_start(self, Clock clock, double timestamp):
        self._tx_tracker.c_start(clock, timestamp)
        MarketBase.c_start(self, clock, timestamp)

    cdef c_stop(self, Clock clock):
        MarketBase.c_stop(self, clock)
        self._async_scheduler.stop()

    async def start_network(self):
        self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        try:
            await self._api_request(method="get", path_url="public/BRLBTC/orders")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t>(self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t>(timestamp / self._poll_interval)
        MarketBase.c_tick(self, timestamp)
        self._tx_tracker.c_tick(timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession(raise_for_status=False)
        return self._shared_client

    async def _api_request(self,
                           method,
                           path_url,
                           params: Optional[Dict[str, Any]] = None,
                           data=None,
                           is_auth_required: bool = False) -> Dict[str, Any]:
        url = BITCOINTRADE_ROOT_API + path_url
        client = await self._http_client()
        headers = {"Content-Type": "application/json"}
        if is_auth_required:
            headers = self._bitcointrade_auth.add_auth_to_params(method, path_url, params)
            """
            if method == "post":
                self.logger().info(f"Url:{url}")
                self.logger().info(f"Method:{method.upper()}")
                self.logger().info(f"Headers:{headers}")
                self.logger().info(f"params:{params}")
                self.logger().info(f"data:{ujson.dumps(data)}")
            """
        
        response_coro = client.request(
                method=method.upper(),
                url=url,
                headers=headers
            )
        if data:
            response_coro = client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                data=ujson.dumps(data),
                timeout=100
            )
            # raise IOError(f"method:{method}\nurl:{url}\nheders:{headers}\ndata:{data}")
        
        
        async with response_coro as response:
            if response.status != 200:
                raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}.")
            try:
                parsed_response = await response.json()
            except Exception:
                raise IOError(f"Error parsing data from {url}.")

            #data = parsed_response.get("data")
            data = parsed_response["data"]
            if data is None:
                self.logger().error(f"Error received from {url}. Response is {parsed_response}.")
                raise BitcoinTradeAPIError({"error": parsed_response})
            if type(data) is list:
                data_list_to_dct = {"list": data}
                return data_list_to_dct
            return data  

    async def _update_balances(self):
        cdef:
            str path_url = "wallets/balance"
            dict data
            list balances
            dict new_available_balances = {}
            dict new_balances = {}
            str asset_name
            object balance
        
        data = await self._api_request("get", path_url=path_url, is_auth_required=True)
        if len(data) > 0:
            for balance_entry in data["list"]:
                asset_name = balance_entry["currency_code"]
                free_balance = Decimal(balance_entry["available_amount"])
                locked_balance = Decimal(balance_entry["locked_amount"])                    

                if free_balance == s_decimal_0 and locked_balance == s_decimal_0:
                    continue                    
                if asset_name not in new_available_balances:
                    new_available_balances[asset_name] = s_decimal_0
                if asset_name not in new_balances:
                    new_balances[asset_name] = s_decimal_0

                new_available_balances[asset_name] = free_balance
                new_balances[asset_name] = free_balance + locked_balance
                    

            self._account_available_balances.clear()
            self._account_available_balances = new_available_balances
            self._account_balances.clear()
            self._account_balances = new_balances       

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):
        is_maker = order_type is OrderType.LIMIT
        return estimate_fee("bitcointrade", is_maker)

    async def _update_trading_rules(self):
        cdef:
            # The poll interval for trade rules is 60 seconds.
            int64_t last_tick = <int64_t>(self._last_timestamp / 60.0)
            int64_t current_tick = <int64_t>(self._current_timestamp / 60.0)
        self._trading_rules.clear()
        self._trading_rules['BRLBTC'] = TradingRule(
                                trading_pair='BRLBTC',
                                min_order_size=Decimal(0.00005),
                                max_order_size=Decimal(10),
                                min_price_increment=Decimal(.01),
                                min_base_amount_increment=Decimal(0.00000001),
                                min_quote_amount_increment=Decimal(0.01),
                                min_notional_size=Decimal(25)
                            )
        self._trading_rules['BRLETH'] = TradingRule(
                                trading_pair='BRLETH',
                                min_order_size=Decimal(0.0007),
                                max_order_size=Decimal(100),
                                min_price_increment=Decimal(.01),
                                min_base_amount_increment=Decimal(0.00000001),
                                min_quote_amount_increment=Decimal(0.01),
                                min_notional_size=Decimal(25)
                            )
        self._trading_rules['BRLLTC'] = TradingRule(
                                trading_pair='BRLLTC',
                                min_order_size=Decimal(0.007),
                                max_order_size=Decimal(1000),
                                min_price_increment=Decimal(.01),
                                min_base_amount_increment=Decimal(0.00000001),
                                min_quote_amount_increment=Decimal(0.01),
                                min_notional_size=Decimal(25)
                            )
        self._trading_rules['BRLBCH'] = TradingRule(
                                trading_pair='BRLBCH',
                                min_order_size=Decimal(0.001),
                                max_order_size=Decimal(1000),
                                min_price_increment=Decimal(.01),
                                min_base_amount_increment=Decimal(0.00000001),
                                min_quote_amount_increment=Decimal(0.01),
                                min_notional_size=Decimal(25)
                            )
        self._trading_rules['BRLDAI'] = TradingRule(
                                trading_pair='BRLDAI',
                                min_order_size=Decimal(1),
                                max_order_size=Decimal(1000000),
                                min_price_increment=Decimal(.01),
                                min_base_amount_increment=Decimal(0.00000001),
                                min_quote_amount_increment=Decimal(0.01),
                                min_notional_size=Decimal(25)
                            )
        self._trading_rules['BRLEOS'] = TradingRule(
                                trading_pair='BRLEOS',
                                min_order_size=Decimal(0.2),
                                max_order_size=Decimal(10000),
                                min_price_increment=Decimal(.01),
                                min_base_amount_increment=Decimal(0.00000001),
                                min_quote_amount_increment=Decimal(0.01),
                                min_notional_size=Decimal(25)
                            )
        self._trading_rules['BRLXRP'] = TradingRule(
                                trading_pair='BRLXRP',
                                min_order_size=Decimal(1.0),
                                max_order_size=Decimal(1000000),
                                min_price_increment=Decimal(.01),
                                min_base_amount_increment=Decimal(0.00000001),
                                min_quote_amount_increment=Decimal(0.01),
                                min_notional_size=Decimal(25)
                            )


    async def get_order_status(self, exchange_order_id: str, order_pair: str) -> Dict[str, Any]:
        """
        Example:
        {
            "id": 59378,
            "symbol": "ethusdt",
            "account-id": 100009,
            "amount": "10.1000000000",
            "price": "100.1000000000",
            "created-at": 1494901162595,
            "type": "buy-limit",
            "field-amount": "10.1000000000",
            "field-cash-amount": "1011.0100000000",
            "field-fees": "0.0202000000",
            "finished-at": 1494901400468,
            "user-id": 1000,
            "source": "api",
            "state": "filled",
            "canceled-at": 0,
            "exchange": "huobi",
            "batch": ""
        }

        "id": "U2FsdGVkX19BlCM4tNmxuvt24vgigdOyrEPUvGXIiEU=",
        "code": "SkvtQoOZf",
        "type": "buy",
        "subtype": "limited",
        "requested_amount": 0.02347418,
        "remaining_amount": 0,
        "unit_price": 42600,
        "status": "executed_completely",
        "create_date": "2017-12-08T23:42:54.960Z",
        "update_date": "2017-12-13T21:48:48.817Z",
        "pair": "BRLBTC",
        "total_price": 1000,
        "executed_amount": 0.02347418,
        "remaining_price": 0,
        "transactions": [

            {

                "amount": 0.2,
                "create_date": "2020-02-21 20:24:43.433",
                "total_price": 1000,
                "unit_price": 5000

            },

            {
                "amount": 0.2,
                "create_date": "2020-02-21 20:49:37.450",
                "total_price": 1000,
                "unit_price": 5000
            }

        ]
        """
        # https://api.bitcointrade.com.br/market/user_orders/{code}
        path_url = f"market/user_orders/{exchange_order_id}"
        return await self._api_request("get", path_url=path_url, is_auth_required=True)

    async def _update_order_status(self):
        cdef:
            # The poll interval for order status is 10 seconds.
            int64_t last_tick = <int64_t>(self._last_poll_timestamp / self.UPDATE_ORDERS_INTERVAL)
            int64_t current_tick = <int64_t>(self._current_timestamp / self.UPDATE_ORDERS_INTERVAL)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            for tracked_order in tracked_orders:
                exchange_order_id = await tracked_order.get_exchange_order_id()
                pair = tracked_order.order_pair
                try:
                    order_update = await self.get_order_status(exchange_order_id, pair)
                    self.logger().info(f"Order status  update response: {order_update}")
                except BitcoinTradeAPIError as e:
                    err_code = e.error_payload.get("error").get("err-code")
                    self.c_stop_tracking_order(tracked_order.client_order_id)
                    self.logger().info(f"The limit order {tracked_order.client_order_id} "
                                       f"has failed according to order status API. - {err_code}")
                    self.c_trigger_event(
                        self.MARKET_ORDER_FAILURE_EVENT_TAG,
                        MarketOrderFailureEvent(
                            self._current_timestamp,
                            tracked_order.client_order_id,
                            tracked_order.order_type
                        )
                    )
                    continue

                if order_update is None:
                    self.logger().network(
                        f"Error fetching status update for the order {tracked_order.client_order_id}: "
                        f"{order_update}.",
                        app_warning_msg=f"Could not fetch updates for the order {tracked_order.client_order_id}. "
                                        f"The order has either been filled or canceled."
                    )
                    continue

                order_state = order_update["status"]
                # possible order states are executed_completely / executed_partially / waiting / canceled

                if order_state not in ["executed_completely", "executed_partially", "waiting", "canceled"]:
                    self.logger().debug(f"Unrecognized order update response - {order_update} status:{order_state}")

                # Calculate the newly executed amount for this update.
                tracked_order.last_state = order_state
                new_confirmed_amount = Decimal(order_update["executed_amount"])  
                execute_amount_diff = Decimal(order_update["remaining_amount"])
                #? execute_amount_diff = new_confirmed_amount - tracked_order.executed_amount_base

                if execute_amount_diff > s_decimal_0:
                    tracked_order.executed_amount_base = new_confirmed_amount
                    tracked_order.executed_amount_quote = Decimal(order_update["executed_amount"]) * Decimal(order_update["unit_price"])
                    tracked_order.fee_paid = Decimal(order_update["executed_amount"]) * Decimal(order_update["unit_price"])
                    #? tracked_order.fee_paid = Decimal(order_update["fee"])
                    execute_price = Decimal(order_update["unit_price"])
                    order_filled_event = OrderFilledEvent(
                        self._current_timestamp,
                        tracked_order.client_order_id,
                        tracked_order.trading_pair,
                        tracked_order.trade_type,
                        tracked_order.order_type,
                        execute_price,
                        execute_amount_diff,
                        self.c_get_fee(
                            tracked_order.base_asset,
                            tracked_order.quote_asset,
                            tracked_order.order_type,
                            tracked_order.trade_type,
                            execute_price,
                            execute_amount_diff,
                        ),
                    )
                    self.logger().info(f"Filled {execute_amount_diff} out of {tracked_order.amount} of the "
                                       f"order {tracked_order.client_order_id}.")
                    self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)

                if tracked_order.is_open:
                    continue

                if tracked_order.is_done:
                    if not tracked_order.is_cancelled:  # Handles "filled" order
                        self.c_stop_tracking_order(tracked_order.client_order_id)
                        if tracked_order.trade_type is TradeType.BUY:
                            self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                               f"according to order status API.")
                            self.c_trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                                 BuyOrderCompletedEvent(self._current_timestamp,
                                                                        tracked_order.client_order_id,
                                                                        tracked_order.base_asset,
                                                                        tracked_order.quote_asset,
                                                                        tracked_order.fee_asset or tracked_order.base_asset,
                                                                        tracked_order.executed_amount_base,
                                                                        tracked_order.executed_amount_quote,
                                                                        tracked_order.fee_paid,
                                                                        tracked_order.order_type))
                        else:
                            self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                               f"according to order status API.")
                            self.c_trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                                 SellOrderCompletedEvent(self._current_timestamp,
                                                                         tracked_order.client_order_id,
                                                                         tracked_order.base_asset,
                                                                         tracked_order.quote_asset,
                                                                         tracked_order.fee_asset or tracked_order.quote_asset,
                                                                         tracked_order.executed_amount_base,
                                                                         tracked_order.executed_amount_quote,
                                                                         tracked_order.fee_paid,
                                                                         tracked_order.order_type))
                    else:  # Handles "canceled" or "partial-canceled" order
                        self.c_stop_tracking_order(tracked_order.client_order_id)
                        self.logger().info(f"The market order {tracked_order.client_order_id} "
                                           f"has been cancelled according to order status API.")
                        self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                             OrderCancelledEvent(self._current_timestamp,
                                                                 tracked_order.client_order_id))

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self._current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Bitcoin Trade. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def _trading_rules_polling_loop(self):
        while True:
            await self._update_trading_rules()
            await asyncio.sleep(60 * 5)


    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "account_id_initialized": self._account_id != "" if self._trading_required else True,
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    async def _get_ticker(self, pair: str):
        path_url = f'public/{pair}/ticker'
        return await self._api_request(
            "get",
            path_url=path_url,
            params=None,
            data=None,
            is_auth_required=False
        ) 

    async def place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal) -> str:
        path_url = 'market/create_order'
        side = 'buy' if is_buy else 'sell'
        order_type_str = 'limited' if order_type is OrderType.LIMIT else 'market'
        data = {
            'unit_price': f'{price:f}',
            'amount': f'{amount:f}',
            'subtype': order_type_str,
            'type': side,
            'pair': trading_pair.upper()
        }
        # raise ValueError(f"'unit_price': '{price:f}',\n'amount': '{amount:f}',\n'subtype': {order_type_str},\n'type': {side},\n'pair': {trading_pair.upper()}")
        if order_type is OrderType.MARKET:
            ticker = await self._get_ticker(trading_pair)
            data['unit_price'] = ticker['sell'] if is_buy else ticker['buy']

        place_order_resp = await self._api_request(
            "post",
            path_url=path_url,
            params=None,
            data=data,
            is_auth_required=True
        )
        return place_order_resp['code']

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object quote_amount
            object decimal_amount
            object decimal_price
            str exchange_order_id
            object tracked_order

        """
        if order_type is OrderType.MARKET:
            quote_amount = self.c_get_quote_volume_for_base_amount(trading_pair, True, amount).result_volume
            # Quantize according to price rules, not base token amount rules.
            decimal_amount = self.c_quantize_order_price(trading_pair, Decimal(quote_amount))
            decimal_price = s_decimal_0
        else:
            decimal_amount = self.c_quantize_order_amount(trading_pair, amount)
            decimal_price = self.c_quantize_order_price(trading_pair, price)
            if decimal_amount < trading_rule.min_order_size:
                raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                                 f"{trading_rule.min_order_size}.")
        """

        decimal_amount = self.c_quantize_order_amount(trading_pair, amount)
        decimal_price = self.c_quantize_order_price(trading_pair, price)
        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                            f"{trading_rule.min_order_size}.")

        try:
            temp_order_id = order_id
            exchange_order_id = await self.place_order(order_id, trading_pair, decimal_amount, True, order_type, decimal_price)
            self.c_start_tracking_order(
                client_order_id=temp_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.BUY,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} buy order {order_id} for {decimal_amount} {trading_pair}.")
            self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                 BuyOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     trading_pair,
                                     decimal_amount,
                                     decimal_price,
                                     order_id
                                 ))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(order_id)
            order_type_str = "MARKET" if order_type == OrderType.MARKET else "LIMIT"
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Bitcoin Trade for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to Bitcoin Trade. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, order_id, order_type))

    cdef str c_buy(self,
                   str trading_pair,
                   object amount,
                   object order_type=OrderType.MARKET,
                   object price=s_decimal_0,
                   dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str order_id = f"buy-{trading_pair}-{tracking_nonce}"

        safe_ensure_future(self.execute_buy(order_id, trading_pair, amount, order_type, price))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object decimal_amount
            object decimal_price
            str exchange_order_id
            object tracked_order

        decimal_amount = self.quantize_order_amount(trading_pair, amount)
        decimal_price = (self.c_quantize_order_price(trading_pair, price)
                         if order_type is OrderType.LIMIT
                         else s_decimal_0)
        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            temp_order_id = order_id
            exchange_order_id = await self.place_order(order_id, trading_pair, decimal_amount, False, order_type, decimal_price)
            self.c_start_tracking_order(
                client_order_id=temp_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.SELL,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} sell order {order_id} for {decimal_amount} {trading_pair}.")
            self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                 SellOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     trading_pair,
                                     decimal_amount,
                                     decimal_price,
                                     order_id
                                 ))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(order_id)
            order_type_str = "MARKET" if order_type is OrderType.MARKET else "LIMIT"
            self.logger().network(
                f"Error submitting sell {order_type_str} order to Bitcoin Trade for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to Bitcoin Trade. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, order_id, order_type))

    cdef str c_sell(self,
                    str trading_pair,
                    object amount,
                    object order_type=OrderType.MARKET, object price=s_decimal_0,
                    dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str order_id = f"sell-{trading_pair}-{tracking_nonce}"
        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, price))
        return order_id

    async def execute_cancel(self, trading_pair: str, order_id: str):
        try:
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is None:
                raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
            path_url = "market/user_orders"
            data = {
                'code': tracked_order.exchange_order_id
            }
            # raise ValueError(f"id:{tracked_order.exchange_order_id}")
            response = await self._api_request("delete", path_url=path_url, data=data, is_auth_required=True)
            self.logger().info(f"cancel response: {response}")
            return order_id

        except BitcoinTradeAPIError as e:
            order_state = e.error_payload.get("error").get("order-state")
            if order_state == 7:
                # order-state is canceled
                self.c_stop_tracking_order(tracked_order.client_order_id)
                self.logger().info(f"The order {tracked_order.client_order_id} has been cancelled according"
                                   f" to order status API. order_state - {order_state}")
                self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                     OrderCancelledEvent(self._current_timestamp,
                                                         tracked_order.client_order_id))
            else:
                self.logger().network(
                    f"Failed to cancel order {order_id}: {str(e)}",
                    exc_info=True,
                    app_warning_msg=f"Failed to cancel the order {order_id} on Bitcoin Trade. "
                                    f"Check API key and network connection."
                )

        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Bitcoin Trade. "
                                f"Check API key and network connection."
            )
        return None

    cdef c_cancel(self, str trading_pair, str order_id):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        *required
        Async function that cancels all active orders.
        Used by bot's top level stop and exit commands (cancelling outstanding orders on exit)
        :returns: List of CancellationResult which indicates whether each order is successfully cancelled.
        """
        incomplete_orders = [o for o in self._in_flight_orders.values() if not o.is_done] 
        tasks = [self.execute_cancel(o.trading_pair, o.client_order_id) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []

        #raise Exception(f"incomplete_order exchId {incomplete_orders[0].exchange_order_id}  pair{incomplete_orders[0].trading_pair}")

        try:
            async with timeout(timeout_seconds):
                results = await safe_gather(*tasks, return_exceptions=True)
                for client_order_id in results:
                    if type(client_order_id) is str:
                        order_id_set.remove(client_order_id)
                        successful_cancellations.append(CancellationResult(client_order_id, True))
                    else:
                        self.logger().warning(
                            f"failed to cancel order with error: "
                            f"{repr(client_order_id)}"
                        )
        except Exception as e:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order on BitcoinTrade. Check API key and network connection."
            )

        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef:
            dict order_books = self._order_book_tracker.order_books      
        if trading_pair not in order_books:            
            raise ValueError(f"No order book exists for '{trading_pair}'.")

        return order_books.get(trading_pair)

    cdef c_did_timeout_tx(self, str tracking_id):
        self.c_trigger_event(self.MARKET_TRANSACTION_FAILURE_EVENT_TAG,
                             MarketTransactionFailureEvent(self._current_timestamp, tracking_id))

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object order_type,
                                object trade_type,
                                object price,
                                object amount):
        self._in_flight_orders[client_order_id] = BitcoinTradeInFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        price_quantum = trading_rule.min_price_increment
        if "USD" in trading_pair:
            price_quantum = Decimal("0.01")
        return price_quantum

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price=s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object quantized_amount = MarketBase.c_quantize_order_amount(self, trading_pair, amount)
            object current_price = self.c_get_price(trading_pair, False)
            object notional_size

        # Check against min_order_size. If not passing check, return 0.
        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        # Check against max_order_size. If not passing check, return maximum.
        if quantized_amount > trading_rule.max_order_size:
            return trading_rule.max_order_size

        if price == s_decimal_0:
            notional_size = current_price * quantized_amount
        else:
            notional_size = price * quantized_amount
        # Add 1% as a safety factor in case the prices changed while making the order.
        if notional_size < trading_rule.min_notional_size * Decimal("1.01"):
            return s_decimal_0

        return quantized_amount
