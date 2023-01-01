import aiohttp
from aiohttp.test_utils import TestClient
import asyncio
from decimal import Decimal
# from libc.stdint import int64_t
import logging
from async_timeout import timeout
import time
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
    TYPE_CHECKING
)
import ujson

from hummingbot.core.clock import Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.connector.exchange.ripio_trade.ripio_trade_user_stream_tracker import RipioTradeUserStreamTracker
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketTransactionFailureEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.ripio_trade.ripio_trade_api_order_book_data_source import RipioTradeAPIOrderBookDataSource
from hummingbot.connector.exchange.ripio_trade.ripio_trade_auth import RipioTradeAuth
from hummingbot.connector.exchange.ripio_trade.ripio_trade_in_flight_order import RipioTradeInFlightOrder
from hummingbot.connector.exchange.ripio_trade.ripio_trade_order_book_tracker import RipioTradeOrderBookTracker
from hummingbot.connector.exchange.ripio_trade.ripio_trade_utils import (
    convert_to_exchange_trading_pair,
    convert_from_exchange_trading_pair,
    get_new_client_order_id,)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.core.utils.estimate_fee import estimate_fee

import hummingbot.connector.exchange.ripio_trade.ripio_trade_constants as CONSTANTS
import hummingbot.connector.exchange.ripio_trade.ripio_trade_web_utils as web_utils

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

hm_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("NaN")
# RIPIOTRADE_ROOT_API = "https://api.ripiotrade.co/v3"

# # BITCOINTRADE
# class RipioTradeAPIError(IOError):
#     def __init__(self, error_payload: Dict[str, Any]):
#         super().__init__(str(error_payload))
#         self.error_payload = error_payload


# class RipioTradeExchangeTransactionTracker(TransactionTracker):
#         RipioTradeExchange _owner

#     def __init__(self, owner: RipioTradeExchange):
#         super().__init__()
#         self._owner = owner

#     cdef c_did_timeout_tx(self, str tx_id):
#         TransactionTracker.c_did_timeout_tx(self, tx_id)
#         self._owner.c_did_timeout_tx(tx_id)


class RipioTradeExchange(ExchangeBase):
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated
    API_CALL_TIMEOUT = 10.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 120.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global hm_logger
        if hm_logger is None:
            hm_logger = logging.getLogger(__name__)
        return hm_logger

    def __init__(self,
                client_config_map: "ClientConfigAdapter",
                ripio_trade_api_key: str,
                # poll_interval: float = 30.0,
                trading_pairs: Optional[List[str]] = None,
                trading_required: bool = True):

        super().__init__(client_config_map)
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._ev_loop = asyncio.get_event_loop()
        self._ripiotrade_auth = RipioTradeAuth(api_key=ripio_trade_api_key)
        self._shared_client = aiohttp.ClientSession()
        self._order_book_tracker = RipioTradeOrderBookTracker(
            trading_pairs=trading_pairs
        )
        self._user_stream_tracker = RipioTradeUserStreamTracker(
            self._ripiotrade_auth, trading_pairs, self._shared_client
        )
        self._precision = {}
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._trading_required = trading_required
        self._in_flight_orders = {}
        self._order_not_found_records = {}
        self._trading_rules = {}
        self._last_poll_timestamp = 0
        
        self._status_polling_task = None
        self._trading_rules_polling_task = None
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None
        # self._poll_interval = poll_interval
        # self._tx_tracker = RipioTradeExchangeTransactionTracker(self)

    @property
    def name(self) -> str:
        return "ripio_trade"
    
    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books
    
    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, RipioTradeInFlightOrder]:
        return self._in_flight_orders
    
    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        A dictionary of statuses of various connector's components.
        """
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "user_stream_initialized":
                self._user_stream_tracker.data_source.last_recv_time > 0 if self._trading_required else True,
        }

    @property
    def ready(self) -> bool:
        """
        :return True when all statuses pass, this might take 5-10 seconds for all the connector's components and
        services to be ready.
        """
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]
        
    @property
    def tracking_states(self) -> Dict[str, Any]:
        """
        :return active in-flight orders in json format, is used to save in sqlite db.
        """
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
            if not value.is_done
        }
        
    @property
    def shared_client(self) -> str:
        return self._shared_client

    @shared_client.setter
    def shared_client(self, client: aiohttp.ClientSession):
        self._shared_client = client
        
    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        """
        Restore in-flight orders from saved tracking states, this is st the connector can pick up on where it left off
        when it disconnects.
        :param saved_states: The saved tracking_states.
        """
        self._in_flight_orders.update({
            key: RipioTradeInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })   
        
        
    def supported_order_types(self):
        """
        :return a list of OrderType supported by this connector.
        Note that Market order type is no longer required and will not be used.
        """
        return [OrderType.LIMIT]
        
    def start(self, clock: Clock, timestamp: float):
        """
        This function is called automatically by the clock.
        """
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        """
        This function is called automatically by the clock.
        """
        super().stop(clock)
        
    async def start_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        It starts tracking order book, polling trading rules,
        updating statuses and tracking user data.
        """
        # self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    # def _stop_network(self):
    #     self._order_book_tracker.stop()
    #     if self._status_polling_task is not None:
    #         self._status_polling_task.cancel()
    #         self._status_polling_task = None
    #     if self._trading_rules_polling_task is not None:
    #         self._trading_rules_polling_task.cancel()
    #         self._trading_rules_polling_task = None

    # TODO: check 
    async def stop_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        """
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
            self._user_stream_tracker_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None   
        
    async def check_network(self) -> NetworkStatus:
        """
        This function is required by NetworkIterator base class and is called periodically to check
        the network connection. Simply ping the network (or call any light weight public API).
        """
        try:
            resp = await self._api_request(method="GET", path_url=CONSTANTS.SERVER_TIME_PATH_URL)
            if "timestamp" not in resp:
                raise
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED   
        
    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client
        
    async def _trading_rules_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60 * 5)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rules.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Ripio Trade. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)
        
    async def _update_trading_rules(self):
        pairs_info = await self._api_request(
            method="GET",
            path_url=CONSTANTS.PAIRS_PATH_URL
        )
        currencies_info = await self._api_request(
            "GET", 
            path_url=CONSTANTS.CURRENCIES
        )
        self._trading_rules.clear()
        self._trading_rules = self._format_trading_rules(pairs_info, currencies_info)
        # pairs_info = await self._api_request("get", path_url="public/pairs")
        # currencies_info = await self._api_request("get", path_url="public/currencies")
        # currencies_info_map = { i["code"] : i for i in currencies_info["list"]}
        # self._trading_rules.clear()            
        # for pair_info in pairs_info["list"]:
        #     if pair_info["enabled"]:
        #         trading_pair = convert_from_exchange_trading_pair(pair_info["symbol"])
        #         self._trading_rules[trading_pair] = TradingRule(
        #             trading_pair=trading_pair,
        #             min_order_size=Decimal(pair_info["min_amount"]),
        #             # max_order_size=Decimal(10),
        #             min_price_increment=Decimal(pair_info["price_tick"]), #
        #             min_base_amount_increment=Decimal(10**(-currencies_info_map[pair_info["base"]]["precision"])),
        #             # min_quote_amount_increment=Decimal(0.01), #
        #             min_notional_size=Decimal(pair_info["min_value"])
        #         )
    
    def _format_trading_rules(self, pairs_info: Dict[str, Any], currencies_info: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.
        :param market_info: The json API response
        :return A dictionary of trading rules.
        Response Example:
        {
            "message": null,
            "data": [
                {
                "base": "BTC",
                "base_name": "Bitcoin",
                "enabled": true,
                "min_amount": 0.0005,
                "min_value": 10,
                "price_tick": 1,
                "quote": "BRL",
                "quote_name": "Brazilian real",
                "symbol": "BTC_BRL"
                }
            ]
        }
        """
        currencies_info_map = { i["code"] : i for i in currencies_info["list"]}
        trading_rules = {}
        for pair_info in pairs_info["list"]:
            if pair_info["enabled"]:
                trading_pair = convert_from_exchange_trading_pair(pair_info["symbol"])
                self._precision[pair_info["base"]] = currencies_info_map[pair_info["base"]]["precision"]
                trading_rules[trading_pair] = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=Decimal(pair_info["min_amount"]),
                    # max_order_size=Decimal(10),
                    min_price_increment=Decimal(pair_info["price_tick"]), #
                    min_base_amount_increment=Decimal(10**(-currencies_info_map[pair_info["base"]]["precision"])),
                    # min_quote_amount_increment=Decimal(10**(-currencies_info_map[pair_info["quote"]]["precision"])), #
                    min_notional_size=Decimal(pair_info["min_value"])
                )
        return trading_rules
           
    async def _api_request(self,
                           method: str,
                           path_url: str,
                           pair: str = None,
                           params: Optional[Dict[str, Any]] = None,
                           data=None,
                           is_auth_required: bool = False) -> Dict[str, Any]:
        url = ''
        headers = None
        if is_auth_required:   
            url = web_utils.private_rest_url(path_url, params)
            headers = self._ripiotrade_auth.add_auth_to_params()
        else:
            url = web_utils.public_rest_url(path_url, pair, params)
        client = self._shared_client or await self._http_client()
        # if headers is not None:
        response_coro = client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                # params=params,
                data= ujson.dumps(data) if data is not None else None,
                timeout=self.API_CALL_TIMEOUT
            )
        # else:
        #      response_coro = await client.get(
        #             url=url
        #         )
        
        
        async with response_coro as response:
            if response.status != 200:
                raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}.")
            try:
                parsed_response = await response.json()
            except Exception:
                raise IOError(f"Error parsing data from {url}.")

            data = parsed_response["data"]
            if data is None:
                self.logger().error(f"Error received for {url}. Response is {parsed_response}.")
                raise Exception({"error": parsed_response})
            if type(data) is list:
                data_list_to_dct = {"list": data}
                return data_list_to_dct
            return data

    def get_order_price_quantum(self, trading_pair: str , price: Decimal):
        """
        Returns a price step, a minimum price increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal):
        """
        Returns an order amount step, a minimum amount increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def get_order_book(self, trading_pair: str):
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type = OrderType.LIMIT,
            price: Decimal = s_decimal_0,
            **kwargs) -> str:
        """
        Buys an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for BuyOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to buy from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        client_id = get_new_client_order_id(is_buy=True, trading_pair=trading_pair, max_id_len=CONSTANTS.MAX_ORDER_ID_LEN)        
        safe_ensure_future(self.execute_order(TradeType.BUY, client_id, trading_pair, amount, order_type, price))
        return client_id

    def sell(self,
            trading_pair: str,
            amount: Decimal,
            order_type = OrderType.LIMIT,
            price: Decimal = s_decimal_0,
            **kwargs) -> str:
        """
        Sells an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for SellOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to sell from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        client_id = get_new_client_order_id(is_buy=False, trading_pair=trading_pair, max_id_len=CONSTANTS.MAX_ORDER_ID_LEN)        
        safe_ensure_future(self.execute_order(TradeType.SELL, client_id, trading_pair, amount, order_type, price))
        return client_id
    
    def cancel(self, trading_pair: str, order_id: str):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id
    
    async def execute_order(self,
                          trade_type: TradeType,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0):
        trading_rule = self._trading_rules[trading_pair]
        decimal_amount = self.quantize_order_amount(trading_pair, Decimal(amount))
        decimal_price = self.quantize_order_price(trading_pair, Decimal(price))
        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"{trade_type} order amount {decimal_amount} is lower than the minimum order size {trading_rule.min_order_size}.")

        is_buy = True if trade_type is TradeType.BUY else False
        try:
            temp_order_id = order_id
            exchange_order_id = await self.place_order(order_id, trading_pair, decimal_amount, is_buy, order_type, decimal_price)
            self.start_tracking_order(
                client_order_id=temp_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=trade_type,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} {trade_type} order {order_id} for {decimal_amount} {trading_pair}.")
            if trade_type is TradeType.BUY:
                self.trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                    BuyOrderCreatedEvent(
                                        self.current_timestamp,
                                        order_type,
                                        trading_pair,
                                        decimal_amount,
                                        decimal_price,
                                        order_id,
                                        tracked_order.creation_timestamp
                                    ))
            else:
                self.trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                    SellOrderCreatedEvent(
                                        self.current_timestamp,
                                        order_type,
                                        trading_pair,
                                        decimal_amount,
                                        decimal_price,
                                        order_id,
                                        tracked_order.creation_timestamp
                                    ))
        except asyncio.CancelledError:
            raise
        except ValueError:
            self.logger().network(
                f"Error submitting {trade_type} LIMIT order to Ripio Trade for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit {trade_type} order to Ripio Trade. The order price is greater than or equal to the lowest selling price in the market."
            )
            self.trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))
        except Exception:
            self.stop_tracking_order(order_id)
            order_type_str = "LIMIT" #"MARKET" if order_type == OrderType.MARKET else "LIMIT"
            self.logger().network(
                f"Error submitting {trade_type} {order_type_str} order to Ripio Trade for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit {trade_type} order to Ripio Trade. Check API key and network connection."
            )
            self.trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))
    
    def start_tracking_order(self,
                            client_order_id: str,
                            exchange_order_id: str,
                            trading_pair: str,
                            order_type: OrderType,
                            trade_type: TradeType,
                            price: Decimal,
                            amount: Decimal):
        """
        Starts tracking an order by simply adding it into _in_flight_orders dictionary.
        """
        self._in_flight_orders[client_order_id] = RipioTradeInFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount,
            creation_timestamp=self.current_timestamp
        )
        
    def stop_tracking_order(self, order_id: str):
        """
        Stops tracking an order by simply removing it from _in_flight_orders dictionary.
        """
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]
            
    async def execute_cancel(self, trading_pair: str, order_id: str):
        """
        Executes order cancellation process by first calling cancel-order API. The API result doesn't confirm whether
        the cancellation is successful, it simply states it receives the request.
        :param trading_pair: The market trading pair
        :param order_id: The internal order id
        order.last_state to change to CANCELED
        """
        try:
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is None:
                raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
            path_url = CONSTANTS.CANCEL_PATH_URL
            data = {
                'id': tracked_order.exchange_order_id
            }
            # raise ValueError(f"id:{tracked_order.exchange_order_id}")
            response = await self._api_request("DELETE", path_url=path_url, data=data, is_auth_required=True)
            self.logger().info(f"cancel response: {response}")
            return order_id

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Ripio Trade. "
                                f"Check API key and network connection.")
            
    async def _status_polling_loop(self):
        """
        Periodically update user balances and order status via REST API. This serves as a fallback measure for web
        socket API updates.
        """
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Ripio Trade. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)
            
    async def _update_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        new_available_balances = {}
        new_balances = {}
        data = await self._api_request("GET", path_url=CONSTANTS.BALANCE_PATH_URL, is_auth_required=True)
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
    
    
    def _order_update(self, tracked_order, order_update):
        order_state = order_update["status"]
        client_order_id = tracked_order.client_order_id
        

        tracked_order.last_state = order_state
        
        new_confirmed_amount = Decimal(order_update["executed_amount"])
        execute_amount_diff = new_confirmed_amount - tracked_order.executed_amount_base
        
        if tracked_order.is_cancelled:
            self.logger().info(f"Successfully canceled order {client_order_id}.")
            self.trigger_event(MarketEvent.OrderCancelled,
                               OrderCancelledEvent(
                                   self.current_timestamp,
                                   client_order_id))
            # tracked_order.cancelled_event.set()
            self.stop_tracking_order(client_order_id)
            return

        if execute_amount_diff > s_decimal_0:
            tracked_order.executed_amount_base = new_confirmed_amount
            tracked_order.executed_amount_quote = Decimal(order_update["executed_amount"]) * Decimal(order_update["price"])
            tracked_order.fee_paid = Decimal(order_update["executed_amount"]) * Decimal(order_update["price"])
            # tracked_order.fee_paid = Decimal(order_update["fee"])
            execute_price = Decimal(order_update["price"])
            order_filled_event = OrderFilledEvent(
                self.current_timestamp,
                tracked_order.client_order_id,
                tracked_order.trading_pair,
                tracked_order.trade_type,
                tracked_order.order_type,
                execute_price,
                execute_amount_diff,
                # self.get_fee(
                #     tracked_order.base_asset,
                #     tracked_order.quote_asset,
                #     tracked_order.order_type,
                #     tracked_order.trade_type,
                #     execute_price,
                #     execute_amount_diff,
                # ),
            )
            self.logger().info(f"Filled {execute_amount_diff} out of {tracked_order.amount} of the "
                                f"order {tracked_order.client_order_id}.")
            self.trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)

        if tracked_order.is_open:
            return

        if tracked_order.is_done:
            self.stop_tracking_order(tracked_order.client_order_id)
            if tracked_order.trade_type is TradeType.BUY:
                self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                    f"according to order status API.")
                self.trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                        BuyOrderCompletedEvent(self.current_timestamp,
                                                            tracked_order.client_order_id,
                                                            tracked_order.base_asset,
                                                            tracked_order.quote_asset,
                                                            tracked_order.executed_amount_base,
                                                            tracked_order.executed_amount_quote,
                                                            tracked_order.order_type))
            else:
                self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                    f"according to order status API.")
                self.trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                        SellOrderCompletedEvent(self.current_timestamp,
                                                                tracked_order.client_order_id,
                                                                tracked_order.base_asset,
                                                                tracked_order.quote_asset,
                                                                tracked_order.executed_amount_base,
                                                                tracked_order.executed_amount_quote,
                                                                tracked_order.order_type))
            # else:  # Handles "canceled"
            #     self.stop_tracking_order(tracked_order.client_order_id)
            #     self.logger().info(f"The market order {tracked_order.client_order_id} "
            #                         f"has been cancelled according to order status API.")
            #     self.trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
            #                             OrderCancelledEvent(self.current_timestamp,
            #                                                 tracked_order.client_order_id))
    
    async def _update_order_status(self):
        """
        Calls REST API to get status update for each in-flight order.
        """
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        
        order_update = None
        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            for tracked_order in tracked_orders:
                exchange_order_id = await tracked_order.get_exchange_order_id()
                try:
                    order_update = await self.get_order_status(exchange_order_id)
                    self.logger().info(f"Order status  update response: {order_update}")
                except Exception as e:
                    # err_code = e.error_payload.get("error").get("err-code")
                    self.stop_tracking_order(tracked_order.client_order_id)
                    self.logger().info(f"The limit order {tracked_order.client_order_id} "
                                       f"has failed according to order status API. - {e}")
                    self.trigger_event(
                        self.MARKET_ORDER_FAILURE_EVENT_TAG,
                        MarketOrderFailureEvent(
                            self.current_timestamp,
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
                
                self._order_update(tracked_order, order_update)

                # order_state = order_update["status"]
                # # possible order states are executed_completely / executed_partially / open / canceled

                # if order_state not in ["executed_completely", "executed_partially", "open", "canceled"]:
                #     self.logger().debug(f"Unrecognized order update response - {order_update} status:{order_state}")

                # Calculate the newly executed amount for this update.
                # tracked_order.last_state = order_state
                # new_confirmed_amount = Decimal(order_update["executed_amount"])
                # execute_amount_diff = new_confirmed_amount - tracked_order.executed_amount_base

                # if execute_amount_diff > s_decimal_0:
                #     tracked_order.executed_amount_base = new_confirmed_amount
                #     tracked_order.executed_amount_quote = Decimal(order_update["executed_amount"]) * Decimal(order_update["price"])
                #     tracked_order.fee_paid = Decimal(order_update["executed_amount"]) * Decimal(order_update["price"])
                #     # tracked_order.fee_paid = Decimal(order_update["fee"])
                #     execute_price = Decimal(order_update["price"])
                #     order_filled_event = OrderFilledEvent(
                #         self.current_timestamp,
                #         tracked_order.client_order_id,
                #         tracked_order.trading_pair,
                #         tracked_order.trade_type,
                #         tracked_order.order_type,
                #         execute_price,
                #         execute_amount_diff,
                #         # self.get_fee(
                #         #     tracked_order.base_asset,
                #         #     tracked_order.quote_asset,
                #         #     tracked_order.order_type,
                #         #     tracked_order.trade_type,
                #         #     execute_price,
                #         #     execute_amount_diff,
                #         # ),
                #     )
                #     self.logger().info(f"Filled {execute_amount_diff} out of {tracked_order.amount} of the "
                #                        f"order {tracked_order.client_order_id}.")
                #     self.trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)

                # if tracked_order.is_open:
                #     continue

                # if tracked_order.is_done:
                #     if not tracked_order.is_cancelled:  # Handles "filled" order
                #         self.stop_tracking_order(tracked_order.client_order_id)
                #         if tracked_order.trade_type is TradeType.BUY:
                #             self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                #                                f"according to order status API.")
                #             self.trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                #                                  BuyOrderCompletedEvent(self.current_timestamp,
                #                                                         tracked_order.client_order_id,
                #                                                         tracked_order.base_asset,
                #                                                         tracked_order.quote_asset,
                #                                                         tracked_order.executed_amount_base,
                #                                                         tracked_order.executed_amount_quote,
                #                                                         tracked_order.order_type))
                #         else:
                #             self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                #                                f"according to order status API.")
                #             self.trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                #                                  SellOrderCompletedEvent(self.current_timestamp,
                #                                                          tracked_order.client_order_id,
                #                                                          tracked_order.base_asset,
                #                                                          tracked_order.quote_asset,
                #                                                          tracked_order.executed_amount_base,
                #                                                          tracked_order.executed_amount_quote,
                #                                                          tracked_order.order_type))
                #     else:  # Handles "canceled"
                #         self.stop_tracking_order(tracked_order.client_order_id)
                #         self.logger().info(f"The market order {tracked_order.client_order_id} "
                #                            f"has been cancelled according to order status API.")
                #         self.trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                #                              OrderCancelledEvent(self.current_timestamp,
                #                                                  tracked_order.client_order_id))
    
    # TODO             
    # _process_order_message
    def _process_trade_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        """
        client_order_id = order_msg["user_id"]
        if client_order_id not in self._in_flight_orders:
            return
        tracked_order = self._in_flight_orders[client_order_id]

        # Update order execution status
        # tracked_order.last_state = order_msg["status"]
        
        # if tracked_order.is_cancelled:
        #     self.logger().info(f"Successfully canceled order {client_order_id}.")
        #     self.trigger_event(MarketEvent.OrderCancelled,
        #                        OrderCancelledEvent(
        #                            self.current_timestamp,
        #                            client_order_id))
        #     tracked_order.cancelled_event.set()
        #     self.stop_tracking_order(client_order_id)
        #     return
            
        # if tracked_order.is_done() and not tracked_order.is_canceled():
        self._order_update(tracked_order, order_msg)
            # updated = tracked_order.update_with_trade_update(order_msg)
            # if not updated:
            #     return
            
            # self.trigger_event(
            #     MarketEvent.OrderFilled,
            #     OrderFilledEvent(
            #         self.current_timestamp,
            #         tracked_order.client_order_id,
            #         tracked_order.trading_pair,
            #         tracked_order.trade_type,
            #         tracked_order.order_type,
            #         Decimal(str(order_msg["price"])),
            #         Decimal(str(order_msg["amount"])),
            #         exchange_trade_id=order_msg["id"]
            #     )
            # )
            # self.logger().info(f"The {tracked_order.trade_type.name} order "
            #                    f"{tracked_order.client_order_id} has completed "
            #                    f"according to order status API.")
            # event_tag = MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY \
            #     else MarketEvent.SellOrderCompleted
            # event_class = BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY \
            #     else SellOrderCompletedEvent
            # self.trigger_event(event_tag,
            #                    event_class(self.current_timestamp,
            #                                tracked_order.client_order_id,
            #                                tracked_order.base_asset,
            #                                tracked_order.quote_asset,
            #                                tracked_order.executed_amount_base,
            #                                tracked_order.executed_amount_quote,
            #                                tracked_order.order_type,
            #                                tracked_order.exchange_order_id))
            # self.stop_tracking_order(tracked_order.client_order_id)

        
            
        
        # if order_msg["status"] != "settled":
        #     return

        # ex_order_id = order_msg["order_id"]

        # client_order_id = None
        # for track_order in self.in_flight_orders.values():
        #     if track_order.exchange_order_id == ex_order_id:
        #         client_order_id = track_order.client_order_id
        #         break

        # if client_order_id is None:
        #     return

        # tracked_order = self.in_flight_orders[client_order_id]
        # updated = tracked_order.update_with_trade_update(order_msg)
        # if not updated:
        #     return

        # self.trigger_event(
        #     MarketEvent.OrderFilled,
        #     OrderFilledEvent(
        #         self.current_timestamp,
        #         tracked_order.client_order_id,
        #         tracked_order.trading_pair,
        #         tracked_order.trade_type,
        #         tracked_order.order_type,
        #         Decimal(str(order_msg["price"])),
        #         Decimal(str(order_msg["quantity"])),
        #         AddedToCostTradeFee(
        #             flat_fees=[TokenAmount(order_msg["fee_currency_id"], Decimal(str(order_msg["fee_amount"])))]
        #         ),
        #         exchange_trade_id=order_msg["id"]
        #     )
        # )
        # if math.isclose(tracked_order.executed_amount_base, tracked_order.amount) or \
        #         tracked_order.executed_amount_base >= tracked_order.amount:
        #     tracked_order.last_state = "filled"
        #     self.logger().info(f"The {tracked_order.trade_type.name} order "
        #                        f"{tracked_order.client_order_id} has completed "
        #                        f"according to order status API.")
        #     event_tag = MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY \
        #         else MarketEvent.SellOrderCompleted
        #     event_class = BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY \
        #         else SellOrderCompletedEvent
        #     self.trigger_event(event_tag,
        #                        event_class(self.current_timestamp,
        #                                    tracked_order.client_order_id,
        #                                    tracked_order.base_asset,
        #                                    tracked_order.quote_asset,
        #                                    tracked_order.executed_amount_base,
        #                                    tracked_order.executed_amount_quote,
        #                                    tracked_order.order_type,
        #                                    tracked_order.exchange_order_id))
        #     self.stop_tracking_order(tracked_order.client_order_id)
    
    # TODO
    # get_open_orders
            
    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        open_orders = [o for o in self._in_flight_orders.values() if o.is_open]
        # if len(open_orders) == 0:
        #     return []
        cancel_order_ids = [o.client_order_id for o in open_orders]
        order_id_set = set(cancel_order_ids)
        # self.logger().debug(f"cancel_order_ids {cancel_order_ids} {open_orders}")

        tasks = [self.execute_cancel(o.trading_pair, o.client_order_id) for o in open_orders]
        cancellation_results = []

        try:
            async with timeout(timeout_seconds):
                results = await safe_gather(*tasks, return_exceptions=True)
                for client_order_id in results:
                    if type(client_order_id) is str:
                        order_id_set.remove(client_order_id)
                        cancellation_results.append(CancellationResult(client_order_id, True))
                    else:
                        self.logger().warning(
                            f"Failed to cancel order with RipioTrade error: "
                            f"{repr(client_order_id)}"
                        )
        except Exception as e:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order on Ripio Trade. Check API key and network connection."
            )

        for client_order_id in order_id_set:
            cancellation_results.append(CancellationResult(client_order_id, False))

        return cancellation_results        
            
    def tick(self,  timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp
        
    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))
        # return estimate_fee("ripio_trade", is_maker)
        
    # TODO
    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Ripio Trade. Check API key and network connection."
                )
                await asyncio.sleep(1.0)
                
    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue. The messages are put in by
        RipioTradeAPIUserStreamDataSource.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                if "success" in event_message or "topic" not in event_message and event_message["topic"] not in CONSTANTS.WS_PRIVATE_CHANNELS:
                    continue
                channel = event_message["topic"]

                if channel in ["balance"]:
                    for balance_details in event_message["body"]["balances"]:
                        self._account_balances[balance_details["currency"]] = Decimal(str(balance_details["amount"]))
                        self._account_available_balances[balance_details["currency"]] = Decimal(str(balance_details["amount"])) - Decimal(str(balance_details["locked_amount"]))
                # elif channel in ["open_order"]:
                #     for order_update in event_message["data"]:
                #         self._process_order_message(order_update)
                elif channel in ["order_status"]:
                    self._process_trade_message(event_message["body"])

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)
    
    async def all_trading_pairs(self) -> List[str]:
        # This method should be removed and instead we should implement _initialize_trading_pair_symbol_map
        return await RipioTradeAPIOrderBookDataSource.fetch_trading_pairs(client=self._shared_client)

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        # This method should be removed and instead we should implement _get_last_traded_price
        return await RipioTradeAPIOrderBookDataSource.get_last_traded_prices(trading_pairs=trading_pairs, client=self._shared_client)
    
    # TODO check
    async def get_order_status(self, exchange_order_id: str) -> Dict[str, Any]:
        """
        Example:
        {
            "message": null,
            "data": {
                "average_execution_price": 42600,
                "create_date": "2017-12-08T23:42:54.960Z",
                "executed_amount": 0.02347418,
                "id": "8DE12108-4643-4E9F-8425-0172F1B96876",
                "remaining_amount": 0,
                "requested_amount": 0.02347418,
                "remaining_value": 0,
                "pair": "BTC_BRL",
                "price": 42600,
                "side": "buy",
                "status": "executed_completely",
                "tax_amount": 0.002,
                "total_value": 1000,
                "type": "limit",
                "update_date": "2017-12-13T21:48:48.817Z",
                "transactions": [
                {
                    "amount": 0.2,
                    "create_date": "2020-02-21 20:24:43.433",
                    "price": 5000,
                    "total_value": 1000
                },
                {
                    "amount": 0.2,
                    "create_date": "2020-02-21 20:49:37.450",
                    "price": 5000,
                    "total_value": 1000
                }
                ]
            }
        }
        """
        path_url = CONSTANTS.ORDER_STATUS_BY_ID_URL_PATH.format(exchange_order_id) #f"market/user_orders/{exchange_order_id}"
        return await self._api_request("GET", path_url=path_url, is_auth_required=True)    

    async def place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal) -> str:
        # path_url = web_utils.private_rest_url(CONSTANTS.NEW_ORDER_PATH_URL)
        side = 'buy' if is_buy else 'sell'
        # order_type_str = 'market' if order_type is OrderType.MARKET else 'limited'
        base = trading_pair.split("-")[0]
        data = {
            'external_id': order_id,
            'pair': convert_to_exchange_trading_pair(trading_pair),
            'side': side,
            'type': 'limit',
            'amount': amount, #f'{amount:.{self._precision[base]}f}',
            'price': price, #f'{price:f}',
        }

        # raise ValueError(f"'unit_price': '{price:f}',\n'amount': '{amount:f}',\n'subtype': {order_type_str},\n'type': {side},\n'pair': {trading_pair.upper()}")
        # if order_type is OrderType.MARKET:
        #     data['unit_price'] = self.get_price(trading_pair, is_buy)

        # is_crossed = False
        # Reject limit maker order if they price cross the orderbook
        # if is_buy and order_type is OrderType.LIMIT_MAKER and self.get_price(trading_pair, is_buy) <= price:
        #     is_crossed = True
        # if not is_buy and order_type is OrderType.LIMIT_MAKER and self.get_price(trading_pair, is_buy) >= price:
        #     is_crossed = True

        # if is_crossed:
        #     raise ValueError("Error placing order. The price of such order cross the orderbook.")

        place_order_resp = await self._api_request(
            "POST",
            path_url=CONSTANTS.NEW_ORDER_PATH_URL,
            params=None,
            data=data,
            is_auth_required=True
        )
        return place_order_resp['id']

    # def get_price(self, trading_pair: str, is_buy: bool) -> Decimal:
    #     return self.c_get_price(trading_pair, is_buy)    