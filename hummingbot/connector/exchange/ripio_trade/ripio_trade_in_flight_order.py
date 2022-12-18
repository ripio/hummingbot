from decimal import Decimal
from typing import (
    Any,
    Dict
)

from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


class RipioTradeInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 creation_timestamp: float,
                 initial_state: str = "waiting"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            creation_timestamp,
            initial_state 
        )
        self.fee_asset = self.quote_asset

    @property
    def is_done(self) -> bool:
        return self.last_state in {"canceled", "executed_completely"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"canceled"}

    @property
    def is_failure(self) -> bool:
        # TODO: ProBit does not have a 'fail' order status.
        return NotImplementedError
        # return self.last_state in {"canceled"}

    @property
    def is_open(self) -> bool:
        return self.last_state in {"waiting", "executed_partially"}