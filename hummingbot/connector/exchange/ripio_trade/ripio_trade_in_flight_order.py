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
        # TODO: RipioTrade does not have a 'fail' order status.
        return NotImplementedError
        # return self.last_state in {"canceled"}

    @property
    def is_open(self) -> bool:
        return self.last_state in {"open", "executed_partially"}
    
    # def update_with_trade_update(self, trade_update: Dict[str, Any]) -> bool:
    #     """
    #     Updates the in flight order with trade update (from GET /trade_history end point)
    #     return: True if the order gets updated otherwise False
    #     """
    #     # trade_id = trade_update["id"]
    #     if str(trade_update["id"]) != self.exchange_order_id:
    #         return False
    #     # self.trade_id_set.add(trade_id)
    #     if "executed_amount" in trade_update:
    #         self.executed_amount_base = Decimal(str(trade_update["executed_amount"]))
    #     # self.fee_paid += Decimal(str(trade_update["fee_amount"]))
    #         if "average_execution_price" in trade_update:
    #             self.executed_amount_quote = Decimal(str(trade_update["executed_amount"])) * Decimal(str(trade_update["average_execution_price"]))
    #     # if not self.fee_asset:
    #     #     self.fee_asset = trade_update["fee_currency_id"]
    #     return True