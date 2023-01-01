import re
import os
import socket
import uuid
from typing import (
    Optional,
    Tuple)
# from hummingbot.client.config.config_var import ConfigVar
# from hummingbot.client.config.config_methods import using_exchange
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from pydantic import Field, SecretStr
from hashlib import md5


# TRADING_PAIR_SPLITTER_FROM_EXCHANGE = re.compile(r"^(ARS|USDC|BTC)(\w+)$")
# TRADING_PAIR_SPLITTER_TO_EXCHANGE = re.compile(r"^(\w+)(ARS|USDC|BTC)$")

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDC"

DEFAULT_FEES = [0.0, 0.0]


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    # splitted = TRADING_PAIR_SPLITTER_FROM_EXCHANGE.match(exchange_trading_pair)
    # if splitted is None:
    #     return None

    # quote_asset, base_asset = splitted.group(1), splitted.group(2)[1:]
    # return f"{quote_asset}-{base_asset}"
    base, quote = exchange_trading_pair.split("_")
    return f"{base}-{quote}"
    


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    base, quote = hb_trading_pair.split("-")
    return f"{base}_{quote}"

def get_new_client_order_id(
        is_buy: bool, 
        trading_pair: str, 
        hbot_order_id_prefix: str = "", 
        max_id_len: Optional[int] = None
    ) -> str:
    """
    Creates a client order id for a new order

    Note: If the need for much shorter IDs arises, an option is to concatenate the host name, the PID,
    and the nonce, and hash the result.

    :param is_buy: True if the order is a buy order, False otherwise
    :param trading_pair: the trading pair the order will be operating with
    :param hbot_order_id_prefix: The hummingbot-specific identifier for the given exchange
    :param max_id_len: The maximum length of the ID string.
    :return: an identifier for the new order to be used in the client
    """
    side = "B" if is_buy else "S"
    exch_symbol = convert_to_exchange_trading_pair(trading_pair)
    client_instance_id = uuid. uuid4()
    client_order_id = f"{hbot_order_id_prefix}{side}{exch_symbol}{client_instance_id}"
    if max_id_len is not None:
        client_order_id = client_order_id[:max_id_len]
    return client_order_id
    


class RipioTradeConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="ripio_trade", client_data=None)
    ripio_trade_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Ripio Trade API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    class Config:
        title = "ripio_trade"

KEYS = RipioTradeConfigMap.construct()

# KEYS = {
#     "ripio_trade_api_key":
#         ConfigVar(key="ripio_trade_api_key",
#                   prompt="Enter your Ripio Trade API key >>> ",
#                   required_if=using_exchange("ripio_trade"),
#                   is_secure=True,
#                   is_connect_key=True)
# }
