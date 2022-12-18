import re
from typing import (
    Optional,
    Tuple)
# from hummingbot.client.config.config_var import ConfigVar
# from hummingbot.client.config.config_methods import using_exchange
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from pydantic import Field, SecretStr


TRADING_PAIR_SPLITTER_FROM_EXCHANGE = re.compile(r"^(ARS|USDC|BTC)(\w+)$")
TRADING_PAIR_SPLITTER_TO_EXCHANGE = re.compile(r"^(\w+)(ARS|USDC|BTC)$")

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDC"

DEFAULT_FEES = [0.0, 0.0]


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    splitted = TRADING_PAIR_SPLITTER_FROM_EXCHANGE.match(exchange_trading_pair)
    if splitted is None:
        return None

    quote_asset, base_asset = splitted.group(1), splitted.group(2)
    return f"{base_asset}-{quote_asset}"


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    cut_pair = hb_trading_pair.replace("-", "")
    splitted = TRADING_PAIR_SPLITTER_TO_EXCHANGE.match(cut_pair)
    base_asset, quote_asset = splitted.group(1), splitted.group(2)
    
    # !!! currency unit prices are quoted as currency pairs CurrencyBase
    return f"{quote_asset}{base_asset}"


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
