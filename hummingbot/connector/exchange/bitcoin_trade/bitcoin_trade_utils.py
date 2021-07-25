import re
from typing import (
    Optional,
    Tuple)
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange


TRADING_PAIR_SPLITTER_FROM_EXCHANGE = re.compile(r"^(BRL)(\w+)$")
TRADING_PAIR_SPLITTER_TO_EXCHANGE = re.compile(r"^(\w+)(BRL)$")

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-BRL"

DEFAULT_FEES = [0.0, 0.0]


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    try:
        cut_pair = trading_pair.replace("-", "")
        m = TRADING_PAIR_SPLITTER_FROM_EXCHANGE.match(cut_pair)
        if m is None:
            m = TRADING_PAIR_SPLITTER_TO_EXCHANGE.match(cut_pair)
            if m is None:
                raise Exception(f"Incorrect slpit: {trading_pair} => zero")
        # !!! currency unit prices are quoted as currency pairs CurrencyBase
        return m.group(1).upper() if not m.group(1).upper() == 'BRL' else m.group(2).upper(), 'BRL'
    # Exceptions are now logged as warnings in trading pair fetcher
    except Exception:
        return None


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None

    base_asset, quote_asset = split_trading_pair(exchange_trading_pair)
    return f"{base_asset}-{quote_asset}"


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    base_asset, quote_asset = split_trading_pair(hb_trading_pair)
    # !!! currency unit prices are quoted as currency pairs CurrencyBase
    return f"{quote_asset}{base_asset}"


KEYS = {
    "bitcoin_trade_api_key":
        ConfigVar(key="bitcoin_trade_api_key",
                  prompt="Enter your Bitcoin Trade API key >>> ",
                  required_if=using_exchange("bitcoin_trade"),
                  is_secure=True,
                  is_connect_key=True)
}
