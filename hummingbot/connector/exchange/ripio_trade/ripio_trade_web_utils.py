import  hummingbot.connector.exchange.ripio_trade.ripio_trade_constants as CONSTANTS
from typing import (
    Any,
    Dict,
    Optional,
)

def __query(params: Optional[Dict[str, Any]] = None):
    if params is None:
        return ""
    
    result = '?'
    for k, v in params.items():
        result += str(k) + '=' + str(v) + '&'
        
    return result[:-1]
    

def public_rest_url(path_url: str, pair: str = None, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + CONSTANTS.API_VERSION + "/public" + path_url + (pair or "") + __query(params)


def private_rest_url(path_url: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + CONSTANTS.API_VERSION + path_url +  __query(params)