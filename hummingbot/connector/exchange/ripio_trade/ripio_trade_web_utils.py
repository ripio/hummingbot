import  hummingbot.connector.exchange.ripio_trade.ripio_trade_constants as CONSTANTS

def public_rest_url(path_url: str, pair: str = None) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + CONSTANTS.API_VERSION + "/public" + path_url + (pair or "")


def private_rest_url(path_url: str) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + CONSTANTS.API_VERSION + path_url