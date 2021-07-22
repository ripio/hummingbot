import time
import hmac
import hashlib
import urllib
from typing import Dict, Any, Tuple

import ujson


class BitcoinTradeAuth:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key: str = api_key
        self.secret_key: str = secret_key

    def add_auth_to_params(self,
                           method: str,
                           path_url: str,
                           args: Dict[str, Any]=None) -> Dict[str, Any]:
        auth_string = f'{self.api_key}'
        request = {
            "Content-Type": "application/json",
            "x-api-key": auth_string
        }
        if args is not None:
            request.update(args)
        
        return request
