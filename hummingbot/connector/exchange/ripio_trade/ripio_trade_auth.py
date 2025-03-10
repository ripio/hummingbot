from typing import (
    Any,
    Dict
)


class RipioTradeAuth:
    def __init__(self, api_key: str):
        self.api_key: str = api_key

    def add_auth_to_params(self,
                           method: str,
                           path_url: str,
                           args: Dict[str, Any] = None) -> Dict[str, Any]:
        auth_string = f'{self.api_key}'
        request = {
            "Content-Type": "application/json",
            "x-api-key": auth_string
        }
        if args is not None:
            request.update(args)
        return request
