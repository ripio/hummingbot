import aiohttp
import hummingbot.connector.exchange.ripio_trade.ripio_trade_constants as CONSTANTS
import hummingbot.connector.exchange.ripio_trade.ripio_trade_web_utils as web_utils
from typing import (
    Any,
    Dict
)


class RipioTradeAuth:
    def __init__(self, api_key: str):
        self.api_key: str = api_key
        self.auth_ticket = None

    def add_auth_to_params(self,
                           args: Dict[str, Any] = None) -> Dict[str, Any]:
        auth_string = f'{self.api_key}'
        request = {
            "Content-Type": "application/json",
            "Authorization": auth_string
        }
        if args is not None:
            request.update(args)
        return request

    async def get_ws_ticket(self) -> str:
        if self.auth_ticket is not None:
            return self.auth_ticket
        
        self.auth_ticket = await self.get_auth_token()
        return self.auth_ticket
        
    async def get_auth_ticket(self):
        http_client = aiohttp.ClientSession()
        try:
            resp = await http_client.post(url=web_utils.private_rest_url(CONSTANTS.TICKET_PATH_URL),
                                            headers=self.add_auth_to_params())
            token_resp = await resp.json()

            if resp.status != 200:
                raise ValueError(f"Error occurred retrieving new Auth Ticket. Response: {token_resp}")

            # POST /token endpoint returns both access_token and expires_in
            # Updates _oauth_token_expiration_time

            return token_resp["data"]["ticket"]
        except Exception as e:
            raise e