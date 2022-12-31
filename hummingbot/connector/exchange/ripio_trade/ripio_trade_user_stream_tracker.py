import logging
from typing import (
    List,
    Optional,
)

import aiohttp

import hummingbot.connector.exchange.ripio_trade.ripio_trade_constants as CONSTANTS
from hummingbot.connector.exchange.ripio_trade.ripio_trade_api_user_stream_data_source import \
    RipioTradeAPIUserStreamDataSource
from hummingbot.connector.exchange.ripio_trade.ripio_trade_auth import RipioTradeAuth
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.logger import HummingbotLogger


class RipioTradeUserStreamTracker(UserStreamTracker):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 ripiotrade_auth: Optional[RipioTradeAuth] = None,
                 trading_pairs: Optional[List[str]] = None,
                 shared_client: Optional[aiohttp.ClientSession] = None):
        self._shared_client = shared_client
        self._ripiotrade_auth: RipioTradeAuth = ripiotrade_auth
        self._trading_pairs: List[str] = trading_pairs or []
        super().__init__(data_source=RipioTradeAPIUserStreamDataSource(
            ripio_trade_auth=self._ripiotrade_auth,
            trading_pairs=self._trading_pairs,
            shared_client=self._shared_client,
        ))

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        """
        *required
        Initializes a user stream data source (user specific order diffs from live socket stream)
        :return: OrderBookTrackerDataSource
        """
        if not self._data_source:
            self._data_source = RipioTradeAPIUserStreamDataSource(
                ripiotrade_auth=self._ripiotrade_auth,
                trading_pairs=self._trading_pairs,
                domain=self._domain,
                shared_client=self._shared_client,
            )
        return self._data_source

    @property
    def exchange_name(self) -> str:
        """
        *required
        Name of the current exchange
        """
        return CONSTANTS.EXCHANGE_NAME

    async def start(self):
        """
        *required
        Start all listeners and tasks
        """
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
