import contextlib
import logging
from binance import AsyncClient, BinanceSocketManager
import logs
from data_func import CacheAggtrades
from data_staging import usdt_with_bnb_symbols_aggtrades


# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class QueueOverflow(Exception): pass


async def execute_ws_trades():
    cache_symbols_parsed = CacheAggtrades()

    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket(usdt_with_bnb_symbols_aggtrades()) as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
            except Exception as e:
                with contextlib.suppress(KeyError):
                    if ws_trade['m'] == 'Queue overflow. Message not filled':
                        raise QueueOverflow("Queue Overflor error while trying to parse websocket trade with data: '%s'.", ws_trade)

                LOG.exception("Error while trying to parse websocket trade with data: '%s'.", ws_trade)
                exit(2)

            cache_symbols_parsed.append(ws_trade['data'])
            cache_symbols_parsed.insert_clear()



