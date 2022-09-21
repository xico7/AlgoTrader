import contextlib
import logging


from binance import AsyncClient, BinanceSocketManager
import logs
from data_handling.data_func import CacheAggtrades
from data_handling.data_helpers.data_staging import usdt_with_bnb_symbols_aggtrades


# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.
# TODO: Add "recover data" from https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
#  if/when ws-trades fails.
# TODO: Implement ws-trades validator from startup time.. with recover from above's TODO,
#  with this i can assume the program is self-healing, only run parse_trades after that.


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class QueueOverflow(Exception): pass


def execute_past_trades():
    print("here")


async def execute_ws_trades():
    from datetime import datetime
    import time
    print(datetime.fromtimestamp(time.time()))
    cache_symbols_parsed = CacheAggtrades()

    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket(usdt_with_bnb_symbols_aggtrades()) as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
            except Exception:
                with contextlib.suppress(KeyError):
                    if ws_trade['m'] == 'Queue overflow. Message not filled':
                        raise QueueOverflow("Queue Overflow error while trying to parse websocket trade with data: '%s'.", ws_trade)

                LOG.exception("Error while trying to parse websocket trade with data: '%s'.", ws_trade)
                exit(2)

            cache_symbols_parsed.append(ws_trade['data'])
            cache_symbols_parsed.insert_clear()



