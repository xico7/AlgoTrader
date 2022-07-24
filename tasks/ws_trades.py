import traceback
from pymongo.errors import ServerSelectionTimeoutError
from binance import AsyncClient, BinanceSocketManager
import logging
from data_func import Aggtrade, CacheAggtrades
from vars_constants import AGGTRADE_PYCACHE
from data_staging import usdt_with_bnb_symbols_stream


# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.

class QueueOverflow(Exception):
    pass


async def execute_ws_trades():

    cache_symbols_parsed = CacheAggtrades()
    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket(usdt_with_bnb_symbols_stream("@aggTrade")) as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
                cache_symbols_parsed.append(vars(Aggtrade(**ws_trade['data'])))
                if len(cache_symbols_parsed) > AGGTRADE_PYCACHE:
                    cache_symbols_parsed.insert_clear()

            except ServerSelectionTimeoutError as e:
                if "localhost:27017" in e.args[0]:
                    logging.exception("Cannot connect to mongo DB")
                    raise
                else:
                    logging.exception("Unexpected error")
                    raise
            except Exception as e:
                traceback.print_exc()
                print(f"{e}, {ws_trade}")

                if ws_trade['m'] == 'Queue overflow. Message not filled':
                    raise QueueOverflow
                exit(1)


