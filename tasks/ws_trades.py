import traceback
from pymongo.errors import ServerSelectionTimeoutError
from binance import AsyncClient, BinanceSocketManager
import logging

from data_func import Aggtrade, CacheAggtrades
from vars_constants import AGGTRADE_PYCACHE


class QueueOverflow(Exception):
    pass

# TODO: clean symbols that start with usdt and not finish with them, acho que é um erro do binance... mas a variavel das moedas
#  tem 39 simbolos e os dicts 38, verificar qual falta.
# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.
#TODO: IF ws_trades is not running, have a self_healing_aggtrades with the binance API.


async def execute_ws_trades():
    from data_staging import usdt_with_bnb_symbols_stream

    cache = CacheAggtrades()
    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket(usdt_with_bnb_symbols_stream("@aggTrade")) as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
                cache.append(vars(Aggtrade(**ws_trade['data'])))
                if len(cache) > AGGTRADE_PYCACHE:
                    cache.insert_clear()

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


