from binance import AsyncClient, BinanceSocketManager
from data_func import Aggtrade, CacheAggtrades
from support.helper_func import output_error
from vars_constants import AGGTRADE_PYCACHE
from data_staging import usdt_with_bnb_symbols_stream, lower_add_aggtrade


# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.

class QueueOverflow(Exception):
    pass


async def execute_ws_trades():
    cache_symbols_parsed = CacheAggtrades()

    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket(lower_add_aggtrade(usdt_with_bnb_symbols_stream())) as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
                cache_symbols_parsed.append(vars(Aggtrade(**ws_trade['data'])))
                if len(cache_symbols_parsed) > AGGTRADE_PYCACHE:
                    cache_symbols_parsed.insert_clear()
            except Exception as e:
                if ws_trade['m'] == 'Queue overflow. Message not filled':
                    raise QueueOverflow

                output_error(f"{e}, {ws_trade}")




