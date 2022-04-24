import traceback
from pymongo.errors import ServerSelectionTimeoutError
from binance import AsyncClient, BinanceSocketManager
import logging


class QueueOverflow(Exception):
    pass


coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"
AGGTRADE_PYCACHE = 1000
AGGREGATED_TRADE_WS = "@aggTrade"
PRICE_P = 'p'
QUANTITY = 'q'
SYMBOL = 's'
EVENT_TIMESTAMP = 'E'


class AggtradeData(dict):
    def __init__(self):
        super().__init__()
        self.append_to_list_update()

    def append_to_list_update(self, symbol_pair=None, ohlc_data=None):
        if symbol_pair and ohlc_data:
            if symbol_pair in self:
                self[symbol_pair].append(ohlc_data)
            else:
                self[symbol_pair] = [ohlc_data]


class DatabaseCache:
    _cached_aggtrade_data = AggtradeData()

    @property
    def aggtrade_data(self):
        return self._cached_aggtrade_data


async def execute_ws_trades(alive_debug_secs):
    from data_staging import get_current_time, print_alive_if_passed_timestamp, get_data_from_keys, usdt_with_bnb_symbols_stream
    from MongoDB.db_actions import insert_many_to_aggtrade_db
    cache = DatabaseCache()
    pycache_counter = 0
    debug_running_execution = get_current_time()

    async with BinanceSocketManager(
            await AsyncClient.create()).multiplex_socket(usdt_with_bnb_symbols_stream(AGGREGATED_TRADE_WS)) as tscm:
        while True:
            try:


                ws_trade = await tscm.recv()
                aggtrade_data = ws_trade['data']
                aggtrade_symbol_pair = aggtrade_data[SYMBOL]

                pycache_counter += 1

                cache.aggtrade_data.append_to_list_update(aggtrade_symbol_pair, get_data_from_keys(aggtrade_data, EVENT_TIMESTAMP, PRICE_P, QUANTITY))

                if pycache_counter > AGGTRADE_PYCACHE:
                    await insert_many_to_aggtrade_db(cache.aggtrade_data)
                    cache.aggtrade_data.clear()
                    pycache_counter -= AGGTRADE_PYCACHE

                if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
                    debug_running_execution += alive_debug_secs

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

# TODO: clean symbols that start with usdt and not finish with them, acho que é um erro do binance... mas a variavel das moedas
#  tem 39 simbolos e os dicts 38, verificar qual falta.
# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.
