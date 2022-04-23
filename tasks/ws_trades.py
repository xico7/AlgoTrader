import copy
import traceback
import requests
from pymongo.errors import ServerSelectionTimeoutError
from binance import AsyncClient, BinanceSocketManager
import MongoDB.db_actions as mongo
import logging
from data_staging import TIMESTAMP, OHLC_LOW, OHLC_CLOSE, OHLC_HIGH, OHLC_OPEN, TIME, RS, PRICE, VOLUME


class QueueOverflow(Exception):
    pass


coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"
AGGTRADE_PYCACHE = 1000
RS_CACHE = 2500
ATOMIC_INSERT_TIME = 2
OHLC_CACHE_PERIODS = 3
REL_STRENGTH_PERIODS = OHLC_CACHE_PERIODS - 1
CANDLESTICK_WS = "kline"
CANDLESTICKS_ONE_MINUTE_WS = f"@{CANDLESTICK_WS}_1m"
AGGREGATED_TRADE_WS = "@aggTrade"
PRICE_P = 'p'
QUANTITY = 'q'
SYMBOL = 's'
EVENT_TIMESTAMP = 'E'

SP500_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT', 'BCHUSDT',
                            'ALGOUSDT',
                            'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT', 'ICPUSDT',
                            'FILUSDT',
                            'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
                            'NEARUSDT', 'BTTUSDTXTZUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
                            'LRCUSDT']


class CoinsVolume(dict):

    def __init__(self):
        super().__init__()
        self.sum_value()

    def sum_value(self, symbol_pair=None, quantity=None):
        if symbol_pair and quantity:
            if symbol_pair in self:
                self[symbol_pair] += quantity
            else:
                self[symbol_pair] = quantity


class CoinsOhlcData(dict):
    def __init__(self):
        super().__init__()
        self.ohlc_update()

    def ohlc_update(self, new_ohlc_symbol=None, new_ohlc_values=None, cache_periods=None):
        if new_ohlc_symbol and new_ohlc_values:
            if new_ohlc_symbol not in self:
                self.update({new_ohlc_symbol: {1: new_ohlc_values}})
            else:
                atr_last_index = max(list(self[new_ohlc_symbol]))
                if atr_last_index < cache_periods:
                    self[new_ohlc_symbol][atr_last_index + 1] = new_ohlc_values
                else:
                    for elem in self[new_ohlc_symbol]:
                        if not elem == atr_last_index:
                            self[new_ohlc_symbol][elem] = self[new_ohlc_symbol][elem + 1]
                        else:
                            self[new_ohlc_symbol][atr_last_index] = new_ohlc_values


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


class SymbolCurrentOhlc(dict):
    def symbol_dict_update(self, symbol_pair=None, ohlc_data=None):
        if symbol_pair and ohlc_data:
            if symbol_pair in self:
                self[symbol_pair].append(ohlc_data)
            else:
                self[symbol_pair] = ohlc_data

    def symbol_ohlc_update(self, symbol_pair, ohlc_trade_data):
        symbol_data = self[symbol_pair]

        symbol_data[OHLC_CLOSE] = ohlc_trade_data[OHLC_CLOSE]
        symbol_data[VOLUME] = ohlc_trade_data[VOLUME]  # Binance already provides the total volume, no need to sum.

        if ohlc_trade_data[OHLC_HIGH] > symbol_data[OHLC_HIGH]:
            symbol_data[OHLC_HIGH] = ohlc_trade_data[OHLC_HIGH]
        if ohlc_trade_data[OHLC_LOW] < symbol_data[OHLC_LOW]:
            symbol_data[OHLC_LOW] = ohlc_trade_data[OHLC_LOW]


class MarketcapCurrentOhlc(dict):
    def __init__(self):
        super().__init__({'t': 0, 'h': 0, 'o': 0, 'l': 999999999999999, 'c': 0})
        self.refresh_ohlc()

    def __setitem__(self, key, value):
        super().__setitem__(key, value)

    def refresh_ohlc(self, cache=None):
        if cache:
            if self[TIMESTAMP] != cache.marketcap_latest_timestamp:
                self[TIMESTAMP] = cache.marketcap_latest_timestamp
                self[OHLC_OPEN] = 0
                self[OHLC_HIGH] = self[OHLC_CLOSE] = self[OHLC_LOW] = cache.marketcap_sum
            else:
                if self[OHLC_OPEN] == 0:
                    self[OHLC_OPEN] = cache.marketcap_sum

                self[OHLC_CLOSE] = cache.marketcap_sum
                if cache.marketcap_sum > self[OHLC_HIGH]:
                    self[OHLC_HIGH] = cache.marketcap_sum
                if cache.marketcap_sum < self[OHLC_LOW]:
                    self[OHLC_LOW] = cache.marketcap_sum


class MarketcapOHLC(dict):
    def __init__(self, current_marketcap_candle=None):
        super().__init__()
        self.my_update(current_marketcap_candle)

    def my_update(self, current_marketcap_ohlc):
        copy_cached_marketcap_ohlc = copy.deepcopy(current_marketcap_ohlc)
        if copy_cached_marketcap_ohlc:
            if not self:
                self.update({1: copy_cached_marketcap_ohlc})
                return self

            last_index = max(list(self))

            if last_index < OHLC_CACHE_PERIODS:
                self.update({last_index + 1: copy_cached_marketcap_ohlc})
            else:
                for elem in self:
                    if not elem == last_index:
                        self[elem] = self[elem + 1]
                    else:
                        self.update({OHLC_CACHE_PERIODS: copy_cached_marketcap_ohlc})


class RelStrength(dict):
    def __init__(self, cache=None):
        super().__init__()
        self.my_update(cache)

    def my_update(self, cache):
        if cache:
            from data_staging import remove_usdt, get_current_time

            def calculate_relative_strength(coin_ohlc_data, cached_marketcap_ohlc_data) -> float:
                coin_change_percentage = ((float(coin_ohlc_data[len(coin_ohlc_data)][OHLC_OPEN]) / float(
                    coin_ohlc_data[1][OHLC_OPEN])) - 1) * 100
                try:
                    market_change_percentage = ((cached_marketcap_ohlc_data[len(coin_ohlc_data)][OHLC_OPEN] /
                                                 cached_marketcap_ohlc_data[1][OHLC_OPEN]) - 1) * 100
                except ZeroDivisionError:
                    return 0  # unlikely case, no better solution found.

                return coin_change_percentage - market_change_percentage

            for coin_ohlc_data in cache.coins_ohlc_data.items():
                if len(coin_ohlc_data[1]) == OHLC_CACHE_PERIODS:
                    try:
                        get_coin_volume = cache.coins_volume[remove_usdt(coin_ohlc_data[0])]
                        get_coin_moment_price = cache.coins_moment_price[remove_usdt(coin_ohlc_data[0])]
                    except KeyError:
                        continue

                    self[coin_ohlc_data[0]] = {TIME: get_current_time(),
                                               RS: calculate_relative_strength(coin_ohlc_data[1], copy.deepcopy(
                                                   cache.marketcap_ohlc_data)),
                                               VOLUME: get_coin_volume, PRICE: get_coin_moment_price}


class UpdateException(Exception):
    pass


class DatabaseCache:
    _cached_coins_volume = CoinsVolume()
    _cached_coins_moment_price = {}
    _cached_marketcap_coins_value = {}
    _cached_marketcap_sum = 0

    _cached_marketcap_latest_timestamp = 0
    _cached_marketcap_current_ohlc = MarketcapCurrentOhlc()
    _cached_coins_current_ohlcs = SymbolCurrentOhlc()

    _cached_marketcap_ohlc_data = MarketcapOHLC()
    _cached_coins_ohlc_data = CoinsOhlcData()

    _cached_aggtrade_data = AggtradeData()
    _coins_rel_strength = RelStrength()

    @property
    def aggtrade_data(self):
        return self._cached_aggtrade_data

    @property
    def coins_volume(self):
        return self._cached_coins_volume

    @property
    def coins_moment_price(self):
        return self._cached_coins_moment_price

    @property
    def marketcap_coins_value(self):
        return self._cached_marketcap_coins_value

    @property
    def marketcap_sum(self):
        return self._cached_marketcap_sum

    @marketcap_sum.setter
    def marketcap_sum(self, value):
        self._cached_marketcap_sum = value

    @property
    def marketcap_latest_timestamp(self):
        return self._cached_marketcap_latest_timestamp

    @marketcap_latest_timestamp.setter
    def marketcap_latest_timestamp(self, value):
        self._cached_marketcap_latest_timestamp = value

    @property
    def marketcap_current_ohlc(self):
        return self._cached_marketcap_current_ohlc

    @property
    def coins_current_ohlcs(self):
        return self._cached_coins_current_ohlcs

    @property
    def marketcap_ohlc_data(self):
        return self._cached_marketcap_ohlc_data

    @property
    def coins_ohlc_data(self):
        return self._cached_coins_ohlc_data

    @property
    def coins_rel_strength(self):
        return self._coins_rel_strength


async def execute_ws_trades(alive_debug_secs):
    from MongoDB.db_ohlc_create import insert_ohlc_data
    from data_staging import update_ohlc_cached_values, get_current_time, get_last_minute, \
        remove_usdt, print_alive_if_passed_timestamp, get_data_from_keys, usdt_with_bnb_symbols_stream, get_coin_fund_ratio

    coin_ratio = get_coin_fund_ratio(remove_usdt(SP500_SYMBOLS_USDT_PAIRS), requests.get(coingecko_marketcap_api_link).json())
    ta_lines_db = mongo.connect_to_ta_lines_db()
    initiate_time_counter = debug_running_execution = get_current_time()

    cache = DatabaseCache()
    pycache_counter = 0
    done_timestamp = 0

    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket(
            usdt_with_bnb_symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) + usdt_with_bnb_symbols_stream(
                AGGREGATED_TRADE_WS)) as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
                cur_time = get_current_time()
                if cur_time > initiate_time_counter + ATOMIC_INSERT_TIME and cache.marketcap_latest_timestamp > 0:
                    initiate_time_counter += ATOMIC_INSERT_TIME
                    cache.marketcap_current_ohlc.refresh_ohlc(cache)
                    if len(cache.marketcap_ohlc_data) == OHLC_CACHE_PERIODS:
                        #cache.coins_rel_strength.my_update(cache)
                        cache.coins_volume.clear()

                        if cur_time % 15 in [-1, 0, 1]:
                            #await mongo.rs_insert_many_from_dict_async_two(cache.coins_rel_strength)
                            cache.coins_rel_strength.clear()

                        if cur_time % 60 in [-1, 0, 1] and (
                        latest_ohlc_open_timestamp := get_last_minute(cur_time - 3)) != done_timestamp:
                            done_timestamp = latest_ohlc_open_timestamp
                            insert_ohlc_data(latest_ohlc_open_timestamp)

                if CANDLESTICK_WS in ws_trade['stream']:
                    await update_ohlc_cached_values(ws_trade['data']['k'], cache)

                elif AGGREGATED_TRADE_WS in ws_trade['stream']:
                    pycache_counter += 1

                    aggtrade_data, symbol_pair, coin_symbol = ws_trade['data'], ws_trade['data'][SYMBOL], remove_usdt(ws_trade['data'][SYMBOL])
                    coin_moment_price = float(aggtrade_data[PRICE_P])

                    cache.coins_moment_price.update({coin_symbol: coin_moment_price})
                    cache.coins_volume.sum_value(coin_symbol, float(aggtrade_data[QUANTITY]))

                    if symbol_pair in SP500_SYMBOLS_USDT_PAIRS:
                        cache.marketcap_coins_value.update(
                            {coin_symbol: (float(aggtrade_data[PRICE_P]) * coin_ratio[coin_symbol])})
                        cache.marketcap_sum = sum(list(cache.marketcap_coins_value.values()))

                    cache.aggtrade_data.append_to_list_update(symbol_pair, get_data_from_keys(aggtrade_data, EVENT_TIMESTAMP, PRICE_P, QUANTITY))

                    if pycache_counter > AGGTRADE_PYCACHE:
                        await mongo.insert_many_from_dict_async_two(ta_lines_db, cache.aggtrade_data)
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
