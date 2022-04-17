import traceback
import requests
from pymongo.errors import ServerSelectionTimeoutError
from binance import AsyncClient, BinanceSocketManager
import MongoDB.db_actions as mongo
import logging




class QueueOverflow(Exception):
    pass


# TODO: can i  remove setters with this new way of updating values???

coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"
AGGTRADE_PYCACHE = 1000
RS_CACHE = 2500
ATOMIC_INSERT_TIME = 2
CANDLESTICK_WS = "kline"
CANDLESTICKS_ONE_MINUTE_WS = f"@{CANDLESTICK_WS}_1m"
AGGREGATED_TRADE_WS = "@aggTrade"
PRICE_P = 'p'
QUANTITY = 'q'
SYMBOL = 's'
EVENT_TIMESTAMP = 'E'
OHLC_CACHE_PERIODS = 70
REL_STRENGTH_PERIODS = OHLC_CACHE_PERIODS - 1

SP500_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT', 'BCHUSDT',
                            'ALGOUSDT',
                            'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT', 'ICPUSDT',
                            'FILUSDT',
                            'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
                            'NEARUSDT', 'BTTUSDTXTZUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
                            'LRCUSDT']


class DatabaseCache:
    _cached_coins_volume = {}
    _cached_coins_moment_price = {}
    _cached_marketcap_coins_value = {}
    _cached_marketcap_sum = 0

    _cached_marketcap_latest_timestamp = 0
    _cached_marketcap_current_ohlc = {'t': 0, 'h': 0, 'o': 0, 'l': 999999999999999, 'c': 0}
    _cached_coins_current_ohlcs = {}

    _cached_marketcap_ohlc_data = {}
    _cached_coins_ohlc_data = {}

    _cached_aggtrade_data = {}
    _coins_rel_strength = {}

    def update_cache(self):
        print("here")

    def update_coins_volume(self, coin_symbol, coin_moment_trade_quantity):
        if coin_symbol in self._cached_coins_volume:
            self._cached_coins_volume[coin_symbol] += coin_moment_trade_quantity
        else:
            self._cached_coins_volume.update({coin_symbol: coin_moment_trade_quantity})

    def update_marketcap_coins_value(self, coin_symbol: str, coin_moment_price: float, coin_ratio):
        self._cached_marketcap_coins_value.update({coin_symbol: (float(coin_moment_price) * coin_ratio[coin_symbol])})

    def update_current_marketcap_ohlc_data(self):
        from data_staging import TIMESTAMP, OHLC_OPEN, OHLC_HIGH, OHLC_CLOSE, OHLC_LOW
        if self.marketcap_current_ohlc[TIMESTAMP] != self.marketcap_latest_timestamp:
            self.marketcap_current_ohlc[TIMESTAMP] = self.marketcap_latest_timestamp
            self.marketcap_current_ohlc[OHLC_OPEN] = 0
            self.marketcap_current_ohlc[OHLC_HIGH] = self.marketcap_current_ohlc[OHLC_CLOSE] = \
                self.marketcap_current_ohlc[OHLC_LOW] = self.marketcap_sum
        else:
            if self.marketcap_current_ohlc[OHLC_OPEN] == 0:
                self.marketcap_current_ohlc[OHLC_OPEN] = self.marketcap_sum

            self.marketcap_current_ohlc[OHLC_CLOSE] = self.marketcap_sum
            if self.marketcap_sum > self.marketcap_current_ohlc[OHLC_HIGH]:
                self.marketcap_current_ohlc[OHLC_HIGH] = self.marketcap_sum
            if self.marketcap_sum < self.marketcap_current_ohlc[OHLC_LOW]:
                self.marketcap_current_ohlc[OHLC_LOW] = self.marketcap_sum


    @property
    def aggtrade_data(self):
        return self._cached_aggtrade_data

    @aggtrade_data.setter
    def aggtrade_data(self, value):
        if value == {}:
            self._cached_aggtrade_data = {}
            return

        coin_symbol = list(value.keys())[0]
        coin_data = list(value.values())[0]

        if coin_symbol not in self._cached_aggtrade_data:
            self._cached_aggtrade_data[coin_symbol] = [coin_data]
        else:
            self._cached_aggtrade_data[coin_symbol].append(coin_data)

    @property
    def coins_volume(self):
        return self._cached_coins_volume

    @coins_volume.setter
    def coins_volume(self, value):
        self._cached_coins_volume = value

    @property
    def coins_moment_price(self):
        return self._cached_coins_moment_price

    @coins_moment_price.setter
    def coins_moment_price(self, value):
        self._cached_coins_moment_price = value

    @property
    def marketcap_coins_value(self):
        return self._cached_marketcap_coins_value

    @marketcap_coins_value.setter
    def marketcap_coins_value(self, value):
        self._cached_marketcap_coins_value = value

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

    @marketcap_current_ohlc.setter
    def marketcap_current_ohlc(self, value):
        self._cached_marketcap_current_ohlc = value

    @property
    def coins_current_ohlcs(self):
        return self._cached_coins_current_ohlcs

    @coins_current_ohlcs.setter
    def coins_current_ohlcs(self, value):
        self._cached_coins_current_ohlcs = value

    @property
    def marketcap_ohlc_data(self):
        return self._cached_marketcap_ohlc_data

    @marketcap_ohlc_data.setter
    def marketcap_ohlc_data(self, value):
        self._cached_marketcap_ohlc_data = value

    @property
    def coins_ohlc_data(self):
        return self._cached_coins_ohlc_data

    @coins_ohlc_data.setter
    def coins_ohlc_data(self, value):
        self._cached_coins_ohlc_data = value

    @property
    def coins_rel_strength(self):
        return self._coins_rel_strength

    @coins_rel_strength.setter
    def coins_rel_strength(self, value):
        if value == {}:
            self._coins_rel_strength = {}
            return
        for coin_symbol, coin_rel_strength in value.items():
            if coin_symbol not in self._coins_rel_strength:
                self._coins_rel_strength[coin_symbol] = [coin_rel_strength]
            else:
                self._coins_rel_strength[coin_symbol].append(coin_rel_strength)


class TACache:
    _ta_chart_value = {}
    _average_true_range_percentage = {}
    _relative_volume = {}

    @property
    def atrp(self):
        return self._average_true_range_percentage

    @atrp.setter
    def atrp(self, value):
        _atrp = value

    @property
    def rel_vol(self):
        return self._relative_volume

    @rel_vol.setter
    def rel_vol(self, value):
        _relative_volume = value

    @property
    def ta_chart(self):
        return self._ta_chart_value

    @ta_chart.setter
    def ta_chart(self, value):
        _ta_chart_value = value


async def execute_ws_trades(alive_debug_secs):
    from MongoDB.db_ohlc_create import insert_ohlc_data
    from data_staging import get_current_time, update_current_marketcap_ohlc_data, update_relative_strength_cache, \
        remove_usdt, update_ohlc_cached_values, print_alive_if_passed_timestamp, get_data_from_keys, usdt_with_bnb_symbols_stream, \
        update_cached_marketcap_coins_value, update_cached_coin_volumes, get_coin_fund_ratio

    abc = BinanceSocketManager(await AsyncClient.create())
    multisocket_candle = abc.multiplex_socket(
        usdt_with_bnb_symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) + usdt_with_bnb_symbols_stream(AGGREGATED_TRADE_WS))

    coin_ratio = get_coin_fund_ratio(remove_usdt(SP500_SYMBOLS_USDT_PAIRS), requests.get(coingecko_marketcap_api_link).json())
    ta_lines_db = mongo.connect_to_ta_lines_db()
    rel_strength_db = mongo.connect_to_rs_db()
    ohlc_1m_db = mongo.connect_to_1m_ohlc_db()
    ohlc_5m_db = mongo.connect_to_5m_ohlc_db()
    ohlc_15m_db = mongo.connect_to_15m_ohlc_db()
    ohlc_1h_db = mongo.connect_to_1h_ohlc_db()
    ohlc_4h_db = mongo.connect_to_4h_ohlc_db()
    ohlc_1d_db = mongo.connect_to_1d_ohlc_db()

    initiate_time_counter = debug_running_execution = get_current_time()
    db_cache = DatabaseCache()
    pycache_counter = 0
    rs_cache_counter = 0
    duplicated_finished_ohlc_open_timestamp = 0

    async with multisocket_candle as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
                cur_time = get_current_time()
                if cur_time > initiate_time_counter + ATOMIC_INSERT_TIME and db_cache.marketcap_latest_timestamp > 0:
                    initiate_time_counter += ATOMIC_INSERT_TIME
                    db_cache.marketcap_current_ohlc = db_cache.update_current_marketcap_ohlc_data()
                    if len(db_cache.marketcap_ohlc_data) == OHLC_CACHE_PERIODS:
                        db_cache.coins_rel_strength, rs_cache_counter = update_relative_strength_cache(
                            db_cache.marketcap_ohlc_data, db_cache.coins_ohlc_data,
                            db_cache.coins_volume, db_cache.coins_moment_price, rs_cache_counter)
                        db_cache.coins_volume = {}
                        is_new_minute_ohlc = cur_time % 60 == 0 or (cur_time + 1) % 60 == 0 or (
                                cur_time - 1) % 60 == 0

                        if rs_cache_counter > RS_CACHE or is_new_minute_ohlc:
                            await mongo.duplicate_insert_data_rs_volume_price(rel_strength_db,
                                                                              db_cache.coins_rel_strength)
                            db_cache.coins_rel_strength = {}
                            rs_cache_counter = 0
                            if is_new_minute_ohlc:
                                finished_ohlc_open_timestamp = cur_time
                                while (finished_ohlc_open_timestamp - 3) % 60 != 0:
                                    finished_ohlc_open_timestamp -= 1
                                finished_ohlc_open_timestamp -= 3
                                if finished_ohlc_open_timestamp != duplicated_finished_ohlc_open_timestamp:
                                    duplicated_finished_ohlc_open_timestamp = finished_ohlc_open_timestamp
                                    insert_ohlc_data(finished_ohlc_open_timestamp,
                                                                   ohlc_1m_db, ohlc_5m_db, ohlc_15m_db,
                                                                   ohlc_1h_db, ohlc_4h_db, ohlc_1d_db)


                if CANDLESTICK_WS in ws_trade['stream']:
                    db_cache.coins_current_ohlcs, db_cache.coins_ohlc_data, db_cache.marketcap_ohlc_data, db_cache.marketcap_latest_timestamp = \
                        await update_ohlc_cached_values(ws_trade['data']['k'], db_cache)

                elif AGGREGATED_TRADE_WS in ws_trade['stream']:
                    pycache_counter += 1

                    aggtrade_data, symbol_pair = ws_trade['data'], ws_trade['data'][SYMBOL]
                    coin_moment_price = float(aggtrade_data[PRICE_P])
                    coin_symbol = remove_usdt(symbol_pair)

                    if coin_symbol:
                        db_cache.coins_moment_price.update({coin_symbol: coin_moment_price})
                        db_cache.update_coins_volume(coin_symbol, float(aggtrade_data[QUANTITY]))

                        if symbol_pair in SP500_SYMBOLS_USDT_PAIRS:
                            db_cache.update_marketcap_coins_value(coin_symbol, coin_moment_price, coin_ratio)
                            db_cache.marketcap_sum = sum(list(db_cache.marketcap_coins_value.values()))

                    db_cache.aggtrade_data = {symbol_pair: get_data_from_keys(aggtrade_data, EVENT_TIMESTAMP, PRICE_P, QUANTITY)}

                    if pycache_counter > AGGTRADE_PYCACHE:
                        await mongo.duplicate_insert_aggtrade_data(ta_lines_db, db_cache.aggtrade_data)
                        db_cache.aggtrade_data = {}
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


# async def execute_ws_trades(debug_time):
#     from data_staging import usdt_symbols_stream, get_coin_fund_ratio, remove_usdt
#     bm = BinanceSocketManager(await AsyncClient.create())
#     bm.multiplex_socket(usdt_symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) + usdt_symbols_stream(AGGREGATED_TRADE_WS))
#     while True:
#         try:
#
#             await _binance_to_mongodb()
#         except QueueOverflow as e:
#             pass
#         except Exception as e:
#             exit(1)


# TODO: clean symbols that start with usdt and not finish with them, acho que é um erro do binance... mas a variavel das moedas
#  tem 39 simbolos e os dicts 38, verificar qual falta.
# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.


