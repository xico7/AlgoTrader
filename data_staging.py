from datetime import datetime

import pymongo
from pymongo.errors import OperationFailure

import MongoDB.db_actions as mongo
import re
import time
from typing import Union, List
import requests as requests

from argparse_func import LOG
from tasks.transform_trade_data import EVENT_TS, PRICE, QUANTITY

SP500_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT',
                            'BCHUSDT',
                            'ALGOUSDT',
                            'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT',
                            'ICPUSDT',
                            'FILUSDT',
                            'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
                            'NEARUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
                            'LRCUSDT']


#### Timeframes ####

SECONDS_TO_MS_APPEND = '000'

WEEK_DAYS = 7
TEN_SECONDS = 10
ONE_MIN_IN_SECS = 60
FIVE_MIN_IN_SECS = ONE_MIN_IN_SECS * 5
FIFTEEN_MIN_IN_SECS = ONE_MIN_IN_SECS * 15
THIRTY_MIN_IN_SECS = FIFTEEN_MIN_IN_SECS * 2
ONE_HOUR_IN_SECS = ONE_MIN_IN_SECS * 60
FOUR_HOUR_IN_SECS = ONE_HOUR_IN_SECS * 4
ONE_DAY_IN_SECS = ONE_HOUR_IN_SECS * 24

ONE_DAY_IN_MS = ONE_DAY_IN_SECS * 1000
ONE_HOUR_IN_MS = ONE_HOUR_IN_SECS * 1000
FIFTEEN_MIN_IN_MS = int(str(FIFTEEN_MIN_IN_SECS) + SECONDS_TO_MS_APPEND)
ONE_MIN_IN_MS = int(str(ONE_MIN_IN_SECS) + SECONDS_TO_MS_APPEND)
TEN_SECONDS_IN_MS = int(str(TEN_SECONDS) + SECONDS_TO_MS_APPEND)
FIVE_SECS_IN_MS = 5000

###################

USDT = "USDT"
SYMBOL = 'symbol'
BEGIN_TIMESTAMP = "begin_timestamp"

class MongoDB:
    EQUAL = '$eq'
    LOWER_EQ = '$lte'
    LOWER = '$lt'
    HIGHER_EQ = '$gte'
    HIGHER = '$gt'
    AND = '$and'


def remove_usdt(symbols: Union[List[str], str]):
    if isinstance(symbols, str):
        try:
            return re.match('(^(.+?)USDT)', symbols).groups()[1].upper()
        except AttributeError as e:
            return None
    else:
        return [re.match('(^(.+?)USDT)', symbol).groups()[1].upper() for symbol in symbols]


def query_db_last_minute(db_name):
    try:
        from tasks.transform_trade_data import END_TS
        return ONE_MIN_IN_MS + list(mongo.connect_to_db(db_name).get_collection(
            mongo.connect_to_db(db_name).list_collection_names()[0]).find(
            {END_TS: {MongoDB.HIGHER_EQ: 0}}))[-1][END_TS]
    except IndexError as e:
        return get_last_minute(get_current_second_in_ms())
    except OperationFailure as e:
        return get_last_minute(get_current_second_in_ms())


def get_current_second() -> int:
    return int(time.time())


def get_current_second_in_ms() -> int:
    return sec_to_ms(get_current_second())


def sec_to_ms(time_value):
    return int(str(time_value) + SECONDS_TO_MS_APPEND)


def get_last_minute(timestamp):
    return round_to_last_n_secs(timestamp, 60)


def get_last_second(timestamp):
    round_to_last_n_secs(timestamp, 1)


def sleep_until_time_match(fixed_timestamp):
    if len(str(fixed_timestamp)) == 13:
        time.sleep((fixed_timestamp - get_current_second_in_ms()) / 1000)
    else:
        time.sleep(fixed_timestamp - get_current_second())
    return


def round_to_last_n_secs(timestamp, number_of_seconds):
    if len(str(timestamp)) == 13:
        while timestamp % int((str(number_of_seconds) + SECONDS_TO_MS_APPEND)) != 0:
            timestamp -= 1
    while timestamp % number_of_seconds != 0:
        timestamp -= 1

    return timestamp


def is_new_minute(current_minute, current_time):
    if len(str(current_time)) == 13:
        if get_last_minute(current_time) != current_minute:
            return True
    if get_last_minute(current_time) != current_minute:
        return True


def get_timeframe():
    if sp500_elements := list(mongo.connect_to_sp500_db_collection().find({EVENT_TS: {MongoDB.HIGHER_EQ: 0}})):
        return sp500_elements[-1][EVENT_TS]

    return round_to_last_n_secs(get_current_second_in_ms(), ONE_MIN_IN_SECS)


def fill_symbol_prices(symbol_prices, end_ts):
    for symbol in symbol_prices.keys():
        list_trades = list(mongo.connect_to_aggtrade_data_db().get_collection(symbol).find(
            {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: end_ts - FIFTEEN_MIN_IN_MS}},
                           {EVENT_TS: {MongoDB.LOWER_EQ: end_ts}}]}))

        if not list_trades:
            LOG.error("No trades present in aggtrade, make sure trade aggregator is running.")
            raise
        symbol_prices[symbol] = float(list_trades[0][PRICE])

    return


def coin_ratio():
    from tasks.ws_trades import coingecko_marketcap_api_link
    return get_symbols_normalized_fund_ratio(remove_usdt(SP500_SYMBOLS_USDT_PAIRS), requests.get(coingecko_marketcap_api_link).json())


def sum_values(key_values):
    total = 0
    for value in key_values.values():
        total += value

    return total


def debug_prints(start_time):
    iteration_time = time.ctime(int(str(int(start_time)).replace(SECONDS_TO_MS_APPEND, "")))
    time_now = time.ctime(time.time())
    print(f"the time is now {time_now}")
    print(f"finished minute {iteration_time}")
    print("exec time in milliseconds", get_current_second_in_ms() - start_time)


def get_data_from_keys(data, *keys):
    data_keys = {}
    for key in keys:
        data_keys.update({key: data[key]})
    return data_keys


def usdt_symbols_stream(type_of_trade: str) -> list:
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()

    binance_symbols = []
    for symbol_info in binance_symbols_price:
        symbol = symbol_info["symbol"]
        if USDT in symbol:
            binance_symbols.append(symbol)

    return [f"{symbol.lower()}{type_of_trade}" for symbol in binance_symbols]


# TODO: isto não está a fazer nada.. lol
def usdt_with_bnb_symbols_stream(type_of_trade: str) -> list:
    symbols = usdt_symbols_stream(type_of_trade)
    bnb_symbols = []
    for symbol in symbols:
        bnb_suffix_elem_search = symbol.replace(USDT, "BNB")
        bnb_prefix_elem_search = "BNB" + symbol.replace(USDT, "")
        if bnb_suffix_elem_search in symbol or bnb_prefix_elem_search in symbol:
            bnb_symbols.append(symbol)

    return bnb_symbols


def get_symbols_normalized_fund_ratio(symbol_pairs: dict, symbols_information: dict):
    coin_ratio = {}
    total_marketcap = 0
    for symbol_info in symbols_information:
        if symbol_info[SYMBOL].upper() in symbol_pairs:
            total_marketcap += symbol_info['market_cap']

    for symbol_info in symbols_information:
        current_symbol = symbol_info[SYMBOL].upper()
        if current_symbol in symbol_pairs:
            coin_ratio.update({current_symbol+USDT: symbol_info['market_cap'] / symbol_info['current_price']})

    return coin_ratio


def get_counter(min_value, range, price):
    counter = 0
    difference = price - min_value
    while difference > 0:
        difference -= range
        counter += 1

    return str(counter)


def print_alive_if_passed_timestamp(timestamp):
    if get_current_second() > timestamp:
        print(datetime.fromtimestamp(get_current_second()))
        return True


def get_last_ts_from_db(database_conn, collection):
    try:
        return list(database_conn.get_collection(collection).find(
            {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: 0}},
                           {EVENT_TS: {MongoDB.LOWER_EQ: get_current_second_in_ms()}}]}).sort(
            EVENT_TS, pymongo.ASCENDING).limit(1))[-1][EVENT_TS]
    except IndexError:
        return None
