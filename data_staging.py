from datetime import datetime
import requests
import re
import time
from typing import Union, List
from vars_constants import coingecko_marketcap_api_link, MongoDB, DB_TS, default_parse_interval, DEFAULT_COL_SEARCH, \
    SECONDS_TO_MS_APPEND, USDT


class UntradedSymbol(Exception): pass


def remove_usdt(symbols: Union[List[str], str]):
    if isinstance(symbols, str):
        try:
            return re.match('(^(.+?)USDT)', symbols).groups()[1].upper()
        except AttributeError as e:
            return None
    else:
        return [re.match('(^(.+?)USDT)', symbol).groups()[1].upper() for symbol in symbols]


def get_current_second() -> int:
    return int(time.time())


def get_current_second_in_ms() -> float:
    return sec_to_ms(get_current_second())


def sec_to_ms(time_value):
    time_value_str = str(time_value)
    if '.' not in str(time_value):
        return float(time_value_str + SECONDS_TO_MS_APPEND)
    else:
        split_decimal_tv = time_value_str.split('.')
        return float(split_decimal_tv[0] + SECONDS_TO_MS_APPEND + '.' + split_decimal_tv[1])


def ms_to_secs(time_value):
    return time_value / 1000



def get_last_minute(timestamp):
    return round_last_n_secs(timestamp, 60)


def get_last_second(timestamp):
    round_last_n_secs(timestamp, 1)


def sleep_until_time_match(fixed_timestamp):
    if len(str(fixed_timestamp)) == 13:
        time.sleep((fixed_timestamp - get_current_second_in_ms()) / 1000)
    else:
        time.sleep(fixed_timestamp - get_current_second())
    return


def round_last_n_secs(timestamp, number_of_seconds: int):
    if len(str(timestamp)) == 13:
        number_of_seconds = int(number_of_seconds * 1000)  # Convert to ms

    return timestamp - int(str(timestamp)[-(len(str(number_of_seconds)) - 1):])


def current_milli_time():
    return round(time.time() * 1000)

def is_new_minute(current_minute, current_time):
    if len(str(current_time)) == 13:
        if get_last_minute(current_time) != current_minute:
            return True
    if get_last_minute(current_time) != current_minute:
        return True

#
# def coin_ratio():
#     return get_symbols_normalized_fund_ratio(remove_usdt(SP500_SYMBOLS_USDT_PAIRS), requests.get(coingecko_marketcap_api_link).json())
#

def sum_values(key_values):
    total = 0
    for value in key_values.values():
        total += value

    return total


def transform_data(data, *keys):
    data_keys = {}
    for key in keys:
        if isinstance(key, list):
            type_cast = key[1]
            data_keys.update({key[0]: type_cast(data[key[0]])})
        else:
            data_keys.update({key: data[key]})
    return data_keys




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




#
# def get_timeframe():
#     if sp500_elements := list(mongo.connect_to_sp500_db_collection().find({DB_TS: {MongoDB.HIGHER_EQ: 0}})):
#         return sp500_elements[-1][DB_TS]
#
#     return round_last_n_secs(get_current_second_in_ms(), ONE_MIN_IN_SECS)
#
#
# def fill_symbol_prices(symbol_prices, end_ts):
#     for symbol in symbol_prices.keys():
#         list_trades = list(mongo.connect_to_bundled_aggtrade_db().get_collection(symbol).find(
#             {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: end_ts - FIFTEEN_MIN_IN_MS}},
#                            {DB_TS: {MongoDB.LOWER_EQ: end_ts}}]}))
#
#         if not list_trades:
#             LOG.error("No trades present in aggtrade, make sure trade aggregator is running.")
#             raise
#         symbol_prices[symbol] = float(list_trades[0][PRICE])
#
#     return

def usdt_symbols_stream(type_of_trade: str) -> list:
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()

    binance_symbols = []
    for symbol_info in binance_symbols_price:
        symbol = symbol_info["symbol"]
        if USDT in symbol:
            binance_symbols.append(symbol)

    return [f"{symbol.lower()}{type_of_trade}" for symbol in binance_symbols]
#
# def get_symbols_normalized_fund_ratio(symbol_pairs: dict, symbols_information: dict):
#     coin_ratio = {}
#     total_marketcap = 0
#     for symbol_info in symbols_information:
#         if symbol_info[SYMBOL].upper() in symbol_pairs:
#             total_marketcap += symbol_info['market_cap']
#
#     for symbol_info in symbols_information:
#         current_symbol = symbol_info[SYMBOL].upper()
#         if current_symbol in symbol_pairs:
#             coin_ratio.update({current_symbol+USDT: symbol_info['market_cap'] / symbol_info['current_price']})
#
#     return coin_ratio
