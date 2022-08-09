import requests
import re
import time
from typing import Union, List
from vars_constants import USDT, BNB, TEN_SECS_MS, coingecko_marketcap_api_link, SP500_SYMBOLS_USDT_PAIRS


def remove_usdt(symbols: Union[List[str], str]):
    def match(symbol_or_symbols):
        return re.match('(^(.+?)USDT)', symbol_or_symbols).groups()[1].upper()

    return match(symbols) if isinstance(symbols, str) else [match(symbol) for symbol in symbols]


def get_current_second() -> int:
    return int(time.time())


def get_current_second_in_ms() -> float:
    return get_current_second() * 1000


def mins_to_ms(minutes):
    mins_in_ms = minutes * 60 * 1000
    return int(mins_in_ms) if isinstance(minutes, int) else mins_in_ms


def round_last_ten_secs(timestamp):
    return timestamp - TEN_SECS_MS + (TEN_SECS_MS - (timestamp % TEN_SECS_MS))


def current_milli_time():
    return round(time.time() * 1000)


def coin_ratio():
    total_marketcap = 0
    SP500_SYMBOLS = remove_usdt(SP500_SYMBOLS_USDT_PAIRS)
    for symbol in requests.get(coingecko_marketcap_api_link).json():
        if symbol in SP500_SYMBOLS:
            total_marketcap += symbol['market_cap']



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


def usdt_with_bnb_symbols_stream() -> list:
    all_symbols = [symbol_data['symbol'] for symbol_data in requests.get("https://api.binance.com/api/v3/ticker/price").json()]
    usdt_symbols = [symbol for symbol in all_symbols if USDT in symbol]

    return [symbol for symbol in usdt_symbols
            if symbol.replace(USDT, BNB) in all_symbols or BNB + symbol.replace(USDT, '') in all_symbols]


def get_counter(min_value, range, price):
    counter = 0
    while difference := price - min_value:
        difference -= range
        counter += 1

    return str(counter)
