import requests
import re
import time
from typing import Union, List
from vars_constants import USDT, BNB, TEN_SECS_MS, coingecko_marketcap_api_link, SP500_SYMBOLS_USDT_PAIRS


def remove_usdt(symbols: Union[List[str], str]):
    return [re.match('(^(.+?)USDT)', symbol).groups()[1].lower() for symbol in symbols]


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


def coin_ratio_marketcap():
    fund_marketcap = 0
    symbols_price_weight_marketcap, coin_ratio = {}, {}
    sp500_symbols = {symbol: re.match('(^(.+?)USDT)', symbol).groups()[1].lower() for symbol in SP500_SYMBOLS_USDT_PAIRS}
    for symbol_data in requests.get(coingecko_marketcap_api_link).json():
        try:
            symbol_key = [k for k, symbol in sp500_symbols.items() if symbol == symbol_data['symbol']][0]
        except IndexError:
            continue
        symbols_price_weight_marketcap[symbol_key] = {"price": symbol_data['current_price'],
                                                      "price_weight": symbol_data['market_cap'] / symbol_data['current_price'],
                                                      "marketcap": symbol_data['market_cap']}

        fund_marketcap += symbol_data['market_cap']

    return symbols_price_weight_marketcap, fund_marketcap


def transform_data(data, *keys):
    data_keys = {}
    for key in keys:
        if isinstance(key, list):
            type_cast = key[1]
            data_keys.update({key[0]: type_cast(data[key[0]])})
        else:
            data_keys.update({key: data[key]})
    return data_keys


def usdt_with_bnb_symbols_aggtrades() -> list:
    all_symbols = [symbol_data['symbol'] for symbol_data in requests.get("https://api.binance.com/api/v3/ticker/price").json()]
    usdt_symbols = [symbol for symbol in all_symbols if USDT in symbol]

    return [symbol.lower() + '@aggTrade' for symbol in usdt_symbols if symbol.replace(USDT, BNB) in all_symbols
            or BNB + symbol.replace(USDT, '') in all_symbols or symbol == 'BNBUSDT']


def get_counter(min_value, range, price):
    counter = 0
    while difference := price - min_value:
        difference -= range
        counter += 1

    return str(counter)
