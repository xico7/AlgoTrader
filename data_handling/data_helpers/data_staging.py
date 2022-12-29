import requests
import re
import time

from data_handling.data_helpers.vars_constants import USDT, BNB, TEN_SECONDS_IN_MS, coingecko_marketcap_api_link, FUND_SYMBOLS_USDT_PAIRS


def get_current_second_in_ms():
    return int(time.time()) * 1000


def seconds_to_ms(seconds):
    seconds_in_ms = seconds * 1000
    return int(seconds_in_ms) if isinstance(seconds_in_ms, int) else seconds_in_ms


def mins_to_ms(minutes):
    return seconds_to_ms(minutes) * 60


def get_next_millisecond_modulo(start_timestamp, milliseconds: int):
    return start_timestamp + milliseconds - (start_timestamp % milliseconds)


def current_time_in_ms(cur_time_in_secs: float):
    return cur_time_in_secs * 1000


def round_last_ten_secs(timestamp):
    return timestamp - TEN_SECONDS_IN_MS + (TEN_SECONDS_IN_MS - (timestamp % TEN_SECONDS_IN_MS))


def coin_ratio_marketcap():
    symbols_price_weight_marketcap = {}
    fund_match_uppercase_usdt_symbols = {symbol: re.match('(^(.+?)USDT)', symbol).groups()[1].lower() for symbol in FUND_SYMBOLS_USDT_PAIRS}
    for symbol_data in requests.get(coingecko_marketcap_api_link).json():
        try:
            symbol_key = [uppercase_usdt_symbol for uppercase_usdt_symbol, symbol in fund_match_uppercase_usdt_symbols.items() if symbol == symbol_data['symbol']][0]
        except IndexError:
            continue
        symbols_price_weight_marketcap[symbol_key] = {"price": symbol_data['current_price'],
                                                      "price_weight": symbol_data['market_cap'] / symbol_data['current_price'],
                                                      "marketcap": symbol_data['market_cap']}

    return symbols_price_weight_marketcap


def usdt_with_bnb_symbols() -> list:
    all_symbols = [symbol_data['symbol'] for symbol_data in requests.get("https://api.binance.com/api/v3/ticker/price").json()]
    usdt_symbols = [symbol for symbol in all_symbols if USDT in symbol]

    return [symbol for symbol in usdt_symbols if symbol.replace(USDT, BNB) in all_symbols
            or BNB + symbol.replace(USDT, '') in all_symbols or symbol == f'{BNB}{USDT}']

