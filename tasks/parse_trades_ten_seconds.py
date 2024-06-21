import logging
import re
import time
from datetime import timedelta

import requests
import logs
from support.data_handling.data_structures import SymbolsTimeframeTrade, FundTimeframeTrade
from support.data_handling.data_helpers.vars_constants import FUND_SYMBOLS_USDT_PAIRS, coingecko_marketcap_api_link

from support.generic_helpers import mins_to_ms

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class APIException(Exception): pass


def coin_ratio_marketcap():
    symbols_price_weight_marketcap = {}
    fund_match_uppercase_usdt_symbols = {symbol: re.match('(^(.+?)USDT)', symbol).groups()[1].lower() for symbol in FUND_SYMBOLS_USDT_PAIRS}

    marketcap = requests.get(coingecko_marketcap_api_link)
    if marketcap.status_code != 200:
        error_message = f"Invalid response from coingecko with status code {marketcap.status_code} and text {marketcap.text}"
        LOG.error(error_message)
        raise APIException(error_message)

    for symbol_data in marketcap.json():
        try:
            symbol_key = [uppercase_usdt_symbol for uppercase_usdt_symbol, symbol in
                          fund_match_uppercase_usdt_symbols.items() if symbol == symbol_data['symbol']][0]
        except IndexError:
            continue
        symbols_price_weight_marketcap[symbol_key] = {"price": symbol_data['current_price'],
                                                      "price_weight": symbol_data['market_cap'] / symbol_data['current_price'],
                                                      "marketcap": symbol_data['market_cap']}

    return symbols_price_weight_marketcap


def parse_trades_ten_seconds():
    LOG.info("Beginning to parse ten seconds trades.")

    coin_ratio = coin_ratio_marketcap()

    parse_fund_data = None
    parse_aggtrade = None
    while True:
        parse_aggtrade = SymbolsTimeframeTrade() if not parse_aggtrade else (
            SymbolsTimeframeTrade(parse_aggtrade.end_ts + timedelta(minutes=parse_aggtrade.timeframe)))
        parse_fund_data = FundTimeframeTrade(coin_ratio) if not parse_fund_data else (
            FundTimeframeTrade(coin_ratio, parse_fund_data.end_ts + timedelta(minutes=parse_fund_data.timeframe)))

        if parse_fund_data.finished and parse_aggtrade.finished:
            LOG.info("Finished parsing ten seconds trades, sleeping for ten minutes.")
            time.sleep(60)

            parse_fund_data.finished = False
            parse_aggtrade.finished = False
        else:
            if not parse_fund_data.finished:
                parse_fund_data.parse_and_insert_trades()
            if not parse_aggtrade.finished:
                parse_aggtrade.parse_and_insert_trades()
