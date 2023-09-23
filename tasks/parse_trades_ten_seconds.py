import logging
import re
import time
import requests
import logs
from data_handling.data_structures import SymbolsTimeframeTrade, FundTimeframeTrade, InvalidValidatorTimestamps
from data_handling.data_helpers.vars_constants import TEN_SECONDS_IN_MS, FUND_SYMBOLS_USDT_PAIRS, \
    coingecko_marketcap_api_link

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
    coin_ratio = coin_ratio_marketcap()
    aggtrades_stop_and_save_timeframes = 0
    fund_stop_and_save_timeframe = 0

    LOG.info("Beginning to parse ten seconds trades.")

    parse_fund_data = None
    parse_aggtrade = None
    while not parse_fund_data or not parse_aggtrade:
        try:
            parse_fund_data = FundTimeframeTrade(coin_ratio)
            parse_aggtrade = SymbolsTimeframeTrade()
        except InvalidValidatorTimestamps:
            LOG.info("No valid timestamps provided from 'parsed aggtrades' database, waiting for 10 minutes and trying again")
            time.sleep(600)

    while True:
        if fund_stop_and_save_timeframe:
            parse_fund_data = FundTimeframeTrade(coin_ratio, fund_stop_and_save_timeframe)
        else:
            parse_fund_data = FundTimeframeTrade(coin_ratio, parse_fund_data.end_ts + TEN_SECONDS_IN_MS)

        if aggtrades_stop_and_save_timeframes:
            parse_aggtrade = SymbolsTimeframeTrade(aggtrades_stop_and_save_timeframes)
        else:
            parse_aggtrade = SymbolsTimeframeTrade(parse_aggtrade.end_ts + TEN_SECONDS_IN_MS)

        if parse_fund_data.finished and parse_aggtrade.finished:
            LOG.info("Finished parsing ten seconds trades, sleeping for ten minutes.")
            time.sleep(600)

            if not aggtrades_stop_and_save_timeframes:
                aggtrades_stop_and_save_timeframes = parse_aggtrade.start_ts + TEN_SECONDS_IN_MS
            if not fund_stop_and_save_timeframe:
                fund_stop_and_save_timeframe = parse_fund_data.start_ts + TEN_SECONDS_IN_MS
            parse_fund_data.finished = False
            parse_aggtrade.finished = False
        else:
            if not parse_fund_data.finished:
                parse_fund_data.parse_and_insert_trades()
                fund_stop_and_save_timeframe = 0
            if not parse_aggtrade.finished:
                parse_aggtrade.parse_and_insert_trades()
                aggtrades_stop_and_save_timeframes = 0

