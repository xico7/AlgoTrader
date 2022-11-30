import logging
import time
import logs
from data_handling.data_func import SymbolsTimeframeTrade, FundTimeframeTrade
from data_handling.data_helpers.data_staging import coin_ratio_marketcap
from data_handling.data_helpers.vars_constants import TEN_SECONDS_IN_MS

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():
    coin_ratio = coin_ratio_marketcap()

    parse_fund_data = None
    parse_aggtrade = None

    LOG.info("Beginning to parse ten seconds trades.")
    while True:
        parse_fund_data = FundTimeframeTrade(coin_ratio) if not parse_fund_data else FundTimeframeTrade(
            coin_ratio, parse_fund_data.end_ts + TEN_SECONDS_IN_MS)
        parse_aggtrade = SymbolsTimeframeTrade() if not parse_aggtrade else SymbolsTimeframeTrade(
            {symbol: parse_aggtrade.end_ts[symbol] + TEN_SECONDS_IN_MS for symbol in parse_aggtrade.symbols})

        if parse_fund_data.finished and parse_aggtrade.finished:
            LOG.info("Finished parsing ten seconds trades, sleeping for ten minutes.")
            time.sleep(600)
            parse_fund_data.finished = False
            parse_aggtrade.finished = False
        else:
            parse_fund_data.parse_and_insert_trades()
            parse_aggtrade.parse_and_insert_trades()
