import logging
import time
import logs
from data_handling.data_func import SymbolsTimeframeTrade, FundTimeframeTrade
from data_handling.data_helpers.data_staging import coin_ratio_marketcap

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():
    coin_ratio = coin_ratio_marketcap()

    parse_fund_data = None
    parse_aggtrade = None

    LOG.info("Beginning to parse ten seconds trades.")
    while True:
        if not parse_fund_data:
            parse_fund_data = FundTimeframeTrade(coin_ratio)
        else:
            parse_fund_data = FundTimeframeTrade(coin_ratio, parse_fund_data.end_ts)

        if not parse_aggtrade:
            parse_aggtrade = SymbolsTimeframeTrade()
        else:
            parse_aggtrade = SymbolsTimeframeTrade({symbol: parse_aggtrade.end_ts[symbol] for symbol in parse_aggtrade.symbols})

        if parse_fund_data.finished or parse_aggtrade.finished:
            LOG.info("Finished parsing ten seconds trades, sleeping for one minute.")
            time.sleep(60)
        else:
            parse_fund_data.parse_and_insert_trades()
            parse_aggtrade.parse_and_insert_trades()
