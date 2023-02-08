import logging
import time
import logs
from data_handling.data_structures import SymbolsTimeframeTrade, FundTimeframeTrade
from data_handling.data_helpers.data_staging import coin_ratio_marketcap
from data_handling.data_helpers.vars_constants import TEN_SECONDS_IN_MS

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():
    coin_ratio = coin_ratio_marketcap()

    parse_fund_data = None
    parse_aggtrade = None
    aggtrades_stop_and_save_timeframes = []
    fund_stop_and_save_timeframe = 0

    LOG.info("Beginning to parse ten seconds trades.")
    while True:
        if not parse_fund_data:
            parse_fund_data = FundTimeframeTrade(coin_ratio)
        elif fund_stop_and_save_timeframe:
            parse_fund_data = FundTimeframeTrade(coin_ratio, fund_stop_and_save_timeframe)
        else:
            parse_fund_data = FundTimeframeTrade(coin_ratio, parse_fund_data.end_ts + TEN_SECONDS_IN_MS)

        if not parse_aggtrade:
            parse_aggtrade = SymbolsTimeframeTrade()
        elif aggtrades_stop_and_save_timeframes:
            parse_aggtrade = SymbolsTimeframeTrade(aggtrades_stop_and_save_timeframes)
        else:
            parse_aggtrade = SymbolsTimeframeTrade(
                {symbol: parse_aggtrade.end_ts[symbol] + TEN_SECONDS_IN_MS for symbol in parse_aggtrade.symbols})

        if parse_fund_data.finished and parse_aggtrade.finished:
            if not aggtrades_stop_and_save_timeframes:
                aggtrades_stop_and_save_timeframes = {symbol: parse_aggtrade.start_ts[symbol] + TEN_SECONDS_IN_MS for symbol in parse_aggtrade.symbols}
            if not fund_stop_and_save_timeframe:
                fund_stop_and_save_timeframe = parse_fund_data.start_ts + TEN_SECONDS_IN_MS
            LOG.info("Finished parsing ten seconds trades, sleeping for ten minutes.")
            time.sleep(600)
            parse_fund_data.finished = False
            parse_aggtrade.finished = False
        else:
            if not parse_fund_data.finished:
                parse_fund_data.parse_and_insert_trades()
                fund_stop_and_save_timeframe = 0
            if not parse_aggtrade.finished:
                parse_aggtrade.parse_and_insert_trades()
                aggtrades_stop_and_save_timeframes = []

