import logging
import time
import logs
from vars_constants import ONE_MIN_IN_MS, PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, PRICE, QUANTITY, MARKETCAP
from data_func import SymbolsTimeframeTrade, FundTimeframeTrade
from MongoDB.db_actions import query_parsed_aggtrade_multiple_timeframes, list_db_cols
from data_staging import current_milli_time


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():

    parse_aggtrade = SymbolsTimeframeTrade()

    while parse_aggtrade.get_last_end_ts() < current_milli_time() - ONE_MIN_IN_MS:
        parse_aggtrade += query_parsed_aggtrade_multiple_timeframes(
            list_db_cols(PARSED_AGGTRADES_DB), parse_aggtrade.start_ts, parse_aggtrade.end_ts)


        parse_aggtrade.insert_in_db()
        parse_aggtrade.reset_add_interval()

        print("1hour done")
        print(parse_aggtrade.start_ts[DEFAULT_SYMBOL_SEARCH])

        if parse_aggtrade.start_ts[DEFAULT_SYMBOL_SEARCH] > (time.time() * 1000):
            print("success.")

    print("DONE.")


def parse_fund_trades_ten_seconds():

    parse_fund_data = FundTimeframeTrade()

    while parse_fund_data.end_ts[DEFAULT_SYMBOL_SEARCH] < current_milli_time() - ONE_MIN_IN_MS:

        for tf in parse_fund_data.tf_marketcap_quantity.keys():
            volume_traded, current_marketcap = 0, 0

            for symbol in parse_fund_data._symbols:
                if parse_fund_data.ts_data[symbol][tf][PRICE]:
                    current_marketcap += parse_fund_data.ts_data[symbol][tf][PRICE] * \
                                         parse_fund_data.ratios[DEFAULT_SYMBOL_SEARCH][MARKETCAP] / parse_fund_data.ratios[DEFAULT_SYMBOL_SEARCH][PRICE]
                    volume_traded += parse_fund_data.ts_data[symbol][tf][PRICE] * parse_fund_data.ts_data[symbol][tf][QUANTITY]
                else:
                    LOG.warning(f"Untraded 10 second timeframe from a fund symbol '{symbol}', this makes data less reliable.")
            else:
                parse_fund_data.tf_marketcap_quantity[tf][MARKETCAP] = current_marketcap
                parse_fund_data.tf_marketcap_quantity[tf][QUANTITY] = volume_traded

        parse_fund_data.insert_in_db()
        parse_fund_data = FundTimeframeTrade(parse_fund_data.end_ts[DEFAULT_SYMBOL_SEARCH])

