import logging
import logs
from datetime import datetime
from vars_constants import ONE_MIN_IN_MS, PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, PRICE, QUANTITY, MARKETCAP
from data_func import SymbolsTimeframeTrade, FundTimeframeTrade
from MongoDB.db_actions import connect_to_db, query_db_col_between
from data_staging import current_milli_time, get_current_second_in_ms

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():

    parse_aggtrade = SymbolsTimeframeTrade()

    while max(parse_aggtrade.end_ts.values()) < current_milli_time() - ONE_MIN_IN_MS:
        parse_aggtrade += {symbol: query_db_col_between(PARSED_AGGTRADES_DB, symbol, parse_aggtrade.start_ts, parse_aggtrade.end_ts)
                           for symbol in connect_to_db(PARSED_AGGTRADES_DB).list_collection_names()}

        parse_aggtrade.insert_in_db()
        LOG.info(f"Parsed symbol pairs from {datetime.fromtimestamp(min(parse_aggtrade.start_ts.values())/1000)} to "
                 f"{datetime.fromtimestamp(max(parse_aggtrade.end_ts.values())/1000)}.")
        parse_aggtrade.reset_add_interval()

        if parse_aggtrade.start_ts[DEFAULT_SYMBOL_SEARCH] > get_current_second_in_ms():
            LOG.info("Finished parsing symbol pairs trades, now exiting.")
            exit(0)


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

