import time
from vars_constants import ONE_HOUR_IN_MS, ONE_MIN_IN_MS, PARSED_AGGTRADES_DB
from data_func import SymbolsTimeframeTrade, FundTimeframeTrade
from MongoDB.db_actions import query_parsed_aggtrade_multiple_timeframes, list_db_cols
from data_staging import current_milli_time


# TODO: Fund trades are not parsed here..
# TODO: don't forget coin_ratios


def parse_trades_ten_seconds():

    parse_aggtrade = SymbolsTimeframeTrade()

    while parse_aggtrade.get_last_end_ts() < current_milli_time() - ONE_MIN_IN_MS:
        parse_aggtrade += query_parsed_aggtrade_multiple_timeframes(
            list_db_cols(PARSED_AGGTRADES_DB), parse_aggtrade.start_ts, parse_aggtrade.end_ts)


        parse_aggtrade.insert_in_db()
        parse_aggtrade.reset_add_interval()

        print("1hour done")
        print(parse_aggtrade.start_ts['BTCUSDT'])

        if parse_aggtrade.start_ts['BTCUSDT'] > (time.time() * 1000):
            print("success.")

    print("DONE.")


def parse_fund_trades_ten_seconds():

    parse_fund_data = FundTimeframeTrade()
    print("here")