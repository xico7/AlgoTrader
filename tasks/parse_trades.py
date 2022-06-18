import time
from data_func import ParseAggtradeData
from vars_constants import millisecs_timeframe, ONE_MIN_IN_MS


# TODO: Fund trades are not parsed here..
# TODO: don't forget coin_ratios
def parse_trades_ten_seconds():
    from MongoDB.Queries import query_parsed_aggtrade_multiple_timeframes
    from MongoDB.db_actions import connect_to_parsed_aggtrade_db
    from data_staging import current_milli_time

    parse_aggtrade = ParseAggtradeData()

    while any(parse_aggtrade.start_ts) < current_milli_time() - ONE_MIN_IN_MS:
        parse_aggtrade += query_parsed_aggtrade_multiple_timeframes(
            connect_to_parsed_aggtrade_db().list_collection_names(), parse_aggtrade.start_ts, parse_aggtrade.end_ts)

        parse_aggtrade.insert_in_db()
        parse_aggtrade.reset_add_interval()
        print("1hour done")
        print(parse_aggtrade.start_ts['BTCUSDT'])
    else:
        while True:
            pass
        # Do the bundled technique



        ts_begin += millisecs_timeframe

        print(time.time() - time1)

        if ts_begin > (time.time() * 1000):
            print("success.")
            exit(0)

