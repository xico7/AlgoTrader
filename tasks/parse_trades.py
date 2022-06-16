import time


from data_func import ParseAggtradeData
from vars_constants import millisecs_timeframe, default_parse_interval

alive_debug_secs = 90


# TODO: Fund trades are not parsed here..
# TODO: don't forget coin_ratios
def parse_trades_ten_seconds():
    from MongoDB.Queries import query_parsed_aggtrade
    from MongoDB.db_actions import connect_to_parsed_aggtrade_db
    from data_staging import coin_ratio, current_milli_time

    parse_aggtrade = ParseAggtradeData()

    while any(parse_aggtrade.start_ts) < current_milli_time() - 60000:
        for symbol in connect_to_parsed_aggtrade_db().list_collection_names():
            if trades := query_parsed_aggtrade(symbol, parse_aggtrade.start_ts[symbol], parse_aggtrade.end_ts[symbol]):
                parse_aggtrade.parse_trades(trades, symbol)
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



