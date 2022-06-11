import time
from datetime import datetime

from data_func import ParseSymbolsAggtradeData
from vars_constants import millisecs_timeframe, secs_parse_interval

alive_debug_secs = 90


# TODO: Fund trades are not parsed here..
# TODO: don't forget coin_ratios
def parse_trades_ten_seconds():
    from MongoDB.Queries import query_wstrade_data_col_timeframe, connect_to_aggtrade_data_db
    from MongoDB.db_actions import insert_n_secs_parsed_trades_in_db, \
        parsed_trades_all_symbols_db_name
    from data_staging import coin_ratio, get_current_second, print_alive_if_passed_timestamp

    coin_ratios = coin_ratio()
    debug_running_execution = get_current_second()

    symbols = connect_to_aggtrade_data_db().list_collection_names()
    while True:
        parse_aggtrade = ParseSymbolsAggtradeData(symbols)
        time1 = time.time()

        for symbol in symbols:
            if trades := query_wstrade_data_col_timeframe(symbol, parse_aggtrade.start_ts[symbol], parse_aggtrade.end_ts[symbol]):
                parse_aggtrade.parse_trades(trades)

        parse_aggtrade.insert_in_db()
        insert_n_secs_parsed_trades_in_db(parsed_trades_all_symbols_db_name.format(secs_parse_interval), parse_aggtrade)

        ts_begin += millisecs_timeframe

        print(time.time() - time1)

        if ts_begin > (time.time() * 1000):
            print("success.")
            exit(0)

        if time.time() - time1 > millisecs_timeframe / 1000:
            print("Parsing time is greater than range it is parsing, please check the code!!!")

        if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
            print(datetime.fromtimestamp(ts_begin/1000))
            debug_running_execution += alive_debug_secs

