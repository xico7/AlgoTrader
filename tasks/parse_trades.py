import time

import pymongo

alive_debug_secs = 90


def parse_trades_ten_seconds():
    from MongoDB.Queries import query_aggtrade_data_col_timeframe
    from tasks.transform_trade_data import PRICE, QUANTITY
    from MongoDB.db_actions import connect_to_aggtrade_data_db, insert_n_secs_parsed_trades_in_db, \
        connect_to_n_ms_parsed_trades_db, parsed_trades_all_symbols_db_name, parsed_trades_fund_db_name
    from data_staging import coin_ratio, optional_add_secs_in_ms, round_to_last_n_secs, \
        SP500_SYMBOLS_USDT_PAIRS, get_last_ts_from_db, get_current_second, print_alive_if_passed_timestamp, trades_in_range

    all_symbols = connect_to_aggtrade_data_db().list_collection_names()

    coin_ratios = coin_ratio()
    debug_running_execution = get_current_second()

    ms_parse_interval = 10000
    secs_parse_interval = int(ms_parse_interval / 1000)
    ms_ts_range = 240000
    iterations = range(0, ms_ts_range, ms_parse_interval)

    fund_symbol_ratio_prices = {iteration: {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)} for iteration in iterations}

    if not (ts_begin := optional_add_secs_in_ms((get_last_ts_from_db(connect_to_n_ms_parsed_trades_db(secs_parse_interval), "BTCUSDT", filter=pymongo.DESCENDING)), ms_parse_interval)):
        ts_begin = round_to_last_n_secs(get_last_ts_from_db(connect_to_aggtrade_data_db(), "BTCUSDT"), secs_parse_interval)

    while True:
        time1 = time.time()
        symbol_prices = {iteration: {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)} for iteration in iterations}
        symbol_volumes = {iteration: {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)} for iteration in iterations}
        fund_symbol_volumes = {iteration: {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)} for iteration in iterations}

        for collection in connect_to_aggtrade_data_db().list_collection_names():
            if trades := query_aggtrade_data_col_timeframe(collection, ts_begin, ms_ts_range):
                for iteration in iterations:
                    leftover_trades, partial_trades_list = trades_in_range(trades, ts_begin, iteration + ms_parse_interval)
                    trades = leftover_trades
                    if partial_trades_list:
                        price = (partial_trades_list[0][PRICE] + partial_trades_list[-1][PRICE]) / 2
                        volume = sum([elem[PRICE] * elem[QUANTITY] for elem in partial_trades_list])
                        symbol_prices[iteration][collection] = price
                        symbol_volumes[iteration][collection] = volume

                        if collection in SP500_SYMBOLS_USDT_PAIRS:
                            fund_symbol_ratio_prices[iteration][collection] = price * coin_ratios[collection]
                            fund_symbol_volumes[iteration][collection] = volume

        insert_n_secs_parsed_trades_in_db(parsed_trades_all_symbols_db_name.format(secs_parse_interval), ts_begin, symbol_prices, symbol_volumes)
        insert_n_secs_parsed_trades_in_db(parsed_trades_fund_db_name.format(secs_parse_interval), ts_begin, symbol_prices, symbol_volumes)

        ts_begin += ms_ts_range

        print(time.time() - time1)

        if ts_begin > (time.time() * 1000):
            print("success.")
            exit(0)

        if time.time() - time1 > ms_ts_range / 1000:
            print("Parsing time is greater than range it is parsing, please check the code!!!")

        if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
            debug_running_execution += alive_debug_secs

