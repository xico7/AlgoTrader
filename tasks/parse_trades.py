import time

alive_debug_secs = 90


def parse_trades_ten_seconds():
    from MongoDB.Queries import query_aggtrade_data_col_timeframe
    from tasks.transform_trade_data import PRICE, QUANTITY
    from MongoDB.db_actions import connect_to_aggtrade_data_db, insert_n_secs_parsed_trades_db, \
        connect_to_n_ms_parsed_trades_db, insert_n_secs_fund_db
    from data_staging import coin_ratio, optional_add_secs_in_ms, round_to_last_n_secs, sec_to_ms, ms_to_secs, \
        SP500_SYMBOLS_USDT_PAIRS, get_last_ts_from_db, get_current_second, print_alive_if_passed_timestamp, trades_in_range

    all_symbols = connect_to_aggtrade_data_db().list_collection_names()

    coin_ratios = coin_ratio()
    debug_running_execution = get_current_second()

    ms_to_parse = 10000
    ts_range_in_ms = 120000
    iterations = range(divmod(ts_range_in_ms, ms_to_parse)[0])

    fund_symbol_ratio_prices = {iteration * ms_to_parse: {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)} for iteration in iterations}

    if not (ts_begin := optional_add_secs_in_ms((get_last_ts_from_db(connect_to_n_ms_parsed_trades_db("ms_to_parse"), "BTCUSDT")), ms_to_parse)):
        ts_begin = round_to_last_n_secs(get_last_ts_from_db(connect_to_aggtrade_data_db(), "BTCUSDT"), ms_to_secs(ms_to_parse))

    while True:
        symbol_prices = {iteration * ms_to_parse: {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)} for iteration in iterations}
        symbol_volumes = {iteration * ms_to_parse: {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)} for iteration in iterations}
        fund_symbol_volumes = {iteration * ms_to_parse: {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)} for iteration in iterations}

        time1 = time.time()

        for collection in connect_to_aggtrade_data_db().list_collection_names():
            if trades := query_aggtrade_data_col_timeframe(collection, ts_begin, ts_range_in_ms):
                for iteration in iterations:
                    leftover_trades, partial_trades_list = trades_in_range(trades, ts_begin, ms_to_parse, iteration + 1)
                    trades = leftover_trades
                    if partial_trades_list:
                        price = (partial_trades_list[0][PRICE] + partial_trades_list[-1][PRICE]) / 2
                        volume = sum([elem[PRICE] * elem[QUANTITY] for elem in partial_trades_list])
                        symbol_prices[iteration][collection] = price = price
                        symbol_volumes[iteration][collection] = volume
                        if collection in SP500_SYMBOLS_USDT_PAIRS:
                            fund_symbol_ratio_prices[iteration][collection] = price * coin_ratios[collection]
                            fund_symbol_volumes[iteration][collection] = volume
        print(time.time() - time1)
        time2 = time.time()
        insert_n_secs_parsed_trades_db(ts_begin, symbol_prices, symbol_volumes, ms_to_parse)
        insert_n_secs_fund_db(ts_begin, fund_symbol_ratio_prices, fund_symbol_volumes, ms_to_parse)

        ts_begin += sec_to_ms(ts_range_in_ms)
        print(time.time() - time2)

        if ts_begin > int(str(int(time.time())) + '000'):
            print("success.")
            exit(0)

        if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
            debug_running_execution += alive_debug_secs

