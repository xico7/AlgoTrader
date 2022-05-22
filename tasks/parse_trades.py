import time



alive_debug_secs = 90


def parse_trades_ten_seconds():
    from MongoDB.Queries import query_aggtrade_data_col_timeframe
    from tasks.transform_trade_data import PRICE, QUANTITY
    from MongoDB.db_actions import connect_to_aggtrade_data_db, insert_many_to_ten_secs_parsed_trades, \
        connect_to_ten_secs_parsed_trades_db
    from data_staging import coin_ratio, add_n_secs, round_to_last_n_secs, \
        SP500_SYMBOLS_USDT_PAIRS, get_last_ts_from_db, get_current_second, print_alive_if_passed_timestamp

    all_symbols = connect_to_aggtrade_data_db().list_collection_names()

    coin_ratios = coin_ratio()
    debug_running_execution = get_current_second()

    fund_symbol_ratio_prices = {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}

    while True:
        symbol_prices = {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)}
        symbol_volumes = {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)}
        fund_symbol_volumes = {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
        all_sum = 0
        time1 = time.time()

        add_secs = 10
        if not (ts_begin := add_n_secs((get_last_ts_from_db(connect_to_ten_secs_parsed_trades_db(), "BTCUSDT")), add_secs)):
            ts_begin = round_to_last_n_secs(get_last_ts_from_db(connect_to_aggtrade_data_db(), "BTCUSDT"), add_secs)

        for collection in connect_to_aggtrade_data_db().list_collection_names():
            if list_trades := query_aggtrade_data_col_timeframe(collection, ts_begin, 10):
                price = (list_trades[0][PRICE] + list_trades[-1][PRICE]) / 2
                volume = sum([elem[PRICE] * elem[QUANTITY] for elem in list_trades])

                symbol_prices[collection] = price
                symbol_volumes[collection] = volume
                if collection in SP500_SYMBOLS_USDT_PAIRS:
                    fund_symbol_ratio_prices[collection] = price * coin_ratios[collection]
                    fund_symbol_volumes[collection] = volume

        time2 = time.time()
        insert_many_to_ten_secs_parsed_trades(ts_begin, symbol_prices, symbol_volumes)
        # insert_ten_secs_fund_db({EVENT_TS: ts_begin, PRICE: sum_values(fund_symbol_ratio_prices),
        #                          QUANTITY: sum_values(fund_symbol_volumes)})
        print(time.time() - time2)
        all_sum += time.time() - time2
        print(time.time() - time1)

        if ts_begin > int(str(int(time.time())) + '000'):
            print("success.")
            exit(0)

        if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
            debug_running_execution += alive_debug_secs

