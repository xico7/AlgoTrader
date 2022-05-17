import time


def parse_trades_ten_seconds():
    from tasks.transform_trade_data import EVENT_TS, PRICE, QUANTITY
    from MongoDB.db_actions import connect_to_aggtrade_data_db, insert_many_to_ten_secs_parsed_trades, \
        connect_to_ten_secs_parsed_trades_db, insert_ten_secs_fund_db
    from data_staging import coin_ratio, round_to_last_n_secs, TEN_SECONDS_IN_MS, \
        SP500_SYMBOLS_USDT_PAIRS, get_last_ts_from_db, MongoDB, sum_values, get_current_second, print_alive_if_passed_timestamp

    alive_debug_secs = 90
    all_symbols = connect_to_aggtrade_data_db().list_collection_names()

    coin_ratios = coin_ratio()
    debug_running_execution = get_current_second()

    fund_symbol_ratio_prices = {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in
                                enumerate(SP500_SYMBOLS_USDT_PAIRS)}  # Doesn't reset because of fund price stability.

    while True:
        symbol_prices = {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)}
        symbol_volumes = {all_symbols[i]: 0 for i, elem in enumerate(all_symbols)}
        fund_symbol_volumes = {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}

        time1 = time.time()
        for symbol in connect_to_aggtrade_data_db().list_collection_names():
            if not (oldest_ts := (get_last_ts_from_db(connect_to_ten_secs_parsed_trades_db(), symbol) + TEN_SECONDS_IN_MS)):
                oldest_ts = get_last_ts_from_db(connect_to_aggtrade_data_db(), symbol)
            # TODO: Dividir o oldest ts
            ts_begin = round_to_last_n_secs(oldest_ts, 10)

            list_trades = list(connect_to_aggtrade_data_db().get_collection(symbol).find(
                {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: ts_begin}},
                               {EVENT_TS: {MongoDB.LOWER_EQ: ts_begin + TEN_SECONDS_IN_MS}}]}))

            if list_trades:
                price = (float(list_trades[0][PRICE]) + float(list_trades[-1][PRICE])) / 2
                volume = sum([(float(elem[PRICE]) * float(elem[QUANTITY])) for elem in list_trades])

                symbol_prices[symbol] = price
                symbol_volumes[symbol] = volume
                if symbol in SP500_SYMBOLS_USDT_PAIRS:
                    fund_symbol_ratio_prices[symbol] = price * coin_ratios[symbol]
                    fund_symbol_volumes[symbol] = volume

        insert_many_to_ten_secs_parsed_trades(ts_begin, symbol_prices, symbol_volumes)
        insert_ten_secs_fund_db({EVENT_TS: ts_begin, PRICE: sum_values(fund_symbol_ratio_prices),
                                 QUANTITY: sum_values(fund_symbol_volumes)})

        print(time.time() - time1)

        if ts_begin > int(str(int(time.time())) + '000'):
            print("success.")
            exit(0)

        if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
            debug_running_execution += alive_debug_secs

