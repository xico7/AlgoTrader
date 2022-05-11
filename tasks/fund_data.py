import time


def insert_fund_data():
    from tasks.transform_trade_data import EVENT_TS, PRICE, QUANTITY
    from MongoDB.db_actions import connect_to_aggtrade_data_db, insert_one_to_sp500_db
    from data_staging import get_current_second_in_ms, coin_ratio, TEN_SECONDS, get_timeframe, \
        SP500_SYMBOLS_USDT_PAIRS, fill_symbol_prices, MongoDB, TEN_SECONDS_IN_MS, sum_values, get_current_second, print_alive_if_passed_timestamp

    alive_debug_secs = 90
    symbol_prices = {SP500_SYMBOLS_USDT_PAIRS[i]: None for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
    timeframe = get_timeframe()
    coin_ratios = coin_ratio()
    debug_running_execution = get_current_second()

    fill_symbol_prices(symbol_prices, timeframe)
    while True:
        add_constant = {"seconds": TEN_SECONDS, "milliseconds": TEN_SECONDS * 1000}

        symbol_ratio_value = {}
        symbol_volumes = {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
        for symbol in symbol_prices.keys():
            list_trades = list(connect_to_aggtrade_data_db().get_collection(symbol).find(
                {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: timeframe}},
                               {EVENT_TS: {MongoDB.LOWER_EQ: timeframe + TEN_SECONDS_IN_MS}}]}))

            if list_trades:
                symbol_prices[symbol] = float(list_trades[1][PRICE])

                for elem in list_trades:
                    symbol_volumes[symbol] += (float(elem[PRICE]) * float(elem[QUANTITY]))

        for symbol in symbol_prices:
            symbol_ratio_value[symbol] = symbol_prices[symbol] * coin_ratios[symbol]

        total_volume = sum_values(symbol_volumes)
        total_fund_value = sum_values(symbol_ratio_value)
        insert_one_to_sp500_db({EVENT_TS: timeframe, QUANTITY: total_volume, PRICE: total_fund_value})

        timeframe += add_constant["milliseconds"]
        if timeframe > get_current_second_in_ms():
            time.sleep(add_constant["seconds"] + 20)

        if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
            debug_running_execution += alive_debug_secs

