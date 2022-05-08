import time


def insert_fund_data():
    from data_staging import get_current_second_in_ms, coin_ratio, TEN_SECONDS_IN_MS, get_timeframe, \
        SP500_SYMBOLS_USDT_PAIRS, fill_symbol_prices, insert_fund_data, get_current_second, print_alive_if_passed_timestamp

    alive_debug_secs = 90
    symbol_prices = {SP500_SYMBOLS_USDT_PAIRS[i]: None for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
    timeframe = get_timeframe()
    fill_symbol_prices(symbol_prices, timeframe)
    coin_ratios = coin_ratio()
    debug_running_execution = get_current_second()
    while True:

        # TODO
        add_constant = {"seconds":TEN_SECONDS_IN_MS

        insert_fund_data(symbol_prices, timeframe, coin_ratios)
        timeframe += add_constant
        if timeframe > get_current_second_in_ms():
            time.sleep(add_constant + 20)

        if print_alive_if_passed_timestamp(debug_running_execution + alive_debug_secs):
            debug_running_execution += alive_debug_secs

