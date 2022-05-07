import time
from data_staging import TEN_SECONDS_IN_MS


def insert_fund_data():
    from MongoDB.Queries import SP500_SYMBOLS_USDT_PAIRS, get_timeframe, fill_symbol_prices, fund_data
    from data_staging import get_current_second_in_ms
    symbol_prices = {SP500_SYMBOLS_USDT_PAIRS[i]: None for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
    timeframe = get_timeframe()
    fill_symbol_prices(symbol_prices, timeframe)
    while True:
        fund_data(symbol_prices, timeframe)
        timeframe += TEN_SECONDS_IN_MS
        if timeframe > get_current_second_in_ms():
            time.sleep(20)

