import time

import requests
from data_staging import ONE_DAY_IN_SECS, MongoDB, get_symbols_normalized_fund_ratio, remove_usdt, get_current_time_ms, get_last_n_seconds
from MongoDB.db_actions import connect_to_timeframe_db, connect_to_aggtrade_data_db, insert_one_to_sp500_db
import matplotlib.pyplot as plt

from tasks.transform_aggtrades import EVENT_TS, PRICE, QUANTITY

buffer_time_in_ms = 10000
ten_seconds = 10
ten_seconds_in_ms = ten_seconds * 1000

SP500_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT',
                            'BCHUSDT',
                            'ALGOUSDT',
                            'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT',
                            'ICPUSDT',
                            'FILUSDT',
                            'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
                            'NEARUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
                            'LRCUSDT']

def coin_ratio():
    from tasks.ws_trades import coingecko_marketcap_api_link
    return get_symbols_normalized_fund_ratio(remove_usdt(SP500_SYMBOLS_USDT_PAIRS), requests.get(coingecko_marketcap_api_link).json())


def fill_symbol_prices(symbol_prices):

    for symbol in symbol_prices.keys():
        list_trades = list(connect_to_aggtrade_data_db().get_collection(symbol).find(
            {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: get_last_n_seconds(get_current_time_ms() - buffer_time_in_ms, ten_seconds)}},
                           {EVENT_TS: {MongoDB.LOWER_EQ: get_current_time_ms()}}]}))

        if list_trades:
            symbol_prices[symbol] = float(list_trades[-1]['p'])

    for symbol_value in symbol_prices.values():
        if not symbol_value:
            time.sleep(ten_seconds)
            fill_symbol_prices(symbol_prices)

    return


def sum_values(key_values):
    total = 0
    for value in key_values.values():
        total += value

    return total


def fund_data(values, start_time, time_range):

    symbol_ratio_value = {}
    symbol_volumes = {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
    for symbol in values.keys():
        list_trades = list(connect_to_aggtrade_data_db().get_collection(symbol).find(
            {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: start_time}},
                           {EVENT_TS: {MongoDB.LOWER_EQ: start_time + time_range}}]}))

        if list_trades:
            values[symbol] = float(list_trades[-1][PRICE])

            for elem in list_trades:
                symbol_volumes[symbol] += (float(elem[PRICE]) * float(elem[QUANTITY]))

    coin_ratios = coin_ratio()
    for symbol in values:
        symbol_ratio_value[symbol] = values[symbol] * coin_ratios[symbol]

    total_volume = sum_values(symbol_volumes)
    total_fund_value = sum_values(symbol_ratio_value)
    insert_one_to_sp500_db({"timestamp": start_time, "volume": total_volume, "value": total_fund_value})

    print("here")


def insert_fund_range(timeframe):
    total_count = 0
    all_hits_count = 0

    values = []
    for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
        for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
            {"begin_timestamp": {MongoDB.HIGHER_EQ: 0}})):
            if (element['last_price_counter'] == '2' or element['last_price_counter'] == '1') and element['range_percentage'] > 10 and \
                    (element['0'] + element['1']) < 7:
                all_hits_count += 1
                continue
            total_count += 1

    print("total hits", all_hits_count)
    print("total count", total_count)
    print("here")


def query_all_ranges(timeframe):
    total_count = 0
    all_hits_count = 0

    values = []
    for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
        for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
            {"begin_timestamp": {MongoDB.HIGHER_EQ: 0}})):
            if (element['last_price_counter'] == '2' or element['last_price_counter'] == '1') and element['range_percentage'] > 10 and \
                    (element['0'] + element['1']) < 7:
                all_hits_count += 1
                continue
            total_count += 1

    print("total hits", all_hits_count)
    print("total count", total_count)
    print("here")


def show_range_percentage_plot(timeframe):
    values = []
    for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
        for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
            {"begin_timestamp": {MongoDB.HIGHER_EQ: 0}})):
            values.append(element['range_percentage'])

        print("here")

    n, bins, patches = plt.hist(values)
    plt.show()


def get_timeframe():
    get_last_n_seconds(get_current_time_ms(), 1)

    # return 60000 + list(mongo.connect_to_timeframe_db(timeframe).get_collection("BTCUSDT").find(
    #     {END_TS: {MongoDB.HIGHER_EQ: 0}}))[-1][END_TS]

symbol_prices = {SP500_SYMBOLS_USDT_PAIRS[i]: None for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
fill_symbol_prices(symbol_prices)
while True:
    fund_data(symbol_prices, get_timeframe(), ten_seconds)

