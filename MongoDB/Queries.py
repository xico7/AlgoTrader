import time

import requests
from data_staging import ONE_DAY_IN_SECS, MongoDB, get_symbols_normalized_fund_ratio, remove_usdt, \
    get_current_second_in_ms, get_last_n_seconds, TEN_SECONDS, get_last_second, ONE_HOUR_IN_MS, FIFTEEN_MIN_IN_MS, \
    TEN_SECONDS_IN_MS
from MongoDB.db_actions import connect_to_timeframe_db, connect_to_aggtrade_data_db, insert_one_to_sp500_db, \
    connect_to_sp500_db_collection
import matplotlib.pyplot as plt

from tasks.transform_aggtrades import EVENT_TS, PRICE, QUANTITY


BEGIN_TIMESTAMP = "begin_timestamp"

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


def sum_values(key_values):
    total = 0
    for value in key_values.values():
        total += value

    return total


def fund_data(values, start_time):
    symbol_ratio_value = {}
    symbol_volumes = {SP500_SYMBOLS_USDT_PAIRS[i]: 0 for i, elem in enumerate(SP500_SYMBOLS_USDT_PAIRS)}
    for symbol in values.keys():
        list_trades = list(connect_to_aggtrade_data_db().get_collection(symbol).find(
            {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: start_time}},
                           {EVENT_TS: {MongoDB.LOWER_EQ: start_time + TEN_SECONDS_IN_MS}}]}))

        if list_trades:
            values[symbol] = float(list_trades[-1][PRICE])

            for elem in list_trades:
                symbol_volumes[symbol] += (float(elem[PRICE]) * float(elem[QUANTITY]))

    coin_ratios = coin_ratio()
    for symbol in values:
        symbol_ratio_value[symbol] = values[symbol] * coin_ratios[symbol]

    total_volume = sum_values(symbol_volumes)
    total_fund_value = sum_values(symbol_ratio_value)
    insert_one_to_sp500_db({BEGIN_TIMESTAMP: start_time, "volume": total_volume, "value": total_fund_value})



def insert_fund_range(timeframe):
    total_count = 0
    all_hits_count = 0

    for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
        for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
                {BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}})):
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

    for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
        for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
                {BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}})):
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
                {BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}})):
            values.append(element['range_percentage'])

        print("here")

    n, bins, patches = plt.hist(values)
    plt.show()


def fill_symbol_prices(symbol_prices, end_ts):
    for symbol in symbol_prices.keys():
        list_trades = list(connect_to_aggtrade_data_db().get_collection(symbol).find(
            {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: end_ts - FIFTEEN_MIN_IN_MS}},
                           {EVENT_TS: {MongoDB.LOWER_EQ: end_ts}}]}))

        if list_trades:
            symbol_prices[symbol] = float(list_trades[-1][PRICE])

    return


def get_timeframe():
    sp500_elements = list(connect_to_sp500_db_collection().find({BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}}))
    if sp500_elements:
        return sp500_elements[-1][BEGIN_TIMESTAMP]
    else:
        return get_last_n_seconds(get_current_second_in_ms(), 60)

