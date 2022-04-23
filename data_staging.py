from datetime import datetime

import MongoDB.db_actions as mongo
import copy
import re
import time
from typing import Optional, Union, List
import requests as requests
from enum import Enum
import numpy as np
import talib
from numpy import double


#### OHLC ####

OHLC_OPEN = 'o'
OHLC_CLOSE = 'c'
OHLC_HIGH = 'h'
OHLC_LOW = 'l'

##############


#### Timeframes ####
WEEK_DAYS = 7

ONE_MIN_IN_SECS = 60
FIVE_MIN_IN_SECS = ONE_MIN_IN_SECS * 5
FIFTEEN_MIN_IN_SECS = ONE_MIN_IN_SECS * 15
THIRTY_MIN_IN_SECS = FIFTEEN_MIN_IN_SECS * 2
ONE_HOUR_IN_SECS = ONE_MIN_IN_SECS * 60
FOUR_HOUR_IN_SECS = ONE_HOUR_IN_SECS * 4
ONE_DAY_IN_SECS = ONE_HOUR_IN_SECS * 24

###################

USDT = "USDT"
TS = 'timestamp'
TIME = 'Time'
TIMESTAMP = 't'
SYMBOLS_VOLUME = 'symbols_volume'
PRICE = 'Price'
RS = 'RS'
AVG_RS = 'average_rs'
SYMBOL = 'symbol'
VOLUME = 'v'
VALUE = 'Value'
TOTAL_VOLUME = 'TotalVolume'


class MongoDB:
    EQUAL = '$eq'
    LOWER_EQ = '$lte'
    LOWER = '$lt'
    HIGHER_EQ = '$gte'
    HIGHER = '$gt'
    AND = '$and'


class db_to_timestamp(Enum):
    OHLC_1day = 60 * 60 * 24
    OHLC_5minutes = 60 * 5
    TA_RS_VOL = None


def get_element_chart_percentage_line(pricevalue, rs_chart):
    key_pricevalue: tuple = (None, 999999999)
    for k, v in rs_chart.items():
        if int(k) > 0:
            if pricevalue > v['Value'] and pricevalue < 999999999:
                key_pricevalue = (k, pricevalue)
        else:
            if pricevalue < v['Value'] and pricevalue > key_pricevalue[1]:
                key_pricevalue = (k, pricevalue)

    return key_pricevalue[0]


#TODO: refactor.. this is getting too many values.. i only need one.. perf implicactions.
def get_minutes_after_ts(symbol, timestamp):
    one_min_db = mongo.connect_to_1m_ohlc_db()
    return list(one_min_db.get_collection(symbol).find({MongoDB.AND: [
        {TIME: {MongoDB.HIGHER_EQ: timestamp}},
        {TIME: {MongoDB.LOWER_EQ: timestamp + 60 * 1200}}]
    }))[::-1]


def get_current_time() -> int:
    return int(time.time())


def get_last_minute(timestamp):
    while timestamp % 60 != 0:
        timestamp -= 1
    return timestamp


def remove_usdt(symbols: Union[List[str], str]):
    if isinstance(symbols, str):
        try:
            return re.match('(^(.+?)USDT)', symbols).groups()[1].upper()
        except AttributeError as e:
            return None
    else:
        return [re.match('(^(.+?)USDT)', symbol).groups()[1].upper() for symbol in symbols]


def get_data_from_keys(data, *keys):
    data_keys = {}
    for key in keys:
        data_keys.update({key: data[key]})
    return data_keys


def usdt_symbols_stream(type_of_trade: str) -> list:
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()

    binance_symbols = []
    for symbol_info in binance_symbols_price:
        symbol = symbol_info["symbol"]
        if USDT in symbol:
            binance_symbols.append(symbol)

    return [f"{symbol.lower()}{type_of_trade}" for symbol in binance_symbols]


def usdt_with_bnb_symbols_stream(type_of_trade: str) -> list:
    symbols = usdt_symbols_stream(type_of_trade)
    bnb_symbols = []
    for symbol in symbols:
        bnb_suffix_elem_search = symbol.replace(USDT, "BNB")
        bnb_prefix_elem_search = "BNB" + symbol.replace(USDT, "")
        if bnb_suffix_elem_search in symbol or bnb_prefix_elem_search in symbol:
            bnb_symbols.append(symbol)

    return bnb_symbols


def non_existing_record(collection_feed, timestamp):
    return not bool(collection_feed.find({TS: {MongoDB.EQUAL: timestamp}}).count())


async def update_ohlc_cached_values(ws_trade_data: dict, cache):
    from tasks.ws_trades import OHLC_CACHE_PERIODS
    symbol_pair = ws_trade_data['s']
    ohlc_trade_data = get_data_from_keys(ws_trade_data, TIMESTAMP, VOLUME, OHLC_OPEN, OHLC_HIGH, OHLC_LOW, OHLC_CLOSE)
    latest_timestamp = ohlc_trade_data[TIMESTAMP]

    if symbol_pair not in cache.coins_current_ohlcs:
        cache.coins_current_ohlcs.symbol_dict_update(symbol_pair, ohlc_trade_data)

    cache.coins_current_ohlcs.symbol_ohlc_update(symbol_pair, ohlc_trade_data)

    if latest_timestamp > cache.coins_current_ohlcs[symbol_pair][TIMESTAMP]:
        cache.coins_ohlc_data.ohlc_update(symbol_pair, cache.coins_current_ohlcs[symbol_pair], OHLC_CACHE_PERIODS)

        del cache.coins_current_ohlcs[symbol_pair]

        if latest_timestamp > cache.marketcap_latest_timestamp:
            cache.marketcap_latest_timestamp = latest_timestamp
            if (cache.marketcap_current_ohlc[TIMESTAMP] > 0 and not cache.marketcap_ohlc_data) or \
                    (cache.marketcap_ohlc_data and (
                    cache.marketcap_ohlc_data[len(cache.marketcap_ohlc_data)][TIMESTAMP] != cache.marketcap_current_ohlc[TIMESTAMP])):
                cache.marketcap_ohlc_data.my_update(cache.marketcap_current_ohlc)


def get_coin_fund_ratio(symbol_pairs: dict, symbols_information: dict):
    coin_ratio = {}

    for symbol_info in symbols_information:
        current_symbol = symbol_info[SYMBOL].upper()
        if current_symbol in symbol_pairs:
            coin_ratio.update({current_symbol: symbol_info['market_cap'] / symbol_info['current_price']})

    return coin_ratio


def query_db_documents(db_feed, collection, number_of_periods, current_minute):
    def query_ohlc_db(ohlc_timeframe):
        query = list(db_feed.get_collection(collection).find({MongoDB.AND: [
            {TIME: {MongoDB.HIGHER_EQ: current_minute - ohlc_timeframe * (number_of_periods + 1)}},
            {TIME: {MongoDB.LOWER_EQ: current_minute}}
        ]
        }))

        return query[0] if number_of_periods == 1 else query

    if db_feed.name == db_to_timestamp.OHLC_1day.name:
        return query_ohlc_db(db_to_timestamp.OHLC_1day.value)
    elif db_feed.name == db_to_timestamp.OHLC_5minutes.name:
        return query_ohlc_db(db_to_timestamp.OHLC_5minutes.value)
    elif db_feed.name == db_to_timestamp.TA_RS_VOL.name:
        return db_feed.get_collection(collection).find_one(sort=[("E", -1)])
    else:
        return NotImplementedError("Please implement.")


def query_latest_collection(db_feed, collection):
    return query_db_documents(db_feed, collection, 1, get_current_time())


def query_rel_vol(current_minute, rel_vol_db, number_of_periods):
    symbols_relative_volume = {}
    for collection in rel_vol_db.list_collection_names():
        symbol_last_n_periods_volume = 0
        try:
            for elem in query_db_documents(rel_vol_db, collection, number_of_periods, current_minute)[
                        1:number_of_periods]:
                symbol_last_n_periods_volume += elem[VOLUME] / number_of_periods
            symbols_relative_volume[collection] = query_latest_collection(rel_vol_db, collection)[VOLUME] / symbol_last_n_periods_volume
        except ZeroDivisionError:
            continue
        except IndexError:
            continue
    return symbols_relative_volume


def query_atr(current_minute, atr_db, number_of_periods):
    coins_fiveminutes_atr = {}
    for collection in atr_db.list_collection_names():
        high, low, close = [], [], []

        for elem in query_db_documents(atr_db, collection, number_of_periods, current_minute)[:number_of_periods]:
            high.append(elem['h'])
            low.append(elem['l'])
            close.append(elem['c'])

        if len(high) == number_of_periods:
            coins_fiveminutes_atr[collection] = \
            talib.ATR(np.array(high), np.array(low), np.array(close), timeperiod=number_of_periods - 1)[-1]

    return coins_fiveminutes_atr


def add_elem_to_chart(element, symbols_data, symbol_moment_price):
    counter = get_counter(element['Price'] * 100 / symbol_moment_price - 100)

    if counter not in symbols_data:
        symbols_data.update({str(counter): {VALUE: symbol_moment_price * (1 + (counter * 0.01)),
                                            VOLUME: element[VOLUME], AVG_RS: element['RS']}}, )
    else:
        symbols_data[str(counter)][VOLUME] += element[VOLUME]
        symbols_data[str(counter)][AVG_RS] += element[VOLUME] / symbols_data[counter][VOLUME] * element['RS']

    return symbols_data


def create_last_day_rs_chart(timestamp, rs_vol_db):
    timestamp_minus_one_day = timestamp - (60 * 60 * 24)
    rel_strength_db = mongo.connect_to_rs_db()


    coins_moment_prices = {}
    for col in rs_vol_db.list_collection_names():
        coins_moment_prices[col] = query_latest_collection(rs_vol_db, col)['p']

    symbol_data, all_symbols_data = {}, {}
    for col in rel_strength_db.list_collection_names():
        for elem in list(rel_strength_db.get_collection(col).find(
                {MongoDB.AND: [{TIME: {MongoDB.HIGHER_EQ: timestamp_minus_one_day}},
                               {TIME: {MongoDB.LOWER_EQ: timestamp_minus_one_day + ONE_DAY_IN_SECS}}]}).rewind()):
            add_elem_to_chart(elem, symbol_data, float(coins_moment_prices[col]))

        volume_sum = 0
        for value in list(symbol_data.values()):
            volume_sum += value[VOLUME]
        for value in list(symbol_data.values()):
            value['pct_vol'] = value[VOLUME] / volume_sum
        all_symbols_data.update({col: symbol_data})
        symbol_data = {}

    return all_symbols_data


def get_counter(number):
    counter = 0
    if number < 0:
        while number < 0:
            number += 0.5
            counter -= 1
    else:
        while number > 0:
            number -= 0.5
            counter += 1

    return counter


def query_rs_signal_chart(timestamp):
    from tasks.ta_signal import RS_SANITY_VALUE_THRESHOLD, RS_THRESHOLD
    signal = {}

    if query_last_day_rs_chart(past_30_min_timestamp(timestamp)):
        for symbol, rs_chart in query_last_day_rs_chart(past_30_min_timestamp(timestamp)):
            for elem_one_minute in query_symbol_1m_ohlc_data(symbol, timestamp, future_30_min_timestamp(timestamp)):
                symbol_rs_chart_pct = get_element_chart_percentage_line(elem_one_minute[OHLC_CLOSE], rs_chart)

                if symbol_rs_chart_pct and (elem_one_minute[RS] > rs_chart[symbol_rs_chart_pct][AVG_RS] > 0):
                    rs_difference = abs(elem_one_minute[RS] - (rs_chart[symbol_rs_chart_pct][AVG_RS]))

                    if RS_SANITY_VALUE_THRESHOLD > rs_difference > RS_THRESHOLD:
                        signal_values = {str(elem_one_minute[TIME]): {
                            'RS_difference': rs_difference,
                            'Value': elem_one_minute[OHLC_CLOSE],
                            'RS_Chart': rs_chart}}
                        if symbol not in signal:
                            signal[symbol] = [signal_values]
                        else:
                            signal[symbol].append(signal_values)

        return signal  # cache_signals


def query_db_ta_value(colletion, timestamp, value_to_filter, collection_item_key, database=mongo.connect_to_ta_analysis_db()):
    symbols_ta_values = {}

    for elem in list(database.get_collection(colletion).find({MongoDB.AND: [
        {TS: {MongoDB.HIGHER_EQ: past_30_min_timestamp(timestamp)}},
        {TS: {MongoDB.LOWER_EQ: timestamp}}]})):
        if elem[collection_item_key] != 0:
            for ta_symbol, ta_value in elem[collection_item_key].items():
                if ta_value > value_to_filter:
                    collection_values = {elem[TS]: ta_value}
                    if ta_symbol not in symbols_ta_values:
                        symbols_ta_values[ta_symbol] = [collection_values]
                    else:
                        symbols_ta_values[ta_symbol].append(collection_values)

    return symbols_ta_values


def past_30_min_timestamp(timestamp: int) -> int:
    return timestamp - 1800


def future_30_min_timestamp(timestamp: int) -> int:
    return timestamp + 1800


def query_last_day_rs_chart(timestamp: int):
    try:
        return list(mongo.connect_to_ta_analysis_db().get_collection("last_day_rs_chart").find({MongoDB.AND: [
        {TS: {MongoDB.HIGHER_EQ: timestamp}}, {TS: {MongoDB.LOWER_EQ: timestamp + 1}}]}))[0]['rs_chart'].items()
    except IndexError:
        return None


def query_symbol_1m_ohlc_data(symbol, begin_timestamp, end_timestamp):
    one_min_ohlc_db = mongo.connect_to_1m_ohlc_db()
    return list(one_min_ohlc_db.get_collection(symbol).find({MongoDB.AND: [
        {TIME: {MongoDB.HIGHER_EQ: begin_timestamp}},
        {TIME: {MongoDB.LOWER_EQ: end_timestamp - 1}}]}))


def get_ta_indicator_when_rs_threshold(ta_indicator_signal, rs_signal):
    merged_signal = {}
    for symbol, symbol_rs_value in rs_signal.items():
        try:
            for rs_dict in symbol_rs_value:
                for ta_ind_elem in ta_indicator_signal[symbol]:
                    if int(list(rs_dict.keys())[0]) == list(ta_ind_elem.keys())[0]:
                        if symbol not in merged_signal:
                            merged_signal[symbol] = [ta_ind_elem]
                        else:
                            merged_signal[symbol].append(ta_ind_elem)
        except KeyError:
            continue

    return merged_signal


def get_joined_signals(rs, long_vol, short_vol, atrp):
    final_signal = {}
    for symbol, short_vol_values in short_vol.items():
        for short_vol_elem in short_vol_values:
            try:
                for long_vol_elem in long_vol[symbol]:
                    for atrp_elem in atrp[symbol]:
                        if short_vol_elem.keys() == long_vol_elem.keys() == atrp_elem.keys():
                            ts_key, short_vol_v = list(short_vol_elem.items())[0]
                            ts_key2, long_vol_v = list(long_vol_elem.items())[0]
                            ts_key3, atrp_v = list(atrp_elem.items())[0]

                            rs_values = {}
                            for elem in rs[symbol]:
                                if list(elem.items())[0][0] == str(list(short_vol_elem.items())[0][0]):
                                    rs_values = list(elem.values())[0]
                                    break
                            value = {str(ts_key): {'short_rvol': short_vol_v, 'long_rvol': long_vol_v,
                                                   'atr': atrp_v, 'value': rs_values['Value'],
                                                   'rs_difference': rs_values['RS_difference'], 'rs_chart': rs_values['RS_Chart']}}
                            if symbol not in final_signal:
                                final_signal[symbol] = [value]
                            else:
                                final_signal[symbol].append(value)
            except KeyError:
                continue

    return final_signal


def print_alive_if_passed_timestamp(timestamp):
    if get_current_time() > timestamp:
        print(datetime.fromtimestamp(get_current_time()))
        return True
