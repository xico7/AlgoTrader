import pymongo
from MongoDB.db_actions import connect_to_db, parsed_aggtrades
from data_staging import round_last_n_secs, get_current_second_in_ms
from vars_constants import DB_TS, MongoDB, default_parse_interval, DEFAULT_COL_SEARCH


def query_parsed_aggtrade(symbol, ts_begin, ts_end):
    return query_db_col_highereq_lowereq(parsed_aggtrades, symbol, ts_begin, ts_end)


def query_db_col_highereq_lowereq(db_name, col_name, highereq, lowereq, column_name=DB_TS):
    return list(connect_to_db(db_name).get_collection(col_name).find(
        {MongoDB.AND: [{column_name: {MongoDB.HIGHER_EQ: highereq}},
                       {column_name: {MongoDB.LOWER_EQ: lowereq}}]}))


def query_parsed_aggtrade_multiple_timeframes(symbols: list, ts_begin, ts_end):
    return {symbol: query_parsed_aggtrade(symbol, ts_begin[symbol], ts_end[symbol]) for symbol in symbols}

def get_first_ts_from_db(database_conn, collection, ts_filter=pymongo.ASCENDING):
    return query_db_col_oldest_ts(database_conn, collection, ts_filter=ts_filter)


def query_db_col_oldest_ts(db_name, collection, round_secs=default_parse_interval, ts_filter=pymongo.ASCENDING):
    return round_last_n_secs(list(connect_to_db(db_name).get_collection(collection).find(
        {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: 0}},
                       {DB_TS: {MongoDB.LOWER_EQ: get_current_second_in_ms()}}]}).sort(
        DB_TS, ts_filter).limit(1))[0][DB_TS], round_secs)


def query_db_col_newest_ts(db_name, collection, round_secs=default_parse_interval, init_db=None, ts_filter=pymongo.DESCENDING):
    try:
        return round_last_n_secs(list(connect_to_db(db_name).get_collection(collection).find(
            {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: 0}},
                           {DB_TS: {MongoDB.LOWER_EQ: get_current_second_in_ms()}}]}).sort(
            DB_TS, ts_filter).limit(1))[0][DB_TS], round_secs)
    except IndexError:
        if not init_db:
            return None
        else:
            return query_db_col_oldest_ts(init_db, collection)


def query_existing_ws_trades(min_val, max_val, ms_parse_interval):
    existing_trade_test_interval_in_ms = 3000000
    existing_trades = []
    test_range = list(range(min_val, max_val, existing_trade_test_interval_in_ms))

    for elem in test_range:
        if query_parsed_aggtrade(DEFAULT_COL_SEARCH, elem, elem + existing_trade_test_interval_in_ms):
            existing_trades += list(range(elem, elem + existing_trade_test_interval_in_ms, ms_parse_interval))
    else:
        if query_parsed_aggtrade(DEFAULT_COL_SEARCH, test_range[-1], max_val):
            existing_trades += list(range(test_range[-1], max_val, ms_parse_interval))

    return existing_trades