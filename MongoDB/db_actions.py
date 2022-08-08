import logging
from operator import itemgetter
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

import logs
from data_staging import get_current_second_in_ms
from vars_constants import DB_TS, MongoDB, DEFAULT_COL_SEARCH, PARSED_AGGTRADES_DB, AGGTRADES_DB

trades_chart = '{}_trades_chart'
localhost = 'localhost:27017/'
base_mongo_conn_string = 'mongodb://localhost:27017/'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def mongo_client_db(db_name: str):
    return MongoClient(f'{base_mongo_conn_string}{db_name}')


mongo_client = MongoClient(f'{base_mongo_conn_string}')


try:
    LOG.info("Querying MongoDB to check if DB is available.")
    mongo_client.list_database_names()
except ServerSelectionTimeoutError as e:
    if (localhost and 'Connection refused') in e.args[0]:
        LOG.exception("Cannot connect to localhosts mongo DB.")
        raise
    else:
        LOG.exception("Unexpected error while trying to connect to MongoDB.")
        raise

def list_dbs():
    return list(mongo_client.list_database_names())
def list_db_cols(db_name):
    return connect_to_db(db_name).list_collection_names()
def query_db_collection(db_name, col):
    return connect_to_db(db_name).get_collection(col)
def connect_to_bundled_aggtrade_db():
    return connect_to_db(AGGTRADES_DB)
def connect_to_parsed_aggtrade_db():
    return connect_to_db(PARSED_AGGTRADES_DB)
def connect_to_db(db_name):
    return mongo_client_db(db_name).get_default_database()


def insert_parsed_aggtrades(data: dict) -> None:
    for key in list(data.keys()):
        connect_to_parsed_aggtrade_db().get_collection(key).insert_many(data[key])


def insert_many_db(db, data, symbol) -> None:
    connect_to_db(db).get_collection(symbol).insert_many(data)


def insert_bundled_aggtrades(data) -> None:
    insert_many_db(AGGTRADES_DB, data, AGGTRADES_DB)


def delete_all_text_dbs(text) -> None:
    for db in list_dbs():
        if text in db:
            mongo_client.drop_database(db)


def create_index_db_cols(db_name, field) -> None:
    for col in list_db_cols(db_name):
        query_db_collection(db_name, col).create_index([(field, -1)])


def query_parsed_aggtrade(symbol, ts_begin, ts_end):
    return query_db_col_between(PARSED_AGGTRADES_DB, symbol, ts_begin, ts_end)


def query_db_col_between(db_name, col, highereq, lowereq, column_name=DB_TS):
    return list(query_db_collection(db_name, col).find(
        {MongoDB.AND: [{column_name: {MongoDB.HIGHER_EQ: highereq}},
                       {column_name: {MongoDB.LOWER_EQ: lowereq}}]}))


def query_all_dbcol_values(db_name, collection) -> list:
    return query_db_col_between(db_name, collection, 0, get_current_second_in_ms())


def query_parsed_aggtrade_multiple_timeframes(symbols: list, ts_begin, ts_end):
    return {symbol: query_parsed_aggtrade(symbol, ts_begin[symbol], ts_end[symbol]) for symbol in symbols}


def query_db_col_earliest_oldest_ts(db_name, collection, earliest=True):
    if values := query_all_dbcol_values(db_name, collection):
        values = sorted(values, key=itemgetter(DB_TS))
        first_value_ts, last_value_ts = values[0][DB_TS], values[-1][DB_TS]

        if earliest:
            return first_value_ts if first_value_ts > last_value_ts else last_value_ts
        else:
            return first_value_ts if first_value_ts < last_value_ts else last_value_ts
    return None


def query_db_col_oldest_ts(db_name, collection):
    return query_db_col_earliest_oldest_ts(db_name, collection, earliest=False)


def query_db_col_earliest(db_name, collection):
    return query_db_col_earliest_oldest_ts(db_name, collection)


def query_starting_ts(db_name, collection, init_db=None):
    if values := query_db_col_earliest(db_name, collection):
        pass
    elif not init_db:
        values = None
    else:
        values = query_db_col_oldest_ts(init_db, collection)

    return values


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


# def query_db_col_oldest_ts(db_name, collection, round_secs=default_parse_interval, ts_filter=pymongo.ASCENDING):
#     return round_last_n_secs(list(connect_to_db(db_name).get_collection(collection).find(
#         {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: 0}},
#                        {DB_TS: {MongoDB.LOWER_EQ: get_current_second_in_ms()}}]}).sort(
#         DB_TS, ts_filter).limit(1))[0][DB_TS], round_secs)

 # def insert_many_to_db(db_name, data: dict) -> None:
 #    for symbol, chart_data in data.items():
 #        for cached_chart_data in chart_data:
 #            try:
 #                connect_to_db(db_name).get_collection(symbol).insert_one(timeframe[key])
 #            except pymongo.errors.DuplicateKeyError:
 #                # skip document because it already exists in new collection
 #                continue