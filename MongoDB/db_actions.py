import contextlib
import logging

import pymongo
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

import logs
from data_staging import mins_to_ms, round_last_ten_secs, get_current_second_in_ms
from vars_constants import TS, MongoDB, DEFAULT_SYMBOL_SEARCH, PARSED_AGGTRADES_DB

trades_chart = '{}_trades_chart'
localhost = 'localhost:27017/'
base_mongo_conn_string = f'mongodb://{localhost}'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class EmptyInitDB(Exception): pass
class InvalidDataProvided(Exception): pass
class InvalidArgumentsProvided(Exception): pass
class EmptyCollectionInDB(Exception): pass


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
    return [mongo_client.list_database_names()]


def connect_to_db(db_name):
    return MongoClient(f'{base_mongo_conn_string}{db_name}').get_default_database()


def insert_many_same_db_col(db, data) -> None:
    connect_to_db(db).get_collection(db).insert_many(data)


def query_db_col_between(db_name, col, highereq, lowereq, column_name=TS, limit=0, sort_value=pymongo.DESCENDING):
    query_val = list(connect_to_db(db_name).get_collection(col).find({
        MongoDB.AND: [{column_name: {MongoDB.HIGHER_EQ: highereq}},
                      {column_name: {MongoDB.LOWER_EQ: lowereq}}]}).sort(column_name, sort_value).limit(limit))

    return query_val[0] if len(query_val) == 1 else query_val


def query_parsed_aggtrade_multiple_timeframes(symbols: list, ts_begin, ts_end):
    return {symbol: query_db_col_between(PARSED_AGGTRADES_DB, symbol, ts_begin[symbol], ts_end[symbol]) for symbol in symbols}


def query_db_col_ts_endpoint(db_name, collection, most_recent: bool = True):
    if query_val := query_db_col_between(db_name, collection, 0, get_current_second_in_ms(), limit=1,
                                         sort_value=pymongo.DESCENDING if most_recent else pymongo.ASCENDING):
        return query_val[TS]


def query_starting_ts(db_name, collection, init_db=None):
    if value := query_db_col_ts_endpoint(db_name, collection):
        return value
    else:
        if init_db:
            with contextlib.suppress(EmptyCollectionInDB):
                return round_last_ten_secs(query_db_col_ts_endpoint(init_db, collection, False))

    raise InvalidDataProvided(f"symbol '{collection}' doesn't have a valid timestamp in db "
                              f"'{db_name}' and no other db to initialize from.")


def query_existing_ws_trades(start_ts, end_ts, ms_parse_interval):  # If BTCUSDT has trades working it assumes all other symbols were working.
    symbols_earliest_ts = min(list(start_ts.values()))
    symbols_oldest_ts = max(list(end_ts.values()))

    existing_trades = []
    assume_existing_trade_parse_interval = mins_to_ms(3)
    for elem in range(symbols_earliest_ts, symbols_oldest_ts, assume_existing_trade_parse_interval):
        if query_db_col_between(PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, elem, elem + assume_existing_trade_parse_interval, limit=1):
            existing_trades += list(range(elem, elem + assume_existing_trade_parse_interval, ms_parse_interval))

    return existing_trades


# def delete_all_text_dbs(text) -> None:
#     for db in list_dbs():
#         if text in db:
#             mongo_client.drop_database(db)
#
#
# def create_index_db_cols(db_name, field) -> None:
#     for col in list_db_cols(db_name):
#         query_db_collection(db_name, col).create_index([(field, -1)])
#         print("Collection Done.")


# create_index_db_cols('parsed_aggtrades', 'timestamp')
# create_index_db_cols('aggtrades', 'timestamp')
#delete_all_text_dbs("symb")

