import logging
from operator import itemgetter
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

import logs
from data_staging import get_current_second_in_ms, mins_to_ms, round_last_ten_secs
from vars_constants import DB_TS, MongoDB, DEFAULT_COL_SEARCH, PARSED_AGGTRADES_DB, AGGTRADES_DB

trades_chart = '{}_trades_chart'
localhost = 'localhost:27017/'
base_mongo_conn_string = 'mongodb://localhost:27017/'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)

class EmptyInitDB(Exception): pass
class InvalidDataProvided(Exception): pass


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


def query_db_col_earliest_oldest_ts(db_name, collection, earliest=True, init_db=False):
    if values := query_all_dbcol_values(db_name, collection):
        values = sorted(values, key=itemgetter(DB_TS))
        first_value_ts, last_value_ts = values[0][DB_TS], values[-1][DB_TS]

        if earliest:
            return first_value_ts if first_value_ts > last_value_ts else last_value_ts
        else:
            return first_value_ts if first_value_ts < last_value_ts else last_value_ts

    if init_db:
        raise EmptyInitDB("Empty init db detected.")

    return None


def query_db_col_oldest_ts(db_name, collection, init_db=False):
    return query_db_col_earliest_oldest_ts(db_name, collection, earliest=False, init_db=init_db)


def query_db_col_earliest(db_name, collection):
    return query_db_col_earliest_oldest_ts(db_name, collection)


def query_starting_ts(db_name, collection, init_db=None):
    if values := query_db_col_earliest(db_name, collection):
        pass
    elif not init_db:
        raise InvalidDataProvided(f"symbol '{collection}' doesn't have a valid timestamp in db "
                                  f"'{db_name}' and no other db to initialize from.")
    else:
        values = round_last_ten_secs(query_db_col_oldest_ts(init_db, collection, init_db=True))

    return values


def query_existing_ws_trades(start_ts, end_ts, ms_parse_interval):
    assume_existing_parse_interval = mins_to_ms(3)
    existing_trades = []

    for elem in list(range(min(list(start_ts.values())), max(list(end_ts.values())), assume_existing_parse_interval)):
        if query_parsed_aggtrade(DEFAULT_COL_SEARCH, elem, elem + assume_existing_parse_interval):
            existing_trades += list(range(elem, elem + assume_existing_parse_interval, ms_parse_interval))

    return existing_trades


def delete_all_text_dbs(text) -> None:
    for db in list_dbs():
        if text in db:
            mongo_client.drop_database(db)

#delete_all_text_dbs("symb")

def create_index_db_cols(db_name, field) -> None:
    for col in list_db_cols(db_name):
        query_db_collection(db_name, col).create_index([(field, -1)])


 # def insert_many_to_db(db_name, data: dict) -> None:
 #    for symbol, chart_data in data.items():
 #        for cached_chart_data in chart_data:
 #            try:
 #                connect_to_db(db_name).get_collection(symbol).insert_one(timeframe[key])
 #            except pymongo.errors.DuplicateKeyError:
 #                # skip document because it already exists in new collection
 #                continue
