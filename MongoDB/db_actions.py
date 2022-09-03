import logging
from operator import itemgetter
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

import logs
from data_staging import get_current_second_in_ms, mins_to_ms, round_last_ten_secs
from vars_constants import TS, MongoDB, DEFAULT_SYMBOL_SEARCH, PARSED_AGGTRADES_DB, AGGTRADES_DB

trades_chart = '{}_trades_chart'
localhost = 'localhost:27017/'
base_mongo_conn_string = 'mongodb://localhost:27017/'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)

class EmptyInitDB(Exception): pass
class InvalidDataProvided(Exception): pass
class InvalidArgumentsProvided(Exception): pass

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


def list_dbs(): return [mongo_client.list_database_names()]
def connect_to_db(db_name): return mongo_client_db(db_name).get_default_database()


def insert_parsed_aggtrades(data: dict) -> None:
    for key in list(data.keys()):
        connect_to_db(PARSED_AGGTRADES_DB).get_collection(key).insert_many(data[key])


def insert_many_db(db, data, col_name=None, same_col_db_name=False) -> None:
    if not same_col_db_name:
        if not col_name:
            raise InvalidArgumentsProvided("If collection name differs from database name, argument must be provided.")
        connect_to_db(db).get_collection(col_name).insert_many(data)
    else:
        connect_to_db(db).get_collection(db).insert_many(data)


def query_db_col_between(db_name, col, highereq, lowereq, column_name=TS):
    return list(connect_to_db(db_name).get_collection(col).find(
        {MongoDB.AND: [{column_name: {MongoDB.HIGHER_EQ: highereq}},
                       {column_name: {MongoDB.LOWER_EQ: lowereq}}]}))


def query_all_dbcol_values(db_name, collection) -> list:
    return query_db_col_between(db_name, collection, 0, get_current_second_in_ms())


def query_parsed_aggtrade_multiple_timeframes(symbols: list, ts_begin, ts_end):
    return {symbol: query_db_col_between(PARSED_AGGTRADES_DB, symbol, ts_begin[symbol], ts_end[symbol]) for symbol in symbols}


def query_db_col_ts_endpoints(db_name, collection, earliest=True, init_db=False):
    if values := query_all_dbcol_values(db_name, collection):
        values = sorted(values, key=itemgetter(TS))
        first_value_ts, last_value_ts = values[0][TS], values[-1][TS]

        if earliest:
            return first_value_ts if first_value_ts > last_value_ts else last_value_ts
        else:
            return first_value_ts if first_value_ts < last_value_ts else last_value_ts

    if init_db:
        raise EmptyInitDB("Empty init db detected.")

    return None


def query_starting_ts(db_name, collection, init_db=None):
    if values := query_db_col_ts_endpoints(db_name, collection):
        pass
    elif not init_db:
        raise InvalidDataProvided(f"symbol '{collection}' doesn't have a valid timestamp in db "
                                  f"'{db_name}' and no other db to initialize from.")
    else:
        values = round_last_ten_secs(query_db_col_ts_endpoints(db_name, collection, earliest=False, init_db=True))

    return values


def query_existing_ws_trades(start_ts, end_ts, ms_parse_interval):
    assume_existing_parse_interval = mins_to_ms(3)
    existing_trades = []

    for elem in list(range(min(list(start_ts.values())), max(list(end_ts.values())), assume_existing_parse_interval)):
        if query_db_col_between(PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, elem, elem + assume_existing_parse_interval):
            existing_trades += list(range(elem, elem + assume_existing_parse_interval, ms_parse_interval))

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

