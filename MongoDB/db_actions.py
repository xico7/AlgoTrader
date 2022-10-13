import contextlib
import logging
import pymongo
from pymongo import MongoClient
import logs
from data_handling.data_helpers.data_staging import round_last_ten_secs, get_current_second_in_ms, remove_none_values
from data_handling.data_helpers.vars_constants import TS, MongoDB

trades_chart = '{}_trades_chart'
localhost = 'localhost:27017/'
base_mongo_conn_string = f'mongodb://{localhost}'
mongo_client = MongoClient(f'{base_mongo_conn_string}')

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class EmptyInitDB(Exception): pass
class InvalidDataProvided(Exception): pass
class InvalidArgumentsProvided(Exception): pass
class EmptyCollectionInDB(Exception): pass


def list_dbs():
    return mongo_client.list_database_names()


def connect_to_db(db_name):
    return MongoClient(f'{base_mongo_conn_string}{db_name}').get_default_database()


def db_col_names(db_name):
    return connect_to_db(db_name).list_collection_names()


def insert_many_db(db, col, data):
    return connect_to_db(db).get_collection(col).insert_many(data)


def insert_one_db(db, col, data) -> None:
    connect_to_db(db).get_collection(col).insert_one(data)


def insert_many_same_db_col(db, data) -> None:
    insert_many_db(db, db, data)


def aggregate_symbol_filled_data(db_name, start_ts, end_ts, symbol, column_name=TS, limit=0, sort_value=pymongo.DESCENDING):
    return query_db_col_between(db_name, symbol, start_ts, end_ts, column_name, limit, sort_value).fill_trades_tf(start_ts, end_ts)


def aggregate_symbols_filled_data(db_name, start_ts, end_ts, column_name=TS, limit=0, sort_value=pymongo.DESCENDING):
    trade_data = remove_none_values({symbol: query_db_col_between(db_name, symbol, start_ts, end_ts, column_name, limit, sort_value)
                                     for symbol in db_col_names(db_name)})
    return {symbol: trade_group.fill_trades_tf(start_ts, end_ts) for symbol, trade_group in trade_data.items()}


def query_db_col_between(db_name, col, highereq, lowereq, column_name=TS, limit=0, sort_value=pymongo.DESCENDING) -> [dict, list]:
    from data_handling.data_func import TradeData
    from data_handling.data_func import TradesGroup
    if query_val := list(connect_to_db(db_name).get_collection(col).find({
        MongoDB.AND: [{column_name: {MongoDB.HIGHER_EQ: highereq}},
                      {column_name: {MongoDB.LOWER_EQ: lowereq}}]}).sort(column_name, sort_value).limit(limit)):

        return TradeData(**query_val[0]) if limit == 1 else TradeData(**query_val[0]) if limit == 1 else \
            TradesGroup(**{'trades': {elem['timestamp']: TradeData(**elem) for elem in query_val},
                           'start_ts': highereq, 'end_ts': lowereq})

    return None


def query_db_col_timestamp_endpoint(db_name, collection, most_recent: bool, ignore_empty=False):
    def query_endpoint(db_name, collection, sort_val):
        if endpoint := query_db_col_between(db_name, collection, 0, get_current_second_in_ms(), limit=1, sort_value=sort_val):
            return endpoint.timestamp
        elif ignore_empty:
            return None
        else:
            raise EmptyCollectionInDB(("DB with name '{0}' and collection '{0}' contains no values.").format(db_name, collection))

    endpoint_1, endpoint_2 = query_endpoint(db_name, collection, pymongo.DESCENDING), query_endpoint(db_name, collection, pymongo.ASCENDING)
    if (endpoint_1 and endpoint_2):
        return max(endpoint_1, endpoint_2) if most_recent else min(endpoint_1, endpoint_2)

    return None


def query_starting_ts(db_name, collection, init_db=None):
    try:
        return query_db_col_timestamp_endpoint(db_name, collection, most_recent=False)
    except EmptyCollectionInDB:
        if init_db:
            with contextlib.suppress(EmptyCollectionInDB):
                return round_last_ten_secs(query_db_col_timestamp_endpoint(init_db, collection, False))

        raise InvalidDataProvided(f"symbol '{collection}' doesn't have a valid timestamp in db "
                                  f"'{db_name}' and no other db to initialize from.") from e


def delete_db(db_name) -> None:
    mongo_client.drop_database(db_name)


def delete_all_text_dbs(text) -> None:
    for db in list_dbs():
        if text in db:
            delete_db(db)


def create_index_db_cols(db, field) -> None:
    for col in db_col_names(db):
        connect_to_db(db).get_collection(col).create_index([(field, -1)])
        print(f"Created index for collection {col}.")
#
#
# create_index_db_cols('parsed_aggtrades', 'ID')
#create_index_db_cols('parsed_aggtrades', 'timestamp')


#delete_all_text_dbs("time")


# def query_existing_ws_trades(start_ts, end_ts, ms_parse_interval):  # If BTCUSDT has trades working it assumes all other symbols were working.
#     symbols_earliest_ts = min(list(start_ts.values()))
#     symbols_oldest_ts = max(list(end_ts.values()))
#
#     existing_trades = []
#     assume_existing_trade_parse_interval = mins_to_ms(3)
#     for elem in range(symbols_earliest_ts, symbols_oldest_ts, assume_existing_trade_parse_interval):
#         if query_db_col_between(PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, elem, elem + assume_existing_trade_parse_interval, limit=1):
#             existing_trades += list(range(elem, elem + assume_existing_trade_parse_interval, ms_parse_interval))
#
#     return existing_trades
