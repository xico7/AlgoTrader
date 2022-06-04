import pymongo
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient as AsyncMotorClient
from pymongo.errors import OperationFailure
from tasks.transform_trade_data import PRICE, QUANTITY, EVENT_TS

sp500_db_collection = "sp500_volume_highlow_chart"
fund_trades_database_name = "ten_secs_fund_trades"
ws_usdt_trades_database_name = "aggtrade_data"
parsed_trades_all_symbols_db_name = "{}_seconds_parsed_trades"
parsed_trades_fund_db_name = "{}_seconds_fund_data_trades"
symbol_price_chart_db_name = 'symbols_price_volume_chart_{}'


def list_database_names():
    return list(MongoClient("mongodb://localhost:27017/").list_database_names())


def async_connect_to_aggtrade_data_db():
    return AsyncMotorClient(f'mongodb://localhost:27017/{ws_usdt_trades_database_name}').get_default_database()


def connect_to_aggtrade_data_db():
    return MongoClient(f'mongodb://localhost:27017/{ws_usdt_trades_database_name}').get_default_database()


def connect_to_n_ms_parsed_trades_db(ms_to_parse):
    return connect_to_db(parsed_trades_all_symbols_db_name.format(ms_to_parse))


def connect_to_db(db_name):
    return MongoClient(f'mongodb://localhost:27017/{db_name}').get_default_database()


def connect_to_sp500_db():
    return MongoClient(f'mongodb://localhost:27017/{fund_trades_database_name}').get_default_database()


def connect_to_sp500_db_collection():
    return connect_to_sp500_db().get_collection(sp500_db_collection)


async def async_insert_many_to_aggtrade_db(data: dict):
    insert_many_from_dict_async_one(async_connect_to_aggtrade_data_db(), data)


def insert_one_to_sp500_db(data: dict):
    insert_one(connect_to_sp500_db(), sp500_db_collection, data)


def insert_n_secs_parsed_trades_in_db(db_name, ts_begin, price_data, volume_data):
    insert_range_db_trades(db_name, ts_begin, price_data, volume_data)


def insert_range_db_trades(db_name, ts_begin, symbols_values, symbols_volumes):
    for timeframe_symbols_values in symbols_values:
        for symbol in symbols_values[timeframe_symbols_values].keys():
            try:
                connect_to_db(db_name).get_collection(symbol).insert_one(
                    {EVENT_TS: ts_begin + timeframe_symbols_values, PRICE: symbols_values[timeframe_symbols_values][symbol], QUANTITY: symbols_volumes[timeframe_symbols_values][symbol]})
            except pymongo.errors.DuplicateKeyError:
                # skip document because it already exists in new collection
                continue


def insert_many_to_db(db_name, data):
    for elem in data:
        for key in list(elem.keys()):

    for timeframe in data:
        for key in list(timeframe.keys()):
            try:
                connect_to_db(db_name).get_collection(key).insert_one(timeframe[key])
            except pymongo.errors.DuplicateKeyError:
                # skip document because it already exists in new collection
                continue


def insert_one_from_dict(database, symbol, data):
    database.get_collection(symbol).insert_one(data)


def insert_one(database, collection, data):
    database.get_collection(collection).insert_one(data)


def insert_many_from_dict_async_one(database, data):
    for key in list(data.keys()):
        database.get_collection(key).insert_many(data[key])


def connect_price_chart_db(timeframe):
    return MongoClient(f'mongodb://localhost:27017/{symbol_price_chart_db_name.format(timeframe)}').get_default_database()


def create_db_time_index(db_name, timestamp_var='E'):
    db_conn = connect_to_db(db_name)
    for col_name in db_conn.list_collection_names():
        try:
            # db_feed.get_collection(col_name).create_index([(timestamp_var, pymongo.DESCENDING)], name='Time_index',
            #                                               unique=True)
            db_conn.get_collection(col_name).create_index([(timestamp_var, pymongo.DESCENDING)], name='Time_index')
        except OperationFailure as e:
            continue


# def connect_to_algo_alpha_db():
#     return MongoClient('mongodb://localhost:27017/algo_alpha').get_default_database()

def delete_all_timeframes_dbs():
    mongo_client = MongoClient(f'mongodb://localhost:27017/')
    for db in mongo_client.list_database_names():
        if "timeframe" in db:
            mongo_client.drop_database(db)


def create_index_db_cols(db_name, field):
    for col in connect_to_db(db_name).list_collection_names():
        connect_to_db(db_name).get_collection(col).create_index([(field, -1)])


# create_db_time_index("10_seconds_fund_data_trades")
# create_db_time_index("10_seconds_parsed_trades")

#create_index_db_cols("ten_secs_parsed_trades", EVENT_TS)
#delete_all_timeframes_dbs()