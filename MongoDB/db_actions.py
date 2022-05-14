import pymongo
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient as AsyncMotorClient
from pymongo.errors import OperationFailure

sp500_db_collection = "sp500_volume_highlow_chart"
fund_trades_database_name = "sp500_data"
usdt_trades_database_name = "aggtrade_data"

def async_connect_to_aggtrade_data_db():
    return AsyncMotorClient(f'mongodb://localhost:27017/{usdt_trades_database_name}').get_default_database()


def connect_to_aggtrade_data_db():
    return MongoClient(f'mongodb://localhost:27017/{usdt_trades_database_name}').get_default_database()


def connect_to_timeframe_db(db_name):
    return MongoClient(f'mongodb://localhost:27017/{db_name}').get_default_database()


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


def insert_many_to_db(db_name, data):
    for key in list(data.keys()):
        try:
            connect_to_db(db_name).get_collection(key).insert_one(data[key])
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


def create_db_time_index(db_feed, timestamp_var='E'):
    for col_name in db_feed.list_collection_names():
        try:
            # db_feed.get_collection(col_name).create_index([(timestamp_var, pymongo.DESCENDING)], name='Time_index',
            #                                               unique=True)
            db_feed.get_collection(col_name).create_index([(timestamp_var, pymongo.DESCENDING)], name='Time_index')
        except OperationFailure as e:
            continue


# def connect_to_algo_alpha_db():
#     return MongoClient('mongodb://localhost:27017/algo_alpha').get_default_database()

def delete_all_timeframes_dbs():
    mongo_client = MongoClient(f'mongodb://localhost:27017/')
    for db in mongo_client.list_database_names():
        if "timeframe" in db:
            mongo_client.drop_database(db)


#delete_all_timeframes_dbs()