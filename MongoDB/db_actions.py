from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient as AsyncMotorClient


CLIENT = MongoClient('mongodb://localhost:27017/')


def connect_to_1m_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_1minutes').get_default_database()


def connect_to_signal_db():
    return MongoClient('mongodb://localhost:27017/Signal').get_default_database()


def connect_to_algo_alpha_db():
    return MongoClient('mongodb://localhost:27017/algo_alpha').get_default_database()


def connect_to_5m_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_5minutes').get_default_database()


def connect_to_15m_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_15minutes').get_default_database()


def connect_to_1h_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_1hour').get_default_database()


def connect_to_4h_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_4hour').get_default_database()


def connect_to_1d_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_1day').get_default_database()


def connect_to_rs_db():
    return AsyncMotorClient('mongodb://localhost:27017/aggtrade_data_client').get_default_database()


def connect_to_ta_lines_db():
    return AsyncMotorClient('mongodb://localhost:27017/TA_RS_VOL').get_default_database()


def connect_to_final_signal_db():
    return MongoClient('mongodb://localhost:27017/Signal').get_default_database()


def connect_to_ta_analysis_db():
    return MongoClient('mongodb://localhost:27017/TA_Analysis').get_default_database()



async def insert_many_from_dict_async_two(db, data: dict):
    insert_many_from_dict_async_one(db, data)


async def rs_insert_many_from_dict_async_two(data: dict):
    insert_many_from_dict_async_one(connect_to_rs_db(), data)


def insert_one_from_dict(database, data):
    for key in list(data.keys()):
        database.get_collection(key).insert_one(data[key])


def insert_many_from_dict_async_one(database, data):
    for key in list(data.keys()):
        database.get_collection(key).insert_many(data[key])



