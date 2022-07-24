import pymongo
from pymongo import MongoClient

fund_trades_database_name = "ten_secs_fund_trades"
ws_usdt_trades_database_name = "aggtrade_data"
parsed_aggtrades = "parsed_aggtrades"
aggtrades = "aggtrades"
parsed_trades_base_db = "{}_seconds_parsed_trades"
parsed_trades_fund_db_name = "{}_seconds_fund_data_trades"
trades_price_quantity_chart_db_name = '{}_trades_chart'


def list_database_names():
    return list(MongoClient("mongodb://localhost:27017/").list_database_names())


def connect_to_bundled_aggtrade_db():
    return MongoClient(f'mongodb://localhost:27017/{aggtrades}').get_default_database()


def connect_to_parsed_aggtrade_db():
    return MongoClient(f'mongodb://localhost:27017/{parsed_aggtrades}').get_default_database()


def connect_to_db(db_name):
    return MongoClient(f'mongodb://localhost:27017/{db_name}').get_default_database()


def insert_parsed_aggtrades(data):
    db = connect_to_parsed_aggtrade_db()
    for key in list(data.keys()):
        db.get_collection(key).insert_many(data[key])


def insert_bundled_aggtrades(data):
    connect_to_bundled_aggtrade_db().get_collection(f'{aggtrades}').insert_many(data)


def insert_many_db(db, data, symbol):
    connect_to_db(db).get_collection(symbol).insert_many(data)


def delete_all_text_dbs(text):
    mongo_client = MongoClient(f'mongodb://localhost:27017/')
    for db in mongo_client.list_database_names():
        if text in db:
            mongo_client.drop_database(db)


def create_index_db_cols(db_name, field):
    for col in connect_to_db(db_name).list_collection_names():
        connect_to_db(db_name).get_collection(col).create_index([(field, -1)])


# create_db_time_index("10_seconds_fund_data_trades")
# create_db_time_index("10_seconds_parsed_trades")

#create_index_db_cols("ten_secs_parsed_trades", EVENT_TS)
#delete_all_timeframes_dbs()
#
#
def insert_many_to_db(db_name, data):
    for symbol, chart_data in data.items():
        for cached_chart_data in chart_data:
            try:
                connect_to_db(db_name).get_collection(key).insert_one(timeframe[key])
            except pymongo.errors.DuplicateKeyError:
                # skip document because it already exists in new collection
                continue