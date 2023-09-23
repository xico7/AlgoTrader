from pymongo import MongoClient

try:
    from MongoDB.db_actions import localhost
except ModuleNotFoundError:
    localhost = 'mongodb://localhost:27017/'

mongo_client = MongoClient(host=localhost, maxPoolSize=0)
for db_name in [deleteable for deleteable in mongo_client.list_database_names() if deleteable not in ['admin', 'config', 'local']]:
    mongo_client.drop_database(db_name)