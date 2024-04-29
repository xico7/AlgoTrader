from MongoDB.db_actions import DB, DBCol
from support.data_handling.data_helpers.vars_constants import TEN_SECS_PARSED_TRADES_DB, DEFAULT_PARSE_INTERVAL_IN_MS

ten_secs_db = DB(TEN_SECS_PARSED_TRADES_DB)
ten_secs_db_symbols = ten_secs_db.list_collection_names()

for symbol in ten_secs_db_symbols:
    timeframe_1 = 0
    timeframe_2 = 0
    for ten_sec_trade in list(DBCol(ten_secs_db, symbol).find_all()):
        if not timeframe_1:
            timeframe_1 = ten_sec_trade['timestamp']
            continue
        if ten_sec_trade['timestamp'] != timeframe_1 + DEFAULT_PARSE_INTERVAL_IN_MS:
            print("HERE")
