from data_staging import MongoDB
from MongoDB.db_actions import connect_to_bundled_aggtrade_db, connect_to_parsed_aggtrade_db
from vars_constants import DB_TS


def query_parsed_aggtrade(symbol, ts_begin, ts_end):
    return list(connect_to_parsed_aggtrade_db().get_collection(symbol).find(
        {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: ts_begin}},
                       {DB_TS: {MongoDB.LOWER_EQ: ts_end}}]}))


def query_bundled_aggtrade(symbol, ts_begin, ts_end):
    return list(connect_to_bundled_aggtrade_db().get_collection(symbol).find(
        {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: ts_begin}},
                       {DB_TS: {MongoDB.LOWER_EQ: ts_end}}]}))



