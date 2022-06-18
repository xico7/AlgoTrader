from MongoDB.db_actions import connect_to_bundled_aggtrade_db, connect_to_parsed_aggtrade_db
from vars_constants import DB_TS, MongoDB


def query_parsed_aggtrade(symbols, ts_begin, ts_end):
    aggtrades = {}

    if isinstance(symbols, str):
        symbols = [symbols]

    for symbol in symbols:
        aggtrades[symbol] = list(connect_to_parsed_aggtrade_db().get_collection(symbol).find(
            {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: ts_begin}},
                           {DB_TS: {MongoDB.LOWER_EQ: ts_end}}]}))

    return aggtrades


def query_parsed_aggtrade_multiple_timeframes(symbols: list, ts_begin, ts_end):
    aggtrades = {}

    for symbol in symbols:
        aggtrades[symbol] = query_parsed_aggtrade(symbol, ts_begin[symbol], ts_end[symbol])[symbol]

    return aggtrades

def query_bundled_aggtrade(symbol, ts_begin, ts_end):
    return list(connect_to_bundled_aggtrade_db().get_collection(symbol).find(
        {MongoDB.AND: [{DB_TS: {MongoDB.HIGHER_EQ: ts_begin}},
                       {DB_TS: {MongoDB.LOWER_EQ: ts_end}}]}))



