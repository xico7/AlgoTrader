from MongoDB.db_actions import connect_to_db, parsed_aggtrades
from vars_constants import DB_TS, MongoDB


def query_parsed_aggtrade(symbol, ts_begin, ts_end):
    return query_db_col_highereq_lowereq(parsed_aggtrades, symbol, ts_begin, ts_end)


def query_db_col_highereq_lowereq(db_name, col_name, highereq, lowereq, column_name=DB_TS):
    return list(connect_to_db(db_name).get_collection(col_name).find(
        {MongoDB.AND: [{column_name: {MongoDB.HIGHER_EQ: highereq}},
                       {column_name: {MongoDB.LOWER_EQ: lowereq}}]}))


def query_parsed_aggtrade_multiple_timeframes(symbols: list, ts_begin, ts_end):
    return {symbol: query_parsed_aggtrade(symbol, ts_begin[symbol], ts_end[symbol]) for symbol in symbols}



