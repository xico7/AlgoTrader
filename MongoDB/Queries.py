from data_staging import MongoDB, sec_to_ms
from MongoDB.db_actions import connect_to_aggtrade_data_db


from tasks.transform_trade_data import EVENT_TS


def query_aggtrade_data_col_timeframe(symbol, ts_begin, timeframe_in_secs):
    return list(connect_to_aggtrade_data_db().get_collection(symbol).find(
        {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: ts_begin}},
                       {EVENT_TS: {MongoDB.LOWER_EQ: ts_begin + sec_to_ms(timeframe_in_secs)}}]}))



