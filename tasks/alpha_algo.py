import bson
from pymongoarrow.api import Schema
import matplotlib.pyplot as plt
from pymongoarrow.monkey import patch_all
import pandas

#from data_handling.data_func import TradesTAIndicators
from data_handling.data_helpers.vars_constants import DEFAULT_COL_SEARCH, ONE_DAY_IN_MS, ONE_HOUR_IN_MS

patch_all()

from MongoDB.db_actions import DB


# def execute_alpha_algo():
#     #schema = Schema({'start_ts': float, 'end_ts': float, 'most_recent_price': float, 'range_price_volume': bson.objectid.ObjectId})
#     schema = Schema({'price': float, 'timestamp': float})
#
#     collection = db_conn('ten_seconds_parsed_trades').BTCUSDT
#     df = collection.find_pandas_all({'timestamp': {'$gt': 0}}, schema=schema)
#     df.plot(x='timestamp', y='price', kind='scatter')
#     plt.show()
#     print("here")

def execute_alpha_algo():
    #TODO: Criar um indicador que extende o volume% e a subida em %, o volume tem de ter um minimo para se considerar representativo..
    chart_db = DB('trades_chart_1440_minutes')
    # symbol_charts = {}
    # tf_to_test = int(ONE_HOUR_IN_MS / 2)
    # for symbol in chart_db.list_collection_names():
    #     start_ts = chart_db.__getattr__(symbol).oldest_timeframe('start_ts')
    #     symbol_chart = {trade.start_ts: trade for trade in chart_db.__getattr__(symbol).column_between(
    #         start_ts, start_ts + ONE_HOUR_IN_MS, 'start_ts', ReturnType=TradesTAIndicators)}
    #     symbol_cache_vals = []
    #     for _, trades in symbol_chart.items():
    #         if not trades.start_ts + tf_to_test > trades.end_ts:
    #            symbol_cache_vals.append(
    #                 trades.range_price_volume[str(trades.last_price_counter)]['sum_volume_percentage'] -
    #                 symbol_chart[start_ts + tf_to_test].range_price_volume[str(symbol_chart[start_ts + tf_to_test].last_price_counter)]['sum_volume_percentage'])
    #
    #     print(max(symbol_cache_vals))
    #     print(min(symbol_cache_vals))




    rise_percentage = {}
    volume_rise_percentage = {}
    for symbol, trades in symbol_charts.items():
        for trade in trades:
            if not trade.start_ts + (ONE_HOUR_IN_MS / 2) > trade.end_ts:
                print("here")
        difference = values.rise_start_ts
        rise_percentage.set_default(symbol, []).append()

    start_ts = query_get_oldest_timeframe('trades_chart_240_minutes', DEFAULT_COL_SEARCH, 'start_ts')
    #for symbol in db_conn('trades_chart_240_minutes').list_collection_names():
    symbol_trades = query_column_between('trades_chart_240_minutes', "AVAUSDT", start_ts, start_ts + ONE_DAY_IN_MS, 'start_ts')
    for trade in symbol_trades:
        trade['sum_volume_percentage'] = trade['range_price_volume'][str(trade['last_price_counter'])]['sum_volume_percentage']
    b = query_column_between('trades_chart_240_minutes', "fund_data", start_ts, start_ts + ONE_DAY_IN_MS, 'start_ts')

    df2 = pandas.DataFrame.from_dict(symbol_trades)
    #df1 = pandas.DataFrame.from_dict(a)
    #df1.plot(x='end_ts', y='last_price_counter', kind='scatter')
    df2.plot(x='end_ts', y='sum_volume_percentage', kind='scatter')
    df2.plot(x='end_ts', y='most_recent_price', kind='scatter')

    plt.show()
    print("here")

    #No fund data, o preço está a subir(x), e tem um volume para aquele 'range' acima da média(y)

    trade.range_price_volume[str(trade.last_price_counter)]









    schema = Schema({str(k + 1): float for k in range(100)})
    # schema = Schema({'price': float, 'quantity': float, 'timestamp': float})

    collection = DB('trades_chart_240_minutes')

    collection.aggregate_pandas_all([
        {'$project': {
            'windDirection': '$wind.direction.angle',
            'windSpeed': '$wind.speed.rate',
        }}
    ],
        schema=Schema({'windDirection': int, 'windSpeed': float})
    )

    list(collection.aggregate([         {'$match': {'_id': bson.ObjectId("63557f0d377ed9f07299245f")}},         {'$project': {             'windDirection': '$1.sum_volume_percentage',             'windSpeed': '$1.volume_percentage',         }}     ]))

    print("here")
    df = collection.find_pandas_all({'1': {'$gt': 0}}, schema=schema)
    df.plot(x='1', y='2', kind='scatter')
    plt.show()
    print("here")

    # collection.aggregate_pandas_all()
    # df = list(collection.aggregate([
    #     {'$match': {'_id': bson.ObjectId("5553a998e4b02cf7151190bf")}},
    #     {'$project': {
    #         'windDirection': '$wind.direction.angle',
    #         'windSpeed': '$wind.speed.rate',
    #     }}
    # ]))



