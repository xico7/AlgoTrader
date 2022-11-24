import types

import bson
from pymongoarrow.api import Schema
import matplotlib.pyplot as plt
from pymongoarrow.monkey import patch_all
import pandas
from matplotlib.axes._subplots import SubplotBase
#from data_handling.data_func import TradesTAIndicators
from data_handling.data_helpers.vars_constants import DEFAULT_COL_SEARCH, ONE_DAY_IN_MS, ONE_HOUR_IN_MS, \
    TEN_SECS_PARSED_TRADES_DB, TS, TEN_SECS_MS

patch_all()

from MongoDB.db_actions import DB, DBCol, trades_chart, ValidatorDB


# TODO: Preciso de ver os vários perfis do 'volume rise' principalment eno fund
# TODO: Lowest range percentage for crypto to be considered,
# TODO: Criar um indicador que extende o volume% e a subida em %, o volume tem de ter um minimo para se considerar representativo..
# TODO: Create test that validates each 10 seconds a value in mongodb for all symbols
TOO_BIG_UNUSED_SYMBOLS = ['BTCUSDT', 'ETHUSDT']

def execute_alpha_algo():
    def test_alpha():
        #  Lowest range percentage considered for coin to be traded, i would say 8% in last 24Hours, 1.5% in last 2 Hours
        #  Lowest range percentage considered for fund data to be relevant, i would say 8% in last 24Hours, 2% in last 2 Hours
        pass




#     symbol = "BTCUSDT"
#
#     figure, axis = plt.subplots(2, 2)
#
#     def standard_plot(timeframe_in_mins, plot: [SubplotBase, types.ModuleType], symbol):
#         chart_tf_db = trades_chart.format(timeframe_in_mins)
#         start_ts = ValidatorDB(chart_tf_db).start_ts
#         symbol_chart = DBCol(chart_tf_db, symbol).column_between(start_ts, start_ts, 'start_ts')[0]
#
#
#         plot.plot([counter for counter in symbol_chart['range_price_volume'].keys()],
#              [vol['sum_volume_percentage'] for vol in symbol_chart['range_price_volume'].values()])
#
#         plot.text(1, 75, f"rise_of_start_end_price_in_percentage: "
#                         f"{symbol_chart['range_price_volume_difference']['rise_of_start_end_price_in_percentage']}")
#         plot.text(1, 80, f"end_price_counter: {symbol_chart['end_price_counter']}")
#         plot.text(1, 85, f"start_price_counter: {symbol_chart['start_price_counter']}")
#         plot.text(1, 90, f"price_range_percentage: {symbol_chart['price_range_percentage']}")
#
#         def match_vol_percentage_with_counter(volume_sum):
#             for i, range_price_volume_data in enumerate(symbol_chart['range_price_volume'].values()):
#                 if volume_sum == range_price_volume_data['sum_volume_percentage']:
#                     return i
#
#         plot.plot(match_vol_percentage_with_counter(symbol_chart['range_price_volume_difference']['start_price_volume_percentage']),
#                  symbol_chart['range_price_volume_difference']['start_price_volume_percentage'], 'ro')
#         plot.plot(match_vol_percentage_with_counter(symbol_chart['range_price_volume_difference']['end_price_volume_percentage']),
#                  symbol_chart['range_price_volume_difference']['end_price_volume_percentage'], 'bo')
#
#
#     standard_plot(1440, axis[0, 0], symbol)
#     standard_plot(1440, axis[1, 0], 'fund_data')
#     standard_plot(480, axis[0, 1], 'fund_data')
#
#     plt.show()

    def range_percentages(timeframe_in_mins):
        chart_tf_db = trades_chart.format(timeframe_in_mins)

        start_ts = 1641131210000
        dif_price_range = 0
        save_start_ts = 0
        # for symbol in DB(chart_tf_db).list_collection_names():
        for i, symbol_chart in enumerate(DBCol(chart_tf_db, "fund_data").column_between(start_ts, start_ts + ONE_DAY_IN_MS * 2, 'start_ts')):
            if not symbol_chart['start_ts'] == save_start_ts + TEN_SECS_MS:
                print("here", i)
            save_start_ts = symbol_chart['start_ts']

            if dif_price_range != symbol_chart['price_range_percentage']:
                dif_price_range = symbol_chart['price_range_percentage']
                print(symbol_chart['price_range_percentage'], i)
            # a += 1
            # plt.plot(symbol_chart[0]['price_range_percentage'], symbol_chart[0]['price_range_percentage'], 'ro')

        #plt.text(symbol_chart['price_range_percentage'], symbol_chart['price_range_percentage'], symbol)

    range_percentages(1440)
    plt.show()


    def plot_volumes(timeframe_in_mins):
        chart_tf_db = trades_chart.format(timeframe_in_mins)

        start_ts = ValidatorDB(chart_tf_db).start_ts
        for symbol in DB(chart_tf_db).list_collection_names():
            symbol_chart = DBCol(chart_tf_db, symbol).column_between(start_ts, start_ts, 'start_ts')[0]
            plt.text(symbol_chart['total_volume'], symbol_chart['total_volume'], symbol)
            plt.plot(symbol_chart['total_volume'], symbol_chart['total_volume'], 'ro')

    plot_volumes(120)
    total_volumes = [v['total_volume'] for v in symbol_chart]

    volumes = [v['range_price_volume_difference']['rise_of_start_end_volume_in_percentage'] for v in symbol_chart
               if v['range_price_volume_difference']]
    volume_sum = []
    for i in range(1, 100):
        volume_sum.append(symbol_chart)
    plt.plot(list(range(1, 100)), total_volumes)
    plt.show()


    symbol_trades = DBCol(TEN_SECS_PARSED_TRADES_DB, symbol).column_between(start_ts, start_ts + ONE_DAY_IN_MS / 24, TS)
    start_timestamps = [v[TS] for v in symbol_trades]
    trades = [v['price'] for v in symbol_trades]
    plt.plot(start_timestamps, trades)
    plt.show()

    print("here")



    df2 = pandas.DataFrame.from_dict(symbol_chart)
    for a, b in df2.range_price_volume_difference.items():
        print("here")
    #df1 = pandas.DataFrame.from_dict(a)
    #df1.plot(x='end_ts', y='last_price_counter', kind='scatter')
    df2.plot(x='min_price', y='range_price_volume_difference.start_price_volume_percentage', kind='scatter')

    #df2.plot(x='end_ts', y='sum_volume_percentage', kind='scatter')
    #df2.plot(x='end_ts', y='most_recent_price', kind='scatter')



    chart_db = DB('trades_chart_1440_minutes')
    symbol_charts = {}
    tf_to_test = int(ONE_HOUR_IN_MS / 2)
    for symbol in chart_db.list_collection_names():
        start_ts = chart_db.__getattr__(symbol).oldest_timeframe('start_ts')
        symbol_chart = {trade.start_ts: trade for trade in chart_db.__getattr__(symbol).column_between(
            start_ts, start_ts + ONE_HOUR_IN_MS, 'start_ts', ReturnType=TradesTAIndicators)}
        symbol_cache_vals = []
        for _, trades in symbol_chart.items():
            if not trades.start_ts + tf_to_test > trades.end_ts:
               symbol_cache_vals.append(
                    trades.range_price_volume[str(trades.last_price_counter)]['sum_volume_percentage'] -
                    symbol_chart[start_ts + tf_to_test].range_price_volume[str(symbol_chart[start_ts + tf_to_test].last_price_counter)]['sum_volume_percentage'])

        print(max(symbol_cache_vals))
        print(min(symbol_cache_vals))




    rise_percentage = {}
    volume_rise_percentage = {}
    for symbol, trades in symçbol_charts.items():
        for trade in trades:
            if not trade.start_ts + (ONE_HOUR_IN_MS / 2) > trade.end_ts:
                print("here")
        difference = values.rise_start_ts
        rise_percentage.set_default(symbol, []).append()

    start_ts = query_get_oldest_timeframe('trades_chart_240_minutes', DEFAULT_COL_SEARCH, 'start_ts')
    #for symbol in db_conn('trades_chart_240_minutes').list_collection_names():
    for trade in symbol_chart:
        trade['sum_volume_percentage'] = trade['range_price_volume'][str(trade['last_price_counter'])]['sum_volume_percentage']
    b = query_column_between('trades_chart_240_minutes', "fund_data", start_ts, start_ts + ONE_DAY_IN_MS, 'start_ts')



    # schema = Schema({str(k + 1): float for k in range(100)})
    # # schema = Schema({'price': float, 'quantity': float, 'timestamp': float})
    #
    # collection = DB('trades_chart_240_minutes')
    #
    # collection.aggregate_pandas_all([
    #     {'$project': {
    #         'windDirection': '$wind.direction.angle',
    #         'windSpeed': '$wind.speed.rate',
    #     }}
    # ],
    #     schema=Schema({'windDirection': int, 'windSpeed': float})
    # )
    #
    # list(collection.aggregate([         {'$match': {'_id': bson.ObjectId("63557f0d377ed9f07299245f")}},         {'$project': {             'windDirection': '$1.sum_volume_percentage',             'windSpeed': '$1.volume_percentage',         }}     ]))
    #
    # print("here")
    # df = collection.find_pandas_all({'1': {'$gt': 0}}, schema=schema)
    # df.plot(x='1', y='2', kind='scatter')
    # plt.show()
    # print("here")
    #
    # # collection.aggregate_pandas_all()
    # # df = list(collection.aggregate([
    # #     {'$match': {'_id': bson.ObjectId("5553a998e4b02cf7151190bf")}},
    # #     {'$project': {
    # #         'windDirection': '$wind.direction.angle',
    # #         'windSpeed': '$wind.speed.rate',
    # #     }}
    # # ]))


# def execute_alpha_algo():
#     #schema = Schema({'start_ts': float, 'end_ts': float, 'most_recent_price': float, 'range_price_volume': bson.objectid.ObjectId})
#     schema = Schema({'price': float, 'timestamp': float})
#
#     collection = db_conn('ten_seconds_parsed_trades').BTCUSDT
#     df = collection.find_pandas_all({'timestamp': {'$gt': 0}}, schema=schema)
#     df.plot(x='timestamp', y='price', kind='scatter')
#     plt.show()
#     print("here")


