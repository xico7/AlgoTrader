import itertools
import types
from collections import namedtuple
from enum import Enum

import bson
from pymongoarrow.api import Schema
import matplotlib.pyplot as plt
from pymongoarrow.monkey import patch_all
import pandas
from matplotlib.axes._subplots import SubplotBase
#from data_handling.data_func import TradesTAIndicators
from data_handling.data_helpers.data_staging import mins_to_ms
from data_handling.data_helpers.vars_constants import DEFAULT_COL_SEARCH, ONE_DAY_IN_MS, ONE_HOUR_IN_MS, \
    TEN_SECS_PARSED_TRADES_DB, TS, TEN_SECONDS_IN_MS, ONE_DAY_IN_MINUTES, TWO_HOURS_IN_MINUTES

patch_all()

from MongoDB.db_actions import DB, DBCol, trades_chart, ValidatorDB


class AlphaAlgoRulesVars(Enum):
    one_day_minimum_range_percentage = 6
    two_hours_minimum_range_percentage = 1.5

# TODO: Preciso de ver os vários perfis do 'volume rise' principalment eno fund


TOO_BIG_UNUSED_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']


def execute_alpha_algo():
    def test_alpha():
        pass
        #  Lowest range percentage considered for coin to be traded, i would say 8% in last 24Hours, 1.5% in last 2 Hours.
        #  Use only coins in top 50s day volume.
        #  Volume must be at least 50% more than the average of last months daily.
        #  In fund, the price is irrelevant, just the volume action to determine if its rising or not, /
        #  in asset to buy the price is relevant because i need to get in cheap.

    def query_chart_db_data(symbol, chart_tf, init_ts, timeframe_to_query):
        return DBCol(chart_tf, symbol).column_between(init_ts, init_ts + timeframe_to_query, 'start_ts')

    def is_fund_data_rising():
        #  Lowest range percentage considered for fund data to be relevant, i would say 6% in last 24Hours, 1.5% in last 2 Hours.
        # Nas últimas 24h o end price vol acima dos 90%, nas últimas 4horas 93%, e nas 1hora 95%.
        # Volume relativo em releção ao último mês de 1.5

        potential_valid_fund_data = []
        append_ts = mins_to_ms(2880)
        symbol = "fund_data"
        MapChartDBToMS = namedtuple("MapChartDBToMS", ["db_name", "db_timeframe_in_milliseconds"])

        def get_chart_db_ms(timeframe_in_minutes: int):
            return MapChartDBToMS(trades_chart.format(timeframe_in_minutes), mins_to_ms(timeframe_in_minutes))

        chart_tf_one_day = get_chart_db_ms(ONE_DAY_IN_MINUTES)
        chart_tf_2h = get_chart_db_ms(TWO_HOURS_IN_MINUTES)
        init_ts = DBCol(chart_tf_one_day.db_name, symbol).oldest_timeframe()
        end_ts = init_ts + mins_to_ms(1440*2)
        a = DBCol(chart_tf_one_day.db_name, DEFAULT_COL_SEARCH).most_recent_timeframe()

        for mapped_trade in zip(DBCol(chart_tf_one_day.db_name, symbol).column_between(init_ts, end_ts, 'start_ts'),
                         DBCol(chart_tf_2h.db_name, symbol).column_between(
                             init_ts + chart_tf_one_day.db_timeframe_in_milliseconds - chart_tf_2h.db_timeframe_in_milliseconds,
                             end_ts + chart_tf_one_day.db_timeframe_in_milliseconds - chart_tf_2h.db_timeframe_in_milliseconds, 'start_ts')):
            if mapped_trade[0]['price_range_percentage'] > AlphaAlgoRulesVars.one_day_minimum_range_percentage \
                    and mapped_trade[0]['price_range_percentage'] > AlphaAlgoRulesVars.two_hours_minimum_range_percentage:
                potential_valid_fund_data.append(mapped_trade[0]['end_ts'])
        a = DBCol(chart_tf_one_day, symbol).column_between(init_ts, end_ts, 'start_ts')

        while init_ts + append_ts <= end_ts:
            for trade in query_chart_db_data(1440, ):
                pass

    is_fund_data_rising()


    symbol = "BTCUSDT"










































    figure, axis = plt.subplots(2, 2)

    def standard_plot(timeframe_in_mins, plot: [SubplotBase, types.ModuleType], symbol):
        chart_tf_db = trades_chart.format(timeframe_in_mins)
        start_ts = ValidatorDB(chart_tf_db).start_ts
        symbol_chart = DBCol(chart_tf_db, symbol).column_between(start_ts, start_ts, 'start_ts')[0]

        plot.plot([counter for counter in symbol_chart['range_price_volume'].keys()],
             [vol['sum_volume_percentage'] for vol in symbol_chart['range_price_volume'].values()])

        plot.text(1, 75, f"rise_of_start_end_price_in_percentage: "
                        f"{symbol_chart['range_price_volume_difference']['rise_of_start_end_price_in_percentage']}")
        plot.text(1, 80, f"end_price_counter: {symbol_chart['end_price_counter']}")
        plot.text(1, 85, f"start_price_counter: {symbol_chart['start_price_counter']}")
        plot.text(1, 90, f"price_range_percentage: {symbol_chart['price_range_percentage']}")

        def match_vol_percentage_with_counter(volume_sum):
            for i, range_price_volume_data in enumerate(symbol_chart['range_price_volume'].values()):
                if volume_sum == range_price_volume_data['sum_volume_percentage']:
                    return i

        plot.plot(match_vol_percentage_with_counter(symbol_chart['range_price_volume_difference']['start_price_volume_percentage']),
                 symbol_chart['range_price_volume_difference']['start_price_volume_percentage'], 'ro')
        plot.plot(match_vol_percentage_with_counter(symbol_chart['range_price_volume_difference']['end_price_volume_percentage']),
                 symbol_chart['range_price_volume_difference']['end_price_volume_percentage'], 'bo')


    standard_plot(1440, axis[0, 0], symbol)
    standard_plot(1440, axis[1, 0], 'fund_data')
    standard_plot(480, axis[0, 1], 'fund_data')

    plt.show()

    def range_percentages(timeframe_in_mins):
        chart_tf_db = trades_chart.format(timeframe_in_mins)

        start_ts = ValidatorDB(chart_tf_db).start_ts
        dif_price_range = 0
        save_start_ts = 0
        # for symbol in DB(chart_tf_db).list_collection_names():
        for i, symbol_chart in enumerate(DBCol(chart_tf_db, "fund_data").column_between(start_ts, start_ts + ONE_DAY_IN_MS / 12, 'start_ts')):
            save_start_ts = symbol_chart['start_ts']

            if dif_price_range != symbol_chart['price_range_percentage']:
                dif_price_range = symbol_chart['price_range_percentage']
                print(symbol_chart['price_range_percentage'], i)
            # a += 1
            # plt.plot(symbol_chart[0]['price_range_percentage'], symbol_chart[0]['price_range_percentage'], 'ro')

        #plt.text(symbol_chart['price_range_percentage'], symbol_chart['price_range_percentage'], symbol)

    # range_percentages(1440)
    # plt.show()

    def plot_volumes(timeframe_in_mins):
        chart_tf_db = trades_chart.format(timeframe_in_mins)

        start_ts = ValidatorDB(chart_tf_db).start_ts
        for symbol in DB(chart_tf_db).list_collection_names():
            symbol_chart = DBCol(chart_tf_db, symbol).find_one(start_ts, start_ts, 'start_ts')
            plt.text(symbol_chart['total_volume'], symbol_chart['total_volume'], symbol)
            plt.plot(symbol_chart['total_volume'], symbol_chart['total_volume'], 'ro')

    plot_volumes(1440)
    plt.show()
    total_volumes = [v['total_volume'] for v in symbol_chart]

    volumes = [v['range_price_volume_difference']['rise_of_start_end_volume_in_percentage'] for v in symbol_chart
               if v['range_price_volume_difference']]
    volume_sum = []
    for i in range(1, 100):
        volume_sum.append(symbol_chart)
    plt.plot(list(range(1, 100)), total_volumes)
    plt.show()




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


