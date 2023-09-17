import logging
import threading
import time
import logs
from MongoDB.db_actions import DBCol, set_universal_start_end_ts, DBMapper, get_top_n_traded_volume
from data_handling.data_helpers.vars_constants import TRADES_CHART_DB_CONTAINING_NAME, ONE_DAY_IN_MS, ONE_MIN_IN_MS, \
    REL_VOLUME_DB_CONTAINING_NAME
from support.threading_helpers import run_algotrader_process


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidMetricBeginTS(Exception): pass


def alpha_algo_metrics_distribution_runner(args):
    universal_start_ts = set_universal_start_end_ts()[0]


    #metric_beginning_ts = args['start_ts'] - ONE_DAY_IN_MS * 180  # last 6 months are the time required for getting metric distribution.

    # TEST PURPOSES, SHOULD BE 180' and start ts should come from args
    metric_beginning_ts = int(set_universal_start_end_ts()[1] - ONE_DAY_IN_MS / 16)  # last 6 months are the time required for getting metric distribution.

    if metric_beginning_ts < universal_start_ts:
        error_message = "Invalid begin ts, universal start ts must be inside the metric begin -- end timestamp."
        LOG.error(error_message)
        raise InvalidMetricBeginTS(error_message)

    def parse_metrics(db_mapper: DBMapper, metrics_to_parse: [str, list]):
        for volumes_timestamp in range(metric_beginning_ts, args['start_ts'], ONE_DAY_IN_MS):
            for symbol in get_top_n_traded_volume(25, volumes_timestamp):
                DBCol(db_mapper.name, symbol).find_timeseries(list(range(metric_beginning_ts, volumes_timestamp, ONE_MIN_IN_MS * 15)))


    trades_chart_db_metrics = ['rise_of_start_end_volume_in_percentage', 'price_range_percentage', 'start_price_counter', 'end_price_counter']
    for db in DBMapper:
        if TRADES_CHART_DB_CONTAINING_NAME in db.name:
            parse_metrics(db, trades_chart_db_metrics)
        elif REL_VOLUME_DB_CONTAINING_NAME in db.name:
            parse_metrics(db, 'relative_volume')

    liets = {}

    for symbol_volume in DBCol("Alpha_algo_stuff", "trades_chart_distribution").find_all():
        if not liets:
            liets = symbol_volume['distribution_values']
        else:
            for counter, value in symbol_volume['distribution_values'].items():
                try:
                    liets[counter] += value
                except KeyError:
                    liets[counter] = value

    aggtrade_threads = []
    symbol_volumes = DBCol('total_volume_60_minutes', 'total_volume').find_timeseries_one(start_ts)['total_volume']
    symbols = sorted(symbol_volumes, key=symbol_volumes.get, reverse=True)[4:31]

    for symbol in symbols:
        aggtrade_threads.append(threading.Thread(target=run_algotrader_process, args=(
            'get-trades-chart-metric-distribution', ['--trades-chart-timeframe', f"{args['timeframe']}",
                                                     '--symbol', f"{symbol}",
                                                     '--trades-chart-metric-name', f"{args['metric_name']}",
                                                     '--trades-chart-decimal-places', "0"])))

    for thread in aggtrade_threads:
        thread.start()
        time.sleep(3)

    while True:
        if not any([thread.is_alive() for thread in aggtrade_threads]):
            print("HEREasddasdas")


        print("HE")
        time.sleep(15)





