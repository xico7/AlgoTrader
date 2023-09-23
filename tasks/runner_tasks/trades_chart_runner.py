import logging
import threading
import time

import logs
from MongoDB.db_actions import ValidatorDB, InvalidStartTimestamp, TradesChartValidatorDB, TradesChartTimeframes
from data_handling.data_helpers.vars_constants import BASE_TRADES_CHART_DB, TEN_SECONDS_IN_MS, \
    TEN_SECS_PARSED_TRADES_DB, ONE_DAY_IN_MS, ONE_DAY_IN_MINUTES
from support.generic_helpers import mins_to_ms
from support.threading_helpers import run_algotrader_process


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def create_run_timeframe_chart_threads(timeframe: int, number_of_threads: int = 1):
    trade_chart_db_conn = ValidatorDB(BASE_TRADES_CHART_DB.format(timeframe))
    threads = []

    if begin_ts := trade_chart_db_conn.finish_ts:
        pass
    elif not (begin_ts := (ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts + mins_to_ms(timeframe))):
        LOG.error(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
        raise InvalidStartTimestamp(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
    parse_time = int(ONE_DAY_IN_MS / 16)

    timeframes = []
    for _ in range(number_of_threads):
        timeframes.append((begin_ts, begin_ts + parse_time))
        begin_ts += parse_time + TEN_SECONDS_IN_MS

    trade_chart_db_conn.drop_collection(trade_chart_db_conn.done_intervals_ts_collection)
    for start_end_tf in timeframes:
        threads.append(threading.Thread(
            target=run_algotrader_process, args=(
                'transform-trade-data', ['--chart-minutes', f'{timeframe}', '--multithread-start-end-timeframe',
                                         f'{start_end_tf[0]}', f'{start_end_tf[1]}'])))

    for thread in threads:
        thread.start()
        time.sleep(5)

    return threads


def trades_chart_runner(args):
    #TODO: Where is logging telling what we are 'starting' to do??
    TradesChartValidatorDB(TradesChartTimeframes.ONE_HOUR.value).set_valid_timestamps()
    # TradesChartValidatorDB(TradesChartTimeframes.TWO_HOURS.value).set_valid_timestamps()
    # TradesChartValidatorDB(TradesChartTimeframes.FOUR_HOURS.value).set_valid_timestamps()
    # TradesChartValidatorDB(TradesChartTimeframes.EIGHT_HOURS.value).set_valid_timestamps()
    # TradesChartValidatorDB(TradesChartTimeframes.ONE_DAY.value).set_valid_timestamps()
    # TradesChartValidatorDB(TradesChartTimeframes.TWO_DAYS.value).set_valid_timestamps()
    # TradesChartValidatorDB(TradesChartTimeframes.FOUR_DAYS.value).set_valid_timestamps()
    # TradesChartValidatorDB(TradesChartTimeframes.EIGHT_DAYS.value).set_valid_timestamps()

    one_hour_threads = create_run_timeframe_chart_threads()

    while True:
        if not any([thread.is_alive() for thread in one_hour_threads]):
            TradesChartValidatorDB(TradesChartTimeframes.ONE_HOUR.value).set_valid_timestamps()
            one_hour_threads = create_run_timeframe_chart_threads(60, args['one_hour_threads_number'])
        # if not any([thread.is_alive() for thread in two_hour_threads]):
        #     TradesChartValidatorDB(TradesChartTimeframes.TWO_HOURS.value).set_valid_timestamps()
        #     two_hour_threads = create_run_timeframe_chart_threads(120, args['two_hours_threads_number'])
        # if not any([thread.is_alive() for thread in four_hour_threads]):
        #     TradesChartValidatorDB(TradesChartTimeframes.FOUR_HOURS.value).set_valid_timestamps()
        #     four_hour_threads = create_run_timeframe_chart_threads(240, args['four_hours_threads_number'])
        # if not any([thread.is_alive() for thread in eight_hour_threads]):
        #     TradesChartValidatorDB(TradesChartTimeframes.EIGHT_HOURS.value).set_valid_timestamps()
        #     eight_hour_threads = create_run_timeframe_chart_threads(480, args['eight_hours_threads_number'])
        # if not any([thread.is_alive() for thread in one_day_threads]):
        #     TradesChartValidatorDB(TradesChartTimeframes.ONE_DAY.value).set_valid_timestamps()
        #     one_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES, args['one_day_threads_number'])
        # if not any([thread.is_alive() for thread in two_day_threads]):
        #     TradesChartValidatorDB(TradesChartTimeframes.TWO_DAYS.value).set_valid_timestamps()
        #     two_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 2, args['two_days_threads_number'])
        # if not any([thread.is_alive() for thread in four_day_threads]):
        #     TradesChartValidatorDB(TradesChartTimeframes.FOUR_DAYS.value).set_valid_timestamps()
        #     four_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 4, args['four_days_threads_number'])
        # if not any([thread.is_alive() for thread in eight_day_threads]):
        #     TradesChartValidatorDB(TradesChartTimeframes.EIGHT_DAYS.value).set_valid_timestamps()
        #     eight_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 8, args['eight_days_threads_number'])

        time.sleep(20)
