import asyncio
import inspect
import logging
import subprocess
import sys
import os
import threading
import time

from pymongo.errors import ServerSelectionTimeoutError
import logs
from MongoDB.db_actions import localhost, list_dbs, BASE_TRADES_CHART_DB, ValidatorDB, DB, query_charts_missing_tf_intervals, \
    DBCol, \
    TradesChartValidatorDB, change_charts_values
from argparse_func import get_argparse_execute_functions
from data_handling.data_helpers.data_staging import mins_to_ms
from data_handling.data_structures import InvalidStartTimestamp
from data_handling.data_helpers.vars_constants import ONE_DAY_IN_MINUTES, ONE_DAY_IN_MS, TEN_SECONDS_IN_MS, \
    TEN_SECS_PARSED_TRADES_DB, VALIDATOR_DB, PARSED_AGGTRADES_DB

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


async def async_main(func, args):
    while True:
        await func(*args) if args else await func()


# TODO: Fix log debug showing without always and not only with -vv.

def main():
    try:
        LOG.info("Querying MongoDB to check if DB is available.")
        list_dbs()
    except ServerSelectionTimeoutError as e:
        if (localhost and 'Connection refused') in e.args[0]:
            LOG.exception("Cannot connect to localhosts mongo DB.")
            raise
        else:
            LOG.exception("Unexpected error while trying to connect to MongoDB.")
            raise

    if function_function_args := get_argparse_execute_functions():
        for function, args in function_function_args:
            if inspect.iscoroutinefunction(function):
                asyncio.run(async_main(function, args))
            elif args:
                function(args)
            else:
                function()
    else:
        LOG.info("Starting Algotrader with default tasks.")

        def run_algotrader_process(task_to_run, task_args=[]):
            use_shell = False
            if os.name == 'nt':
                use_shell = True
            subprocess.call([sys.executable, 'Algotrader.py', task_to_run] + task_args, shell=use_shell)

        # parse_missing_chart_trades_threads = []
        #
        # timeframe = 60
        #
        # LOG.info(f"Verify all symbols for timeframe {timeframe}")
        # for symbol in DB(BASE_TRADES_CHART_DB.format(timeframe)).list_collection_names():
        #     for tf in query_charts_missing_tf_intervals(timeframe, [symbol]):
        #         DB(BASE_TRADES_CHART_DB.format(timeframe)).clear_between(tf[0], tf[1], 'begin_ts', [symbol])
        #         parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #             'transform-trade-data', ['--chart-minutes', timeframe, '--multithread-start-end-timeframe',
        #                                      f'{tf[0]}', f'{tf[1]}', '--symbols', symbol])))
        #         for thread in parse_missing_chart_trades_threads:
        #             thread.start()
        #             time.sleep(5)
        #         for thread in parse_missing_chart_trades_threads:
        #             thread.join()
        #             LOG.info("Finished parsing missing timeframes in trades chart in '%s' DB for '%s' symbol.",
        #                      timeframe, symbol)
        #             parse_missing_chart_trades_threads = []
        # TradesChartValidatorDB(timeframe).update_valid_end_ts()
        #
        # LOG.info(f"Timeframe {timeframe} finished.")
        # exit(0)
        # # one_day_missing_tfs = query_charts_missing_tf_intervals(1440)
        #
        # one_hour_missing_tfs = query_charts_missing_tf_intervals(60)
        # # two_hours_missing_tfs = query_charts_missing_tf_intervals(120)
        # # four_hours_missing_tfs = query_charts_missing_tf_intervals(240)
        # # eight_hours_missing_tfs = query_charts_missing_tf_intervals(480)
        # # two_days_missing_tfs = query_charts_missing_tf_intervals(2880)
        # # four_days_missing_tfs = query_charts_missing_tf_intervals(5760)
        # # eight_days_missing_tfs = query_charts_missing_tf_intervals(11520)
        # print(one_day_missing_tfs)
        # exit(0)
        # # print('60', one_hour_missing_tfs, '120', two_hours_missing_tfs, '240', four_hours_missing_tfs, '480', eight_hours_missing_tfs,
        # #       '1440', one_day_missing_tfs, '2880', two_days_missing_tfs, '5760', '11520', eight_days_missing_tfs)
        #
        # for tf in two_days_missing_tfs:
        #     DB(trades_chart.format(2880)).clear_between(tf[0], tf[1], 'begin_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '2880', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in four_days_missing_tfs:
        #     DB(trades_chart.format(5760)).clear_between(tf[0], tf[1], 'begin_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '5760', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in eight_days_missing_tfs:
        #     DB(trades_chart.format(11520)).clear_between(tf[0], tf[1], 'begin_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '11520', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in one_day_missing_tfs:
        #     DB(trades_chart.format(1440)).clear_between(tf[0], tf[1], 'begin_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '1440', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in one_hour_missing_tfs:
        #     DB(trades_chart.format(60)).clear_between(tf[0], tf[1], 'begin_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '60', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # # for tf in two_hours_missing_tfs:
        # #     DB(trades_chart.format(120)).clear_between(tf[0], tf[1], 'begin_ts')
        # #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        # #         'transform-trade-data', ['--chart-minutes', '120', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # # for tf in four_hours_missing_tfs:
        # #     DB(trades_chart.format(240)).clear_between(tf[0], tf[1], 'begin_ts')
        # #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        # #         'transform-trade-data', ['--chart-minutes', '240', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # # for tf in eight_hours_missing_tfs:
        # #     DB(trades_chart.format(480)).clear_between(tf[0], tf[1], 'begin_ts')
        # #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        # #         'transform-trade-data', ['--chart-minutes', '480', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        #
        # for thread in parse_missing_chart_trades_threads:
        #     thread.start()
        #     time.sleep(25)
        # for thread in parse_missing_chart_trades_threads:
        #     thread.join()
        #     LOG.info("Finished parsing missing timeframes in trades chart in '%s' DB.", ONE_DAY_IN_MINUTES)
        #     exit(0)
        #

        # def create_run_aggtrades_threads():
        #     aggtrades_threads = []
        #
        #     save_aggtrades_count = 3
        #     parse_minutes = 90
        #
        #     if not (begin_ts := DB(PARSED_AGGTRADES_DB).end_ts):
        #         begin_ts = 1640955600000
        #
        #     for thread_index in range(0, save_aggtrades_count):
        #         end_ts = begin_ts + mins_to_ms(parse_minutes)
        #         aggtrades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #             'save-aggtrades', ['--start-ts', f'{begin_ts}', '--end-ts', f'{end_ts}'])))
        #         begin_ts += mins_to_ms(parse_minutes)
        #
        #     for thread in aggtrades_threads:
        #         thread.start()
        #         time.sleep(6)
        #
        #     return aggtrades_threads
        #
        # aggtrades_threads = create_run_aggtrades_threads()
        #
        # while True:
        #     if not any([thread.is_alive() for thread in aggtrades_threads]):
        #         ValidatorDB(PARSED_AGGTRADES_DB).set_valid_timestamps()
        #         aggtrades_threads = create_run_aggtrades_threads()
        #         time.sleep(20)
        #
        #     time.sleep(1)

        # threads = {}
        # # threads['parse-trades-ten-seconds'] = threading.Thread(target=run_algotrader_process, args=('parse-trades-ten-seconds',))
        # threads['relative-volume-60'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '60']))
        # threads['relative-volume-120'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '120']))
        # threads['relative-volume-240'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '240']))
        # threads['relative-volume-480'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '480']))
        # threads['relative-volume-1440'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '1440']))
        # threads['relative-volume-2880'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '2880']))
        # threads['relative-volume-5760'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '5760']))
        # threads['relative-volume-11520'] = threading.Thread(target=run_algotrader_process, args=('relative-volume', ['--timeframe-in-minutes', '11520']))

        # for thread in threads.values():
        #     thread.start()
        #     time.sleep(20)

        def create_run_timeframe_chart_threads(timeframe: int, number_of_threads: int = 1):
            trade_chart_db_conn = ValidatorDB(BASE_TRADES_CHART_DB.format(timeframe))
            threads = []

            if begin_ts := trade_chart_db_conn.finish_ts:
                pass
            elif not (begin_ts := (ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts + mins_to_ms(timeframe))):
                LOG.error(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
                raise InvalidStartTimestamp(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
            parse_time = int(ONE_DAY_IN_MS / 48)

            timeframes = []
            for _ in range(number_of_threads):
                timeframes.append((begin_ts, begin_ts + parse_time))
                begin_ts += parse_time + TEN_SECONDS_IN_MS

            trade_chart_db_conn.drop_collection(trade_chart_db_conn.done_intervals_ts_collection)
            for start_end_tf in timeframes:
                threads.append(threading.Thread(target=run_algotrader_process,
                                                args=('transform-trade-data',
                                                      ['--chart-minutes', f'{timeframe}', '--multithread-start-end-timeframe',
                                                       f'{start_end_tf[0]}', f'{start_end_tf[1]}'])))

            for thread in threads:
                time.sleep(5)
                thread.start()

            return threads

        one_hour_thread_count = 10
        two_hour_thread_count = 4
        four_hour_thread_count = 4
        eight_hour_thread_count = 4
        one_day_thread_count = 4
        two_days_thread_count = 4
        four_days_thread_count = 4
        eight_days_thread_count = 2

        one_hour_threads = create_run_timeframe_chart_threads(60, one_hour_thread_count)
        time.sleep(30)
        two_hour_threads = create_run_timeframe_chart_threads(120, two_hour_thread_count)
        time.sleep(30)
        four_hour_threads = create_run_timeframe_chart_threads(240, four_hour_thread_count)
        time.sleep(30)
        eight_hour_threads = create_run_timeframe_chart_threads(480, eight_hour_thread_count)
        time.sleep(30)
        one_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES, one_day_thread_count)
        time.sleep(30)
        two_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 2, two_days_thread_count)
        time.sleep(30)
        four_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 4, four_days_thread_count)
        time.sleep(30)
        eight_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 8, eight_days_thread_count)

        while True:
            if not any([thread.is_alive() for thread in one_hour_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(60)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                one_hour_threads = create_run_timeframe_chart_threads(60, one_hour_thread_count)
            if not any([thread.is_alive() for thread in two_hour_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(120)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                two_hour_threads = create_run_timeframe_chart_threads(120, two_hour_thread_count)
            if not any([thread.is_alive() for thread in four_hour_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(240)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                four_hour_threads = create_run_timeframe_chart_threads(240, four_hour_thread_count)
            if not any([thread.is_alive() for thread in eight_hour_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(480)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                eight_hour_threads = create_run_timeframe_chart_threads(480, eight_hour_thread_count)
            if not any([thread.is_alive() for thread in one_day_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(1440)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                one_day_threads = create_run_timeframe_chart_threads(1440, one_day_thread_count)
            if not any([thread.is_alive() for thread in two_day_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(2880)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                two_day_threads = create_run_timeframe_chart_threads(2880, two_days_thread_count)
            if not any([thread.is_alive() for thread in four_day_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(5760)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                four_day_threads = create_run_timeframe_chart_threads(5760, four_days_thread_count)
            if not any([thread.is_alive() for thread in eight_day_threads]):
                ValidatorDB(BASE_TRADES_CHART_DB.format(11520)).set_valid_timestamps(timestamps_gap=TEN_SECONDS_IN_MS)
                eight_day_threads = create_run_timeframe_chart_threads(11520, eight_days_thread_count)

            time.sleep(20)



main()



