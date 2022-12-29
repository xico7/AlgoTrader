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
from MongoDB.db_actions import localhost, list_dbs, trades_chart, ValidatorDB, DB, query_charts_missing_tfs, DBCol, \
    TradesChartValidatorDB
from argparse_func import get_argparse_execute_functions
from data_handling.data_func import InvalidStartTimestamp
from data_handling.data_helpers.vars_constants import ONE_DAY_IN_MINUTES, ONE_DAY_IN_MS, TEN_SECONDS_IN_MS, \
    TEN_SECS_PARSED_TRADES_DB, VALIDATOR_DB

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


async def async_main(func, args):
    while True:
        await func(*args) if args else await func()


# TODO: Add test to check different trades_chart lenght in different symbols ( when the program is stopped the inserts can be mid-way ).
# TODO: Fix log debug showing without always and not only with -vv.
# TODO: rename 'valid_end_ts' to '_valid..' and create 'get_valid_end_ts' the exception handling logic etc is there.

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

    if func_func_args := get_argparse_execute_functions():
        for func, f_args in func_func_args:
            if inspect.iscoroutinefunction(func):
                asyncio.run(async_main(func, f_args))
            elif f_args:
                func(f_args)
            else:
                func()
    else:
        LOG.info("Starting Algotrader with default tasks.")

        def run_algotrader_process(task_to_run, task_args=[]):
            use_shell = False
            if os.name == 'nt':
                use_shell = True
            subprocess.call([sys.executable, 'Algotrader.py', task_to_run] + task_args, shell=use_shell)

        # print("Here")
        parse_missing_chart_trades_threads = []

        # one_day_missing_tfs = query_charts_missing_tfs(1440)
        # one_hour_missing_tfs = query_charts_missing_tfs(60)
        # two_hours_missing_tfs = query_charts_missing_tfs(120)
        # four_hours_missing_tfs = query_charts_missing_tfs(240)
        # eight_hours_missing_tfs = query_charts_missing_tfs(480)
        # two_days_missing_tfs = query_charts_missing_tfs(2880)
        # four_days_missing_tfs = query_charts_missing_tfs(5760)
        # eight_days_missing_tfs = query_charts_missing_tfs(11520)
        # #print('60', one_hour_missing_tfs, '1440', one_day_missing_tfs)
        # print('60', one_hour_missing_tfs, '120', two_hours_missing_tfs, '240', four_hours_missing_tfs, '480', eight_hours_missing_tfs,
        #       '1440', one_day_missing_tfs, '2880', two_days_missing_tfs, '5760', four_days_missing_tfs, '11520', eight_days_missing_tfs)

        # for tf in two_days_missing_tfs:
        #     DB(trades_chart.format(2880)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '2880', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in four_days_missing_tfs:
        #     DB(trades_chart.format(5760)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '5760', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in eight_days_missing_tfs:
        #     DB(trades_chart.format(11520)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '11520', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in one_day_missing_tfs:
        #     DB(trades_chart.format(1440)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '1440', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in one_hour_missing_tfs:
        #     DB(trades_chart.format(60)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '60', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in two_hours_missing_tfs:
        #     DB(trades_chart.format(120)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '120', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in four_hours_missing_tfs:
        #     DB(trades_chart.format(240)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '240', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in eight_hours_missing_tfs:
        #     DB(trades_chart.format(480)).clear_between(tf[0], tf[1], 'start_ts')
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '480', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))

        # for thread in parse_missing_chart_trades_threads:
        #     thread.start()
        #     time.sleep(25)
        # for thread in parse_missing_chart_trades_threads:
        #     thread.join()
        #     LOG.info("Finished parsing missing timeframes in trades chart in '%s' DB.", ONE_DAY_IN_MINUTES)
        #     exit(0)


        threads = {}
        threads['parse-trades-ten-seconds'] = threading.Thread(target=run_algotrader_process, args=('parse-trades-ten-seconds',))
        threads['save-aggtrades'] = threading.Thread(target=run_algotrader_process, args=('save-aggtrades',))
        # threads['parse-relative-volume-60'] = threading.Thread(target=run_algotrader_process, args=(
        #     'tech-analysis-relative-volume', ['--timeframe-in-minutes', '60']))
        # threads['parse-relative-volume-120'] = threading.Thread(target=run_algotrader_process, args=(
        #     'tech-analysis-relative-volume', ['--timeframe-in-minutes', '120']))
        # threads['parse-relative-volume-240'] = threading.Thread(target=run_algotrader_process, args=(
        #     'tech-analysis-relative-volume', ['--timeframe-in-minutes', '240']))
        # threads['parse-relative-volume-480'] = threading.Thread(target=run_algotrader_process, args=(
        #     'tech-analysis-relative-volume', ['--timeframe-in-minutes', '480']))
        # threads['parse-relative-volume-1440'] = threading.Thread(target=run_algotrader_process, args=(
        #     'tech-analysis-relative-volume', ['--timeframe-in-minutes', '1440']))

        if not DB(VALIDATOR_DB).list_collection_names():
            threads['save-aggtrades'].start()
            LOG.info("First run detected, starting binance aggtrades and waiting 20 minutes")
            time.sleep(1200)
            del threads['save-aggtrades']
        for thread in threads.values():
            thread.start()
            time.sleep(10)

        def create_run_timeframe_chart_threads(timeframe: int, number_of_threads: int = 1):
            threads = []
            if not (start_ts := ValidatorDB(trades_chart.format(timeframe)).finish_ts) and not (start_ts := ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts):
                LOG.error(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
                raise InvalidStartTimestamp(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")

            start_ts += TEN_SECONDS_IN_MS

            for _ in range(number_of_threads):
                parse_time = int(ONE_DAY_IN_MS / 6)
                threads.append(threading.Thread(target=run_algotrader_process,
                                                args=('transform-trade-data',
                                                      ['--chart-minutes', f'{timeframe}', '--multithread-start-end-timeframe',
                                                       f'{start_ts}', f'{start_ts + parse_time}'])))
                start_ts += parse_time + TEN_SECONDS_IN_MS

            for thread in threads:
                thread.start()

            return threads

        one_hour_threads = create_run_timeframe_chart_threads(60, 4)
        time.sleep(20)
        two_hour_threads = create_run_timeframe_chart_threads(120, 2)
        time.sleep(20)
        four_hour_threads = create_run_timeframe_chart_threads(240, 3)
        time.sleep(20)
        eight_hour_threads = create_run_timeframe_chart_threads(480, 3)
        time.sleep(20)
        one_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES, 4)
        time.sleep(20)
        two_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 2, 5)
        time.sleep(20)
        four_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 4, 5)
        time.sleep(20)
        eight_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 8, 6)

        while True:
            if not any([thread.is_alive() for thread in one_hour_threads]):
                one_hour_threads = create_run_timeframe_chart_threads(60, 4)
            if not any([thread.is_alive() for thread in two_hour_threads]):
                two_hour_threads = create_run_timeframe_chart_threads(120, 3)
            if not any([thread.is_alive() for thread in four_hour_threads]):
                four_hour_threads = create_run_timeframe_chart_threads(240, 3)
            if not any([thread.is_alive() for thread in eight_hour_threads]):
                eight_hour_threads = create_run_timeframe_chart_threads(480, 4)
            if not any([thread.is_alive() for thread in one_day_threads]):
                one_day_threads = create_run_timeframe_chart_threads(1440, 4)
            if not any([thread.is_alive() for thread in two_day_threads]):
                two_day_threads = create_run_timeframe_chart_threads(2880, 5)
            if not any([thread.is_alive() for thread in four_day_threads]):
                four_day_threads = create_run_timeframe_chart_threads(5760, 5)
            if not any([thread.is_alive() for thread in eight_day_threads]):
                eight_day_threads = create_run_timeframe_chart_threads(11520, 6)

            time.sleep(20)


main()



