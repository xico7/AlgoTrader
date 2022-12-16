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
from MongoDB.db_actions import localhost, list_dbs, trades_chart, ValidatorDB, DB, query_missing_tfs
from argparse_func import get_argparse_execute_functions
from data_handling.data_func import InvalidStartTimestamp
from data_handling.data_helpers.vars_constants import ONE_DAY_IN_MINUTES, ONE_DAY_IN_MS, TEN_SECONDS_IN_MS, \
    TEN_SECS_PARSED_TRADES_DB, VALIDATOR_DB

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


async def async_main(func, args):
    while True:
        await func(*args) if args else await func()


# TODO: Add test to check different trades_chart lenght in different symbols ( when the program is stopped the inserts can be mid-way ).
# TODO: First run (or first input in database) in trades_chart should be without threads, so it inits the valid_end_ts, assert this happens.

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

        # parse_missing_chart_trades_threads = []
        # one_hour_missing_tfs = query_missing_tfs(60)
        # two_hours_missing_tfs = query_missing_tfs(120)
        # four_hours_missing_tfs = query_missing_tfs(240)
        # eight_hours_missing_tfs = query_missing_tfs(480)
        # one_day_missing_tfs = query_missing_tfs(1440)
        # two_days_missing_tfs = query_missing_tfs(2880)
        # four_days_missing_tfs = query_missing_tfs(5760)
        # eight_days_missing_tfs = query_missing_tfs(11520)
        #
        # for tf in two_days_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '2880', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in four_days_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '5760', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in eight_days_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '11520', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in one_day_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '1440', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in one_hour_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '60', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in two_hours_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '120', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in four_hours_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '240', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        # for tf in eight_hours_missing_tfs:
        #     parse_missing_chart_trades_threads.append(threading.Thread(target=run_algotrader_process, args=(
        #         'transform-trade-data', ['--chart-minutes', '480', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))
        #
        # for thread in parse_missing_chart_trades_threads:
        #     thread.start()
        #     time.sleep(15)
        # for thread in parse_missing_chart_trades_threads:
        #     thread.join()
        #     LOG.info("Finished parsing missing timeframes in trades chart in '%s' DB.", ONE_DAY_IN_MINUTES)
        #     exit(0)
        

        threads = {}
        threads['parse-trades-ten-seconds'] = threading.Thread(target=run_algotrader_process, args=('parse-trades-ten-seconds',))
        threads['save-aggtrades'] = threading.Thread(target=run_algotrader_process, args=('save-aggtrades',))

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
                parse_time = int(ONE_DAY_IN_MS / 24)
                threads.append(threading.Thread(target=run_algotrader_process,
                                                args=('transform-trade-data', ['--chart-minutes', f'{timeframe}',
                                                                               '--multithread-start-end-timeframe',
                                                                               f'{start_ts}', f'{start_ts + parse_time}'])))
                start_ts += parse_time + TEN_SECONDS_IN_MS

            for thread in threads:
                thread.start()

            return threads

        one_hour_threads = create_run_timeframe_chart_threads(60, 2)
        time.sleep(20)
        two_hour_threads = create_run_timeframe_chart_threads(120, 3)
        time.sleep(20)
        four_hour_threads = create_run_timeframe_chart_threads(240, 4)
        time.sleep(20)
        eight_hour_threads = create_run_timeframe_chart_threads(480, 5)
        time.sleep(20)
        one_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES, 6)
        time.sleep(20)
        two_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 2, 7)
        time.sleep(20)
        four_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 4, 7)
        time.sleep(20)
        eight_day_threads = create_run_timeframe_chart_threads(ONE_DAY_IN_MINUTES * 4, 7)

        while True:
            if not any([thread.is_alive() for thread in one_hour_threads]):
                one_hour_threads = create_run_timeframe_chart_threads(60, 2)
            if not any([thread.is_alive() for thread in two_hour_threads]):
                two_hour_threads = create_run_timeframe_chart_threads(120, 3)
            if not any([thread.is_alive() for thread in four_hour_threads]):
                four_hour_threads = create_run_timeframe_chart_threads(240, 4)
            if not any([thread.is_alive() for thread in eight_hour_threads]):
                eight_hour_threads = create_run_timeframe_chart_threads(480, 5)
            if not any([thread.is_alive() for thread in one_day_threads]):
                one_day_threads = create_run_timeframe_chart_threads(1440, 6)
            if not any([thread.is_alive() for thread in two_day_threads]):
                two_day_threads = create_run_timeframe_chart_threads(2880, 7)
            if not any([thread.is_alive() for thread in four_day_threads]):
                four_day_threads = create_run_timeframe_chart_threads(5760, 7)
            if not any([thread.is_alive() for thread in eight_day_threads]):
                eight_day_threads = create_run_timeframe_chart_threads(11520, 7)

            time.sleep(20)

main()



