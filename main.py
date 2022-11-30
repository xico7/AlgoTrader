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
from MongoDB.db_actions import localhost, list_dbs, DB, DBCol, trades_chart, query_missing_tfs
from argparse_func import get_argparse_execute_functions
from data_handling.data_helpers.vars_constants import VALIDATOR_DB, ONE_DAY_IN_MINUTES, DEFAULT_SYMBOL_SEARCH, \
    ONE_DAY_IN_MS, TEN_SECONDS_IN_MS

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


async def async_main(func, args):
    while True:
        await func(*args) if args else await func()


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

        heal_trade_data_threads = []

        for tf in query_missing_tfs(1440):
            heal_trade_data_threads.append(threading.Thread(target=run_algotrader_process, args=(
                'transform-trade-data', ['--chart-minutes', '1440', '--multithread-start-end-timeframe', f'{tf[0]}', f'{tf[1]}'])))

        for thread in heal_trade_data_threads:
            thread.start()
        for thread in heal_trade_data_threads:
            thread.join()

        threads = {}
        start_ts = DBCol(trades_chart.format(ONE_DAY_IN_MINUTES), DEFAULT_SYMBOL_SEARCH).most_recent_timeframe() + TEN_SECONDS_IN_MS

        NUMBER_OF_ONE_DAY_TRADE_DATA_THREADS = 4

        for i in range(NUMBER_OF_ONE_DAY_TRADE_DATA_THREADS):
            end_ts = start_ts + ONE_DAY_IN_MS
            threads[f'transform-trade-data-1440-{i}'] = threading.Thread(target=run_algotrader_process, args=(
                'transform-trade-data', ['--chart-minutes', '1440', '--multithread-start-end-timeframe',
                                         f'{start_ts}', f'{end_ts}']))
            start_ts += ONE_DAY_IN_MS + TEN_SECONDS_IN_MS

        threads['transform-trade-data-60'] = threading.Thread(target=run_algotrader_process, args=('transform-trade-data', ['--chart-minutes', '60']))
        threads['transform-trade-data-120'] = threading.Thread(target=run_algotrader_process, args=('transform-trade-data', ['--chart-minutes', '120']))
        threads['transform-trade-data-240'] = threading.Thread(target=run_algotrader_process, args=('transform-trade-data', ['--chart-minutes', '240']))
        threads['transform-trade-data-480'] = threading.Thread(target=run_algotrader_process, args=('transform-trade-data', ['--chart-minutes', '480']))
        threads['parse-trades-ten-seconds'] = threading.Thread(target=run_algotrader_process, args=('parse-trades-ten-seconds',))
        threads['save-aggtrades'] = threading.Thread(target=run_algotrader_process, args=('save-aggtrades',))

        if not DB(VALIDATOR_DB).list_collection_names():
            threads['save-aggtrades'].start()
            LOG.info("First run detected, starting binance aggtrades and waiting 20 minutes")
            time.sleep(1200)
            del threads['save-aggtrades']
        for thread in threads.values():
            thread.start()
            time.sleep(30)


main()



