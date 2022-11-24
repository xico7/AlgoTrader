import asyncio
import inspect
import logging
import threading

from pymongo.errors import ServerSelectionTimeoutError
import logs
from MongoDB.db_actions import localhost, list_dbs, ATOMIC_TIMEFRAME_CHART_TRADES
from argparse_func import get_argparse_execute_functions
from tasks.parse_trades_ten_seconds import parse_trades_ten_seconds
from tasks.save_aggtrades import save_aggtrades
from tasks.transform_trade_data import transform_trade_data

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


#TODO: Add create indexes in DB automatically. (timestamp in parsed_aggtrades, etc,.)


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

        # TODO: thread save_aggtrades and parse_trades_ten_seconds Not working in packaged application.. fix.
        threads = []
        threads.append(threading.Thread(target=save_aggtrades))
        threads.append(threading.Thread(target=transform_trade_data, args=({'chart_minutes': ATOMIC_TIMEFRAME_CHART_TRADES},)))
        threads.append(threading.Thread(target=parse_trades_ten_seconds))

        for thread in threads:
            thread.start()



main()




