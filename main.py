import asyncio
import inspect
import logging
import threading
from pymongo.errors import ServerSelectionTimeoutError
from MongoDB.db_actions import localhost, list_dbs
import logs
from argparse_func import get_argparse_execute_functions, RUN_DEFAULT_ARG
from support.threading_helpers import run_mongodb_startup_process, run_algotrader_process
from pymongoarrow.monkey import patch_all
#patch_all()

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


async def async_main(func, args):
    while True:
        await func(*args) if args else await func()


# TODO: Fix log debug showing without always and not only with -vv.

def main():

    # TODO: coin ratio can only be found in the present, so its better to parse data near the present.
    run_mongodb_startup_process()

    try:
        LOG.info("Querying MongoDB to check if DB is available.")
        list_dbs()
        LOG.info("DB Connection is OK.")
    except ServerSelectionTimeoutError as e:
        if (localhost and 'Connection refused') in e.args[0]:
            LOG.exception("Cannot connect to localhosts mongo DB.")
            raise
        else:
            LOG.exception("Unexpected error while trying to connect to MongoDB.")
            raise

    for function, args in get_argparse_execute_functions():
        if function == RUN_DEFAULT_ARG:
            threading.Thread(target=run_algotrader_process, args=('parse-trades-ten-seconds',)).start()
            threading.Thread(target=run_algotrader_process, args=('aggtrades-runner',)).start()
        elif inspect.iscoroutinefunction(function):
            asyncio.run(async_main(function, args))
        elif args:
            
            function(args)
        else:
            function()


main()
