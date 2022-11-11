import asyncio
import inspect
import logging
from pymongo.errors import ServerSelectionTimeoutError
import logs
from MongoDB.db_actions import localhost, list_dbs
from argparse_func import get_argparse_execute_functions

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

    for func, f_args in get_argparse_execute_functions():
        if inspect.iscoroutinefunction(func):
            asyncio.run(async_main(func, f_args))
        elif f_args:
            func(f_args)
        else:
            func()


main()




