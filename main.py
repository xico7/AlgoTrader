import asyncio
import inspect
import logging

from pymongo.errors import ServerSelectionTimeoutError

import argparse_func as argp
import logs
from MongoDB.db_actions import mongo_client, localhost

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


async def async_main(func, args):
    while True:
        await func(*args) if args else await func()


def main():
    try:
        LOG.info("Querying MongoDB to check if DB is available.")
        mongo_client.list_database_names()
    except ServerSelectionTimeoutError as e:
        if (localhost and 'Connection refused') in e.args[0]:
            LOG.exception("Cannot connect to localhosts mongo DB.")
            raise
        else:
            LOG.exception("Unexpected error while trying to connect to MongoDB.")
            raise

    for function, function_args in argp.get_execute_functions(vars(argp.algo_argparse())).items():
        if inspect.iscoroutinefunction(function):
            asyncio.run(async_main(function, function_args))
        elif function_args:
            function(function_args)
        else:
            function()


main()




