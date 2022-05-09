import argparse
import inspect
from inspect import getmembers, isfunction
import pkgutil
import logging

from pymongo import MongoClient

import logs

PROGRAM_NAME = 'Algotrading-Crypto'
fund_data = 'fund-data'
transform_trade_data = 'transform-trade-data'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def algo_argparse():
    import tasks

    parent_parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        fromfile_prefix_chars='@'
    )

    parent_parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")

    subparser = parent_parser.add_subparsers(dest="command")

    for element in pkgutil.iter_modules(tasks.__path__):
        subparser.add_parser(element.name.replace("_", "-"))
        pass

    # TODO: Improve choices, find all collections in MongoDB and those are the choices.
    from MongoDB.db_actions import fund_trades_database_name, usdt_trades_database_name
    subparser.choices[transform_trade_data].add_argument(f"--{transform_trade_data}-db-name", required=True, help="TODO",
                                                         choices=[fund_trades_database_name, usdt_trades_database_name])

    subparser.choices[transform_trade_data].add_argument(f"--{transform_trade_data}-timeframe-in-secs", required=True, type=int,
                                                         help="TODO")
    subparser.choices[transform_trade_data].add_argument(f"--{transform_trade_data}-interval-in-secs", required=True, type=int,
                                                         help="chart calculate each 'time interval")
    return parent_parser.parse_args()


def get_execute_function(parsed_args):
    import tasks
    execute_functions = []

    for element in pkgutil.iter_modules(tasks.__path__):
        if element.name == parsed_args.command.replace("-", "_"):
            __import__(f"{tasks.__name__}.{element.name}")

            for function in getmembers(getattr(tasks, element.name), isfunction):
                if str.lstrip(function[0], '_') == function[0]:
                    execute_functions.append(function[1])

            if len(execute_functions) == 1:
                return execute_functions[0](vars(parsed_args)) if inspect.getfullargspec(execute_functions[0]).args \
                        else execute_functions[0]
            else:
                LOG.error("Task modules need one and only one callable function instead of %s.", execute_functions)
                exit(1)




