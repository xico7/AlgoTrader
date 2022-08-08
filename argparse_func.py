import argparse
import inspect
from inspect import getmembers, isfunction
import pkgutil
import logging
import logs
from MongoDB.db_actions import list_dbs
import tasks

PROGRAM_NAME = 'Algotrading-Crypto'
fund_data = 'fund-data'
transform_trade_data = 'transform-trade-data'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def algo_argparse():

    parent_parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        fromfile_prefix_chars='@'
    )

    parent_parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")

    subparser = parent_parser.add_subparsers(dest="command")

    for element in pkgutil.iter_modules(tasks.__path__):
        subparser.add_parser(element.name.replace("_", "-"))

    subparser.choices[transform_trade_data].add_argument(f"--{transform_trade_data}-db-name", required=True, help="TODO", choices=list_dbs())
    subparser.choices[transform_trade_data].add_argument(f"--{transform_trade_data}-chart-minutes", type=int, required=True, help="TODO")
    return parent_parser.parse_args()


def get_execute_function(parsed_args):

    base_execute_module_name = parsed_args.command.replace("-", "_")
    for element in pkgutil.iter_modules(tasks.__path__):
        if base_execute_module_name == element.name:
            execute_module_path = f"{tasks.__name__}.{base_execute_module_name}"
            __import__(execute_module_path)

            execute_module_functions = getmembers(getattr(tasks, base_execute_module_name), isfunction)
            execute_function = None

            for function in execute_module_functions:
                if function[1].__module__ == execute_module_path:
                    if not execute_function:
                        execute_function = function[1]
                    else:
                        LOG.error("Task modules need one and only one callable function instead of %s.", execute_function)
                        exit(1)
            else:
                function_args = None

                if inspect.getfullargspec(execute_function).args:
                    function_args = {k: v for k, v in vars(parsed_args).items() if base_execute_module_name in k}

                return execute_function, function_args
