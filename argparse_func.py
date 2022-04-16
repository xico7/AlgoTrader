import argparse
from inspect import getmembers, isfunction
import pkgutil
from typing import Callable
import logging
import logs

PROGRAM_NAME = 'Algotrading-Crypto'
WS_TRADES = 'ws-trades'
TA_ANALYSIS = 'ta-analysis'
TA_SIGNAL = 'ta-signal'
ALPHA_ALGO = 'alpha-algo'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)

def algo_argparse():
    import tasks

    parent_parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        fromfile_prefix_chars='@'
    )

    parent_parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")
    parent_parser.add_argument("--debug-secs", default=900, required=False, type=int,
                               help="print heartbeat each X seconds")

    subparser = parent_parser.add_subparsers(dest="command")

    for element in pkgutil.iter_modules(tasks.__path__):
        subparser.add_parser(element.name.replace("_", "-"))
        pass

    subparser.choices[ALPHA_ALGO].add_argument("--hello", default=900, required=True, type=int,
                                                 help="print heartbeat each X seconds")
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
                return execute_functions[0]
            else:
                LOG.error("Task modules need one and only one callable function instead of %s.", execute_functions)
                exit(1)




