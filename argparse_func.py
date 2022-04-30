import argparse
from inspect import getmembers, isfunction
import pkgutil
import logging
import logs

PROGRAM_NAME = 'Algotrading-Crypto'
WS_TRADES = 'ws-trades'
TA_ANALYSIS = 'ta-analysis'
TA_SIGNAL = 'ta-signal'
transform_agtrades = 'transform-aggtrades'

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

    subparser.choices[transform_agtrades].add_argument(f"--{transform_agtrades}-timeframe-in-secs", required=True, type=int,
                                                       help="chart timeframe")
    subparser.choices[transform_agtrades].add_argument(f"--{transform_agtrades}-interval-in-secs", required=True, type=int,
                                                       help="chart calculate each 'time interval'")
    return parent_parser.parse_args()


def get_execute_function(parsed_args):
    import tasks
    execute_functions = []
    args = []

    command_group = parsed_args.command.replace("-", "_")


    for element in pkgutil.iter_modules(tasks.__path__):
        if element.name == parsed_args.command.replace("-", "_"):
            __import__(f"{tasks.__name__}.{element.name}")

            for function in getmembers(getattr(tasks, element.name), isfunction):
                if str.lstrip(function[0], '_') == function[0]:
                    execute_functions.append(function[1])

            if len(execute_functions) == 1:
                return execute_functions[0](vars(parsed_args))
            else:
                LOG.error("Task modules need one and only one callable function instead of %s.", execute_functions)
                exit(1)




