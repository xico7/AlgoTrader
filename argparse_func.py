import argparse
import inspect
from inspect import isfunction
import pkgutil
import logging
import logs
import tasks


logs.setup_logs(verbosity=[logging.INFO, logging.INFO - 5, logging.DEBUG, logging.VERBOSE][:4 + 1][-1])
LOG = logging.getLogger(logs.LOG_BASE_NAME + '.main')


def algo_argparse():
    parent_parser = argparse.ArgumentParser(prog='Algotrading-Crypto', fromfile_prefix_chars='@')
    parent_parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")

    subparser = parent_parser.add_subparsers(dest="command")

    for element in pkgutil.iter_modules(tasks.__path__):
        subparser.add_parser(element.name.replace("_", "-"))

    subparser.choices['transform-trade-data'].add_argument(f"--chart-minutes", type=int, required=True, help="Volume percentile data chart timeframe.")
    return parent_parser.parse_args()


def get_execute_functions(parsed_args):
    base_execute_module_name = parsed_args['command'].replace("-", "_")
    for task in pkgutil.iter_modules(tasks.__path__):
        if base_execute_module_name == task.name:
            execute_module_path = f"{tasks.__name__}.{base_execute_module_name}"

            module_objects = vars(getattr(__import__(execute_module_path), base_execute_module_name))
            execute_module_functions = [obj for obj in module_objects.values() if
                                        isfunction(obj) and obj.__module__ == execute_module_path]
            execute_functions = {}

            for function in execute_module_functions:
                execute_functions[function] = parsed_args if inspect.getfullargspec(function).args else None

            return execute_functions
