import argparse
import inspect
from collections import namedtuple
from inspect import isfunction
import pkgutil
import logging
import logs
import tasks
from MongoDB.db_actions import DB
from data_handling.data_helpers.vars_constants import TEN_SECS_PARSED_TRADES_DB, UNUSED_CHART_TRADE_SYMBOLS

logs.setup_logs(verbosity=[logging.INFO, logging.INFO - 5, logging.DEBUG, logging.VERBOSE][:4 + 1][-1])
LOG = logging.getLogger(logs.LOG_BASE_NAME + '.main')


def get_argparse_execute_functions():
    parent_parser = argparse.ArgumentParser(prog='Algotrading-Crypto', fromfile_prefix_chars='@')
    parent_parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")

    subparser = parent_parser.add_subparsers(dest="command")

    tasks_parser = []
    tasks_path_name = namedtuple('tasks_path_name', ['root_dir_name', 'name',])
    for element in pkgutil.iter_modules(tasks.__path__):
        if element.ispkg:
            for subdir_element in pkgutil.iter_modules([tasks.__path__._path[0] + f"//{element.name}"]):
                tasks_parser.append(tasks_path_name(element.name, subdir_element.name))
        else:
            tasks_parser.append(tasks_path_name(None, element.name))

    for task in tasks_parser:
        subparser.add_parser(task.name.replace("_", "-"))

    subparser.choices['save-aggtrades'].add_argument(f"--start-ts", type=int, required=True, help="start timestamp to get binance API aggtrades.")
    subparser.choices['save-aggtrades'].add_argument(f"--end-ts", type=int, required=True, help="end timestamp to get binance API aggtrades.")

    subparser.choices['transform-trade-data'].add_argument(f"--chart-minutes", type=int, required=False, help="Volume percentile data chart timeframe.")
    subparser.choices['transform-trade-data'].add_argument(f"--multithread-start-end-timeframe", type=int, nargs=2, required=True,
                                                           help="Run trade data with given start/end timestamp (timestamp in ms) so it "
                                                                "can be ran by multiple threads to speed up execution.")

    #TODO: make this 'timeframe-in-minutes' dynamic.
    subparser.choices['simple-moving-average'].add_argument(f"--timeframe-in-minutes", type=int, required=True,
                                                            help="Simple moving average timeframe, in minutes.")
    subparser.choices['relative-volume'].add_argument(f"--timeframe-in-minutes", type=int, required=True,
                                                      help="Relative volume timeframe, in minutes.")

    parsed_args = vars(parent_parser.parse_args())

    if not parsed_args['command']:
        return

    base_execute_module_name = parsed_args['command'].replace("-", "_")
    for task in tasks_parser:
        if base_execute_module_name == task.name:
            execute_module_path = f"tasks.{task.root_dir_name}.{base_execute_module_name}" if task.root_dir_name else \
                f"tasks.{base_execute_module_name}"

            split_task_path = execute_module_path.split('.')
            base_module_obj = getattr(__import__(execute_module_path), split_task_path[1])
            module_objects_vars = vars(getattr(base_module_obj, split_task_path[-1])) if len(split_task_path) == 3 else vars(base_module_obj)

            execute_module_functions = [obj for obj in module_objects_vars.values() if
                                        isfunction(obj) and obj.__module__ == execute_module_path]

            execute_functions = []
            for function in execute_module_functions:
                execute_functions.append((function, parsed_args) if inspect.getfullargspec(function).args else (function, None))

            return execute_functions
