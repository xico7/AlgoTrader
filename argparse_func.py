import argparse
import inspect
from collections import namedtuple
from inspect import isfunction
import pkgutil
import logging
import logs
import tasks


logs.setup_logs(verbosity=[logging.INFO, logging.INFO - 5, logging.DEBUG, logging.VERBOSE][:4 + 1][-1])
LOG = logging.getLogger(logs.LOG_BASE_NAME + '.main')

RUN_DEFAULT_ARG = 'run-default'


class InvalidArgumentsProvided(Exception): pass


def add_tasks_subparsers(parent_parser, tasks):
    subparser = parent_parser.add_subparsers(dest="command")

    for task in tasks:
        subparser.add_parser(task.name.replace("_", "-"))

    subparser.add_parser(RUN_DEFAULT_ARG)
    subparser.choices['save-aggtrades'].add_argument("--start-ts", type=int, required=True, help="start timestamp to get binance API aggtrades.")
    subparser.choices['save-aggtrades'].add_argument("--end-ts", type=int, required=True, help="end timestamp to get binance API aggtrades.")

    subparser.choices['transform-trade-data'].add_argument(
        "--start-end-timeframe", type=int, nargs=2, required=True,
        help="Run trade data with given start/end timestamp (timestamp in ms) "
             "so it can be ran by multiple threads to speed up execution.")

    subparser.choices['metrics-parser'].add_argument("--metric-db-mapper-name", type=str, help="metric from 'DBMapper' to parse.")

    subparser.choices['aggtrades-runner'].add_argument("--threads-number", type=int, choices=range(1, 4), default=2,
                                                            help="Number of threads to run on binance API, max 3.")

    # Trades chart options.
    subparser.choices['trades-chart-runner'].add_argument(
        "--threads-number", type=int, default=4,
        help="Number of trades chart threads to run, each threads needs around 25GB of RAM.")

    subparser.choices['technical-indicators-runner'].add_argument("--helper-text", help="run multiple program processes that parse the TA indicators.")
    subparser.choices['run-default'].add_argument(
        "--helper-text", help="run multiple program processes with binance aggtrades, "
                              "parse the aggtrades and transform them in a trades chart")


def get_argparse_execute_functions():
    parent_parser = argparse.ArgumentParser(prog='Algotrading-Crypto', fromfile_prefix_chars='@')
    parent_parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")

    tasks_parser = []
    tasks_path_name = namedtuple('tasks_path_name', ['root_dir_name', 'name',])
    for element in pkgutil.iter_modules(tasks.__path__):
        if element.ispkg:
            for subdir_element in pkgutil.iter_modules([tasks.__path__[0] + f"//{element.name}"]):
                tasks_parser.append(tasks_path_name(element.name, subdir_element.name))
        else:
            tasks_parser.append(tasks_path_name(None, element.name))

    add_tasks_subparsers(parent_parser, tasks_parser)

    parsed_args = vars(parent_parser.parse_args())

    if not parsed_args['command']:
        LOG.error("No running arguments were provided, please run the help command to see program usage.")
        raise InvalidArgumentsProvided("No running arguments were provided, please run the help command to see program usage.")

    def get_exec_func_with_args(parsed_args_command) -> list:
        for task in tasks_parser:
            base_execute_module_name = parsed_args_command.replace("-", "_")
            if base_execute_module_name == task.name:
                execute_module_path = f"tasks.{task.root_dir_name}.{base_execute_module_name}" if task.root_dir_name else \
                    f"tasks.{base_execute_module_name}"

                split_task_path = execute_module_path.split('.')
                base_module_obj = getattr(__import__(execute_module_path), split_task_path[1])
                module_objects_vars = vars(getattr(base_module_obj, split_task_path[-1])) if len(split_task_path) == 3 else vars(base_module_obj)

                execute_module_functions = [obj for obj in module_objects_vars.values() if
                                            isfunction(obj) and obj.__module__ == execute_module_path]

                for function in execute_module_functions:
                    if function.__name__ == base_execute_module_name:
                        return [(function, parsed_args)] if inspect.getfullargspec(function).args else [(function, None)]

    return [(RUN_DEFAULT_ARG, None)] if parsed_args['command'] == RUN_DEFAULT_ARG else get_exec_func_with_args(parsed_args['command'])
