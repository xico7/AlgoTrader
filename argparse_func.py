import argparse
import inspect
from collections import namedtuple
from inspect import isfunction
import pkgutil
import logging
import logs
import tasks
from data_handling.data_helpers.vars_constants import ONE_HOUR_CHART_THREADS, TWO_HOURS_CHART_THREADS, \
    EIGHT_HOURS_CHART_THREADS, FOUR_HOURS_CHART_THREADS, ONE_DAY_CHART_THREADS, TWO_DAYS_CHART_THREADS, \
    FOUR_DAYS_CHART_THREADS, EIGHT_DAYS_CHART_THREADS

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
    subparser.choices['transform-trade-data'].add_argument("--multithread-start-end-timeframe", type=int, nargs=2, required=True,
                                                           help="Run trade data with given start/end timestamp (timestamp in ms) so it "
                                                                "can be ran by multiple threads to speed up execution.")

    subparser.choices['metrics-parser'].add_argument("--metric-db-mapper-name", type=str, help="DB mapper name.")

    subparser.choices['aggtrades-runner'].add_argument("--threads-number", type=int, choices=range(1, 4), default=3,
                                                            help="Number of threads to run on binance API, max 3.")

    # TODO: Finish this refactor, add virtualization to bios settings.


    # Trades chart options.
    subparser.choices['trades-chart-runner'].add_argument("--one-hour-threads-number", type=int, default=ONE_HOUR_CHART_THREADS,
                                                            help="Number of one hour trades chart threads to run.")
    subparser.choices['trades-chart-runner'].add_argument("--two-hours-threads-number", type=int, default=TWO_HOURS_CHART_THREADS,
                                                            help="Number of two hours trades chart threads to run.")
    subparser.choices['trades-chart-runner'].add_argument("--four-hours-threads-number", type=int, default=FOUR_HOURS_CHART_THREADS,
                                                            help="Number of four hours trades chart threads to run.")
    subparser.choices['trades-chart-runner'].add_argument("--eight-hours-threads-number", type=int, default=EIGHT_HOURS_CHART_THREADS,
                                                            help="Number of eight hours trades chart threads to run.")
    subparser.choices['trades-chart-runner'].add_argument("--one-day-threads-number", type=int, default=ONE_DAY_CHART_THREADS,
                                                            help="Number of one day trades chart threads to run.")
    subparser.choices['trades-chart-runner'].add_argument("--two-days-threads-number", type=int, default=TWO_DAYS_CHART_THREADS,
                                                            help="Number of two days trades chart threads to run.")
    subparser.choices['trades-chart-runner'].add_argument("--four-days-threads-number", type=int, default=FOUR_DAYS_CHART_THREADS,
                                                            help="Number of four days trades chart threads to run.")
    subparser.choices['trades-chart-runner'].add_argument("--eight-days-threads-number", type=int, default=EIGHT_DAYS_CHART_THREADS,
                                                            help="Number of eight days trades chart threads to run.")

    subparser.choices['parse-aggtrades-runner'].add_argument("--h", help="run multiple program processes that parse the aggtrades.")
    subparser.choices['technical-indicators-runner'].add_argument("--h", help="run multiple program processes that parse the TA indicators.")
    subparser.choices['run-default'].add_argument("--h", help="run multiple program processes with binance aggtrades, "
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

    execute_functions = []

    def get_tasks_exec_functions(parsed_args_command) -> list:
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
                        execute_functions.append((function, parsed_args) if inspect.getfullargspec(function).args else (function, None))

    if not parsed_args['command'] == 'run-default':
        get_tasks_exec_functions(parsed_args['command'])
    else:
        get_tasks_exec_functions('parse-aggtrades-runner')
        get_tasks_exec_functions('aggtrades-runner')
        get_tasks_exec_functions('trades-chart-runner')

    return execute_functions
