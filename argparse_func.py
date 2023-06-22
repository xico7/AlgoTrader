import argparse
import enum
import inspect
from collections import namedtuple
from inspect import isfunction
import pkgutil
import logging
import logs
import tasks
from MongoDB.db_actions import TradesChartTimeframes

logs.setup_logs(verbosity=[logging.INFO, logging.INFO - 5, logging.DEBUG, logging.VERBOSE][:4 + 1][-1])
LOG = logging.getLogger(logs.LOG_BASE_NAME + '.main')


class InvalidArgumentsProvided(Exception): pass


class EnumAction(argparse.Action):
    """
    Argparse action for handling Enums
    """
    def __init__(self, **kwargs):
        # Pop off the type value
        enum_type = kwargs.pop("type", None)

        # Ensure an Enum subclass is provided
        if enum_type is None:
            raise ValueError("type must be assigned an Enum when using EnumAction")
        if not issubclass(enum_type, enum.Enum):
            raise TypeError("type must be an Enum when using EnumAction")

        # Generate choices from the Enum
        kwargs.setdefault("choices", tuple(e.value for e in enum_type))

        super(EnumAction, self).__init__(**kwargs)

        self._enum = enum_type

    def __call__(self, parser, namespace, values, option_string=None):
        # Convert value back into an Enum
        value = self._enum(values)
        setattr(namespace, self.dest, value)


def get_argparse_execute_functions():
    parent_parser = argparse.ArgumentParser(prog='Algotrading-Crypto', fromfile_prefix_chars='@')
    parent_parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")

    subparser = parent_parser.add_subparsers(dest="command")

    tasks_parser = []
    tasks_path_name = namedtuple('tasks_path_name', ['root_dir_name', 'name',])
    for element in pkgutil.iter_modules(tasks.__path__):
        if element.ispkg:
            for subdir_element in pkgutil.iter_modules([tasks.__path__[0] + f"//{element.name}"]):
                tasks_parser.append(tasks_path_name(element.name, subdir_element.name))
        else:
            tasks_parser.append(tasks_path_name(None, element.name))

    for task in tasks_parser:
        subparser.add_parser(task.name.replace("_", "-"))

    subparser.choices['save-aggtrades'].add_argument("--start-ts", type=int, required=True, help="start timestamp to get binance API aggtrades.")
    subparser.choices['save-aggtrades'].add_argument("--end-ts", type=int, required=True, help="end timestamp to get binance API aggtrades.")
    subparser.choices['transform-trade-data'].add_argument("--chart-minutes", type=int, required=False, help="Volume percentile data chart timeframe.")
    subparser.choices['transform-trade-data'].add_argument("--multithread-start-end-timeframe", type=int, nargs=2, required=True,
                                                           help="Run trade data with given start/end timestamp (timestamp in ms) so it "
                                                                "can be ran by multiple threads to speed up execution.")
    subparser.choices['simple-moving-average'].add_argument("--timeframe-in-minutes", type=int, required=True,
                                                            help="Simple moving average timeframe, in minutes.")
    subparser.choices['relative-volume'].add_argument("--timeframe-in-minutes", type=int, required=True,
                                                      help="Relative volume timeframe, in minutes.")
    subparser.choices['total-volume'].add_argument("--timeframe-in-minutes", type=int, required=True,
                                                      help="Relative volume timeframe, in minutes.")
    subparser.choices['aggtrades-runner'].add_argument("--threads-number", type=int, choices=range(1, 4), default=3,
                                                            help="Number of threads to run on binance API, max 3.")
    ONE_HOUR_CHART_THREADS = 8
    TWO_HOURS_CHART_THREADS = 8
    FOUR_HOURS_CHART_THREADS = 8
    EIGHT_HOURS_CHART_THREADS = 8
    ONE_DAY_CHART_THREADS = 5
    TWO_DAYS_CHART_THREADS = 6
    FOUR_DAYS_CHART_THREADS = 8
    EIGHT_DAYS_CHART_THREADS = 4

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
    subparser.choices['parse-aggtrades-runner']
    subparser.choices['relative-volume-runner']
    subparser.choices['get-trades-chart-metric-distribution'].add_argument("--trades-chart-timeframe", type=int,
                                                              choices=[timeframe.value for timeframe in TradesChartTimeframes],
                                                              help="Trades chart timeframe.")
    subparser.choices['get-trades-chart-metric-distribution'].add_argument("--symbol", type=str, required=True,
                                                            help="Symbol to get distribution overview.")
    subparser.choices['get-trades-chart-metric-distribution'].add_argument("--trades-chart-metric-name", type=str, required=True,
                                                            help="metric name to parse.")
    subparser.choices['get-trades-chart-metric-distribution'].add_argument("--trades-chart-decimal-places", type=int, required=True,
                                                            help="decimal places for metric results.")

    parsed_args = vars(parent_parser.parse_args())

    if not parsed_args['command']:
        raise InvalidArgumentsProvided("No running arguments were provided, please run the help command to see program usage.")

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
                if function.__name__ == base_execute_module_name:
                    execute_functions.append((function, parsed_args) if inspect.getfullargspec(function).args else (function, None))

            return execute_functions
