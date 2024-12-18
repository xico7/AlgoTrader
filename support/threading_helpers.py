import os
import subprocess
import sys
import threading
import time
from datetime import timedelta, datetime

from MongoDB.db_actions import ValidatorDB, InvalidStartTimestamp, TradesChartTimeframes, InvalidFinishTimestamp, \
    TRADES_CHART_TIMEFRAMES_VALUES, DB
from argparse_func import TRANSFORM_TRADE_DATA_THREAD_NAME, METRICS_PARSER_THREAD_NAME
from support.data_handling.data_helpers.vars_constants import TRADES_CHART_DB, TEN_SECS_PARSED_TRADES_DB, \
    ONE_DAY_IN_MINUTES, DEFAULT_PARSE_INTERVAL_TIMEDELTA, BASE_TRADES_CHART_DB
from support.data_handling.data_helpers.vars_constants import PACKAGED_PROGRAM_NAME
import logging
import logs

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


# Sometimes mongodb's port is being used by other process, replace that process with MongoDB
def run_mongodb_startup_process():
    if os.name == 'nt':
        subprocess_output = subprocess.run(['powershell.exe', 'Get-Service MongoDB'], capture_output=True, shell=True)
        if not 'Running' in subprocess_output.stdout.decode('utf-8'):
            subprocess.run(['powershell.exe', './ForceStartMongoDBWindows.ps1'], shell=True)
            time.sleep(20)


def run_algotrader_process(task_to_run, task_args=[]):
    use_shell = False
    if os.name == 'nt':
        use_shell = True
    subprocess.call([sys.executable, PACKAGED_PROGRAM_NAME, task_to_run] + task_args, shell=use_shell)


def create_run_threads(thread_number, begin_ts, parse_time, algotrader_process_name, algotrader_process_args=[]):
    threads = []
    timeframes = []

    for _ in range(thread_number):
        timeframes.append((begin_ts, begin_ts + parse_time))
        begin_ts += parse_time + DEFAULT_PARSE_INTERVAL_TIMEDELTA

    for start_end_tf in timeframes:
        threads.append(threading.Thread(
            target=run_algotrader_process, args=(
                algotrader_process_name,
                algotrader_process_args + ['--start-end-timeframe', f'{int(datetime.timestamp(start_end_tf[0]) * 1000)}', f'{int(datetime.timestamp(start_end_tf[1]) * 1000)}']
            )
        ))

    for thread in threads:
        thread.start()
        time.sleep(30)

    return threads


def create_run_timeframe_chart_threads(thread_number: int = 2):
    PARSE_TIME = int(ONE_DAY_IN_MINUTES * 1)
    parse_time_minutes = timedelta(minutes=PARSE_TIME)

    if not ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts:
        err_msg = f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry."
        LOG.error(err_msg)
        raise InvalidStartTimestamp(err_msg)

    if begin_ts := ValidatorDB(TRADES_CHART_DB).finish_ts:
        pass
    else:
        LOG.info(f"Settings trades chart start ts to '{TEN_SECS_PARSED_TRADES_DB}' "
                 f"start timestamp, if this is not your first run this is an error.")
        # Add timeframe max value in order to have past values
        begin_ts = ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts + timedelta(minutes=max([timeframes.value for timeframes in TradesChartTimeframes]))

    if ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts < begin_ts + parse_time_minutes * thread_number:
        err_msg = f"Trades from {TEN_SECS_PARSED_TRADES_DB} are still not parsed until the necessary timeframe."
        LOG.error(err_msg)
        raise InvalidFinishTimestamp(err_msg)

    LOG.info("Deleting leftover trade data from previous stopped runs, this process can take a long time.")
    for timeframe in TRADES_CHART_TIMEFRAMES_VALUES:
        DB(BASE_TRADES_CHART_DB.format(timeframe)).clear_collections_between(begin_ts, begin_ts + parse_time_minutes)
    LOG.info("Finished deleting leftover trade data from previous stopped runs.")

    return create_run_threads(thread_number, begin_ts, parse_time_minutes, TRANSFORM_TRADE_DATA_THREAD_NAME)


def create_run_metrics_parser_threads(threads_number, parse_time, metric_db_mapper_name, begin_ts):
    return create_run_threads(threads_number, begin_ts, parse_time, METRICS_PARSER_THREAD_NAME, ['--metric-db-mapper-name', metric_db_mapper_name])