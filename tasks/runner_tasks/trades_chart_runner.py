import logging
import threading
import time
import logs
from MongoDB.db_actions import ValidatorDB, InvalidStartTimestamp, TradesChartValidatorDB, TradesChartTimeframes, InvalidFinishTimestamp
from support.data_handling.data_helpers.vars_constants import TRADES_CHART_DB, TEN_SECS_PARSED_TRADES_DB, ONE_DAY_IN_MS, TEN_SECONDS_IN_MS
from support.threading_helpers import run_algotrader_process
from support.generic_helpers import mins_to_ms

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def create_run_timeframe_chart_threads(number_of_threads: int = 3):
    parse_time = int(ONE_DAY_IN_MS * 2)

    if not ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts:
        LOG.error(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
        raise InvalidStartTimestamp(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")

    if begin_ts := ValidatorDB(TRADES_CHART_DB).finish_ts:
        pass
    else:
        LOG.info(f"Settings trades chart start ts to '{TEN_SECS_PARSED_TRADES_DB}' "
                 f"start timestamp, if this is not your first run this is an error.")
        # Add timeframe max value in order to have past values
        past_values_buffer = mins_to_ms(max([timeframes.value for timeframes in TradesChartTimeframes]))
        begin_ts = ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts + past_values_buffer

    if ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts < begin_ts + parse_time * number_of_threads:
        LOG.error(f"Trades from {TEN_SECS_PARSED_TRADES_DB} are still not parsed until the necessary timeframe.")
        raise InvalidFinishTimestamp(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")

    threads = []

    timeframes = []
    for _ in range(number_of_threads):
        timeframes.append((begin_ts, begin_ts + parse_time))
        begin_ts += parse_time + TEN_SECONDS_IN_MS

    for start_end_tf in timeframes:
        threads.append(threading.Thread(
            target=run_algotrader_process, args=(
                'transform-trade-data', ['--start-end-timeframe', f'{start_end_tf[0]}', f'{start_end_tf[1]}'])))

    for thread in threads:
        thread.start()
        time.sleep(5)

    return threads


def trades_chart_runner(args):
    def run_chart_threads():
        TradesChartValidatorDB(None).set_global_timeframes_valid_timestamps()
        return create_run_timeframe_chart_threads(args['threads_number'])

    LOG.info(f"Starting to parse trades chart runner with '{args['threads_number']}' threads.")
    threads = run_chart_threads()

    while True:
        if not any([thread.is_alive() for thread in threads]):
            threads = run_chart_threads()
            time.sleep(60)

        time.sleep(60)

