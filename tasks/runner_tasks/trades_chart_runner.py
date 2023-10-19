import logging
import threading
import time
import logs
from MongoDB.db_actions import ValidatorDB, InvalidStartTimestamp, TradesChartValidatorDB, TradesChartTimeframes
from data_handling.data_helpers.vars_constants import TRADES_CHART_DB, TEN_SECONDS_IN_MS, \
    TEN_SECS_PARSED_TRADES_DB, ONE_DAY_IN_MS
from support.generic_helpers import mins_to_ms
from support.threading_helpers import run_algotrader_process


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def create_run_timeframe_chart_threads(number_of_threads: int = 5):
    trade_chart_db_conn = ValidatorDB(TRADES_CHART_DB)
    threads = []

    if begin_ts := trade_chart_db_conn.finish_ts:
        pass
    elif not (begin_ts := (ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts + mins_to_ms(timeframe))):
        LOG.error(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
        raise InvalidStartTimestamp(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
    parse_time = int(ONE_DAY_IN_MS / 16)

    timeframes = []
    for _ in range(number_of_threads):
        timeframes.append((begin_ts, begin_ts + parse_time))
        begin_ts += parse_time + TEN_SECONDS_IN_MS

    trade_chart_db_conn.drop_collection(trade_chart_db_conn.done_intervals_ts_collection)
    for start_end_tf in timeframes:
        threads.append(threading.Thread(
            target=run_algotrader_process, args=(
                'transform-trade-data', ['--chart-minutes', f'{timeframe}', '--multithread-start-end-timeframe',
                                         f'{start_end_tf[0]}', f'{start_end_tf[1]}'])))

    for thread in threads:
        thread.start()
        time.sleep(5)

    return threads


def trades_chart_runner(args):
    #TODO: Where is logging telling what we are 'starting' to do??
    #TODO: Aqui estou a fazer um 'mega' refactor.. tirar esta lógica, ter umas 3 threads em que cada uma faz os timeframes todos e tem tipo 2 dias de dados..
    #TradesChartValidatorDB(TradesChartTimeframes.ONE_HOUR.value).set_valid_timestamps()

    threads = create_run_timeframe_chart_threads()

    while True:
        if not any([thread.is_alive() for thread in one_hour_threads]):
            TradesChartValidatorDB(TradesChartTimeframes.ONE_HOUR.value).set_valid_timestamps()
            one_hour_threads = create_run_timeframe_chart_threads(60, args['one_hour_threads_number'])

        time.sleep(20)

