import logging
import time
import logs
from MongoDB.db_actions import TradesChartValidatorDB
from support.threading_helpers import create_run_timeframe_chart_threads

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


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

