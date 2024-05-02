import time
from MongoDB.db_actions import DBMapper
from support.data_handling.data_helpers.vars_constants import ONE_DAY_IN_MS
from support.threading_helpers import create_run_metrics_parser_threads
import logs
import logging

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


# TODO: o log está a meter o "parsing from" do 'partial range', tem de meter no inicial total ao inicio também.


def metrics_parser(args):
    metric_db_name = args['metric_db_mapper_name']

    metric_ti_class_to_instantiate = getattr(DBMapper, metric_db_name).value.metric_class
    instantiated_metric_class = metric_ti_class_to_instantiate(metric_db_name)

    if args['threads_number'] == 1:
        instantiated_metric_class.metric_validator_db_conn.set_timeframe_valid_timestamps(instantiated_metric_class.atomicity_in_ms)
        if not args['start_end_timeframe']:
            instantiated_metric_class.parse_metric()
        else:
            instantiated_metric_class.start_ts_plus_range = args['start_end_timeframe'][0]
            instantiated_metric_class.end_ts = args['start_end_timeframe'][1]
            instantiated_metric_class.parse_metric()
    else:
        def run_metrics_parser_threads():
            instantiated_metric_class.metric_validator_db_conn.set_timeframe_valid_timestamps(instantiated_metric_class.atomicity_in_ms)
            return create_run_metrics_parser_threads(
                args['threads_number'], int(ONE_DAY_IN_MS / 2), args['metric_db_mapper_name'], instantiated_metric_class.start_ts_plus_range)

        LOG.info(f"Starting to parse metric '{args['metric_db_mapper_name']}' with '{args['threads_number']}' number of threads.")
        threads = run_metrics_parser_threads()

        while True:
            if not any([thread.is_alive() for thread in threads]):
                threads = run_metrics_parser_threads()
                time.sleep(10)

            time.sleep(60)




