import logging
import logs
from MongoDB.db_actions import TradesChartValidatorDB
from data_handling.data_helpers.data_staging import mins_to_ms
from tasks.technical_indicators.base_technical_indicator import TradesChartTechnicalIndicator

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass
class MoreParsedDataNecessary(Exception): pass


def relative_volume(args):
    def query_symbol_relative_volume(db_conn, timeframe, timestamp):
        sorted_volume_tfs = dict(sorted(
            {timeframe['start_ts']: timeframe['total_volume'] for timeframe in db_conn.find(
                {"start_ts": {"$in": [*range(timestamp - mins_to_ms(timeframe * 30), timestamp, mins_to_ms(timeframe))]}})}.items()))

        aggregate_tf_volumes = [volume for volume in sorted_volume_tfs.values()]

        def past_relative_volume(tickers_count: int):
            partial_days_volume_values = [value for value in aggregate_tf_volumes[-tickers_count:]]
            return sum(partial_days_volume_values) / len(partial_days_volume_values)

        return db_conn.find_one({'start_ts': timestamp})['total_volume'] / ((past_relative_volume(5) + past_relative_volume(15) + past_relative_volume(30)) / 3)

    rel_vol_ta = TradesChartTechnicalIndicator(mins_to_ms(1), args['timeframe_in_minutes'], args['command'].replace('-', '_'),
                                               TradesChartValidatorDB(args['timeframe_in_minutes']).start_ts + mins_to_ms(args['timeframe_in_minutes'] * 30))

    rel_vol_ta.parse_metric(query_symbol_relative_volume, rel_vol_ta.timeframe_in_minutes)
