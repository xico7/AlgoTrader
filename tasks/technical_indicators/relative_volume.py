import logging
from dataclasses import dataclass
import logs
from MongoDB.db_actions import TradesChartValidatorDB
from support.generic_helpers import mins_to_ms
from tasks.technical_indicators.base_technical_indicator import TradesChartTechnicalIndicator

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass
class MoreParsedDataNecessary(Exception): pass


@dataclass
class RelativeVolume(TradesChartTechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp):
        volume_tfs = {tf_data['end_ts']: tf_data['total_volume'] for tf_data in db_col_conn.find_timeseries(
            range(timestamp - mins_to_ms(self.timeframe_in_minutes * 30), timestamp,  mins_to_ms(self.timeframe_in_minutes)))}
        aggregate_tf_volumes = [volume for volume in volume_tfs.values()]

        def past_relative_volume(tickers_count: int):
            partial_days_volume_values = [value for value in aggregate_tf_volumes[-tickers_count:]]
            return sum(partial_days_volume_values) / len(partial_days_volume_values)

        return db_col_conn.find_timeseries(timestamp)['total_volume'] / ((past_relative_volume(5) + past_relative_volume(15) + past_relative_volume(30)) / 3)


def relative_volume(args):
    rel_vol_ta = RelativeVolume(
        mins_to_ms(1),
        args['timeframe_in_minutes'],
        args['command'].replace('-', '_'),
        TradesChartValidatorDB(args['timeframe_in_minutes']).start_ts + mins_to_ms(args['timeframe_in_minutes'] * 30))

    rel_vol_ta.parse_metric(timeframe_based=False)
