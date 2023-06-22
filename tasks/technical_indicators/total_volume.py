import logging
from dataclasses import dataclass
import logs
from MongoDB.db_actions import TradesChartValidatorDB
from support.generic_helpers import mins_to_ms
from tasks.technical_indicators.base_technical_indicator import TradesChartTechnicalIndicator

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


@dataclass
class TotalVolume(TradesChartTechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp):
        return db_col_conn.find_timeseries(timestamp)['total_volume']


def total_volume(args):
    total_volume_ta = TotalVolume(
        mins_to_ms(30),
        args['timeframe_in_minutes'],
        args['command'].replace('-', '_'),
        TradesChartValidatorDB(args['timeframe_in_minutes']).start_ts + mins_to_ms(args['timeframe_in_minutes']))

    total_volume_ta.parse_metric(timeframe_based=True)
