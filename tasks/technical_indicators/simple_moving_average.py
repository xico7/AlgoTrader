import logging
from dataclasses import dataclass

import logs
from MongoDB.db_actions import TradesChartValidatorDB
from data_handling.data_helpers.data_staging import mins_to_ms
from tasks.technical_indicators.base_technical_indicator import TradesChartTechnicalIndicator

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass

@dataclass
class SMA(TradesChartTechnicalIndicator):
    period: int

    def metric_logic(self, timestamp):
        prices = []
        for ts in range(timestamp - mins_to_ms(self.timeframe_in_minutes * self.period), timestamp, mins_to_ms(self.timeframe_in_minutes)):
            prices.append(self.trades_chart_db_conn.find_one({'end_ts': ts})['end_price'])

        return sum(prices) / len(prices)


def simple_moving_average(args):
    sma_ta = SMA(mins_to_ms(1), args['timeframe_in_minutes'], args['command'].replace('-', '_'),
                 TradesChartValidatorDB(args['timeframe_in_minutes']).start_ts + mins_to_ms(args['timeframe_in_minutes'] * 10), 10)
    sma_ta.parse_metric()

