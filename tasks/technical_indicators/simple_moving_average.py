import logging
from dataclasses import dataclass

import logs
from MongoDB.db_actions import TradesChartValidatorDB
from support.generic_helpers import mins_to_ms
from tasks.technical_indicators.technical_indicators import TechnicalIndicator

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass

@dataclass
class SMA(TechnicalIndicator):
    period: int

    def metric_logic(self, timestamp):
        prices = []
        for ts in range(timestamp - mins_to_ms(self.timeframe_in_minutes * self.period), timestamp, mins_to_ms(self.timeframe_in_minutes)):
            prices.append(self.symbol_based_chart_db_conn.find_one({'end_ts': ts})['end_price'])

        return sum(prices) / len(prices)


def simple_moving_average(args):
    sma_ta = SMA(mins_to_ms(1), args['timeframe_in_minutes'],
                 args['command'].replace('-', '_'),
                 mins_to_ms(args['timeframe_in_minutes'] * 10),
                 10)
    sma_ta.parse_metric()

