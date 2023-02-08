import logging
import logs
from MongoDB.db_actions import DBCol
from data_handling.data_helpers.data_staging import mins_to_ms
from tasks.technical_indicators.base_technical_indicator import TradesChartTechnicalIndicator

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass


def simple_moving_average(args):
    def parse_symbol_simple_moving_average(db_conn: DBCol, start_ts: int, timeframe: int, period: int):
        prices = []
        for tf in range(start_ts, start_ts + mins_to_ms(timeframe * period), mins_to_ms(timeframe)):
            prices.append(db_conn.find_one({'start_ts': start_ts})['end_price'])

        return sum(prices) / len(prices)

    if start_ts := self._metric_validator_db_conn.finish_ts:
        self.start_ts = start_ts + self.metric_granularity
    else:
        self.start_ts = TradesChartValidatorDB(self.timeframe_in_minutes).start_ts

    sma_ta = TradesChartTechnicalIndicator(start_ts, mins_to_ms(1), args['timeframe_in_minutes'], args['command'].replace('-', '_'))
    sma_ta.parse_metric(parse_symbol_simple_moving_average, [sma_ta.start_ts, sma_ta.timeframe_in_minutes, 10])

