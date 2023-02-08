import logging
from dataclasses import field, dataclass
from datetime import datetime
from typing import Optional

import logs
from MongoDB.db_actions import BASE_TRADES_CHART_DB, DB, TradesChartValidatorDB, ValidatorDB
from data_handling.data_helpers.vars_constants import TS

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass
class NoTradesToParse(Exception): pass


@dataclass
class TradesChartTechnicalIndicator:
    metric_granularity: int
    timeframe_in_minutes: int
    metric_name: str
    start_ts: Optional[int]
    end_ts: int = field(init=False)
    trades_chart_db_conn: DB = field(init=False)
    metric_db_conn: DB = field(init=False)
    metric_db_name: str = field(init=False)
    _metric_validator_db_conn: ValidatorDB = field(init=False)

    def __post_init__(self):
        self._parse_at_a_time_rate = 100
        self.metric_db_name = f"{self.metric_name}_{self.timeframe_in_minutes}_minutes"
        self._metric_validator_db_conn = ValidatorDB(self.metric_db_name)
        self.metric_db_conn = DB(self.metric_db_name)

        self.end_ts = TradesChartValidatorDB(self.timeframe_in_minutes).finish_ts
        self.start_ts = ValidatorDB(self.metric_db_name).finish_ts if ValidatorDB(self.metric_db_name).finish_ts else self.start_ts

        if not self.start_ts or not self.end_ts:
            LOG.error("This indicator depends on valid start and end timestamp from trades chart DB.")
            raise UninitializedTradesChart("This indicator depends on valid start and end timestamp from trades chart DB.")

        if self.end_ts < self.start_ts:
            LOG.error("End timestamp attribute is before start timestamp attribute, this means there are no trades left to parse.")
            raise NoTradesToParse("End timestamp attribute is before start timestamp attribute, this means there are no trades left to parse.")

        self.trades_chart_db_conn = DB(BASE_TRADES_CHART_DB.format(self.timeframe_in_minutes))

    def parse_metric(self, metric_data, metric_args):
        parse_at_a_time_rate = 100

        range_total = [*range(self.start_ts, self.end_ts + 1, self.metric_granularity)]
        range_counter = 0
        while range_counter + parse_at_a_time_rate <= len(range_total):
            partial_range = range_total[range_counter: range_counter + parse_at_a_time_rate]
            start_ts, end_ts = partial_range[0], partial_range[-1]

            if not self._metric_validator_db_conn.start_ts:
                self._metric_validator_db_conn.set_start_ts(start_ts)

            self.metric_db_conn.clear_collections_between(start_ts, end_ts)

            LOG.info(f"Starting to parse metric {self.metric_name} for timeframe {self.timeframe_in_minutes} with start date of "
                     f"{datetime.fromtimestamp(start_ts / 1000)} and end date of {datetime.fromtimestamp(end_ts / 1000)}")

            for symbol in self.trades_chart_db_conn.list_collection_names():
                symbol_metric_values = []
                for tf in partial_range:
                    symbol_metric_values.append({TS: tf, self.metric_name: metric_data(getattr(self.trades_chart_db_conn, symbol), *[metric_args, tf])})
                getattr(self.metric_db_conn, symbol).insert_many(symbol_metric_values)

            self._metric_validator_db_conn.set_finish_ts(end_ts)

            LOG.info(f"Parsed metric {self.metric_name} for timeframe {self.timeframe_in_minutes} with start date of "
                     f"{datetime.fromtimestamp(start_ts / 1000)} and end date of {datetime.fromtimestamp(end_ts / 1000)}")

            range_counter += parse_at_a_time_rate

        LOG.info(f"Finished parsing relative volume for timeframe {self.timeframe_in_minutes}.")
        exit(0)


