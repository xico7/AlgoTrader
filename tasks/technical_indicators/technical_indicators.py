import logging
from abc import abstractmethod, ABC
from dataclasses import field, dataclass
from datetime import datetime
import logs
from MongoDB.db_actions import DB, ValidatorDB, DBCol
from data_handling.data_helpers.vars_constants import TS
from support.decorators_extenders import init_only_existing
from support.generic_helpers import mins_to_ms, get_key_values_from_dict_with_dicts, seconds_to_ms

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass
class NoTradesToParse(Exception): pass
class InvalidParameterType(Exception): pass


@init_only_existing
@dataclass
class TechnicalIndicator(ABC):
    technical_indicator_data: 'TechnicalIndicatorDBData'
    metric_db_name: str
    metric_name: str = field(init=False)
    atomicity: int = field(init=False)
    range: int = field(init=False)
    range_granularity: int = field(init=False)
    start_ts_plus_range: int = field(init=False)
    end_ts: int = field(init=False)
    metric_target_db_conn: DB = field(init=False)
    metric_db_conn: DB = field(init=False)
    _metric_validator_db_conn: ValidatorDB = field(init=False)

    def __post_init__(self):
        from MongoDB.db_actions import TechnicalIndicatorDBData

        if not isinstance(self.technical_indicator_data, TechnicalIndicatorDBData):
            raise InvalidParameterType(f"parameter {self.technical_indicator_data} should be of type {type(TechnicalIndicatorDBData)}")

        self.metric_name = self.technical_indicator_data.metric_name
        self.atomicity_in_ms = seconds_to_ms(self.technical_indicator_data.atomicity_in_seconds)
        self.range_in_ms = mins_to_ms(self.technical_indicator_data.range_in_minutes)
        self.range_granularity = self.technical_indicator_data.range_in_ms_quotient_interval
        self._metric_validator_db_conn = ValidatorDB(self.metric_db_name)
        self.metric_db_conn = DB(self.metric_db_name)
        self.metric_target_db_conn = DB(self.technical_indicator_data.metric_target_db_name)
        self.end_ts = ValidatorDB(self.metric_target_db_conn.db_name).finish_ts

        if not self.end_ts:  # TradesChartValidatorDB start_ts is initialized when end_ts is, only need to check one.
            LOG.error("This indicator depends on valid start and end timestamp from trades chart DB.")
            raise UninitializedTradesChart("This indicator depends on valid start and end timestamp from trades chart DB.")

        if ValidatorDB(self.metric_db_name).finish_ts:
            self.start_ts_plus_range = ValidatorDB(self.metric_db_name).finish_ts
        else:
            self.start_ts_plus_range = ValidatorDB(self.metric_target_db_conn.db_name).start_ts + self.range_in_ms

        if self.end_ts < self.start_ts_plus_range:
            error_msg = f"End timestamp attribute is before start timestamp attribute, this means there are no trades " \
                        f"left to parse for metric {self.metric_name} in db {self.technical_indicator_data.metric_target_db_name}."
            LOG.error(error_msg)
            raise NoTradesToParse(error_msg)

    @abstractmethod
    async def metric_logic(self, *args, **kwargs):
        raise NotImplemented()

    def parse_metric(self, timeframe_based: bool):
        range_total = [*range(self.start_ts_plus_range, self.end_ts + 1, self.atomicity_in_ms)]
        range_counter = 0
        parse_at_a_time_rate = 30

        while True:
            if not (partial_range := range_total[range_counter: range_counter + parse_at_a_time_rate]):
                LOG.info("No more trades to parse, exiting.")
                exit(0)
            start_ts, end_ts = partial_range[0], partial_range[-1]

            if not self._metric_validator_db_conn.start_ts:
                self._metric_validator_db_conn.set_start_ts(start_ts)

            self.metric_db_conn.clear_collections_between(start_ts, end_ts)

            log_message = f"metric {self.metric_db_conn.db_name} with start date of {datetime.fromtimestamp(start_ts / 1000)} " \
                          f"and end date of {datetime.fromtimestamp(end_ts / 1000)}"
            LOG.info(f"Starting to parse {log_message}")

            if timeframe_based:
                for tf in partial_range:
                    symbol_metric_values = {}
                    for symbol in self.metric_target_db_conn.list_collection_names():
                        symbol_metric_values[symbol] = self.metric_logic(DBCol(self.metric_target_db_conn, symbol), tf)

                    getattr(self.metric_db_conn, self.metric_name).insert_one({TS: tf, self.metric_name: symbol_metric_values})
            else:  # Symbol based
                for symbol in self.metric_target_db_conn.list_collection_names():
                    symbol_metric_values = []
                    for tf in partial_range:
                        symbol_metric_values.append({TS: tf, self.metric_name: self.metric_logic(DBCol(
                            self.metric_target_db_conn, symbol), tf, int(self.range_in_ms / self.range_granularity))})
                    getattr(self.metric_db_conn, symbol).insert_many(symbol_metric_values)

            self._metric_validator_db_conn.set_finish_ts(end_ts + self.atomicity_in_ms)
            range_counter += parse_at_a_time_rate
            LOG.info(f"Parsed {log_message}.")


@dataclass
class RelativeVolume(TechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp, range_granularity):
        aggregate_tf_volumes = [tf_data['total_volume'] for tf_data in db_col_conn.find_timeseries(
            range(timestamp - self.range_in_ms, timestamp,  range_granularity))]

        def past_relative_volume(tickers_count: int):
            return sum(aggregate_tf_volumes[-tickers_count:]) / len(aggregate_tf_volumes[-tickers_count:])

        return db_col_conn.find_timeseries(timestamp)['total_volume'] / ((past_relative_volume(5) + past_relative_volume(15) + past_relative_volume(30)) / 3)


@dataclass
class TotalVolume(TechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp):
        return db_col_conn.find_timeseries(timestamp)['total_volume']


class MetricDistribution(TechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp, range_granularity):
        distribution_values = {}
        for value in db_col_conn.find_timeseries(range(timestamp - self.range_in_ms, timestamp, range_granularity)):
            metric_value = get_key_values_from_dict_with_dicts(value, self.metric_name)
            metric_value = str(round(metric_value) if isinstance(metric_value, float) else 0)
            try:
                distribution_values[metric_value] += 1
            except KeyError:
                distribution_values[metric_value] = 1

        return distribution_values

