import logging
from abc import abstractmethod, ABC
from dataclasses import field, dataclass
from datetime import datetime
import logs
from MongoDB.db_actions import BASE_TRADES_CHART_DB, DB, TradesChartValidatorDB, ValidatorDB, DBCol
from data_handling.data_helpers.vars_constants import TS
from support.decorators_extenders import init_only_existing
from support.generic_helpers import mins_to_ms, get_key_values_from_dict_with_dicts

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class UninitializedTradesChart(Exception): pass
class NoTradesToParse(Exception): pass
class InvalidParameterType(Exception): pass


@init_only_existing
@dataclass
class TechnicalIndicator(ABC):
    technical_indicator_data: 'TechnicalIndicatorDBData'
    metric_db_name: str
    timeframe_in_minutes: int = field(init=False)
    metric_name: str = field(init=False)
    atomicity: int = field(init=False)
    range: int = field(init=False)
    start_ts_plus_range: int = field(init=False)
    end_ts: int = field(init=False)
    symbol_based_chart_db_conn: DB = field(init=False)
    metric_db_conn: DB = field(init=False)
    _metric_validator_db_conn: ValidatorDB = field(init=False)

    def __post_init__(self):
        from MongoDB.db_actions import TechnicalIndicatorDBData

        if not isinstance(self.technical_indicator_data, TechnicalIndicatorDBData):
            raise InvalidParameterType(f"parameter {self.db_mapper} should be of type {type(TechnicalIndicatorDBData)}")

        self.timeframe_in_minutes = self.technical_indicator_data.trade_chart_timeframe.value
        self.metric_name = self.technical_indicator_data.metric_name
        self.atomicity = self.technical_indicator_data.atomicity
        self.range = self.technical_indicator_data.range_in_seconds
        self._metric_validator_db_conn = ValidatorDB(self.metric_db_name)
        self.metric_db_conn = DB(self.metric_db_name)
        self.symbol_based_chart_db_conn = DB(BASE_TRADES_CHART_DB.format(self.timeframe_in_minutes))
        self.end_ts = TradesChartValidatorDB(self.timeframe_in_minutes).finish_ts

        if not self.end_ts:  # TradesChartValidatorDB start_ts is initialized when end_ts is, only need to check one.
            LOG.error("This indicator depends on valid start and end timestamp from trades chart DB.")
            raise UninitializedTradesChart("This indicator depends on valid start and end timestamp from trades chart DB.")

        if ValidatorDB(self.metric_db_name).finish_ts:
            self.start_ts_plus_range = ValidatorDB(self.metric_db_name).finish_ts
        else:
            self.start_ts_plus_range = TradesChartValidatorDB(self.timeframe_in_minutes).start_ts + self.range

        if self.end_ts < self.start_ts_plus_range:
            LOG.error("End timestamp attribute is before start timestamp attribute, this means there are no trades left to parse.")
            raise NoTradesToParse("End timestamp attribute is before start timestamp attribute, this means there are no trades left to parse.")

    @abstractmethod
    async def metric_logic(self, *args, **kwargs):
        raise NotImplemented()

    def parse_metric(self, timeframe_based: bool):
        range_total = [*range(self.start_ts_plus_range, self.end_ts + 1, self.atomicity)]
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

            LOG.info(f"Starting to parse metric {self.metric_name} for timeframe {self.timeframe_in_minutes} with start date of "
                     f"{datetime.fromtimestamp(start_ts / 1000)} and end date of {datetime.fromtimestamp(end_ts / 1000)}")

            if timeframe_based:
                for tf in partial_range:
                    symbol_metric_values = {}
                    for symbol in self.symbol_based_chart_db_conn.list_collection_names():
                        symbol_db_conn = DBCol(self.symbol_based_chart_db_conn, symbol)
                        symbol_metric_values[symbol] = self.metric_logic(symbol_db_conn, tf)
                    getattr(self.metric_db_conn, self.metric_name).insert_one({TS: tf, self.metric_name: symbol_metric_values})
            else:  # Symbol based
                for symbol in self.symbol_based_chart_db_conn.list_collection_names():
                    symbol_metric_values = []
                    symbol_db_conn = DBCol(self.symbol_based_chart_db_conn, symbol)
                    for tf in partial_range:
                        symbol_metric_values.append({TS: tf, self.metric_name: self.metric_logic(symbol_db_conn, tf)})
                    getattr(self.metric_db_conn, symbol).insert_many(symbol_metric_values)

            self._metric_validator_db_conn.set_finish_ts(end_ts + self.atomicity)

            LOG.info(f"Parsed metric {self.metric_name} for timeframe {self.timeframe_in_minutes} with start date of "
                     f"{datetime.fromtimestamp(start_ts / 1000)} and end date of {datetime.fromtimestamp(end_ts / 1000)}")

            range_counter += parse_at_a_time_rate


@dataclass
class RelativeVolume(TechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp):
        volume_tfs = {tf_data['end_ts']: tf_data['total_volume'] for tf_data in db_col_conn.find_timeseries(
            range(timestamp - mins_to_ms(self.timeframe_in_minutes * 30), timestamp,  mins_to_ms(self.timeframe_in_minutes)))}
        aggregate_tf_volumes = [volume for volume in volume_tfs.values()]

        def past_relative_volume(tickers_count: int):
            partial_days_volume_values = [value for value in aggregate_tf_volumes[-tickers_count:]]
            return sum(partial_days_volume_values) / len(partial_days_volume_values)

        return db_col_conn.find_timeseries(timestamp)['total_volume'] / ((past_relative_volume(5) + past_relative_volume(15) + past_relative_volume(30)) / 3)


@dataclass
class TotalVolume(TechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp):
        return db_col_conn.find_timeseries(timestamp)['total_volume']


class MetricDistribution(TechnicalIndicator):
    def metric_logic(self, db_col_conn, timestamp):
        distribution_values = {}
        for value in db_col_conn.find_timeseries(
                range(self.start_ts_plus_range - self.added_range, self.start_ts_plus_range, mins_to_ms(0.5))):
            metric_value = get_key_values_from_dict_with_dicts(value, self.metric_name)
            metric_value = str(round(metric_value) if isinstance(metric_value, float) else 0)

            try:
                distribution_values[metric_value] += 1
            except KeyError:
                distribution_values[metric_value] = 1

        return distribution_values

