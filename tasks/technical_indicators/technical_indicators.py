import logging
from abc import abstractmethod
from dataclasses import field, dataclass
from datetime import datetime

import logs
from support.data_handling.data_helpers.vars_constants import TS
from support.decorators_extenders import init_only_existing
from support.generic_helpers import mins_to_ms, get_key_values_from_dict_with_dicts, seconds_to_ms, ms_to_mins

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)
PARSE_AT_A_TIME_RATE = 200


class UninitializedTradesChart(Exception): pass
class NoTradesToParse(Exception): pass
class InvalidParameterType(Exception): pass
class InvalidClassAttributes(Exception): pass


@init_only_existing
@dataclass
class TechnicalIndicator:
    metric_db_name: str
    metric_name: str = field(init=False)
    atomicity: int = field(init=False)
    range: int = field(init=False)
    range_granularity: int = field(init=False)
    start_ts_plus_range: int = field(init=False)
    end_ts: int = field(init=False)
    metric_target_db_conn: 'DB' = field(init=False)
    metric_db_conn: 'DB' = field(init=False)
    timeframe_based: bool = field(init=False)
    _metric_validator_db_conn: 'ValidatorDB' = field(init=False)

    def __post_init__(self):
        from MongoDB.db_actions import ValidatorDB, DB, TechnicalIndicatorDetails
        from MongoDB.db_actions import DBMapper

        metric_db_mapper_attributes = getattr(DBMapper, self.metric_db_name).value

        if not isinstance(metric_db_mapper_attributes, TechnicalIndicatorDetails):
            raise InvalidParameterType(f"parameter {metric_db_mapper_attributes.value} should be of type {type(TechnicalIndicatorDetails)}")

        self.timeframe_based = metric_db_mapper_attributes.timeframe_based
        self.values_needed_for_metric = metric_db_mapper_attributes.values_needed
        self.atomicity_in_ms = mins_to_ms(metric_db_mapper_attributes.atomicity_in_minutes)
        self.range_in_ms = mins_to_ms(metric_db_mapper_attributes.range_of_one_value_in_minutes)
        self._metric_validator_db_conn = ValidatorDB(self.metric_db_name)
        self.metric_db_conn = DB(self.metric_db_name)
        self.metric_target_db_conn = DB(metric_db_mapper_attributes.metric_target_db_name)
        self.end_ts = ValidatorDB(self.metric_target_db_conn.db_name).finish_ts

        if not self.end_ts:  # TradesChartValidatorDB start_ts is initialized when end_ts is, only need to check one.
            LOG.error("This indicator depends on valid start and end timestamp from trades chart DB.")
            raise UninitializedTradesChart("This indicator depends on valid start and end timestamp from trades chart DB.")

        if ValidatorDB(self.metric_db_name).finish_ts:
            self.start_ts_plus_range = ValidatorDB(self.metric_db_name).finish_ts
        else:
            self.start_ts_plus_range = ValidatorDB(self.metric_target_db_conn.db_name).start_ts + self.range_in_ms

        if self.end_ts < self.start_ts_plus_range:
            error_msg = (f"End timestamp attribute is before start timestamp attribute, this means there are no trades "
                         f"left to parse for metric with DB name '{self.metric_db_name}' in DB {metric_db_mapper_attributes.metric_target_db_name}.")
            LOG.error(error_msg)
            raise NoTradesToParse(error_msg)

    @abstractmethod
    async def metric_logic(self, *args, **kwargs):
        raise NotImplemented()

    def parse_metric(self):
        from MongoDB.db_actions import DBCol

        values_to_parse = [*range(self.start_ts_plus_range, self.end_ts + 1, self.atomicity_in_ms)]
        range_counter = 0

        if not self._metric_validator_db_conn.start_ts:
            self._metric_validator_db_conn.set_start_ts(values_to_parse[0])

        while True:
            if not (partial_range := values_to_parse[range_counter: range_counter + PARSE_AT_A_TIME_RATE]):
                LOG.info("No more trades to parse, exiting.")
                exit(0)

            start_ts, end_ts = partial_range[0], partial_range[-1]
            self.metric_db_conn.clear_collections_between(start_ts, end_ts)

            log_message = f"metric {self.metric_db_conn.db_name} with start date of {datetime.fromtimestamp(start_ts / 1000)} " \
                          f"and end date of {datetime.fromtimestamp(end_ts / 1000)}"
            LOG.info(f"Starting to parse {log_message}")

            timestamps_needed_for_partial_range = {}
            for value in partial_range:
                range_set_values = {*list(range(value - self.range_in_ms, value + 1, int(self.range_in_ms / self.values_needed_for_metric)))}
                if not timestamps_needed_for_partial_range:
                    timestamps_needed_for_partial_range = range_set_values
                else:
                    timestamps_needed_for_partial_range.update(range_set_values)

            symbols = self.metric_target_db_conn.list_collection_names()

            if self.timeframe_based:
                symbols_metric_values = {}
                for symbol in symbols:
                    symbols_metric_values[symbol] = self.metric_logic(
                        {v['end_ts']: v for v in DBCol(self.metric_target_db_conn, symbol).find_timeseries([v for v in timestamps_needed_for_partial_range])},
                        partial_range)

                symbols_timeframe = []
                for timeframe in partial_range:
                    symbols_timeframe.append({TS: timeframe, "metric_values": [{symbol: symbols_metric_values[symbol][timeframe]} for symbol in symbols_metric_values]})
                getattr(self.metric_db_conn, self.metric_db_name).insert_many(symbols_timeframe)
            else:  # Symbol based
                for symbol in symbols:
                    symbol_metric_values = []
                    for tf in partial_range:
                        symbol_metric_values.append({TS: tf, 'metric_value': self.metric_logic(
                            {v['end_ts']: v for v in DBCol(self.metric_target_db_conn, symbol).find_timeseries([v for v in timestamps_needed_for_partial_range])},
                            range(tf - self.range_in_ms, tf + 1, int(self.range_in_ms / self.values_needed_for_metric))
                        )})
                    getattr(self.metric_db_conn, symbol).insert_many(symbol_metric_values)

            self._metric_validator_db_conn.set_finish_ts(end_ts)
            range_counter += PARSE_AT_A_TIME_RATE
            LOG.info(f"Parsed {log_message}.")


@dataclass
class RelativeVolume(TechnicalIndicator):
    def metric_logic(self, timeseries_values: dict, range_values: range):
        def past_relative_volume(tickers_count: int):
            return sum(aggregate_tf_volumes[-tickers_count:]) / len(aggregate_tf_volumes[-tickers_count:])
        aggregate_tf_volumes = [timeseries_values[tf]['total_volume'] for tf in range_values]
        calculated_past_relative_volume = (past_relative_volume(5) + past_relative_volume(15) + past_relative_volume(30)) / 3
        return aggregate_tf_volumes[-1] / calculated_past_relative_volume if calculated_past_relative_volume != 0 else 0


@dataclass
class TotalVolume(TechnicalIndicator):
    def metric_logic(self, metric_values, timestamps_to_parse):
        parsed_metric_values = {}
        for ts in timestamps_to_parse:
            parsed_metric_values[ts] = metric_values[ts]['total_volume']

        return parsed_metric_values


class MetricDistribution(TechnicalIndicator):
    metric_to_query: str = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        if not self.metric_to_query:
            LOG.error("'MetricDistribution' needs value 'metric_to_query' to be provided.")
            raise InvalidClassAttributes("'MetricDistribution' needs value 'metric_to_query' to be provided.")

    def metric_logic(self, timeseries_values: dict, range_values):
        distribution_values = {}
        for tf in range_values:
            metric_value = get_key_values_from_dict_with_dicts(timeseries_values[tf], self.metric_to_query)
            metric_value = str(round(metric_value) if isinstance(metric_value, float) else 0)
            try:
                distribution_values[metric_value] += 1
            except KeyError:
                distribution_values[metric_value] = 1

        return distribution_values


class RiseOfStartEndVolume(MetricDistribution):
    metric_to_query = 'rise_of_start_end_volume_in_percentage'
