import logging
import time
from abc import abstractmethod
from dataclasses import field, dataclass
from datetime import datetime, timedelta
from typing import Type

import polars
from pymongoarrow.api import find_polars_all

import logs

from support.data_handling.data_helpers.vars_constants import TS, DEFAULT_COL_SEARCH, DEFAULT_PARSE_INTERVAL_SECONDS, \
    TRADES_DB_SCHEMA, DBQueryOperators
from support.decorators_extenders import init_only_existing
from support.generic_helpers import get_value_from_dict_with_path, get_dict_key_path_one_child_only, datetime_range, \
    timedelta_round_following_minute

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)
PARSE_AT_A_TIME_RATE = 300


class InvalidValuesNeededProvided(Exception): pass
class UninitializedTradesChart(Exception): pass
class NoTradesToParse(Exception): pass
class InvalidParameterType(Exception): pass
class InvalidClassAttributes(Exception): pass


@dataclass
class TechnicalIndicatorDetails:
    metric_db_name: str
    metric_target_db_name: str
    range: timedelta
    values_needed: int
    metric_class: Type['TechnicalIndicator']
    atomicity: timedelta
    timeframe_based: bool = False  # As opposed to 'symbol' based.
    threads_number: int = 1

    def __post_init__(self):
        if (self.range / self.values_needed).seconds % DEFAULT_PARSE_INTERVAL_SECONDS != 0:
            LOG.error("Values needed relationship with range of one value needs to a multiple of ten seconds.")
            raise InvalidValuesNeededProvided("Values needed relationship with range of one value needs to a multiple of ten seconds.")


@init_only_existing
@dataclass
class TechnicalIndicator(TechnicalIndicatorDetails):
    start_ts_plus_range: datetime = field(init=False)
    end_ts: datetime = field(init=False)
    metric_target_db_conn: 'DB' = field(init=False)
    metric_db_conn: 'DB' = field(init=False)
    timeframe_based: bool = field(init=False)
    metric_validator_db_conn: 'ValidatorDB' = field(init=False)

    def __post_init__(self):
        from MongoDB.db_actions import ValidatorDB, DB

        self.metric_validator_db_conn = ValidatorDB(self.metric_db_name)
        self.metric_db_conn = DB(self.metric_db_name)
        self.metric_target_db_conn = DB(self.metric_target_db_name)
        self.end_ts = ValidatorDB(self.metric_target_db_name).finish_ts

        if not self.end_ts:  # TradesChartValidatorDB start_ts is initialized when end_ts is, only need to check one.
            LOG.error("This indicator depends on valid start and end timestamp from trades chart DB.")
            raise UninitializedTradesChart("This indicator depends on valid start and end timestamp from trades chart DB.")

        if ValidatorDB(self.metric_db_name).finish_ts:
            self.start_ts_plus_range = ValidatorDB(self.metric_db_name).finish_ts
        else:
            self.start_ts_plus_range = timedelta_round_following_minute(ValidatorDB(self.metric_target_db_conn.db_name).start_ts + self.range)

        if self.end_ts < self.start_ts_plus_range:
            error_msg = (f"End timestamp attribute is before start timestamp attribute, this means there are no trades "
                         f"left to parse for metric with DB name '{self.metric_db_name}' in DB {self.metric_db_name}.")
            LOG.error(error_msg)
            raise NoTradesToParse(error_msg)

    @abstractmethod
    async def metric_logic(self, *args, **kwargs):
        raise NotImplemented()

    def parse_metric(self):
        from MongoDB.db_actions import DBCol

        def query_timeseries_values(symbol, timestamps):
            return find_polars_all(DBCol(self.metric_target_db_conn, symbol), {"timestamp": {"$gte": timestamps[0], "$lte": timestamps[-1] + timedelta(seconds=1)}}, schema=TRADES_DB_SCHEMA)

        def get_timestamps_needed_for_partial_range(partial_range, range_step) -> list:
            timestamps_needed_for_partial_range = {}

            for value in partial_range:
                unordered_range_set_values = {*list(datetime_range(value - self.range + range_step, value + timedelta(seconds=1), range_step))}
                if not timestamps_needed_for_partial_range:
                    timestamps_needed_for_partial_range = unordered_range_set_values
                else:
                    timestamps_needed_for_partial_range.update(unordered_range_set_values)

            timestamps_has_list = [v for v in timestamps_needed_for_partial_range]
            timestamps_has_list.sort()

            return timestamps_has_list

        range_step = self.range / self.values_needed
        values_to_parse = [*datetime_range(self.start_ts_plus_range, self.end_ts + timedelta(seconds=1), (self.atomicity if self.atomicity < range_step else range_step))]
        range_counter = 0

        if not self.metric_validator_db_conn.start_ts:
            self.metric_validator_db_conn.set_start_ts(values_to_parse[0])

        while True:
            if not (partial_range := values_to_parse[range_counter: range_counter + PARSE_AT_A_TIME_RATE]):
                LOG.info("No more trades to parse, exiting.")
                exit(0)

            start_ts, end_ts = partial_range[0], partial_range[-1]
            self.metric_db_conn.clear_collections_between(start_ts, end_ts)

            log_message = (f"metric {self.metric_db_conn.db_name} with start date of {start_ts} and end date of"
                           f" '{values_to_parse[-1]}', dividing into partial ranges, finishing in '{end_ts}' for now.")

            LOG.info(f"Starting to parse {log_message}")

            timestamps_needed_for_partial_range = get_timestamps_needed_for_partial_range(partial_range, range_step)
            symbols = self.metric_target_db_conn.list_collection_names()

            if self.timeframe_based:
                symbols_metric_values = {}
                for symbol in symbols:
                    symbols_metric_values[symbol] = self.metric_logic(query_timeseries_values(symbol, timestamps_needed_for_partial_range))

                symbols_timeframe = []
                for timeframe in symbols_metric_values[DEFAULT_COL_SEARCH].keys():
                    symbols_timeframe.append({TS: timeframe, "metric_values": [{symbol: symbols_metric_values[symbol][timeframe]} for symbol in symbols_metric_values]})

                getattr(self.metric_db_conn, self.metric_db_name).insert_many(symbols_timeframe)
            else:  # Symbol based
                for symbol in symbols:
                    timeseries_values = query_timeseries_values(symbol, timestamps_needed_for_partial_range)
                    symbol_metric_values = []
                    for tf in partial_range:
                        a = (timeseries_values.filter((timeseries_values['timestamp'] < tf + timedelta(seconds=1)) & (timeseries_values['timestamp'] > (tf - self.range))))
                        a = a.filter(a['timestamp'].is_in(a['timestamp'][0:-1:30]))
                        self.metric_logic(a)
                        symbol_metric_values.append({TS: 0, 'metric_value': self.metric_logic(partial_timeseries_values)})
                    for tf, data in list(timeseries_values['metadata']):
                        # #TODO: Improve this line below is not self explanatory.. its working but not easy to understand.
                        # # Only way I found to parse the correct values for all use cases.
                        # if self.atomicity < range_step or (tf % self.atomicity) == (tf % range_step):
                        #     values_buffer = tf - (range_step * self.values_needed)
                        #     try:
                        #         timeseries_values[values_buffer]
                        #     except KeyError:
                        #         continue

                        partial_timeseries_values = {metric_tf: timeseries_values[metric_tf] for metric_tf in reversed(range(values_buffer + range_step, tf + range_step, range_step))}
                        symbol_metric_values.append({TS: tf, 'metric_value': self.metric_logic(timeseries_values)})

                    getattr(self.metric_db_conn, symbol).insert_many(symbol_metric_values)

            self.metric_validator_db_conn.add_done_ts_interval(start_ts, end_ts)
            range_counter += PARSE_AT_A_TIME_RATE
            LOG.info(f"Parsed {log_message}.")


@dataclass
class RelativeVolume(TechnicalIndicator):
    def metric_logic(self, timeseries_values: dict):
        def past_relative_volume(tickers_count: int):
            return sum(aggregate_tf_volumes[-tickers_count:]) / len(aggregate_tf_volumes[-tickers_count:])

        aggregate_tf_volumes = [timeseries_values[tf]['total_volume'] for tf in timeseries_values.keys()]
        calculated_past_relative_volume = (past_relative_volume(5) + past_relative_volume(15) + past_relative_volume(30)) / 3
        return aggregate_tf_volumes[-1] / calculated_past_relative_volume if calculated_past_relative_volume != 0 else 0


@dataclass
class TotalVolume(TechnicalIndicator):
    def metric_logic(self, metric_values):
        return {timestamp: metric_values[timestamp]['total_volume'] for timestamp in metric_values.keys()}


class MetricDistribution(TechnicalIndicator):
    metric_to_query: str = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        if not self.metric_to_query:
            LOG.error("'MetricDistribution' needs value 'metric_to_query' to be provided.")
            raise InvalidClassAttributes("'MetricDistribution' needs value 'metric_to_query' to be provided.")

    def metric_logic(self, timeseries_values):
        distribution_values = {}
        path_to_metric = get_dict_key_path_one_child_only(timeseries_values[next(iter(timeseries_values))], self.metric_to_query)
        for tf in timeseries_values:
            metric_value = get_value_from_dict_with_path(timeseries_values[tf], *path_to_metric)
            metric_value = str(round(metric_value) if isinstance(metric_value, float) else 0)
            try:
                distribution_values[metric_value] += 1
            except KeyError:
                distribution_values[metric_value] = 1
        return distribution_values


class RiseOfStartEndVolume(MetricDistribution):
    metric_to_query = 'rise_of_start_end_volume_in_percentage'
