from __future__ import annotations
import logging
from abc import ABCMeta, ABC
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Union, Iterator, List, Type, Any, Mapping
import pymongo
from bson import timestamp
from pymongo.errors import CollectionInvalid

from support.generic_helpers import get_current_second, mins_to_ms, ms_to_mins, datetime_range
from pymongo import MongoClient, database, cursor
import logs
from support.data_handling.data_helpers.vars_constants import DBQueryOperators, DEFAULT_COL_SEARCH, \
    FINISH_TS_VALIDATOR_DB_SUFFIX, VALIDATOR_DB, START_TS_VALIDATOR_DB_SUFFIX, TEN_SECONDS_IN_MS, \
    ONE_DAY_IN_MINUTES, VALID_END_TS_VALIDATOR_DB_SUFFIX, VALID_END_TS, DONE_INTERVAL_VALIDATOR_DB_SUFFIX, \
    BASE_TRADES_CHART_DB, DEFAULT_PARSE_INTERVAL_IN_MS, DEFAULT_PARSE_INTERVAL_SECONDS, TRADES_CHART_DB, TS, \
    TEN_SECONDS, DEFAULT_PARSE_INTERVAL_TIMEDELTA, TEN_SECS_PARSED_TRADES_DB

from typing import TYPE_CHECKING

from tasks.technical_indicators.technical_indicators import TechnicalIndicator, TotalVolume

if TYPE_CHECKING:
    from support.data_handling.data_structures import TradeData, TradesChart

localhost = 'mongodb://localhost:27017/'  # if ubuntu can't connect --> Find Ipv4 IP automatically (ps ipconfig.. ipv4)

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)

mongo_client = MongoClient(host=localhost, maxPoolSize=0)


class InvalidValuesNeededProvided(Exception): pass
class InvalidOperationForGivenClass(Exception): pass
class InvalidDocumentKeyProvided(Exception): pass
class InvalidRangeStepProvided(Exception): pass
class InvalidDataProvided(Exception): pass
class EmptyDBCol(Exception): pass
class MissingTimeInterval(Exception): pass
class InvalidStartTimestamp(Exception): pass
class InvalidFinishTimestamp(Exception): pass
class InvalidInterval(Exception): pass
class InvalidResultsNumber(Exception): pass
class InvalidAtomicityDataType(Exception): pass
class UnmappedDB(Exception): pass


TimeseriesMinMax = namedtuple("TimeseriesMinMax", ["range_lower_bound", "range_higher_bound"])


class TradesChartTimeframes(Enum):
    ONE_HOUR = 60
    TWO_HOURS = 120
    FOUR_HOURS = 240
    EIGHT_HOURS = 480
    ONE_DAY = 1440
    TWO_DAYS = 2880
    FOUR_DAYS = 5760
    EIGHT_DAYS = 11520


TRADES_CHART_TIMEFRAMES_VALUES = [t.value for t in TradesChartTimeframes]


@dataclass
class Index:
    document: str
    unique: bool


_trades_chart_index = Index('end_ts', True)
_timestamp_index = Index('timestamp', True)


@dataclass
class ParsedAggtrades:
    timeField: timestamp
    price: float
    quantity: float
    bucketMaxSpanSeconds: int
    bucketRoundingSeconds: int


@dataclass
class DBData:
    db_timeframe_index: Optional[Index]
    atomicity: timedelta


@dataclass
class TechnicalIndicatorDetails:
    metric_target_db_name: str
    range_of_one_value_in_minutes: int
    values_needed: int
    metric_class: Type[TechnicalIndicator]
    atomicity_in_minutes: int
    timeframe_based: bool = False  # As opposed to 'symbol' based.
    threads_number: int = 1
    atomicity_in_timedelta: timedelta = field(init=False)
    db_timeframe_index: DBData.db_timeframe_index = field(init=False)

    def __post_init__(self):
        self.atomicity_in_timedelta = timedelta(minutes=self.atomicity_in_minutes)
        values_needed_atomicity = timedelta(minutes=self.range_of_one_value_in_minutes / self.values_needed)
        if values_needed_atomicity.seconds % DEFAULT_PARSE_INTERVAL_SECONDS != 0:
            LOG.error("Values needed relationship with range of one value needs to a multiple of ten seconds.")
            raise InvalidValuesNeededProvided("Values needed relationship with range of one value needs to a multiple of ten seconds.")
        self.db_timeframe_index = _timestamp_index


TradesChartTimeframeAtomicity = namedtuple("TradesChartTimeframeAtomicity", ["timeframe", "atomicity"])


class TradesChartTimeframeValuesAtomicity(Enum):
    ONE_HOUR = TradesChartTimeframeAtomicity(TradesChartTimeframes.ONE_HOUR, DEFAULT_PARSE_INTERVAL_TIMEDELTA)
    TWO_HOURS = TradesChartTimeframeAtomicity(TradesChartTimeframes.TWO_HOURS, DEFAULT_PARSE_INTERVAL_TIMEDELTA)
    FOUR_HOURS = TradesChartTimeframeAtomicity(TradesChartTimeframes.FOUR_HOURS, DEFAULT_PARSE_INTERVAL_TIMEDELTA)
    EIGHT_HOURS = TradesChartTimeframeAtomicity(TradesChartTimeframes.EIGHT_HOURS, DEFAULT_PARSE_INTERVAL_TIMEDELTA)
    ONE_DAY = TradesChartTimeframeAtomicity(TradesChartTimeframes.ONE_DAY, DEFAULT_PARSE_INTERVAL_TIMEDELTA * 2)
    TWO_DAYS = TradesChartTimeframeAtomicity(TradesChartTimeframes.TWO_DAYS, DEFAULT_PARSE_INTERVAL_TIMEDELTA * 6)
    FOUR_DAYS = TradesChartTimeframeAtomicity(TradesChartTimeframes.FOUR_DAYS, DEFAULT_PARSE_INTERVAL_TIMEDELTA * 12)
    EIGHT_DAYS = TradesChartTimeframeAtomicity(TradesChartTimeframes.EIGHT_DAYS, DEFAULT_PARSE_INTERVAL_TIMEDELTA * 48)


TRADES_CHART_TF_ATOMICITY = {trades_chart_tf_atomicity.value.timeframe.value: trades_chart_tf_atomicity.value.atomicity
                             for trades_chart_tf_atomicity in TradesChartTimeframeValuesAtomicity}


InitDBsNameDataclassName = namedtuple("InitDBsNameDataclassName", ["dataclass_name", "mongodb_name", 'values'])


class MapInitDBs(Enum):
    TIMESTAMPS_VALIDATOR = InitDBsNameDataclassName('ValidatorDB', 'Timestamps_Validator', {'timeField': 'timestamp', 'metaField': "metadata", 'granularity': 'seconds'})
    PARSED_AGGTRADES = InitDBsNameDataclassName('ParsedAggtrades', 'parsed_aggtrades', {'timeField': 'timestamp', 'metaField': "metadata", 'bucketMaxSpanSeconds': 10, 'bucketRoundingSeconds': 10})
    TEN_SECONDS_PARSED_TRADES = InitDBsNameDataclassName('Trade', 'ten_seconds_parsed_trades', {'timeField': 'timestamp', 'metaField': "metadata", 'bucketMaxSpanSeconds': 10, 'bucketRoundingSeconds': 10})
    TRADES_CHART_60_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_60_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.ONE_HOUR.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.ONE_HOUR.value.atomicity.seconds
    })
    TRADES_CHART_120_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_120_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.TWO_HOURS.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.TWO_HOURS.value.atomicity.seconds
    })
    TRADES_CHART_240_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_240_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.FOUR_HOURS.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.FOUR_HOURS.value.atomicity.seconds
    })
    TRADES_CHART_480_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_480_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.EIGHT_HOURS.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.EIGHT_HOURS.value.atomicity.seconds
    })
    TRADES_CHART_1440_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_1440_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.ONE_DAY.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.ONE_DAY.value.atomicity.seconds
    })
    TRADES_CHART_2880_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_2880_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.TWO_DAYS.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.TWO_DAYS.value.atomicity.seconds
    })
    TRADES_CHART_5760_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_5760_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.FOUR_DAYS.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.FOUR_DAYS.value.atomicity.seconds
    })
    TRADES_CHART_11520_MINUTES = InitDBsNameDataclassName('TradesChart', 'trades_chart_db_11520_minutes', {
        'timeField': 'timestamp', 'metaField': "metadata",
        'bucketMaxSpanSeconds': TradesChartTimeframeValuesAtomicity.EIGHT_DAYS.value.atomicity.seconds,
        'bucketRoundingSeconds': TradesChartTimeframeValuesAtomicity.EIGHT_DAYS.value.atomicity.seconds
    })


INIT_DBS_NAME_VALUES = {map_init.value.mongodb_name: map_init.value.values for map_init in MapInitDBs}


class DBMapper(Enum):
    from tasks.technical_indicators.technical_indicators import RelativeVolume, RiseOfStartEndVolume

    Timestamps_Validator = None

    # trade_chart_60_minutes_rise_of_start_end_volume = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.ONE_HOUR.value),
    #     300,
    #     300 * 6,
    #     RiseOfStartEndVolume,
    #     5
    # )
    #
    # trade_chart_1440_minutes_rise_of_start_end_volume = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.ONE_DAY.value),
    #     1440 * 15,
    #     2160,
    #     RiseOfStartEndVolume,
    #     30
    # )
    #
    # relative_volume_60_minutes = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.ONE_HOUR.value),
    #     60 * 30,
    #     30,
    #     RelativeVolume,
    #     5
    # )
    #
    # relative_volume_120_minutes = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.TWO_HOURS.value),
    #     120 * 30,
    #     30,
    #     RelativeVolume,
    #     5
    # )
    #
    # relative_volume_240_minutes = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.FOUR_HOURS.value),
    #     240 * 30,
    #     30,
    #     RelativeVolume,
    #     10
    # )
    #
    # relative_volume_480_minutes = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.EIGHT_HOURS.value),
    #     480 * 30,
    #     60,
    #     RelativeVolume,
    #     15
    # )
    #
    # relative_volume_1440_minutes = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.ONE_DAY.value),
    #     1440 * 15,
    #     120,
    #     RelativeVolume,
    #     30
    # )
    #
    # total_ta_volume_1440_minutes = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.ONE_DAY.value),
    #     1440,
    #     1,
    #     TotalVolume,
    #     TradesChartTimeframeValuesAtomicity.ONE_DAY.value.atomicity,
    #     True,
    #     1
    # )
    #
    # total_ta_volume_60_minutes = TechnicalIndicatorDetails(
    #     BASE_TRADES_CHART_DB.format(TradesChartTimeframes.ONE_HOUR.value),
    #     60,
    #     1,
    #     TotalVolume,
    #     TradesChartTimeframeValuesAtomicity.ONE_DAY.value.atomicity,
    #     True,
    #     2
    # )

    trades_chart_db_60_minutes = DBData(_trades_chart_index,
                                        TradesChartTimeframeValuesAtomicity.ONE_HOUR.value.atomicity)
    trades_chart_db_120_minutes = DBData(_trades_chart_index,
                                         TradesChartTimeframeValuesAtomicity.TWO_HOURS.value.atomicity)
    trades_chart_db_240_minutes = DBData(_trades_chart_index,
                                         TradesChartTimeframeValuesAtomicity.FOUR_HOURS.value.atomicity)
    trades_chart_db_480_minutes = DBData(_trades_chart_index,
                                         TradesChartTimeframeValuesAtomicity.EIGHT_HOURS.value.atomicity)
    trades_chart_db_1440_minutes = DBData(_trades_chart_index,
                                          TradesChartTimeframeValuesAtomicity.ONE_DAY.value.atomicity)
    trades_chart_db_2880_minutes = DBData(_trades_chart_index,
                                          TradesChartTimeframeValuesAtomicity.TWO_DAYS.value.atomicity)
    trades_chart_db_5760_minutes = DBData(_trades_chart_index,
                                          TradesChartTimeframeValuesAtomicity.FOUR_DAYS.value.atomicity)
    trades_chart_db_11520_minutes = DBData(_trades_chart_index,
                                           TradesChartTimeframeValuesAtomicity.EIGHT_DAYS.value.atomicity)
    parsed_aggtrades = DBData(Index('timestamp', False), 1)  # one is a valid value, ignore highlight.
    ten_seconds_parsed_trades = DBData(Index('timestamp', True), DEFAULT_PARSE_INTERVAL_TIMEDELTA)
    ten_seconds_parsed_trades_Fund_Data = DBData(Index('timestamp', True), DEFAULT_PARSE_INTERVAL_TIMEDELTA)


class DB(pymongo.database.Database, metaclass=ABCMeta):
    def __init__(self, db_name):
        self.db_name = db_name
        try:
            self._db_map = getattr(DBMapper, self.db_name).value
        except:
            LOG.error(f"DB {self.db_name} is not mapped in {DBMapper}.")
            raise UnmappedDB(f"DB {self.db_name} is not mapped in {DBMapper}.")
        super().__init__(mongo_client, self.db_name)

        if not isinstance(self, ValidatorDB) and db_name != VALIDATOR_DB:
            self.end_ts = ValidatorDB(self.db_name).finish_ts

        if mapped_db := DBMapper.__getitem__(self.db_name).value:
            self.timestamp_doc_key = mapped_db.db_timeframe_index.document if mapped_db.db_timeframe_index else None
            self.atomicity_in_ms = mapped_db.atomicity
        else:
            self.timestamp_doc_key = None
            self.atomicity_in_ms = None

    def __getattr__(self, collection: str):
        return DBCol(self, collection)

    def list_collection_names(
        self,
        session: Optional["ClientSession"] = None,  # pymongo.client_session
        filter: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> List[str]:  # Can't think of a case where I need system databases.
        return super().list_collection_names(session=session,
                                             filter=filter if filter else {"name": {"$regex": r"^((?!system).)*$"}},
                                             comment=comment, **kwargs)

    def clear_higher_than(self, timestamp: int, document_key: str) -> bool:
        for symbol in self.list_collection_names():
            getattr(self, symbol).delete_many({document_key: {DBQueryOperators.HIGHER_EQ.value: timestamp}})
        return True

    def delete_collections_with_text(self, text) -> bool:
        for col in self.list_collection_names():
            if text in col:
                self.drop_collection(col)
        return True

    def clear_collections_between(self, lower_bound, higher_bound) -> bool:
        for col in self.list_collection_names():
            getattr(self, str(col)).clear_between(lower_bound, higher_bound)
        return True


class TechnicalIndicatorDB(DB, ABC):
    def __init__(self, db_name):
        super().__init__(db_name)


class DBCol(pymongo.collection.Collection, metaclass=ABCMeta):
    USE_INTERNAL_MAPPED_TIMESTAMP = object()

    def __init__(self, db_instance_or_name: [str, DB], collection=DEFAULT_COL_SEARCH):
        if isinstance(db_instance_or_name, str):
            self.db_name = db_instance_or_name
            try:
                self._db_map = getattr(DBMapper, self.db_name).value
            except:
                LOG.error(f"DB {self.db_name} is not mapped in {DBMapper}.")
                raise UnmappedDB(f"DB {self.db_name} is not mapped in {DBMapper}.")

        else:
            self._db_map = db_instance_or_name._db_map
            self.db_name = db_instance_or_name.db_name

        self._collection = collection
        super().__init__(db_instance_or_name if isinstance(db_instance_or_name, DB) else DB(self.db_name),
                         self._collection)

    def __getattr__(self, item):
        if (item not in dir(self)):
            raise KeyError("Attribute doesn't exist in the object")
        else:
            return super.__getattribute__(item)

    def column_between(self, lower_bound: datetime, higher_bound: datetime, ReturnType: Union[Optional, TradesChart, TradeData] = None) -> Iterator:
        if not self.find_one({}):
            LOG.error("Queried Collection '%s' does not exist for db '%s'.", self._collection, self.db_name)
            raise EmptyDBCol(f"Queried Collection '{self._collection}' does not exist for db '{self.db_name}'.")

        for res in self.find_timeseries(TimeseriesMinMax(lower_bound, higher_bound)):
            yield res if not ReturnType else ReturnType(**res)

    def most_recent_timeframe(self) -> cursor:
        return self.timeseries_endpoint(True)

    def oldest_timeframe(self) -> cursor:
        return self.timeseries_endpoint(False)

    def timeseries_endpoint(self, query_most_recent: bool) -> cursor:
        try:
            most_recent = next(self._and_query(datetime.fromtimestamp(0), datetime.fromtimestamp(get_current_second())).limit(1).sort(TS, pymongo.DESCENDING))[TS]
            oldest = next(self._and_query(datetime.fromtimestamp(0), datetime.fromtimestamp(get_current_second())).limit(1).sort(TS, pymongo.ASCENDING))[TS]

            return most_recent if query_most_recent else oldest
        except StopIteration as e:
            LOG.error(f"Collection '{self._collection}' from database '{self.db_name}' contains no data.")
            raise InvalidDataProvided(f"Collection '{self._collection}' from database '{self.db_name}' contains no data.")

    def _init_collection(self):
        if not self.find_one({}):
            try:
                self.database.create_collection(self.name, timeseries=INIT_DBS_NAME_VALUES[self.database.name])
            except CollectionInvalid as e:
                if not 'already exists' in e.args[0]:
                    raise
            except pymongo.errors.OperationFailure as e:
                if not 'already exists, but is a view' in e.args[0]:
                    raise

    def find_all(self):
        return super().find({})

    def delete_all(self):
        self.delete_many({})

    def _and_query(self, lower_bound, higher_bound):
        return self.find({DBQueryOperators.AND.value: [{TS: {DBQueryOperators.HIGHER_EQ.value: lower_bound}},
                                                       {TS: {DBQueryOperators.LOWER_EQ.value: higher_bound}}]})

    def find_timeseries_one(self, timestamp_to_query: int):
        try:
            if self.database.atomicity == DEFAULT_PARSE_INTERVAL_IN_MS:
                return next(super().find({self._timestamp_doc_key: timestamp_to_query}))
            else:
                return next(self._and_query(timestamp_to_query - self.database.atomicity_in_ms, timestamp_to_query))
        except StopIteration:
            err_message = f"No valid timestamp value {(datetime.fromtimestamp(timestamp_to_query / 1000))} from db {self.db_name} and collection {self.name}"
            LOG.error(err_message)
            raise InvalidDataProvided(err_message)

    def find_timeseries(self, timestamps_to_query: [TimeseriesMinMax, range, List]):
        if isinstance(timestamps_to_query, TimeseriesMinMax):
            return self._and_query(timestamps_to_query.range_lower_bound, timestamps_to_query.range_higher_bound)
        elif isinstance(timestamps_to_query, range) or isinstance(timestamps_to_query, List):
            if isinstance(timestamps_to_query, range):
                timestamps_to_query = [*range(timestamps_to_query.start, timestamps_to_query.stop, timestamps_to_query.step)]

            db_atomicity = getattr(DBMapper, self.db_name).value.atomicity
            if db_atomicity != DEFAULT_PARSE_INTERVAL_IN_MS:
                for i, ts_to_query in enumerate(timestamps_to_query):
                    timestamps_to_query[i] = ts_to_query + ts_to_query % db_atomicity

            query = {self._timestamp_doc_key: {DBQueryOperators.IN.value: timestamps_to_query}}
            timestamps_count = len(timestamps_to_query)
            query_count = self.count_documents(query)

            if query_count != timestamps_count and query_count < int(timestamps_count / (self.database.atomicity_in_ms / DEFAULT_PARSE_INTERVAL_IN_MS)):
                err_msg = (f"Invalid number of results retrieved from database '{self.db_name}' and collection "
                           f"'{self.name}', expected {timestamps_count} and got {query_count}.")
                LOG.error(err_msg)
                raise InvalidResultsNumber(err_msg)

            return super().find(query)
        else:
            raise InvalidDataProvided("argument provided has invalid type")

    def insert_one(self, data, is_timeseries_db=True):
        if is_timeseries_db:
            self._init_collection()
        return super().insert_one(data)

    def insert_many(self, data, is_timeseries_db=True):
        if is_timeseries_db:
            self._init_collection()
        return super().insert_many(data)

    def clear_between(self, lower_bound, higher_bound):
        self.delete_many({DBQueryOperators.AND.value: [{TS: {DBQueryOperators.HIGHER_EQ.value: lower_bound}},
                                                       {TS: {DBQueryOperators.LOWER_EQ.value: higher_bound}}]})


class ValidatorDB(DB, ABC):
    def __init__(self, validate_db_name):
        self.db_name = VALIDATOR_DB
        self.validate_db_name = validate_db_name
        self.finish_ts_col_conn = DBCol(self.db_name, f"{self.validate_db_name + FINISH_TS_VALIDATOR_DB_SUFFIX}")
        self.start_ts_collection = DBCol(self.db_name, f"{self.validate_db_name + START_TS_VALIDATOR_DB_SUFFIX}")
        self.done_intervals_ts_collection = DBCol(self.db_name, f"{self.validate_db_name + DONE_INTERVAL_VALIDATOR_DB_SUFFIX}")

        super().__init__(self.db_name)

        finish_ts_data = self.finish_ts_col_conn.find_one()
        start_ts_data = self.start_ts_collection.find_one()

        self.finish_ts = finish_ts_data[TS] if finish_ts_data else None
        self.start_ts = start_ts_data[TS] if start_ts_data else None

    def __getattr__(self, item):
        if item not in dir(self):
            LOG.info("Given class does not contain requested '%s' attribute.", item)
            raise InvalidOperationForGivenClass(f"Given class does not contain requested '{item}' attribute.")

    def set_finish_ts(self, finish_ts):
        if not self.finish_ts_col_conn.find_one():
            self.finish_ts_col_conn.insert_one({TS: finish_ts})

        if finish_ts > self.finish_ts_col_conn.find_one({})[TS]:
            self.finish_ts_col_conn.delete_all()
            self.finish_ts_col_conn.insert_one({TS: finish_ts})

    def set_start_ts(self, start_ts):
        if not self.start_ts_collection.find_one():  # init validator db start_ts.
            self.start_ts_collection.insert_one({TS: start_ts})

    def add_done_ts_interval(self, start_ts, end_ts):
        self.done_intervals_ts_collection.insert_one({"done_interval": [start_ts, end_ts]}, is_timeseries_db=False)

    def set_timeframe_valid_timestamps(self, atomicity):
        def get_closest_lower_number(number: int, numbers_to_get_value: List):
            numbers_to_get_value.sort()
            for i, number_in_numbers in enumerate(numbers_to_get_value):
                if number > number_in_numbers:
                    return numbers_to_get_value[i]
            return numbers_to_get_value[-1]

        time_intervals_start_ts = []
        time_intervals_end_ts = []

        start_ts = 0
        if not (done_intervals := [interval['done_interval'] for interval in list(self.done_intervals_ts_collection.find_all())]):
            return False
        else:
            for value in done_intervals:
                time_intervals_start_ts.append(value[0])
                time_intervals_end_ts.append(value[1])

                time_intervals_start_ts.sort()
                time_intervals_end_ts.sort()
                start_ts, end_ts = time_intervals_start_ts[0], time_intervals_end_ts[0]

        interval_errors_start_ts = []
        for interval in done_intervals:
            if interval[0] - atomicity not in time_intervals_end_ts and interval[0] != start_ts:
                interval_errors_start_ts.append(interval[0])
        if interval_errors_start_ts:
            interval_errors_start_ts.sort()
            finish_ts = get_closest_lower_number(interval_errors_start_ts[0], time_intervals_end_ts)
            self.set_finish_ts(finish_ts)
            LOG.info(f"Missing time interval detected, setting end ts to {finish_ts} for db {self.validate_db_name}. if this happens often please check.")
        else:
            self.set_finish_ts(time_intervals_end_ts[-1])
            LOG.info(f"End_ts ts set to {time_intervals_end_ts[-1]} for db {self.validate_db_name}.")

        self.done_intervals_ts_collection.delete_all()
        return True


class AggtradesValidatorDB(ValidatorDB, ABC):
    def set_valid_timestamps(self):
        validate_db_aggtrades_btcusdt = DBCol(self.validate_db_name, DEFAULT_COL_SEARCH)
        most_recent_timeframe = validate_db_aggtrades_btcusdt.most_recent_timeframe()
        if oldest_tf := validate_db_aggtrades_btcusdt.oldest_timeframe():
            parse_from_ts = oldest_tf if not self.finish_ts else self.finish_ts
            for ts in datetime_range(parse_from_ts, most_recent_timeframe, DEFAULT_PARSE_INTERVAL_TIMEDELTA):
                try:
                    next(validate_db_aggtrades_btcusdt.column_between(ts, ts + DEFAULT_PARSE_INTERVAL_TIMEDELTA))
                except StopIteration:
                    err_msg = ("There are missing trades after '%s', setting finish time to '%s' and going from there.",
                               ts, datetime.fromtimestamp(ts / 1000))
                    LOG.error(err_msg)
                    self.set_finish_ts(ts)
                    raise InvalidFinishTimestamp(err_msg)
            else:
                self.set_finish_ts(most_recent_timeframe)
                LOG.info(f"End_ts ts set to {most_recent_timeframe} for db {self.validate_db_name}.")


class TradesChartValidatorDB(ValidatorDB, ABC):
    def __init__(self, trades_chart_timeframe: Optional[int]):
        init_db_name = BASE_TRADES_CHART_DB.format(
            trades_chart_timeframe) if trades_chart_timeframe else TRADES_CHART_DB
        super().__init__(init_db_name)

        self.trades_chart_timeframe = trades_chart_timeframe
        valid_end_ts_data = self.__getattr__(self.validate_db_name + VALID_END_TS_VALIDATOR_DB_SUFFIX).find_one()
        self.valid_end_ts = valid_end_ts_data[VALID_END_TS] if valid_end_ts_data else None

    def __getattr__(self, item):
        try:
            return self.__dict__[item]
        except KeyError:
            setattr(self, item, DBCol(self, self.validate_db_name + VALID_END_TS_VALIDATOR_DB_SUFFIX))
            return getattr(self, item)

    def set_global_timeframes_valid_timestamps(self):
        for timeframe in TRADES_CHART_TIMEFRAMES_VALUES:
            trades_chart_validator_db = TradesChartValidatorDB(timeframe)
            trades_chart_validator_db.set_timeframe_valid_timestamps(TRADES_CHART_TF_ATOMICITY[trades_chart_validator_db.trades_chart_timeframe])
            try:
                start_ts = DBCol(BASE_TRADES_CHART_DB.format(timeframe), DEFAULT_COL_SEARCH).oldest_timeframe()
            except InvalidDataProvided:
                start_ts = ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts + timedelta(minutes=max([timeframes.value for timeframes in TradesChartTimeframes]))
            trades_chart_validator_db.set_start_ts(start_ts)

        valid_finish_timestamp_values = [ValidatorDB(BASE_TRADES_CHART_DB.format(timeframe)).finish_ts for timeframe in TRADES_CHART_TIMEFRAMES_VALUES]
        if not valid_finish_timestamp_values[0]:
            LOG.info("First run of trades chart detected, not setting valid timestamps.")
            return True

        if len(set(valid_finish_timestamp_values)) == 1:
            self.set_finish_ts(valid_finish_timestamp_values[0])
        else:
            LOG.info("Finish timestamps is not equal for all timeframes, setting value to the lowest between them.")
            self.set_finish_ts(datetime.fromtimestamp(min([v.timestamp() for v in valid_finish_timestamp_values if v])))


def set_universal_start_end_ts() -> list:
    universal_start_finish_collection = 'Universal_start_finish_timestamp'
    start_timestamps = []
    finish_timestamps = []

    DB(VALIDATOR_DB).drop_collection(universal_start_finish_collection)

    for collection in DB(VALIDATOR_DB).list_collection_names():
        if 'start_timestamp' in collection:
            start_timestamps.append(DBCol(VALIDATOR_DB, collection).find_one()['start_timestamp'])
        elif 'finish_timestamp' in collection:
            finish_timestamps.append(DBCol(VALIDATOR_DB, collection).find_one()['finish_timestamp'])

    start_ts, end_ts = [max(start_timestamps), min(finish_timestamps)]
    DBCol(VALIDATOR_DB, universal_start_finish_collection).insert_one(
        {"start_finish_timestamp": [start_ts, min(finish_timestamps)]})

    return [start_ts, end_ts]


def get_top_n_traded_volume(top_count: int, timestamp: int):
    symbol_volumes = DBCol('total_volume_60_minutes', 'total_volume').find_timeseries_one(timestamp)[
        'total_volume']  # Typing is not working here.
    return sorted(symbol_volumes, key=symbol_volumes.get, reverse=True)[
           4:top_count]  # its assumed the top 4 are BTC, ETH, FUND_DATA.


def list_dbs():
    return mongo_client.list_database_names()


def change_charts_values(timeframe_in_minutes):
    chart_tf_db = BASE_TRADES_CHART_DB.format(timeframe_in_minutes)
    mins_to_validate_at_a_time = ONE_DAY_IN_MINUTES
    append_ts = mins_to_ms(mins_to_validate_at_a_time)

    for symbol in DB(BASE_TRADES_CHART_DB.format(timeframe_in_minutes)).list_collection_names():
        db_conn = DBCol(chart_tf_db, symbol)
        init_ts, end_ts = db_conn.oldest_timeframe(), db_conn.most_recent_timeframe()
        while True:
            for trade in db_conn.column_between(init_ts, init_ts + append_ts):
                if not trade['total_volume']:
                    db_conn.update_one({'start_ts': trade['start_ts']}, {'$set': {'total_volume': 0}})
            init_ts += append_ts
            if init_ts > end_ts:
                break

    LOG.info(f"Timeframe {timeframe_in_minutes} done.")


def query_charts_missing_tf_intervals(timeframe_in_minutes: int, symbols=['BTCUSDT']):
    chart_tf_db = BASE_TRADES_CHART_DB.format(timeframe_in_minutes)
    mins_to_validate_at_a_time = ONE_DAY_IN_MINUTES
    accepted_trades_number = mins_to_validate_at_a_time * 6 + 1
    append_ts = mins_to_ms(mins_to_validate_at_a_time)

    missing_timeframes = []

    for symbol in symbols:
        if not (init_ts := TradesChartValidatorDB(timeframe_in_minutes).valid_end_ts):
            init_ts = DBCol(chart_tf_db, symbol).oldest_timeframe()

        end_ts = DBCol(chart_tf_db, symbol).most_recent_timeframe()
        while init_ts + append_ts <= end_ts:
            check_partial_trades = list(DBCol(chart_tf_db, symbol).column_between(init_ts, init_ts + append_ts))
            if len(check_partial_trades) != accepted_trades_number:
                parsed_tfs = set([trade['start_ts'] for trade in check_partial_trades])
                missing_timeframes.append(
                    set(list(range(init_ts, init_ts + append_ts, TEN_SECONDS_IN_MS))) - parsed_tfs)
            init_ts += append_ts
        else:
            left_timeframe = end_ts - init_ts
            parsed_tfs = set([trade['start_ts'] for trade in
                              DBCol(chart_tf_db, symbol).column_between(init_ts, init_ts + left_timeframe)])
            missing_timeframes.append(
                set(list(range(init_ts, init_ts + left_timeframe, TEN_SECONDS_IN_MS))) - parsed_tfs)

    merged_missing_tfs = set().union(*missing_timeframes)

    missing_tf_intervals = []
    started_tf = False

    if merged_missing_tfs:
        for elem in range(min(merged_missing_tfs), max(merged_missing_tfs), TEN_SECONDS_IN_MS):
            if elem in merged_missing_tfs and not started_tf:
                started_tf = True
                init_miss_val = elem
            if elem not in merged_missing_tfs and started_tf:
                missing_tf_intervals.append([init_miss_val, elem - TEN_SECONDS_IN_MS])
                started_tf = False

    return missing_tf_intervals


def create_index_db_cols(db, field) -> None:
    db_conn = DB(db)
    for col in db_conn.list_collection_names():
        db_conn.__getattr__(col).create_index([(field, -1)], unique=True)
        print(f"Created index for collection {col}.")


def delete_dbs_and_timestamp_validator_collections_with_text(text: str):
    for db in [dbs for dbs in list_dbs() if text in dbs]:
        mongo_client.drop_database(db)

    DB(VALIDATOR_DB).delete_collections_with_text(text)


def delete_collection_in_db(db_name, collection_name):
    DBCol(db_name, collection_name).delete_all()


# delete_dbs_and_timestamp_validator_collections_with_text('trades_chart')
# pass

# delete_dbs_and_timestamp_validator_collections_with_text('parsed_aggtrades')
# pass
# delete_dbs_and_timestamp_validator_collections_with_text('ten_seconds')
# # delete_dbs_and_timestamp_validator_collections_with_text('fund_data')
# pass

# delete_dbs_and_timestamp_validator_collections_with_text('fund_data')
# delete_dbs_and_timestamp_validator_collections_with_text('Fund_Data')
# pass
# delete_collection_in_db('ten_seconds_parsed_trades', 'fund_data')
# print("HERE")

# TEchnical indicators Delete all.
# delete_dbs_and_timestamp_validator_collections_with_text('relative_vol')
# delete_dbs_and_timestamp_validator_collections_with_text('minutes_rise_of_start_end_volume')
# delete_dbs_and_timestamp_validator_collections_with_text('total_volume_ta')
# delete_dbs_and_timestamp_validator_collections_with_text('total_ta_volume')
#
# pass

# for timeframe in TRADES_CHART_TIMEFRAMES_VALUES:
#     DBCol(BASE_TRADES_CHART_DB.format(timeframe), 'fund_data').delete_all()
# print("HERE")

# DB(VALIDATOR_DB).delete_collections_with_text('EURUSDT')

