from __future__ import annotations
import contextlib
import logging
import time
from abc import ABCMeta, ABC
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Union, Iterator, List
from typing import TYPE_CHECKING
import pymongo

from support.generic_helpers import get_current_second_in_ms, mins_to_ms

if TYPE_CHECKING:
    from data_handling.data_structures import TradeData, TradesChart

from pymongo import MongoClient, database
import logs
from data_handling.data_helpers.vars_constants import DBQueryOperators, DEFAULT_COL_SEARCH, \
    FINISH_TS_VALIDATOR_DB_SUFFIX, \
    VALIDATOR_DB, FINISH_TS, START_TS, START_TS_VALIDATOR_DB_SUFFIX, TEN_SECONDS_IN_MS, \
    ONE_DAY_IN_MINUTES, VALID_END_TS_VALIDATOR_DB_SUFFIX, VALID_END_TS, DONE_INTERVAL_VALIDATOR_DB_SUFFIX, \
    BASE_TRADES_CHART_DB, DEFAULT_PARSE_INTERVAL_IN_MS, ONE_DAY_IN_MS

localhost = '0.0.0.0:27017/'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)

mongo_client = MongoClient(host="172.23.224.1", maxPoolSize=0) #TODO: this ip should be found automatically..


class InvalidOperationForGivenClass(Exception): pass
class InvalidDocumentKeyProvided(Exception): pass
class InvalidDBMapperConfiguration(Exception): pass
class InvalidDataProvided(Exception): pass
class EmptyDBCol(Exception): pass
class MissingTimeInterval(Exception): pass
class InvalidStartTimestamp(Exception): pass
class InvalidInterval(Exception): pass
class InvalidResultsNumber(Exception): pass
class InvalidAtomicityDataType(Exception): pass


TimeseriesMinMax = namedtuple("TimeseriesMinMax", ["range_lower_bound", "range_higher_bound"])


@dataclass
class OneOrTenSecsMSMultiple:
    seconds_interval: int

    def __post_init__(self):
        if self.seconds_interval != 1 and self.seconds_interval % DEFAULT_PARSE_INTERVAL_IN_MS != 0:
            LOG.error(f"Invalid value provided, must be multiple of {DEFAULT_PARSE_INTERVAL_IN_MS}.")
            raise InvalidInterval(f"Invalid value provided, must be multiple of {DEFAULT_PARSE_INTERVAL_IN_MS}.")


@dataclass
class Index:
    document: str
    unique: bool


@dataclass
class DBData:
    db_timeframe_index: Index
    atomicity: OneOrTenSecsMSMultiple = DEFAULT_PARSE_INTERVAL_IN_MS


_trades_chart_index = Index('end_ts', True)
_trades_chart_db_data_default_parse_interval = DBData(_trades_chart_index, DEFAULT_PARSE_INTERVAL_IN_MS)
_relative_volume_db_data = DBData(Index('timestamp', True), DEFAULT_PARSE_INTERVAL_IN_MS)


class TradesChartTimeframes(Enum):
    ONE_HOUR = 60
    TWO_HOURS = 120
    FOUR_HOURS = 240
    EIGHT_HOURS = 480
    ONE_DAY = 1440
    TWO_DAYS = 2880
    FOUR_DAYS = 5760
    EIGHT_DAYS = 11520


class DBMapper(Enum):
    ten_seconds_parsed_trades = DBData(Index('timestamp', True))
    parsed_aggtrades = DBData(Index('timestamp', False), 1)  # one is a valid value, ignore highlight.
    trades_chart_60_minutes = _trades_chart_db_data_default_parse_interval
    trades_chart_120_minutes = _trades_chart_db_data_default_parse_interval
    trades_chart_240_minutes = _trades_chart_db_data_default_parse_interval
    trades_chart_480_minutes = _trades_chart_db_data_default_parse_interval
    trades_chart_1440_minutes = DBData(_trades_chart_index, DEFAULT_PARSE_INTERVAL_IN_MS * 2)
    trades_chart_2880_minutes = DBData(_trades_chart_index, DEFAULT_PARSE_INTERVAL_IN_MS * 6)
    trades_chart_5760_minutes = DBData(_trades_chart_index, DEFAULT_PARSE_INTERVAL_IN_MS * 12)
    trades_chart_11520_minutes = DBData(_trades_chart_index, DEFAULT_PARSE_INTERVAL_IN_MS * 48)
    relative_volume_60_minutes = _relative_volume_db_data
    relative_volume_120_minutes = _relative_volume_db_data
    relative_volume_240_minutes = _relative_volume_db_data
    relative_volume_480_minutes = _relative_volume_db_data
    relative_volume_1440_minutes = _relative_volume_db_data
    total_volume_60_minutes = DBData(Index('timestamp', True), DEFAULT_PARSE_INTERVAL_IN_MS * 360)
    total_volume_1440_minutes = DBData(Index('timestamp', True))


class DB(pymongo.database.Database, metaclass=ABCMeta):
    def __init__(self, db_name):
        self.db_name = db_name
        super().__init__(mongo_client, self.db_name)

        if not isinstance(self, ValidatorDB) and db_name != VALIDATOR_DB:
            self.end_ts = ValidatorDB(self.db_name).finish_ts

        self.timestamp_doc_key = None
        with contextlib.suppress(KeyError):
            self.timestamp_doc_key = DBMapper.__getitem__(self.db_name).value.db_timeframe_index.document

        self.atomicity = None
        with contextlib.suppress(KeyError):
            self.atomicity = DBMapper.__getitem__(self.db_name).value.atomicity

    def __getattr__(self, collection):
        return DBCol(self, collection)

    def clear_higher_than(self, timestamp: int, document_key: str) -> bool:
        for symbol in self.list_collection_names():
            getattr(self, symbol).delete_many({document_key: {DBQueryOperators.HIGHER_EQ.value: timestamp}})
        return True

    def delete_collections_with_text(self, text) -> bool:
        for col in self.list_collection_names():
            if text in col:
                self.drop_collection(col)
        return True

    def clear_collections_between(self, lower_bound, higher_bound, document_key=None) -> bool:
        for col in self.list_collection_names():
            getattr(self, col).clear_between(lower_bound, higher_bound, document_key=document_key)
        return True


class DBCol(pymongo.collection.Collection, metaclass=ABCMeta):
    USE_INTERNAL_MAPPED_TIMESTAMP = object()

    def __init__(self, db_instance_or_name: [str, DB], collection=DEFAULT_COL_SEARCH):
        self.db_name = db_instance_or_name if isinstance(db_instance_or_name, str) else db_instance_or_name.db_name
        try:
            self._timestamp_doc_key = DBMapper.__getitem__(self.db_name).value.db_timeframe_index.document
        except KeyError:
            self._timestamp_doc_key = None

        self._collection = collection
        super().__init__(db_instance_or_name if isinstance(db_instance_or_name, DB) else DB(self.db_name), self._collection)

    def column_between(self, lower_bound, higher_bound, doc_key=USE_INTERNAL_MAPPED_TIMESTAMP, limit=0, sort_value=pymongo.DESCENDING,
                       ReturnType: Union[Optional, TradesChart, TradeData] = None) -> Iterator:
        if not self.find_one({}):
            LOG.error("Queried Collection '%s' does not exist for db '%s'.", self._collection, self.db_name)
            raise EmptyDBCol(f"Queried Collection '{self._collection}' does not exist for db '{self.db_name}'.")

        if doc_key == self.USE_INTERNAL_MAPPED_TIMESTAMP:
            doc_key = self._timestamp_doc_key
            if not doc_key:
                LOG.error("Document key needs to be provided as default internal one is not valid.")
                raise InvalidDocumentKeyProvided("Document key needs to be provided as default internal one is not valid.")

        for res in self.find_timeseries(TimeseriesMinMax(lower_bound, higher_bound)).sort(doc_key, sort_value * -1).limit(limit):
            yield res if not ReturnType else ReturnType(**res)

    def most_recent_timeframe(self, document_key=None) -> int:
        return self._doc_key_endpoint(True, document_key) if document_key else self._doc_key_endpoint(True)

    def oldest_timeframe(self, document_key=None) -> int:
        return self._doc_key_endpoint(False, document_key) if document_key else self._doc_key_endpoint(False)

    def all_tf_column(self, column_name):
        return self.column_between(self.oldest_timeframe(column_name), self.most_recent_timeframe(column_name), column_name)

    def _doc_key_endpoint(self, most_recent: bool, doc_key=None) -> int:
        doc_key = doc_key if doc_key else self._timestamp_doc_key
        try:
            endpoints = next(self.column_between(0, get_current_second_in_ms(), doc_key=doc_key, limit=1))[doc_key], \
                        next(self.column_between(0, get_current_second_in_ms(), doc_key=doc_key, limit=1, sort_value=1))[doc_key]
        except StopIteration as e:
            LOG.error("Collection '%s' from database '%s' contains no data with key '%s'.", self._collection, self.db_name, doc_key)
            raise InvalidDataProvided(f"Collection '{self._collection}' from database '{self.db_name}' contains no data with key '{doc_key}'.") from e

        return max(endpoints) if most_recent else min(endpoints)

    def find_one_column(self, column_name):
        try:
            return self.find_one()[column_name]
        except KeyError:
            return None

    def _init_indexes(self):
        if self._timestamp_doc_key and not self.find_one({}):  # find_one() assumes indexes were correctly initialized on first run.
            self.create_index([(self._timestamp_doc_key, -1)],
                              unique=DBMapper.__getitem__(self.db_name).value.db_timeframe_index.unique)

    def find_all(self):
        return super().find({})

    def delete_all(self):
        self.delete_many({})

    def and_query(self, lower_bound, higher_bound, doc_key=None):
        if not doc_key and not (doc_key := self._timestamp_doc_key):
            LOG.error("Internal Document key needs to be provided as default internal one is not valid.")
            raise InvalidDocumentKeyProvided("Document key needs to be provided as default internal one is not valid.")

        return super().find({DBQueryOperators.AND.value: [{doc_key: {DBQueryOperators.HIGHER_EQ.value: lower_bound}},
                                                          {doc_key: {DBQueryOperators.LOWER_EQ.value: higher_bound}}]})

    def find_timeseries_one(self, timestamp_to_query: int):
        try:
            if self.database.atomicity == DEFAULT_PARSE_INTERVAL_IN_MS:
                return next(super().find({self._timestamp_doc_key: timestamp_to_query}))
            else:
                return next(self.and_query(timestamp_to_query - self.database.atomicity, timestamp_to_query))
        except StopIteration:
            err_message = f"No valid timestamp value {(datetime.fromtimestamp(timestamp_to_query / 1000))} from db {self.db_name} and collection {self.name}"
            LOG.error(err_message)
            raise InvalidDataProvided(err_message)

    def find_timeseries(self, timestamps_to_query: [int, TimeseriesMinMax, List, range]):
        if self.database.atomicity == 1 and not isinstance(timestamps_to_query, TimeseriesMinMax):
            LOG.error("For timestamps of atomicity of 1 use 'TimeseriesMinMax'.")
            raise InvalidAtomicityDataType("For timestamps of atomicity of 1 use 'TimeseriesMinMax'.")
        elif isinstance(timestamps_to_query, int):
            return self.find_timeseries_one(timestamps_to_query)
        elif isinstance(timestamps_to_query, TimeseriesMinMax):
            return self.and_query(timestamps_to_query.range_lower_bound, timestamps_to_query.range_higher_bound)
        elif self.database.atomicity != DEFAULT_PARSE_INTERVAL_IN_MS:
            return super().find({self._timestamp_doc_key: {DBQueryOperators.IN.value: range(
                timestamps_to_query.start, timestamps_to_query.stop, 30000)}})

        timestamps_to_query = timestamps_to_query if isinstance(timestamps_to_query, list) else [*timestamps_to_query]
        query = {self._timestamp_doc_key: {DBQueryOperators.IN.value: timestamps_to_query}}
        timestamps_count = len(timestamps_to_query)
        query_count = self.count_documents(query)

        if query_count != timestamps_count and query_count < int(timestamps_count / (self.database.atomicity / DEFAULT_PARSE_INTERVAL_IN_MS)):
            LOG.error(f"Invalid number of results retrieved, expected {timestamps_count} and got {query_count}.")
            raise InvalidResultsNumber(f"Invalid number of results retrieved, expected {timestamps_count} and got {query_count}.")
        return super().find(query)

    def find(self, *args, **kwargs):
        if args[0]:
            raise NotImplementedError("Find method is not intended to be used in this object, "
                                      "only to find 'all' or 'any one' document, use 'find_timeseries' instead.")
        else:
            return super().find(args[0])  # 'find_all' uses 'find' with null args[0].

    def insert_one(self, data):
        time.sleep(0.15)  # Multiple connections socket saturation delay.
        self._init_indexes()
        return super().insert_one(data)

    def insert_many(self, data):
        time.sleep(0.15)  # Multiple connections socket saturation delay.
        self._init_indexes()
        return super().insert_many(data)

    def clear_between(self, lower_bound, higher_bound, document_key=None):
        if not document_key and not (document_key := self._timestamp_doc_key):
            LOG.error("Timestamp document key needs to be provided.")
            raise InvalidDocumentKeyProvided("Timestamp document key needs to be provided.")
        self.delete_many({DBQueryOperators.AND.value: [{document_key: {DBQueryOperators.HIGHER_EQ.value: lower_bound}},
                                                       {document_key: {DBQueryOperators.LOWER_EQ.value: higher_bound}}]})


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

        self.finish_ts = finish_ts_data[FINISH_TS] if finish_ts_data else None
        self.start_ts = start_ts_data[START_TS] if start_ts_data else None

    def __getattr__(self, item):
        if item not in dir(self):
            LOG.info("Given class does not contain requested '%s' attribute.", item)
            raise InvalidOperationForGivenClass(f"Given class does not contain requested '{item}' attribute.")

    def set_finish_ts(self, finish_ts):
        if not self.finish_ts_col_conn.find_one():  # init validator db end_ts.
            self.finish_ts_col_conn.insert_one({FINISH_TS: finish_ts})

        if finish_ts > self.finish_ts_col_conn.find_one({})[FINISH_TS]:
            self.finish_ts_col_conn.update_one({}, {'$set': {FINISH_TS: finish_ts}})

    def set_start_ts(self, start_ts):
        if not self.start_ts_collection.find_one():  # init validator db start_ts.
            self.start_ts_collection.insert_one({START_TS: start_ts})

    def add_done_ts_interval(self, start_ts, end_ts):
        self.done_intervals_ts_collection.insert_one({"done_interval": [start_ts, end_ts]})


class AggtradesValidatorDB(ValidatorDB, ABC):
    def set_valid_timestamps(self):
        validate_db_default_col_conn = DBCol(self.validate_db_name, DEFAULT_COL_SEARCH)
        oldest_tf = validate_db_default_col_conn.oldest_timeframe()
        most_recent_timeframe = validate_db_default_col_conn.most_recent_timeframe()
        parse_from_ts = oldest_tf if not self.finish_ts else self.finish_ts
        end_ts_log_msg = f"End_ts ts set to " \
                         f"{datetime.fromtimestamp(most_recent_timeframe / 1000)} for db {self.validate_db_name}."
        if oldest_tf:
            for ts in range(parse_from_ts, most_recent_timeframe, TEN_SECONDS_IN_MS):
                try:
                    next(validate_db_default_col_conn.column_between(ts, ts + TEN_SECONDS_IN_MS))
                except StopIteration:
                    LOG.info(end_ts_log_msg)
                    self.set_finish_ts(most_recent_timeframe)
                    LOG.info("There are missing trades after '%s'.", ts)
                    raise StopIteration(("There are missing trades after '%s'.", ts))
            else:
                LOG.info(end_ts_log_msg)
                self.set_finish_ts(most_recent_timeframe)


class TradesChartValidatorDB(ValidatorDB, ABC):
    def __init__(self, trades_chart_timeframe: int):
        super().__init__(BASE_TRADES_CHART_DB.format(trades_chart_timeframe))

        self.trades_chart_timeframe = trades_chart_timeframe
        valid_end_ts_data = self.__getattr__(self.validate_db_name + VALID_END_TS_VALIDATOR_DB_SUFFIX).find_one()
        self.valid_end_ts = valid_end_ts_data[VALID_END_TS] if valid_end_ts_data else None

    def __getattr__(self, item):
        try:
            return self.__dict__[item]
        except KeyError:
            setattr(self, item, DBCol(self, self.validate_db_name + VALID_END_TS_VALIDATOR_DB_SUFFIX))
            return getattr(self, item)

    def set_valid_timestamps(self):
        time.sleep(2)
        time_intervals_start_ts = []
        time_intervals_end_ts = []
        for value in self.done_intervals_ts_collection.find_all():
            time_intervals_start_ts.append(value['done_interval'][0])
            time_intervals_end_ts.append(value['done_interval'][1])

        if not time_intervals_start_ts:
            return False
        else:
            start_ts, end_ts = min(time_intervals_start_ts), max(time_intervals_end_ts)
            possible_start_ts = [start_ts, start_ts - DEFAULT_PARSE_INTERVAL_IN_MS]
            if self.finish_ts and self.finish_ts not in possible_start_ts:
                err_msg = f"starting timestamp does not begin when finish_ts ends, invalid data provided. " \
                          f"finish ts {self.finish_ts} possible start ts {possible_start_ts}, db name {self.validate_db_name}"
                # For some reason sometimes there is a Ten secs gap between start_ts.
                LOG.error(err_msg)
                raise InvalidStartTimestamp(err_msg)

            if not self.start_ts:
                self.set_start_ts(end_ts)  # doesn't actually put the right start_ts, but inserts the end_ts instead.
                LOG.info(f"Start ts set to {end_ts} for db {self.validate_db_name}.")

        time_intervals_end_ts.sort()
        for end_ts_interval in time_intervals_end_ts:
            is_new_end_ts = end_ts_interval == end_ts
            different_threads_gap_ts = end_ts_interval + DEFAULT_PARSE_INTERVAL_IN_MS in time_intervals_start_ts
            end_ts_does_follows_start_ts = end_ts_interval + DB(self.validate_db_name).atomicity in time_intervals_start_ts
            if not end_ts_does_follows_start_ts and not is_new_end_ts and not different_threads_gap_ts:
                LOG.info(f"Missing time interval detected, setting end ts to {datetime.fromtimestamp(end_ts_interval / 1000)} "
                         f"for db {self.validate_db_name}. if this happens often please check.")
                end_ts = end_ts_interval
                self.set_finish_ts(end_ts)
                break
        else:
            self.set_finish_ts(end_ts)

        LOG.info(f"End_ts ts set to {datetime.fromtimestamp(end_ts / 1000)} for db {self.validate_db_name}.")
        self.done_intervals_ts_collection.delete_all()
        return True


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
    symbol_volumes = DBCol('total_volume_60_minutes', 'total_volume').find_timeseries_one(timestamp)['total_volume']  # Typing is not working here.
    return sorted(symbol_volumes, key=symbol_volumes.get, reverse=True)[4:top_count]  #its assumed the top 4 are BTC, ETH, FUND_DATA.


def list_dbs():
    return mongo_client.list_database_names()


def delete_dbs_all():
    for db_name in [deleteable for deleteable in list_dbs() if deleteable not in ['admin', 'config', 'local']]:
        mongo_client.drop_database(db_name)


def delete_dbs_with_text(text) -> None:
    for db in [dbs for dbs in list_dbs() if text in dbs]:
        mongo_client.drop_database(db)


def delete_collections_with_text(text) -> None:
    for db in list_dbs():
        DB(db).delete_collections_with_text(text)


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
                missing_timeframes.append(set(list(range(init_ts, init_ts + append_ts, TEN_SECONDS_IN_MS))) - parsed_tfs)
            init_ts += append_ts
        else:
            left_timeframe = end_ts - init_ts
            parsed_tfs = set([trade['start_ts'] for trade in DBCol(chart_tf_db, symbol).column_between(init_ts, init_ts + left_timeframe)])
            missing_timeframes.append(set(list(range(init_ts, init_ts + left_timeframe, TEN_SECONDS_IN_MS))) - parsed_tfs)

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


#create_index_db_cols('ten_seconds_parsed_trades', 'timestamp')
# create_index_db_cols(trades_chart.format(480), 'start_ts')
# create_index_db_cols(trades_chart.format(240), 'start_ts')
# create_index_db_cols(trades_chart.format(120), 'start_ts')
# create_index_db_cols(trades_chart.format(60), 'start_ts')
# create_index_db_cols(trades_chart.format(5), 'start_ts')

# create_index_db_cols('parsed_aggtrades', 'timestamp')

# delete_dbs_all()
# Tests:
# print("here")
# query_starting_ts('parsed_aggtrades', 'adausdt')
# insert_one_db('end_timestamp_aggtrades_validator_db', 'timestamp', {'timestamp': 1640955601009})

# delete_dbs_with_text("trades_chart")
# DB("Timestamps_Validator").delete_collections_with_text("trades_chart")
# delete_collections_with_text('EURUSDT')

# delete_dbs_with_text("relative_volume")
# DB("Timestamps_Validator").delete_collections_with_text("relative_volume")

# delete_dbs_with_text("ten_seconds")
# DB("Timestamps_Validator").delete_collections_with_text("ten_seconds")
# delete_dbs_with_text("fund_data")
# DB("Timestamps_Validator").delete_collections_with_text("fund_data")

# def query_existing_ws_trades(start_ts, end_ts, ms_parse_interval):  # If BTCUSDT has trades working it assumes all other symbols were working.
#     symbols_earliest_ts = min(list(start_ts.values()))
#     symbols_oldest_ts = max(list(end_ts.values()))
#
#     existing_trades = []
#     assume_existing_trade_parse_interval = mins_to_ms(3)
#     for elem in range(symbols_earliest_ts, symbols_oldest_ts, assume_existing_trade_parse_interval):
#         if query_db_col_between(PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, elem, elem + assume_existing_trade_parse_interval, limit=1):
#             existing_trades += list(range(elem, elem + assume_existing_trade_parse_interval, ms_parse_interval))
#
#     return existing_trades


def clear_trades_chart(timeframe):
    key = 'start_ts'
    if missing_timeframes := query_charts_missing_tf_intervals(timeframe):
        earliest_missing_tf = missing_timeframes[0][0]
        DB(BASE_TRADES_CHART_DB.format(timeframe)).clear_higher_than(earliest_missing_tf, key)
        print(f"Cleared db '{timeframe}' documents with key '{key}' higher than '{earliest_missing_tf}'.")

#clear_trades_chart(60)


def set_clear_trades_chart_valid_end_ts(timeframe: int):
    clear_trades_chart(timeframe)
    end_ts = DBCol(BASE_TRADES_CHART_DB.format(timeframe), DEFAULT_COL_SEARCH).most_recent_timeframe()
    trades_chart_val_db = TradesChartValidatorDB(BASE_TRADES_CHART_DB.format(timeframe))
    trades_chart_val_db.__getattr__(trades_chart_val_db.valid_end_ts_collection).delete_all()
    trades_chart_val_db.__getattr__(trades_chart_val_db.valid_end_ts_collection).insert_one({VALID_END_TS: end_ts})
    trades_chart_val_db.__getattr__(trades_chart_val_db.finish_ts_col_conn).delete_all()
    trades_chart_val_db.__getattr__(trades_chart_val_db.finish_ts_col_conn).insert_one({FINISH_TS: end_ts})


# trades_chart_timeframes = [60, 120, 240, 480, 1440, ONE_DAY_IN_MINUTES * 2, ONE_DAY_IN_MINUTES * 4, ONE_DAY_IN_MINUTES * 8]
# # #trades_chart_timeframes = [60]
# for tf in trades_chart_timeframes:
#     set_clear_trades_chart_valid_end_ts(tf)






















