from __future__ import annotations

import logging
import time
from abc import ABCMeta, ABC
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union, Iterator, List, NamedTuple
from typing import TYPE_CHECKING
import pymongo

if TYPE_CHECKING:
    from data_handling.data_func import TradeData, TradesTAIndicators

from pymongo import MongoClient, database
import logs
from data_handling.data_helpers.data_staging import get_current_second_in_ms, mins_to_ms
from data_handling.data_helpers.vars_constants import MongoDB, DEFAULT_COL_SEARCH, END_TS_VALIDATOR_DB_SUFFIX, \
    VALIDATOR_DB, END_TS, START_TS, START_TS_VALIDATOR_DB_SUFFIX, TEN_SECS_PARSED_TRADES_DB, TEN_SECONDS_IN_MS, \
    ONE_DAY_IN_MINUTES

ATOMIC_TIMEFRAME_CHART_TRADES = 5
done_trades_chart_tf = "parsed_timestamp_trades_chart_{}_minutes"
trades_chart = 'trades_chart_{}_minutes'
trades_chart_base_db = trades_chart.format(ATOMIC_TIMEFRAME_CHART_TRADES)
localhost = 'localhost:27017/'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)

mongo_client = MongoClient(maxPoolSize=0)


class InvalidDocumentKeyProvided(Exception): pass
class InvalidDBMapperConfiguration(Exception): pass
class InvalidDataProvided(Exception): pass
class EmptyDBCol(Exception): pass


@dataclass
class Index:
    document: str
    unique: bool


class DBData(NamedTuple):
    db_name: str
    timeframe_index: Optional[Index]


class DBMapper(Enum):
    ten_seconds_parsed_trades = DBData('ten_seconds_parsed_trades', Index('timestamp', True))
    parsed_aggtrades = DBData('parsed_aggtrades', Index('timestamp', False))
    trades_chart_30_minutes = DBData('trades_chart_30_minutes', Index('start_ts', True))
    trades_chart_60_minutes = DBData('trades_chart_60_minutes', Index('start_ts', True))
    trades_chart_120_minutes = DBData('trades_chart_120_minutes', Index('start_ts', True))
    trades_chart_240_minutes = DBData('trades_chart_240_minutes', Index('start_ts', True))
    trades_chart_480_minutes = DBData('trades_chart_480_minutes', Index('start_ts', True))
    trades_chart_1440_minutes = DBData('trades_chart_1440_minutes', Index('start_ts', True))
    trades_chart_2880_minutes = DBData('trades_chart_2880_minutes', Index('start_ts', True))
    trades_chart_5760_minutes = DBData('trades_chart_5760_minutes', Index('start_ts', True))
    trades_chart_11520_minutes = DBData('trades_chart_11520_minutes', Index('start_ts', True))


class DBCol(pymongo.collection.Collection, metaclass=ABCMeta):
    USE_INTERNAL_MAPPED_TIMESTAMP = object()

    def __init__(self, db_name, collection=DEFAULT_COL_SEARCH):
        self.db_name = db_name
        self.collection = collection
        try:
            if tf_index := DBMapper.__getitem__(self.db_name).value.timeframe_index:
                self.timestamp_doc_key = tf_index.document
            else:
                self.timestamp_doc_key = tf_index
        except KeyError:
            LOG.error(f"DB '{self.db_name}' does not have mapped data, this is undesirable as it means no indexes will be created.")
            raise InvalidDBMapperConfiguration(f"DB '{self.db_name}' does not have mapped data, this is undesirable as it means no indexes will be created.")
        super().__init__(DB(self.db_name), self.collection)

    def column_between(self, lower_bound, higher_bound, doc_key=USE_INTERNAL_MAPPED_TIMESTAMP, limit=0, sort_value=pymongo.DESCENDING,
                       ReturnType: Union[Optional, TradesTAIndicators, TradeData] = None) -> Iterator:
        if not self.find_one({}):
            LOG.error(f"Queried Collection {self.collection}' does not exist for db '{self.db_name}'.")
            raise EmptyDBCol(f"Queried Collection '%s' does not exist for db '%s'.", self.collection, self.db_name)

        if doc_key == self.USE_INTERNAL_MAPPED_TIMESTAMP:
            doc_key = self.timestamp_doc_key
            if not doc_key:
                LOG.error("Document key needs to be provided as default internal one is not valid.")
                raise InvalidDocumentKeyProvided("Document key needs to be provided as default internal one is not valid.")

        for i in self.find({MongoDB.AND: [{doc_key: {MongoDB.HIGHER_EQ: lower_bound}},
                                          {doc_key: {MongoDB.LOWER_EQ: higher_bound}}]}).sort(
            doc_key, sort_value * -1).limit(
            limit):  # sort_value is the opposite of what we want, '* -1' solves this.
            yield i if not ReturnType else ReturnType(**i)

    def most_recent_timeframe(self, document_key=None) -> Optional[float]:
        return self._doc_key_endpoint(True,  document_key if document_key else self.timestamp_doc_key)

    def oldest_timeframe(self, document_key=None) -> Optional[float]:
        return self._doc_key_endpoint(False, document_key if document_key else self.timestamp_doc_key)

    def _doc_key_endpoint(self, most_recent: bool, doc_key) -> Optional[float]:
        try:
            endpoint_1 = next(self.column_between(0, get_current_second_in_ms(), doc_key=doc_key, limit=1, sort_value=-1))
            endpoint_2 = next(self.column_between(0, get_current_second_in_ms(), doc_key=doc_key, limit=1, sort_value=1))
        except StopIteration as e:
            LOG.error("Collection '%s' from database '%s' contains no data with key '%s'.", self.collection, self.db_name, doc_key)
            raise InvalidDataProvided(f"Collection '{self.collection}' from database '{self.db_name}' contains no data with key '{doc_key}'.") from e
        if endpoint_1 and endpoint_2:
            endpoint_tfs = endpoint_1[doc_key], endpoint_2[doc_key]
            try:
                return max(endpoint_tfs) if most_recent else min(endpoint_tfs)
            except TypeError:
                LOG.exception("Invalid document key values provided, values should be a float or int.")
                raise

    def find_one_column(self, column_name):
        try:
            return self.find_one()[column_name]
        except KeyError:
            return None

    def _init_indexes(self):
        if self.timestamp_doc_key and not self.find_one({}):
            self.create_index([(self.timestamp_doc_key, -1)],
                              unique=DBMapper.__getitem__(self.db_name).value.timeframe_index.unique)

    def insert_one(self, data):
        self._init_indexes()
        return super().insert_one(data)

    def insert_many(self, data):
        time.sleep(0.15)  # Multiple connections socket saturation delay.
        self._init_indexes()
        return super().insert_many(data)


class DB(pymongo.database.Database, metaclass=ABCMeta):
    def __init__(self, db_name):
        self.db_name = db_name
        super().__init__(mongo_client, self.db_name)
        if not isinstance(self, ValidatorDB):
            self.end_ts = ValidatorDB(self.db_name).end_ts


class ValidatorDB(DB, ABC):
    def __init__(self, db_to_validate_name):
        self.db_name = VALIDATOR_DB
        self.validate_db_name = db_to_validate_name
        self.end_ts_collection = self.validate_db_name + END_TS_VALIDATOR_DB_SUFFIX
        self.start_ts_collection = self.validate_db_name + START_TS_VALIDATOR_DB_SUFFIX

        super().__init__(self.db_name)
        end_ts_data = self.__getattr__(self.end_ts_collection).find_one()
        start_ts_data = self.__getattr__(self.start_ts_collection).find_one()

        self.end_ts = end_ts_data[END_TS] if end_ts_data else None
        self.start_ts = start_ts_data[START_TS] if start_ts_data else None

    def set_end_ts(self, end_ts):
        if not self.end_ts:  # init validator db end_ts.
            self.__getattr__(self.end_ts_collection).insert_one({END_TS: end_ts})

        if end_ts > self.__getattr__(self.end_ts_collection).find_one({})[END_TS]:
            self.__getattr__(self.end_ts_collection).update_one({}, {'$set': {END_TS: end_ts}})

    def set_start_ts(self, start_ts):
        self.__getattr__(self.start_ts_collection).insert_one({START_TS: start_ts})


def list_dbs():
    return mongo_client.list_database_names()


def ten_seconds_symbols_filled_data(symbols, start_ts, end_ts):
    from data_handling.data_func import TradesTAIndicators, get_trade_data_group
    trade_data = get_trade_data_group(symbols, start_ts, end_ts - 1, TEN_SECS_PARSED_TRADES_DB, filled=True)
    return {symbol: TradesTAIndicators(
        **{'trades': getattr(trade_data, symbol), 'start_ts': start_ts, 'end_ts': end_ts}) for symbol in symbols}


def delete_db(db_name) -> None:
    mongo_client.drop_database(db_name)


def delete_dbs_all():
    undeleteable = ['admin', 'config', 'local']
    for db_name in list_dbs():
        if db_name not in undeleteable:
            mongo_client.drop_database(db_name)


def delete_all_text_dbs(text) -> None:
    for db in list_dbs():
        if text in db:
            delete_db(db)


def query_missing_tfs(timeframe_in_minutes: int, symbols: List = [DEFAULT_COL_SEARCH]):
    chart_tf_db = trades_chart.format(timeframe_in_minutes)
    mins_to_validate_at_a_time = ONE_DAY_IN_MINUTES * 10
    accepted_trades_number = mins_to_validate_at_a_time * 6 + 1
    append_ts = mins_to_ms(mins_to_validate_at_a_time)

    missing_timeframes = []

    for symbol in symbols:
        possible_missing_tfs = {}
        init_ts = save_init_ts = DBCol(chart_tf_db, symbol).oldest_timeframe()
        end_ts = DBCol(chart_tf_db, symbol).most_recent_timeframe()

        while init_ts + append_ts <= end_ts:
            check_partial_trades = list(DBCol(chart_tf_db, symbol).column_between(init_ts, init_ts + append_ts))
            if len(check_partial_trades) != accepted_trades_number:
                parsed_tfs = set([trade['start_ts'] for trade in check_partial_trades])
                missing_tfs = set(list(range(init_ts, init_ts + append_ts, TEN_SECONDS_IN_MS))) - parsed_tfs
                possible_missing_tfs.setdefault(symbol, []).append(missing_tfs)
            time.sleep(0.1)
            init_ts += append_ts
        else:
            left_timeframe = end_ts - init_ts
            parsed_tfs = set([trade['start_ts'] for trade in DBCol(chart_tf_db, symbol).column_between(init_ts, init_ts + left_timeframe)])
            missing_tfs = set(list(range(init_ts, init_ts + left_timeframe, TEN_SECONDS_IN_MS))) - parsed_tfs
            possible_missing_tfs.setdefault(symbol, []).append(missing_tfs)

        concatenated_lists = []
        if possible_missing_tfs:
            for list_elem in possible_missing_tfs[symbol]:
                concatenated_lists += list(list_elem)

        started_tf = False
        for elem in range(save_init_ts, end_ts, TEN_SECONDS_IN_MS):
            if elem in concatenated_lists and not started_tf:
                started_tf = True
                init_miss_val = elem
            if elem not in concatenated_lists and started_tf:
                missing_timeframes.append([init_miss_val, elem - 10000])
                started_tf = False

    return missing_timeframes

#btc = query_missing_tfs(1440, ['BTCUSDT'])
# xtz = validate_fix_trade_data_dbs(1440, ['XTZUSDT'])
#print("here1")
# validate_fix_trade_data_dbs(480)
# print("here2")
# validate_fix_trade_data_dbs(240)
# print("here3")
# validate_fix_trade_data_dbs(120)
# print("here4")
# validate_fix_trade_data_dbs(60)
# print("here5")

# delete_all_text_dbs("chart")


def create_index_db_cols(db, field) -> None:
    db_conn = DB(db)
    for col in db_conn.list_collection_names():
        db_conn.__getattr__(col).create_index([(field, -1)], unique=True)
        print(f"Created index for collection {col}.")


# def query_duplicate_values(db_col: DBCol, document_key):
#     mins_to_validate_at_a_time = ONE_DAY_IN_MINUTES * 10
#     accepted_trades_number = mins_to_validate_at_a_time * 6 + 1
#     append_ts = mins_to_ms(mins_to_validate_at_a_time)
#
#     init_ts = db_col.oldest_timeframe()
#     end_ts = db_col.most_recent_timeframe()
#
#     trades_timestamp_count = {}
#     while init_ts + append_ts <= end_ts:
#         check_partial_trades = list(db_col.column_between(init_ts, init_ts + append_ts, 'timestamp'))
#         if len(check_partial_trades) != accepted_trades_number:
#             for trade in check_partial_trades:
#                 if trade['timestamp'] not in trades_timestamp_count:
#                     trades_timestamp_count[trade['timestamp']] = 1
#                 else:
#                     trades_timestamp_count[trade['timestamp']] += 1
#
#             trades_timestamp_count = {k: v for k, v in trades_timestamp_count.items() if v > 1}
#
#             init_ts += append_ts
#             #[trade['_id'].__str__() ]
#     else:
#         # REpeat one more time.. here.. todo
#         pass


#query_duplicate_values(DBCol('ten_seconds_parsed_trades', 'BTCUSDT'), 'timestamp')
#create_index_db_cols('ten_seconds_parsed_trades', 'timestamp')
# create_index_db_cols(trades_chart.format(480), 'start_ts')
# create_index_db_cols(trades_chart.format(240), 'start_ts')
# create_index_db_cols(trades_chart.format(120), 'start_ts')
# create_index_db_cols(trades_chart.format(60), 'start_ts')
# create_index_db_cols(trades_chart.format(5), 'start_ts')

# create_index_db_cols('parsed_aggtrades', 'timestamp')

#delete_dbs_all()
# Tests:
# print("here")
# query_starting_ts('parsed_aggtrades', 'adausdt')
# insert_one_db('end_timestamp_aggtrades_validator_db', 'timestamp', {'timestamp': 1640955601009})
# delete_all_text_dbs("chart")


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
