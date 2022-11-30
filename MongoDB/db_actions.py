from __future__ import annotations

import contextlib
import logging
import time
from abc import ABCMeta, ABC
from dataclasses import fields
from typing import Optional, Union, Iterator, Dict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_handling.data_func import TradeData, TradesTAIndicators

import pymongo
from pymongo import MongoClient, database

import logs

from data_handling.data_helpers.data_staging import round_last_ten_secs, get_current_second_in_ms, mins_to_ms
from data_handling.data_helpers.vars_constants import TS, MongoDB, DEFAULT_COL_SEARCH, END_TS_VALIDATOR_DB_SUFFIX, \
    VALIDATOR_DB, END_TS, START_TS, START_TS_VALIDATOR_DB_SUFFIX, TEN_SECS_PARSED_TRADES_DB, TEN_SECS_MS

ATOMIC_TIMEFRAME_CHART_TRADES = 5
done_trades_chart_tf = "parsed_timestamp_trades_chart_{}_minutes"


trades_chart = 'trades_chart_{}_minutes'
trades_chart_base_db = trades_chart.format(ATOMIC_TIMEFRAME_CHART_TRADES)

localhost = 'localhost:27017/'

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidDataProvided(Exception): pass


class EmptyDBCol(Exception): pass


class DBClient(MongoClient):
    def __init__(self):
        super().__init__()

    def delete_db(self, db_name):
        self.drop_database(db_name)

    def insert_one(self, db, collection, data):
        return self.__getattr__(db).__getattr__(collection).insert_one(data)

    def insert_many(self, db, collection, data):
        return self.__getattr__(db).__getattr__(collection).insert_many(data)


class DBCol(pymongo.collection.Collection, metaclass=ABCMeta):
    def __init__(self, db_name, collection=DEFAULT_COL_SEARCH):
        self.db_name = db_name
        self.collection = collection
        super().__init__(DB(self.db_name), self.collection)

    def column_between(self, lower_bound, higher_bound, doc_key, limit=0, sort_value=pymongo.DESCENDING,
                       ReturnType: Union[Optional, TradesTAIndicators, TradeData] = None) -> Iterator:
        for i in self.find({MongoDB.AND: [{doc_key: {MongoDB.HIGHER_EQ: lower_bound}},
                                          {doc_key: {MongoDB.LOWER_EQ: higher_bound}}]}).sort(
            doc_key, sort_value * -1).limit(
            limit):  # sort_value is the opposite of what we want, '* -1' solves this.
            yield i if not ReturnType else ReturnType(**i)

    def most_recent_timeframe(self, document_key=TS) -> Optional[float]:
        return self._doc_key_endpoint(True, document_key)

    def oldest_timeframe(self, document_key=TS) -> Optional[float]:
        return self._doc_key_endpoint(False, document_key)

    def _doc_key_endpoint(self, most_recent: bool, doc_key=TS) -> Optional[float]:
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

    def chart_endpoint_ts(self, most_recent: bool, init_db=None):
        try:
            return self._doc_key_endpoint(most_recent=most_recent, doc_key='start_ts')
        except EmptyDBCol as e:
            if init_db:
                with contextlib.suppress(EmptyDBCol):
                    return round_last_ten_secs(DBCol(init_db, self.collection)._doc_key_endpoint(False))

            raise InvalidDataProvided(f"symbol '{self.collection}' doesn't have a valid timestamp in db "
                                      f"'{self.db_name}' and no other db to initialize from.") from e

    def insert_many(self, data):
        time.sleep(0.15)  # Multiple connections socket saturation delay.
        return super().insert_many(data)


class DB(pymongo.database.Database, metaclass=ABCMeta):
    def __init__(self, db_name):
        self.db_name = db_name
        super().__init__(MongoClient(maxPoolSize=0), self.db_name)
        if not isinstance(self, ValidatorDB):
            self.end_ts = ValidatorDB(self.db_name).end_ts


class ValidatorDB(DB, ABC):
    def __init__(self, col_name):
        self.db_name = VALIDATOR_DB
        self.end_ts_collection = col_name + END_TS_VALIDATOR_DB_SUFFIX
        self.start_ts_collection = col_name + START_TS_VALIDATOR_DB_SUFFIX

        super().__init__(self.db_name)
        end_ts_data = self.__getattr__(self.end_ts_collection).find_one()
        start_ts_data = self.__getattr__(self.start_ts_collection).find_one()

        self.end_ts = end_ts_data[END_TS] if end_ts_data else None
        self.start_ts = start_ts_data[START_TS] if start_ts_data else None

    def set_end_ts(self, end_ts):
        self.__getattr__(self.end_ts_collection).find_one_and_update({}, {'$set': {END_TS: end_ts}})

    def set_start_ts(self, start_ts):
        self.__getattr__(self.start_ts_collection).insert_one({START_TS: start_ts})


def list_dbs():
    return DBClient().list_database_names()


def ten_seconds_symbols_filled_data(symbols, start_ts, end_ts):
    from data_handling.data_func import TradesTAIndicators, get_trade_data_group
    trade_data = get_trade_data_group(symbols, start_ts, end_ts - 1, TEN_SECS_PARSED_TRADES_DB, filled=True)
    symbols = [s.name for s in fields(trade_data)]
    return {
        symbol: TradesTAIndicators(**{'trades': getattr(trade_data, symbol), 'start_ts': start_ts, 'end_ts': end_ts})
        for symbol in symbols}


def delete_db(db_name) -> None:
    MongoClient().drop_database(db_name)


def delete_all_text_dbs(text) -> None:
    for db in list_dbs():
        if text in db:
            delete_db(db)


def validate_trade_data_dbs(timeframe_in_minutes: int):
    chart_tf_db = trades_chart.format(timeframe_in_minutes)
    mins_to_validate_at_a_time = 1440
    accepted_trades_number = mins_to_validate_at_a_time * 6 + 1
    validate_db = DB(trades_chart.format(timeframe_in_minutes))

    missing_elements = {}

    for symbol in validate_db.list_collection_names():
        save_missing_tfs = {}
        init_ts = DBCol(chart_tf_db, symbol).chart_endpoint_ts(False)
        save_init_ts = init_ts
        end_ts = DBCol(chart_tf_db, symbol).chart_endpoint_ts(True)
        append_ts = mins_to_ms(mins_to_validate_at_a_time)

        while init_ts + append_ts <= end_ts:
            check_partial_trades = list(DBCol(chart_tf_db, symbol).column_between(init_ts, init_ts + append_ts, 'start_ts'))
            if len(check_partial_trades) != accepted_trades_number:
                parsed_tfs = set([trade['start_ts'] for trade in check_partial_trades])
                tfs_to_parse = set(list(range(init_ts, init_ts + append_ts, TEN_SECS_MS)))
                missing_tfs = tfs_to_parse - parsed_tfs
                save_missing_tfs.setdefault(symbol, []).append(missing_tfs)
            time.sleep(0.1)
            init_ts += append_ts

        concatenated_lists = []
        for list_elem in save_missing_tfs[symbol]:
            concatenated_lists += list(list_elem)

        started_tf = False
        for elem in range(save_init_ts, end_ts, TEN_SECS_MS):
            if elem in concatenated_lists and not started_tf:
                started_tf = True
                init_miss_val = elem
            if elem not in concatenated_lists and started_tf:
                missing_elements.setdefault(symbol, []).append({(init_miss_val, elem)})
                started_tf = False
    if missing_elements:
        print(missing_elements)
        # TODO: add error handling.

#validate_trade_data_dbs(1440)
# validate_trade_data_dbs(480)
# validate_trade_data_dbs(240)
# validate_trade_data_dbs(120)
# validate_trade_data_dbs(60)

# delete_all_text_dbs("chart")

# def create_index_db_cols(db, field) -> None:
#     db_conn = DB(db)
#     for col in db_conn.list_collection_names():
#         db_conn.__getattr__(col).create_index([(field, -1)])
#         print(f"Created index for collection {col}.")
#
#
# create_index_db_cols('parsed_aggtrades', 'ID')
# create_index_db_cols('parsed_aggtrades', 'timestamp')


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
