import copy
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple
import logs
from support.decorators_extenders import init_only_existing
from support.data_handling.data_helpers.vars_constants import (PRICE, QUANTITY, TS, DEFAULT_PARSE_INTERVAL_SECONDS, \
    UNUSED_CHART_TRADE_SYMBOLS, TEN_SECS_PARSED_TRADES_DB, PARSED_AGGTRADES_DB, MARKETCAP, DEFAULT_TEN_SECONDS_PARSE_TIMEFRAME_IN_MINUTES, \
    END_TS_AGGTRADES_VALIDATOR_DB, FUND_DATA_COLLECTION, START_TS_AGGTRADES_VALIDATOR_DB, TRADE_DATA_PYTHON_CACHE_SIZE, TEN_SECONDS, DEFAULT_PARSE_INTERVAL_TIMEDELTA)
from dataclasses import dataclass, field, fields
from MongoDB.db_actions import DB, DBCol, ValidatorDB, TradesChartValidatorDB, \
    BASE_TRADES_CHART_DB
from support.generic_helpers import datetime_range


class InvalidValidatorTimestamps(Exception): pass
class InvalidTradeTimestamp(Exception): pass


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class CacheAggtrades(dict):
    def __init__(self, start_ts, end_ts):
        super().__init__()
        self.symbol_parsed_trades = {}
        self.start_datetime = datetime.fromtimestamp(start_ts)
        self.end_datetime = datetime.fromtimestamp(end_ts)

    def __len__(self):
        length = 0
        for trades in self.symbol_parsed_trades.values():
            length += len(trades)
        return length

    def append(self, symbol, trades):
        self.symbol_parsed_trades[symbol] = [Aggtrade(**trade, **{'asdict': True}).data for trade in trades]

    def insert_in_db_clear(self):
        DB(PARSED_AGGTRADES_DB).clear_collections_between(self.start_datetime, self.end_datetime)
        for symbol, trades in self.symbol_parsed_trades.items():
            if trades:
                DBCol(PARSED_AGGTRADES_DB, symbol).insert_many(trades)
        ValidatorDB(PARSED_AGGTRADES_DB).add_done_ts_interval(self.start_datetime, self.end_datetime)

        self.symbol_parsed_trades.clear()
        return True


@init_only_existing
@dataclass
class Aggtrade:
    data: dict

    def _pre_init__(self, *args, **kwargs):
        kwargs['data'] = {'metadata': {PRICE: float(kwargs['p']), QUANTITY: float(kwargs['q'])}, TS: datetime.fromtimestamp(int(kwargs['T'] / 1000))}
        return args, kwargs


class TradeDataGroup:
    def __init__(self, timeframe: int, timestamp: datetime, trades_db: str, filled: bool,
                 symbols: [set, list], atomicity: timedelta = DEFAULT_PARSE_INTERVAL_TIMEDELTA):
        self.timeframe = timeframe
        self.timestamp = timestamp
        self.symbols_data_group = {}
        self.atomicity = atomicity

        start_ts = self.timestamp - timedelta(minutes=self.timeframe)
        end_ts = self.timestamp + timedelta(seconds=1)

        for symbol in symbols:
            trades = DBCol(trades_db, symbol).column_between(start_ts, end_ts, ReturnType=TradeData)
            if filled:
                empty_filled_trades = {i: TradeData(0, 0, i) for i in datetime_range(start_ts, end_ts, DEFAULT_PARSE_INTERVAL_TIMEDELTA)}
                for trade in trades:
                    empty_filled_trades[trade.timestamp] = trade
                trades = tuple(v for v in empty_filled_trades.values())
            else:
                trades = tuple(v for v in trades)
            if trades:
                self.symbols_data_group[symbol] = TradesChart(**{'trades': trades})

    def parse_trades_interval(self, future_trades, new_timestamp):
        number_of_trades_to_add = self.atomicity.seconds // DEFAULT_PARSE_INTERVAL_SECONDS

        for symbol, symbol_trade_info in self.symbols_data_group.items():
            trades_to_add = tuple(future_trades[symbol][self.timestamp + (n + 1) * DEFAULT_PARSE_INTERVAL_TIMEDELTA] for n in range(0, number_of_trades_to_add))
            if trades_to_add[0].timestamp != self.symbols_data_group[symbol].trades[-1].timestamp + DEFAULT_PARSE_INTERVAL_TIMEDELTA:
                LOG.error("Invalid timestamp of trades to be added provided.")
                raise InvalidTradeTimestamp("Invalid timestamp of trades to be added provided.")
            self.symbols_data_group[symbol].trades = self.symbols_data_group[symbol].trades[number_of_trades_to_add:] + trades_to_add
            self.symbols_data_group[symbol].refresh_obj_from_trades(self.atomicity)

        self.timestamp = new_timestamp

    def convert_to_db_timeseries(self):
        symbols_timeseries = {}
        for symbol in list(self.symbols_data_group.keys()):
            timeseries = {TS: self.symbols_data_group[symbol].end_ts, 'metadata': {
                t.name: getattr(self.symbols_data_group[symbol], t.name) for t in fields(TradesChart) if t.name != 'trades'}}
            symbols_timeseries[symbol] = timeseries
        return symbols_timeseries


class CacheTradesChartData(dict):
    def __init__(self, timeframe):
        super().__init__()
        self.db_conn = DB(BASE_TRADES_CHART_DB.format(timeframe))
        self.validator_db_conn = TradesChartValidatorDB(timeframe)
        self.timeframe = timeframe
        self._cache_db = {}

    def insert_in_db_clear_cache(self):
        begin_ts = self._cache_db[1].timestamp
        end_ts = self._cache_db[len(self._cache_db)].timestamp

        self.db_conn.clear_collections_between(begin_ts, end_ts)
        symbols = list(self._cache_db[1].symbols_data_group.keys())
        insert_in_db = {symbol: [] for symbol in list(self._cache_db[1].symbols_data_group.keys())}
        for trade_data_group in self._cache_db.values():
            timeseries_symbols = trade_data_group.convert_to_db_timeseries()

            for symbol in symbols:
                insert_in_db[symbol].append(timeseries_symbols[symbol])
        for symbol in symbols:
            getattr(self.db_conn, symbol).insert_many(insert_in_db[symbol])

        self.validator_db_conn.add_done_ts_interval(begin_ts, end_ts)
        self._cache_db = {}

        LOG.info(f"Transformed data starting from {begin_ts} to {end_ts} for db {self.db_conn.db_name}")

    def append_update_insert_in_db(self, trade_taindicator_data, timestamp):
        save_trades_temp = {}
        for symbol, taindicator_data in trade_taindicator_data.symbols_data_group.items():
            save_trades_temp[symbol] = taindicator_data.trades
            del taindicator_data.trades

        trade_taindicator_data.timestamp = timestamp
        self._cache_db[len(self._cache_db) + 1] = copy.deepcopy(trade_taindicator_data)

        for symbol, taindicator_data in trade_taindicator_data.symbols_data_group.items():
            taindicator_data.trades = save_trades_temp[symbol]

        if len(self._cache_db) >= TRADE_DATA_PYTHON_CACHE_SIZE:
            self.insert_in_db_clear_cache()

        return self


@init_only_existing
@dataclass
class TradeData:
    price: float
    quantity: float
    timestamp: datetime

    # On purpose, pycharm highligts dataclass type as non callable with Typing library.
    def __call__(self, *args, **kwargs):
        pass

    def _pre_init__(self, *args, **kwargs):
        if kwargs:
            try:
                kwargs['price'] = kwargs['metadata']['price']
            except KeyError as e:
                kwargs['price'] = kwargs['metadata']['marketcap']
            kwargs['quantity'] = kwargs['metadata']['quantity']

        return args, kwargs


@dataclass
class TradesChart:
    trades: Tuple[TradeData]
    start_price: Optional[float] = None
    end_price: Optional[float] = None
    one_percent: Optional[float] = None
    range_price_min_max: Optional[dict] = None
    range_price_volume: Optional[dict] = None
    range_price_volume_difference: Optional[dict] = None
    price_range_percentage: Optional[float] = None
    total_volume: Optional[float] = 0
    start_price_counter: Optional[int] = None
    end_price_counter: Optional[int] = None
    _distinct_trades: Optional[int] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    start_ts: datetime = field(init=False)
    end_ts: datetime = field(init=False)

    def __post_init__(self):
        from operator import itemgetter
        try:
            self.start_ts = self.trades[0].timestamp
        except Exception:
            raise
        self.end_ts = self.trades[-1].timestamp

        if not (aggregate_prices := [tf.price for tf in self.trades if tf.price]):
            self._distinct_trades = 0
            return  # No trades or one trade was done in this timeframe.
        else:
            self._distinct_trades = len(set(aggregate_prices))

        self.min_price, self.max_price = min(aggregate_prices), max(aggregate_prices)
        self.total_volume = sum([trade.quantity * trade.price for trade in self.trades])
        self.end_price = max([(tf.timestamp, tf.price) for tf in self.trades if tf.price], key=itemgetter(0))[1]
        self.start_price = min([(tf.timestamp, tf.price) for tf in self.trades if tf.price], key=itemgetter(0))[1]

        if self._distinct_trades == 1:
            return  # Same min and max value, no point calculating range percentages.

        self.one_percent = (self.max_price - self.min_price) / 100
        self.price_range_percentage = (self.max_price - self.min_price) * 100 / self.max_price
        self.parse_range_min_max()

        if self.price_range_percentage != 0 and (not self.start_price_counter or not self.range_price_volume_difference or not self.end_price_counter):
            self.end_price_counter = self.get_counter_factor_one_hundred(self.end_price)
            self.start_price_counter = self.get_counter_factor_one_hundred(self.start_price)
            self.range_price_volume_difference = {}

            self.range_price_volume = {}
            for i in range(100):
                self.range_price_volume[str(i + 1)] = {'volume_percentage': 0, 'sum_volume_percentage': 0}
            total_quantity = sum([tf.quantity for tf in self.trades])

            ratio = 100 / total_quantity

            for trade in self.trades:
                if trade.quantity:
                    self.range_price_volume[str(self.get_counter_factor_one_hundred(trade.price))]['volume_percentage'] += (trade.quantity * ratio)

            sum_quantity_percentage = 0
            for range_price in self.range_price_volume.values():
                sum_quantity_percentage += range_price['volume_percentage']
                range_price['sum_volume_percentage'] = sum_quantity_percentage

            self.range_price_volume_difference['rise_of_start_end_price_in_percentage'] = (100 - ((self.end_price * 100) / self.start_price)) * -1
            self.range_price_volume_difference['start_price_volume_percentage'] = self.range_price_volume[str(self.start_price_counter)]['sum_volume_percentage']
            self.range_price_volume_difference['end_price_volume_percentage'] = self.range_price_volume[str(self.end_price_counter)]['sum_volume_percentage']
            self.range_price_volume_difference['rise_of_start_end_volume_in_percentage'] = (self.range_price_volume_difference['start_price_volume_percentage'] -
                                                                                            self.range_price_volume_difference['end_price_volume_percentage']) * -1

    def get_counter_factor_one_hundred(self, trade_price):
        if trade_price == 0:
            return 1
        if (counter := int(((trade_price - self.min_price) // self.one_percent) + 1)) > 100:
            counter = 100
        return counter

    def parse_range_min_max(self):
        self.range_price_min_max = {}
        for i in range(100):
            self.range_price_min_max[str(i + 1)] = {'max': self.min_price + (self.one_percent * (i + 1)),
                                                    'min': self.min_price + (self.one_percent * i)}

    def refresh_obj_from_trades(self, parse_atomicity: timedelta):
        self.start_ts += parse_atomicity
        self.end_ts += parse_atomicity
        new_obj = TradesChart(**{'trades': self.trades})
        self.end_price = new_obj.end_price
        self.end_price_counter = new_obj.end_price_counter
        self.max_price = new_obj.max_price
        self.min_price = new_obj.min_price
        self.one_percent = new_obj.one_percent
        self.price_range_percentage = new_obj.price_range_percentage
        self.range_price_min_max = new_obj.range_price_min_max
        self.range_price_volume = new_obj.range_price_volume
        self.range_price_volume_difference = new_obj.range_price_volume_difference
        self.start_price = new_obj.start_price
        self.start_price_counter = new_obj.start_price_counter
        self.total_volume = new_obj.total_volume
        self._distinct_trades = new_obj._distinct_trades
        return True


class Trade:
    def __init__(self, symbols):
        self.ts_data = {}
        self.start_price = {}
        self.end_price = {}
        self.symbols = symbols
        self.timeframe = DEFAULT_TEN_SECONDS_PARSE_TIMEFRAME_IN_MINUTES
        self.parse_interval = DEFAULT_PARSE_INTERVAL_SECONDS
        self.db_name = TEN_SECS_PARSED_TRADES_DB
        self.finished: bool = False
        validator_db = ValidatorDB(PARSED_AGGTRADES_DB)
        self._first_run_start_data = validator_db.start_ts
        self._finish_run_ts = validator_db.finish_ts

        if not self._first_run_start_data:
            LOG.error(f"Starting value not found for {START_TS_AGGTRADES_VALIDATOR_DB}.")
            raise InvalidValidatorTimestamps(f"Starting value not found for {START_TS_AGGTRADES_VALIDATOR_DB}.")

        if not self._finish_run_ts:
            LOG.error(f"Starting value not found for {END_TS_AGGTRADES_VALIDATOR_DB}.")
            raise InvalidValidatorTimestamps(f"Starting value not found for {END_TS_AGGTRADES_VALIDATOR_DB}.")

        for symbol in self.symbols:
            self.start_price[symbol] = 0
            self.end_price[symbol] = 0
            self.ts_data[symbol] = {}

    def add_trades(self, symbols: list, timeframe: int, timestamp: datetime):
        trades_data_group = TradeDataGroup(timeframe, timestamp, PARSED_AGGTRADES_DB, False, symbols)
        untraded_symbols = []
        symbols_trades = {}
        for symbol in symbols:
            try:
                symbols_trades[symbol] = trades_data_group.symbols_data_group[symbol].trades
            except KeyError:
                untraded_symbols.append(symbol)

        traded_symbols = [symbol for symbol in symbols if symbol not in untraded_symbols]
        for symbol in traded_symbols:
            for timeseries in datetime_range(timestamp - timedelta(minutes=timeframe), timestamp, DEFAULT_PARSE_INTERVAL_TIMEDELTA):
                self.ts_data[symbol][str(timeseries)] = {PRICE: 0, QUANTITY: 0, TS: timeseries}

        for symbol in traded_symbols:
            for trade in symbols_trades[symbol]:
                if trade.price:
                    self.end_price[symbol] = trade.price
                    if not self.start_price[symbol]:
                        self.start_price[symbol] = trade.price

                rounded_last_ten_seconds_timestamp = str(trade.timestamp - timedelta(seconds=(trade.timestamp.second % TEN_SECONDS)))
                self.ts_data[symbol][rounded_last_ten_seconds_timestamp][PRICE] += \
                    ((trade.price - self.ts_data[symbol][rounded_last_ten_seconds_timestamp][PRICE]) *
                     trade.quantity / (self.ts_data[symbol][rounded_last_ten_seconds_timestamp][QUANTITY] + trade.quantity))
                self.ts_data[symbol][rounded_last_ten_seconds_timestamp][QUANTITY] += trade.quantity

        return self


class SymbolsTimeframeTrade(Trade):
    def __init__(self, timestamp: int = None):
        if not (symbols := [elem for elem in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if elem != 'fund_data']):
            symbols = set(DB(PARSED_AGGTRADES_DB).list_collection_names()) - set(UNUSED_CHART_TRADE_SYMBOLS)
        super().__init__(symbols)

        if timestamp:
            self.timestamp = timestamp
        elif start_ts := ValidatorDB(self.db_name).finish_ts:
            self.timestamp = start_ts
        else:
            self.timestamp = ValidatorDB(PARSED_AGGTRADES_DB).start_ts + timedelta(minutes=self.timeframe)

        self.start_ts = self.timestamp - timedelta(minutes=self.timeframe)
        self.end_ts = self.timestamp

        if self.timestamp > self._finish_run_ts:
            self.finished = True
        else:
            self.add_trades(self.symbols, self.timeframe, self.timestamp)

    def parse_and_insert_trades(self):
        if self.finished:
            return

        for symbol in self.symbols:
            trades = [{TS: values[TS], 'metadata': {PRICE: values[PRICE], QUANTITY: values[QUANTITY]}} for values in self.ts_data[symbol].values()]
            if trades:
                db_symbol_conn = DBCol(self.db_name, symbol)
                db_symbol_conn.clear_between(self.start_ts, self.end_ts)
                db_symbol_conn.insert_many(trades)

        if not (validator_db := ValidatorDB(TEN_SECS_PARSED_TRADES_DB)).start_ts:
            validator_db.set_start_ts(self.start_ts)
        validator_db.set_finish_ts(self.end_ts)

        LOG.info(f"Parsed 1 hour symbol pairs with a start time of {self.start_ts} and endtime of {self.end_ts}.")


class FundTimeframeTrade(Trade):
    def __init__(self, ratio, timestamp: Optional[int] = None):
        from support.data_handling.data_helpers.vars_constants import FUND_SYMBOLS_USDT_PAIRS

        try:
            super().__init__(FUND_SYMBOLS_USDT_PAIRS)
        except InvalidValidatorTimestamps:
            raise
        self.db_conn = DBCol(self.db_name, FUND_DATA_COLLECTION)

        if timestamp:
            self.timestamp = timestamp
        elif start_ts := ValidatorDB(self.db_name).finish_ts:
            self.timestamp = start_ts
        else:
            self.timestamp = ValidatorDB(PARSED_AGGTRADES_DB).start_ts + timedelta(minutes=self.timeframe)

        self.start_ts = self.timestamp - timedelta(minutes=self.timeframe)
        self.end_ts = self.timestamp

        self.ratios = ratio
        self.tf_marketcap_quantity = []

        if self.end_ts > self._finish_run_ts:
            self.finished = True
        else:
            self.add_trades(FUND_SYMBOLS_USDT_PAIRS, self.timeframe, self.timestamp)

    def parse_and_insert_trades(self):
        for tf in datetime_range(self.start_ts, self.end_ts, timedelta(seconds=self.parse_interval)):
            volume_traded, current_marketcap = 0, 0

            for symbol in self.symbols:
                tf_trade = self.ts_data[symbol][str(tf)]
                current_marketcap += tf_trade[PRICE] * self.ratios[symbol]['price_weight']
                volume_traded += tf_trade[PRICE] * tf_trade[QUANTITY]

            self.tf_marketcap_quantity.append({TS: tf, 'metadata': {MARKETCAP: current_marketcap, QUANTITY: volume_traded}})

        fund_validator_db_col = ValidatorDB(self.db_name)

        self.db_conn.clear_between(self.start_ts, self.end_ts)
        self.db_conn.insert_many(self.tf_marketcap_quantity)
        if not fund_validator_db_col.start_ts:
            fund_validator_db_col.set_start_ts(self.start_ts)
        fund_validator_db_col.set_finish_ts(self.end_ts)

        LOG.info(f"Parsed fund trades from {self.start_ts} to {self.end_ts}.")

