import copy
import logging
from datetime import datetime
from typing import Optional, Tuple
import logs
from support.decorators_extenders import init_only_existing
from support.data_handling.data_helpers.vars_constants import PRICE, QUANTITY, TS, DEFAULT_PARSE_INTERVAL_SECONDS, \
    UNUSED_CHART_TRADE_SYMBOLS, TEN_SECS_PARSED_TRADES_DB, PARSED_AGGTRADES_DB, MARKETCAP, DEFAULT_TEN_SECONDS_PARSE_TIMEFRAME_IN_MINUTES, \
    END_TS_AGGTRADES_VALIDATOR_DB, DEFAULT_PARSE_INTERVAL_IN_MS, FUND_DATA_COLLECTION, START_TS_AGGTRADES_VALIDATOR_DB, \
    DEFAULT_COL_SEARCH, TRADE_DATA_PYTHON_CACHE_SIZE
from dataclasses import dataclass, asdict, field
from MongoDB.db_actions import DB, DBCol, ValidatorDB, TradesChartValidatorDB, \
    BASE_TRADES_CHART_DB, OneOrTenSecsMSMultiple
from support.generic_helpers import round_last_ten_secs, mins_to_ms


class InvalidValidatorTimestamps(Exception): pass
class InvalidTradeTimestamp(Exception): pass

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class CacheAggtrades(dict):
    def __init__(self, start_ts, end_ts):
        super().__init__()
        self.symbol_parsed_trades = {}
        self.start_ts = start_ts
        self.end_ts = end_ts

    def __len__(self):
        length = 0
        for trades in self.symbol_parsed_trades.values():
            length += len(trades)
        return length

    def append(self, symbol, trades):
        self.symbol_parsed_trades[symbol] = [Aggtrade(**trade, **{'asdict': True}).data for trade in trades]

    def insert_in_db_clear(self):
        DB(PARSED_AGGTRADES_DB).clear_collections_between(self.start_ts, self.end_ts)
        for symbol, trades in self.symbol_parsed_trades.items():
            if trades:
                DBCol(PARSED_AGGTRADES_DB, symbol).insert_many(trades)
        ValidatorDB(PARSED_AGGTRADES_DB).add_done_ts_interval(self.start_ts, self.end_ts)

        self.symbol_parsed_trades.clear()
        return True

@init_only_existing
@dataclass
class Aggtrade:
    data: dict

    def _pre_init__(self, *args, **kwargs):
        try:
            if kwargs['asdict']:
                kwargs['data'] = {'ID': kwargs['a'], PRICE: float(kwargs['p']), QUANTITY: float(kwargs['q']), TS: int(kwargs['T'])}
        except KeyError:
            kwargs['data'] = TradeData(kwargs['a'], float(kwargs['p']), float(kwargs['q']), int(kwargs['T']))
        return args, kwargs


class TradeDataGroup:
    def __init__(self, timeframe_in_minutes: int, timestamp: int, trades_db: str, filled: bool,
                 symbols: [set, list], atomicity: int = DEFAULT_PARSE_INTERVAL_SECONDS):
        self.timeframe = timeframe_in_minutes
        self.timestamp = timestamp
        self.symbols_data_group = {}
        self.atomicity = atomicity

        start_ts = self.timestamp - mins_to_ms(self.timeframe)
        end_ts = self.timestamp + 1
        for symbol in symbols:
            trades = list(DBCol(trades_db, symbol).column_between(start_ts, end_ts, ReturnType=TradeData))
            if filled:
                empty_filled_trades = {i: TradeData(None, 0, 0, i) for i in range(start_ts, end_ts, DEFAULT_PARSE_INTERVAL_IN_MS)}
                for trade in trades:
                    empty_filled_trades[trade.timestamp] = trade
                trades = tuple(v for v in empty_filled_trades.values())
            else:
                trades = tuple(v for v in trades)
            if trades:
                self.symbols_data_group[symbol] = TradesChart(**{'trades': trades})

    def parse_trades_interval(self, future_trades, new_timestamp):
        number_of_trades_to_add = self.atomicity // DEFAULT_PARSE_INTERVAL_IN_MS

        for symbol, symbol_trade_info in self.symbols_data_group.items():
            trades_to_add = tuple(future_trades[symbol][self.timestamp + n * DEFAULT_PARSE_INTERVAL_IN_MS] for n in range(1, number_of_trades_to_add + 1))
            if trades_to_add[0].timestamp != self.symbols_data_group[symbol].trades[-1].timestamp + DEFAULT_PARSE_INTERVAL_IN_MS:
                LOG.error("Invalid timestamp of trades to be added provided.")
                raise InvalidTradeTimestamp("Invalid timestamp of trades to be added provided.")
            self.symbols_data_group[symbol].trades = self.symbols_data_group[symbol].trades[number_of_trades_to_add:] + trades_to_add
            self.symbols_data_group[symbol].refresh_obj_from_trades(self.atomicity)

        self.timestamp = new_timestamp


class CacheTradesChartData(dict):
    def __init__(self, timeframe):
        super().__init__()
        self.db_conn = DB(BASE_TRADES_CHART_DB.format(timeframe))
        self.validator_db_conn = TradesChartValidatorDB(timeframe)
        self.timeframe = timeframe
        self._cache_db = {}

    def insert_in_db_clear_cache(self):
        symbols = list(self._cache_db[1].symbols_data_group.keys())

        begin_ts = self._cache_db[1].timestamp
        end_ts = self._cache_db[len(self._cache_db)].timestamp

        self.db_conn.clear_collections_between(begin_ts, end_ts)

        insert_in_db = {symbol: [] for symbol in symbols}
        for symbol in symbols:
            for trade_data_group in self._cache_db.values():
                insert_in_db[symbol].append(trade_data_group.symbols_data_group[symbol])
            getattr(self.db_conn, symbol).insert_many([t.__dict__ for t in insert_in_db[symbol]])

        self.validator_db_conn.add_done_ts_interval(begin_ts, end_ts)
        self._cache_db = {}

        LOG.info(f"Transformed data starting from {(datetime.fromtimestamp(begin_ts / 1000))} to "
                 f"{datetime.fromtimestamp(end_ts / 1000)} for db {self.db_conn.db_name}")

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
    ID: Optional[float]
    price: float
    quantity: float
    timestamp: Optional[int]

    # On purpose, pycharm highligts dataclass type as non callable with Typing library.
    def __call__(self, *args, **kwargs):
        pass

    def _pre_init__(self, *args, **kwargs):
        if 'marketcap' in kwargs:
            kwargs['price'] = kwargs['marketcap']
            kwargs.pop('marketcap')

        if kwargs and 'ID' not in kwargs:
            kwargs['ID'] = None

        return args, kwargs


@dataclass
class TradesChart:
    trades: Tuple[TradeData]
    min_price: Optional[float] = None
    max_price: Optional[float] = None
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
    start_ts: int = field(init=False)
    end_ts: int = field(init=False)

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

    def refresh_obj_from_trades(self, parse_atomicity: int):
        verified_parse_atomicity = OneOrTenSecsMSMultiple(parse_atomicity)
        self.start_ts += verified_parse_atomicity.seconds_interval
        self.end_ts += verified_parse_atomicity.seconds_interval
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
        self.ms_parse_interval = DEFAULT_PARSE_INTERVAL_SECONDS * 1000
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

    def add_trades(self, symbols: list, timeframe: int, timestamp: int):
        trades_data_group = TradeDataGroup(timeframe, timestamp, PARSED_AGGTRADES_DB, False, symbols)
        untraded_symbols = []
        symbols_trades = {}
        for symbol in symbols:
            try:
                symbols_trades[symbol] = trades_data_group.symbols_data_group[symbol].trades
            except KeyError:
                untraded_symbols.append(symbol)

        for symbol in symbols:
            if symbol not in untraded_symbols:
                for trade in symbols_trades[symbol]:
                    if trade.price:
                        self.end_price[symbol] = trade.price
                        if not self.start_price[symbol]:
                            self.start_price[symbol] = trade.price
                    try:
                        tf_trades = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)]
                        tf_trades.price += (trade.price - tf_trades.price) * trade.quantity / (tf_trades.quantity + trade.quantity)
                    except KeyError:
                        tf_trades = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)] = (TradeData(None, trade.price, 0, round_last_ten_secs(trade.timestamp)))
                        tf_trades.price = trade.price

                    tf_trades.quantity += trade.quantity
        return self


class SymbolsTimeframeTrade(Trade):
    def __init__(self, timestamp: int = None):
        if not (symbols := [elem for elem in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if elem != 'fund_data']):
            symbols = set(DB(PARSED_AGGTRADES_DB).list_collection_names()) - set(UNUSED_CHART_TRADE_SYMBOLS)
        super().__init__(symbols)

        if timestamp:
            self.timestamp = timestamp
        elif not ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts:  # Init DB.
            self.timestamp = ValidatorDB(PARSED_AGGTRADES_DB).start_ts + mins_to_ms(self.timeframe)
        else:
            self.timestamp = DBCol(self.db_name, DEFAULT_COL_SEARCH).most_recent_timeframe()

        if self.timestamp > self._finish_run_ts:
            self.finished = True
            return

        self.start_ts = self.timestamp - mins_to_ms(self.timeframe)
        self.end_ts = self.timestamp

        self.add_trades(self.symbols, self.timeframe, self.timestamp)

    def parse_and_insert_trades(self):
        if self.finished:
            return

        for symbol in self.symbols:
            if trades := [asdict(v) for v in self.ts_data[symbol].values()]:
                db_symbol_conn = DBCol(self.db_name, symbol)
                db_symbol_conn.clear_between(self.start_ts, self.end_ts)
                db_symbol_conn.insert_many(trades)

        if not (validator_db := ValidatorDB(TEN_SECS_PARSED_TRADES_DB)).start_ts:
            validator_db.set_start_ts(self.start_ts)
        validator_db.set_finish_ts(self.end_ts)

        LOG.info(f"Parsed 1 hour symbol pairs with a start time of {datetime.fromtimestamp(self.start_ts / 1000)} "
                 f"and endtime of {datetime.fromtimestamp(self.end_ts / 1000)}.")


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
            self.timestamp = ValidatorDB(PARSED_AGGTRADES_DB).start_ts

        self.start_ts = self.timestamp - mins_to_ms(self.timeframe)
        self.end_ts = self.timestamp

        self.ratios = ratio
        self.tf_marketcap_quantity = []

        if self.end_ts > self._finish_run_ts:
            self.finished = True
        else:
            self.add_trades(FUND_SYMBOLS_USDT_PAIRS, self.timeframe, self.timestamp)

    def parse_and_insert_trades(self):
        for tf in range(self.start_ts, self.end_ts, self.ms_parse_interval):
            volume_traded, current_marketcap = 0, 0

            for symbol in self.symbols:
                try:
                    tf_trade = self.ts_data[symbol][tf]
                    current_marketcap += tf_trade.price * self.ratios[symbol]['price_weight']
                    volume_traded += tf_trade.price * tf_trade.quantity
                except KeyError:  # No trades done in this timeframe.
                    continue

            self.tf_marketcap_quantity.append({TS: tf, MARKETCAP: current_marketcap, QUANTITY: volume_traded})

        fund_validator_db_col = ValidatorDB(self.db_name)

        self.db_conn.clear_between(self.start_ts, self.end_ts)
        self.db_conn.insert_many(self.tf_marketcap_quantity)
        if not fund_validator_db_col.start_ts:
            fund_validator_db_col.set_start_ts(self.start_ts)
        fund_validator_db_col.set_finish_ts(self.end_ts)

        LOG.info(f"Parsed fund trades from {datetime.fromtimestamp(self.start_ts / 1000)} to "
                 f"{datetime.fromtimestamp(self.end_ts / 1000)}.")

