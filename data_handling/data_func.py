import dataclasses
import logging
from datetime import datetime
from typing import Optional, List, Dict

from pymongo.errors import BulkWriteError

import logs
from support.decorators_extenders import init_only_existing
from data_handling.data_helpers.vars_constants import PRICE, QUANTITY, TS, DEFAULT_PARSE_INTERVAL, UNUSED_CHART_TRADE_SYMBOLS, \
    TEN_SECS_PARSED_TRADES_DB, PARSED_AGGTRADES_DB, MARKETCAP, DEFAULT_TIMEFRAME_IN_MS, \
    END_TS_AGGTRADES_VALIDATOR_DB, DEFAULT_PARSE_INTERVAL_IN_MS, TEN_SECONDS_IN_MS, FUND_DATA_COLLECTION, START_TS_AGGTRADES_VALIDATOR_DB, TRADE_DATA_CACHE_TIME_IN_MS, DEFAULT_SYMBOL_SEARCH, TRADE_DATA_PYTHON_CACHE_SIZE
from dataclasses import dataclass, asdict
from MongoDB.db_actions import DB, DBCol, ValidatorDB
from data_handling.data_helpers.data_staging import round_last_ten_secs


class InvalidStartTimestamp(Exception): pass


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
        for symbol, trades in self.symbol_parsed_trades.items():
            if trades:
                DBCol(PARSED_AGGTRADES_DB, symbol).insert_many(trades)

        if not ValidatorDB(PARSED_AGGTRADES_DB).start_ts:
            ValidatorDB(PARSED_AGGTRADES_DB).set_start_ts_add_index(self.start_ts, 'timestamp')
        ValidatorDB(PARSED_AGGTRADES_DB).set_end_ts(self.end_ts)

        self.symbol_parsed_trades.clear()
        return True


class CacheTradeData(dict):
    def __init__(self, db_name, symbols, ignore_dup_keys=False):
        super().__init__()
        self.db_name = db_name
        self.symbols = symbols
        self.cache_db = {}
        self.ignore_keys = ignore_dup_keys
        self._init_reset_cache()

    def __len__(self):
        return len(self.cache_db[DEFAULT_SYMBOL_SEARCH]) if self.cache_db else 0

    def _init_reset_cache(self):
        self.cache_db = {symbol: {} for symbol in self.symbols}

    def append_update(self, trade_taindicator_data):
        for symbol in self.symbols:
            save_trades = trade_taindicator_data[symbol].trades
            trade_taindicator_data[symbol].trades = []
            self.cache_db[symbol].update({str(len(self) + 1): dataclasses.asdict(trade_taindicator_data[symbol])})
            trade_taindicator_data[symbol].trades = save_trades

        if len(self) >= TRADE_DATA_PYTHON_CACHE_SIZE:
            self.insert_in_db_clear()
        return self

    def insert_in_db_clear(self):
        if not self.cache_db:
            LOG.debug("No trades to insert.")
            return

        for symbol, cached_trades in self.cache_db.items():
            try:
                DBCol(self.db_name, symbol).insert_many(list(cached_trades.values()))
            except BulkWriteError:
                LOG.warning("Duplicate key while trying to insert data in DB '%s' for symbol '%s'", self.db_name, symbol)
                continue

        start_ts = self.cache_db[DEFAULT_SYMBOL_SEARCH]['1']['start_ts']
        end_ts = self.cache_db[DEFAULT_SYMBOL_SEARCH][str(len(self.cache_db[DEFAULT_SYMBOL_SEARCH]))]['end_ts']

        validator_db = ValidatorDB(self.db_name)
        if not validator_db.start_ts:
            ValidatorDB(self.db_name).set_start_ts_add_index(start_ts, 'start_ts', unique=True)

        validator_db.set_end_ts(end_ts)
        self._init_reset_cache()

        LOG.info(f"Transformed data starting from {(datetime.fromtimestamp(start_ts / 1000))} to "
                 f"{datetime.fromtimestamp(end_ts / 1000)} for db {self.db_name}")


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


@dataclass
class Trades:
    def fill_trades_tf(self, start_ts, end_ts):
        for symbol_field in dataclasses.fields(self):
            base_filled_trades = {i: TradeData(None, 0, 0, i) for i in range(start_ts[symbol_field.name], end_ts[symbol_field.name] + 1, DEFAULT_PARSE_INTERVAL_IN_MS)}
            for trade in getattr(self, symbol_field.name):
                base_filled_trades[trade.timestamp] = trade
            self.__setattr__(symbol_field.name, list(base_filled_trades.values()))

        return self


@dataclass
class TradeDataGroup(Trades):
    def del_update_cache(self, chart_filtered_symbols):
        if len(getattr(self, DEFAULT_SYMBOL_SEARCH)) == 1:
            end_ts = getattr(self, DEFAULT_SYMBOL_SEARCH)[0].timestamp
            for symbol in dataclasses.fields(self):
                del getattr(self, symbol.name)[0]
            refresh_cache = get_trade_data_group(chart_filtered_symbols, end_ts + TEN_SECONDS_IN_MS,
                                                 end_ts + TRADE_DATA_CACHE_TIME_IN_MS, TEN_SECS_PARSED_TRADES_DB, filled=True)
            for symbol in dataclasses.fields(refresh_cache):
                setattr(self, symbol.name, getattr(refresh_cache, symbol.name))
        else:
            for symbol in dataclasses.fields(self):
                del getattr(self, symbol.name)[0]




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

    def fill_trades_tf(self, start_ts, end_ts):
        ts_trades = {i: TradeData(None, 0, 0, i) for i in range(start_ts, end_ts + 1, DEFAULT_PARSE_INTERVAL_IN_MS)}
        for trade in self.trades:
            ts_trades[trade.timestamp] = trade
        self.trades = [v for v in ts_trades.values()][::-1]

        return self


def get_trade_data_group(symbols: list, start_ts: [int, dict], end_ts, trades_db, filled: bool = False):
    start_ts = {symbol: start_ts for symbol in symbols} if not isinstance(start_ts, dict) else start_ts
    end_ts = {symbol: end_ts for symbol in symbols} if not isinstance(end_ts, dict) else end_ts

    trades = {symbol: list(DBCol(trades_db, symbol).column_between(start_ts[symbol], end_ts[symbol], ReturnType=TradeData)) for symbol in symbols}
    trade_data_group = dataclasses.make_dataclass('TradeDataGroup', [(symbol, dict) for symbol in symbols], bases=(TradeDataGroup,))(**trades)

    return trade_data_group.fill_trades_tf(start_ts, end_ts) if filled else trade_data_group


@dataclass
class TradesTAIndicators:
    start_ts: int
    end_ts: int
    trades: List[TradeData]
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    start_price: Optional[float] = None
    end_price: Optional[float] = None
    one_percent: Optional[float] = None
    range_price_min_max: Optional[dict] = None
    range_price_volume: Optional[dict] = None
    range_price_volume_difference: Optional[dict] = None
    price_range_percentage: Optional[float] = None
    total_volume: Optional[float] = None
    start_price_counter: Optional[int] = None
    end_price_counter: Optional[int] = None
    _distinct_trades: Optional[int] = None

    # On purpose, pycharm highligts dataclass type as non callable with Typing library.
    def __call__(self, *args, **kwargs):
        pass

    def __post_init__(self):
        from operator import itemgetter

        if not (aggregate_prices := [tf.price for tf in self.trades if tf.price]):
            self._distinct_trades = 0
            return  # No trades or one trade was done in this timeframe.
        else:
            self._distinct_trades = len(set(aggregate_prices))

        self.min_price = self._init_min_price = min(aggregate_prices)
        self.max_price = self._init_max_price = max(aggregate_prices)
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

    def __iadd__(self, trade_to_add):
        self.start_ts += DEFAULT_PARSE_INTERVAL_IN_MS
        self.end_ts += DEFAULT_PARSE_INTERVAL_IN_MS
        del self.trades[0]
        self.trades.append(trade_to_add)
        return TradesTAIndicators(**{'start_ts': self.start_ts, 'end_ts': self.end_ts, 'trades': self.trades})


class Trade:
    def __init__(self, symbols):
        self.ts_data = {}
        self.start_price = {}
        self.end_price = {}
        self.symbols = symbols
        self.timeframe = DEFAULT_TIMEFRAME_IN_MS
        self.ms_parse_interval = DEFAULT_PARSE_INTERVAL * 1000
        self.db_name = TEN_SECS_PARSED_TRADES_DB
        self.finished: bool = False
        validator_db = ValidatorDB(PARSED_AGGTRADES_DB)
        self._first_run_start_data = validator_db.start_ts
        self._finish_run_ts = validator_db.end_ts

        if not self._first_run_start_data:
            LOG.error(f"Starting value not found for {START_TS_AGGTRADES_VALIDATOR_DB}.")
            raise InvalidStartTimestamp(f"Starting value not found for {START_TS_AGGTRADES_VALIDATOR_DB}.")

        if not self._finish_run_ts:
            LOG.error(f"Starting value not found for {END_TS_AGGTRADES_VALIDATOR_DB}.")
            raise InvalidStartTimestamp(f"Starting value not found for {END_TS_AGGTRADES_VALIDATOR_DB}.")

        for symbol in self.symbols:
            self.start_price[symbol] = 0
            self.end_price[symbol] = 0
            self.ts_data[symbol] = {}

    def add_trades(self, symbols: list, start_ts: Dict[str, float], end_ts: Dict[str, float]):
        trade_data_group = get_trade_data_group(symbols, start_ts, end_ts, PARSED_AGGTRADES_DB)

        for symbol in symbols:
            for trade in getattr(trade_data_group, symbol):
                if t := trade.price:
                    self.end_price[symbol] = t
                    if not self.start_price[symbol]:
                        self.start_price[symbol] = t

                try:
                    tf_trades = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)]
                    tf_trades.price += (trade.price - tf_trades.price) * trade.quantity / (tf_trades.quantity + trade.quantity)
                except KeyError:
                    tf_trades = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)] = (TradeData(None, trade.price, 0, round_last_ten_secs(trade.timestamp)))
                    tf_trades.price = trade.price

                tf_trades.quantity += trade.quantity

        return self


class SymbolsTimeframeTrade(Trade):
    def __init__(self, start_ts: Dict[str, int] = None):
        symbols = set(DB(PARSED_AGGTRADES_DB).list_collection_names()) - set(UNUSED_CHART_TRADE_SYMBOLS)
        super().__init__(symbols)

        if not ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts:  # Init DB.
            self.start_ts = {symbol: ValidatorDB(PARSED_AGGTRADES_DB).start_ts for symbol in DB(PARSED_AGGTRADES_DB).list_collection_names()}
        elif start_ts:
            self.start_ts = start_ts
        else:
            self.start_ts = {}
            for symbol in symbols:
                start_ts = DBCol(self.db_name, symbol).most_recent_timeframe()
                self.start_ts[symbol] = start_ts + TEN_SECONDS_IN_MS if start_ts else ValidatorDB(PARSED_AGGTRADES_DB).start_ts

        self.end_ts = {symbol: self.start_ts[symbol] + self.timeframe for symbol in symbols}

        if max(self.end_ts[symbol] for symbol in symbols) > self._finish_run_ts:
            self.finished = True
        else:
            self.add_trades(self.symbols, self.start_ts, self.end_ts)

    def parse_and_insert_trades(self):
        if self.finished:
            return

        for symbol in self.symbols:
            if trades := [asdict(v) for v in self.ts_data[symbol].values()]:
                DBCol(self.db_name, symbol).insert_many(trades)

        validator_db = ValidatorDB(TEN_SECS_PARSED_TRADES_DB)
        if not validator_db.start_ts:
            validator_db.set_start_ts_add_index(min([self.start_ts[symbol] for symbol in self.symbols]), 'timestamp', unique=True)
        validator_db.set_end_ts(max([self.end_ts[symbol] for symbol in self.symbols]))

        LOG.info(f"Parsed 1 hour symbol pairs with a maximum start time of "
                 f"{datetime.fromtimestamp(max(self.start_ts[symbol] for symbol in self.symbols) / 1000)}.")


class FundTimeframeTrade(Trade):
    def __init__(self, ratio, start_ts: Optional[int] = None):
        from data_handling.data_helpers.vars_constants import FUND_SYMBOLS_USDT_PAIRS

        super().__init__(FUND_SYMBOLS_USDT_PAIRS)
        self.db_conn = DBCol(self.db_name, FUND_DATA_COLLECTION)

        if start_ts:
            self.start_ts = start_ts
        elif start_ts := ValidatorDB(FUND_DATA_COLLECTION).end_ts:
            self.start_ts = start_ts
        else:
            self.start_ts = ValidatorDB(PARSED_AGGTRADES_DB).start_ts

        self.end_ts = self.start_ts + self.timeframe
        self.ratios = ratio
        self.tf_marketcap_quantity = []

        if self.end_ts > self._finish_run_ts:
            self.finished = True
        else:
            self.add_trades(FUND_SYMBOLS_USDT_PAIRS, self.start_ts, self.end_ts)

    def parse_and_insert_trades(self):
        if self.finished:
            return

        for tf in range(self.start_ts, self.end_ts, self.ms_parse_interval):
            volume_traded, current_marketcap = 0, 0

            for symbol in self.symbols:
                try:
                    tf_trade = self.ts_data[symbol][tf]
                    current_marketcap += tf_trade.price * self.ratios[symbol][MARKETCAP] / self.ratios[symbol][PRICE]
                    volume_traded += tf_trade.price * tf_trade.quantity
                except KeyError:  # No trades done in this timeframe.
                    continue

            self.tf_marketcap_quantity.append({TS: tf, MARKETCAP: current_marketcap, QUANTITY: volume_traded})

        self.db_conn.insert_many(self.tf_marketcap_quantity)
        fund_validator_db_col = ValidatorDB(FUND_DATA_COLLECTION)
        if not fund_validator_db_col.start_ts:
            fund_validator_db_col.set_start_ts_add_index(self.start_ts, 'timestamp', unique=True)

        fund_validator_db_col.set_end_ts(self.end_ts)

        LOG.info(f"Parsed fund trades from {datetime.fromtimestamp(self.start_ts / 1000)} to "
                 f"{datetime.fromtimestamp((self.start_ts + self.timeframe) / 1000)}.")

