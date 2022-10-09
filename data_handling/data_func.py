import logging
import time
from typing import Optional, List, Dict

import logs
from support.decorators_extenders import init_only_existing
from data_handling.data_helpers.vars_constants import PRICE, QUANTITY, SYMBOL, TS, DEFAULT_PARSE_INTERVAL, \
    PARSED_TRADES_BASE_DB, PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, FUND_DB, MARKETCAP, AGGTRADES_DB, \
    DEFAULT_TIMEFRAME_IN_MS, AGGTRADES_VALIDATOR_DB_TS, DEFAULT_PARSE_INTERVAL_IN_MS
from dataclasses import dataclass, asdict, field
from MongoDB.db_actions import insert_many_same_db_col, query_starting_ts, connect_to_db, insert_many_db, delete_db
from data_handling.data_helpers.data_staging import round_last_ten_secs


class InvalidParametersProvided(Exception): pass
class NoMoreParseableTrades(Exception): pass


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class CacheAggtrades(dict):
    def __init__(self):
        super().__init__()
        self.bundled_trades = []
        self.symbol_parsed_trades = {}

    def __len__(self):
        return len(self.bundled_trades)

    def append(self, trade):
        aggtrade = Aggtrade(**trade, **{'asdict': True})
        self.bundled_trades.append({aggtrade.symbol: aggtrade.data})
        self.symbol_parsed_trades.setdefault(aggtrade.symbol, []).append(aggtrade.data)

    def insert_clear(self, end_ts):
        for symbol, trades in self.symbol_parsed_trades.items():
            insert_many_db(PARSED_AGGTRADES_DB, symbol, trades)

        delete_db(AGGTRADES_VALIDATOR_DB_TS)
        connect_to_db(AGGTRADES_VALIDATOR_DB_TS).get_collection(TS).insert_one({TS: end_ts})

        insert_many_same_db_col(AGGTRADES_DB, self.bundled_trades)
        self.symbol_parsed_trades.clear()
        self.bundled_trades.clear()
        return True


@init_only_existing
@dataclass
class Aggtrade:
    symbol: str
    data: dict

    def _pre_init__(self, *args, **kwargs):
        kwargs[SYMBOL] = kwargs['s']
        try:
            if kwargs['asdict']:
                kwargs['data'] = {'ID': kwargs['a'], PRICE: float(kwargs['p']), QUANTITY: float(kwargs['q']), TS: int(kwargs['T'])}
        except KeyError:
            kwargs['data'] = TradeData(kwargs['a'], float(kwargs['p']), float(kwargs['q']), int(kwargs['T']))
        return args, kwargs


@init_only_existing
@dataclass
class TradeData:
    ID: Optional[float]
    price: float
    quantity: float
    timestamp: Optional[int]


@dataclass
class TradesGroup:
    trades: Dict[int, TradeData]
    start_ts: int
    end_ts: int
    min_price: float = field(init=False)
    max_price: float = field(init=False)
    most_recent_price: float = field(init=False)
    one_percent: float = field(init=False)
    range_price_volume: dict = field(init=False)
    price_range_percentage: float = field(init=False)
    total_volume: float = field(init=False)
    last_price_counter: int = field(init=False)
    _timestamps: List[int] = field(init=False)

    def __post_init__(self):
        self._timestamps = [tf.timestamp for tf in self.trades.values()]
        aggregate_prices = [tf.price for tf in self.trades.values()]
        self.min_price = min(aggregate_prices)
        self.max_price = max(aggregate_prices)
        self.one_percent = (self.max_price - self.min_price) / 100
        self.range_price_volume = {str(i + 1): {'max': self.min_price + (self.one_percent * (i + 1)),
                                                'min': self.min_price + (self.one_percent * i), QUANTITY: 0} for i in range(100)}
        self.price_range_percentage = (self.max_price - self.min_price) * 100 / self.max_price
        self.most_recent_price = self.trades[max(self._timestamps)].price
        self.total_volume = sum([trade.quantity * trade.price for trade in self.trades.values()])
        self.last_price_counter = self.get_counter_factor_one_hundred(self.most_recent_price)

    def get_counter_factor_one_hundred(self, trade_price):
        if (counter := int(((trade_price - self.min_price) // self.one_percent) + 1)) > 100:
            counter = 100
        return counter

    def fill_trades_tf(self, start_ts=None, end_ts=None):
        if not start_ts:
            start_ts = self.start_ts
        if not end_ts:
            end_ts = self.end_ts

        for tf in range(start_ts, end_ts + 1, DEFAULT_PARSE_INTERVAL_IN_MS):
            if tf not in self._timestamps:
                try:
                    self.trades[tf] = (TradeData(None, self.trades[tf - DEFAULT_PARSE_INTERVAL_IN_MS].price, 0, tf))
                except KeyError:
                    self.trades[tf] = self.trades[min(self._timestamps)]

        return self

    def asdict_without_keys(self, do_not_parse_keys: list):
        return {k: v for k, v in self.__dict__.items() if k not in do_not_parse_keys}


class Trade:
    def __init__(self, symbols=None):
        self.ts_data = {}
        self.start_price = {}
        self.end_price = {}
        self._symbols = symbols
        self.timeframe = DEFAULT_TIMEFRAME_IN_MS
        self.ms_parse_interval = DEFAULT_PARSE_INTERVAL * 1000
        self.start_ts = 0

        for symbol in self._symbols:
            self.start_price[symbol] = 0
            self.end_price[symbol] = 0
            self.ts_data[symbol] = {}

    def add_trades(self, symbol, symbol_trades):
        for trade in symbol_trades:
            if t := trade.price:
                self.end_price[symbol] = t
                if not self.start_price[symbol]:
                    self.start_price[symbol] = t

            try:
                tf_trades = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)]
            except KeyError as e:
                tf_trades = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)] = (TradeData(None, 0, 0, round_last_ten_secs(trade.timestamp)))
            if not tf_trades.quantity:
                tf_trades.price = trade.price
            else:
                tf_trades.price += (trade.price - tf_trades.price) * trade.quantity / (tf_trades.quantity + trade.quantity)
            tf_trades.quantity += trade.quantity
        return self

    def init_start_ts(self, start_ts, db_name, validator_db_name):
        if start_ts:
            self.start_ts = start_ts
        elif start_ts := connect_to_db(validator_db_name).get_collection(TS).find_one():
            self.start_ts = start_ts[TS]
        else:
            self.start_ts = query_starting_ts(db_name, DEFAULT_SYMBOL_SEARCH, init_db=PARSED_AGGTRADES_DB)


class SymbolsTimeframeTrade(Trade):
    def __init__(self, start_ts=None):
        super().__init__(connect_to_db(PARSED_AGGTRADES_DB).list_collection_names())

        self.db_name = PARSED_TRADES_BASE_DB
        self.validator_db = PARSED_TRADES_BASE_DB + "_timestamp_validator_db"
        self.init_start_ts(start_ts, self.db_name, self.validator_db)

        self.end_ts = self.start_ts + self.timeframe

        if self.end_ts > connect_to_db(AGGTRADES_VALIDATOR_DB_TS).get_collection(TS).find_one()[TS]:
            LOG.error("No more trades to parse.")
            raise NoMoreParseableTrades("No more trades to parse.")

    def insert_in_db(self):
        for symbol in self._symbols:
            if data := [asdict(v) for v in self.ts_data[symbol].values()]:
                connect_to_db(self.db_name).get_collection(symbol).insert_many(data)
            time.sleep(0.1)  # Multiple connections socket saturation delay.

        delete_db(self.validator_db)
        connect_to_db(self.validator_db).get_collection(TS).insert_one({TS: self.end_ts})


class FundTimeframeTrade(Trade):
    def __init__(self, start_ts=None):
        from data_handling.data_helpers.data_staging import coin_ratio_marketcap
        from data_handling.data_helpers.vars_constants import SP500_SYMBOLS_USDT_PAIRS
        from MongoDB.db_actions import query_db_col_between

        super().__init__(SP500_SYMBOLS_USDT_PAIRS)

        self.db_name = FUND_DB
        self.validator_db = FUND_DB + "_timestamp_validator_db"
        self.init_start_ts(start_ts, self.db_name, self.validator_db)

        self.end_ts = self.start_ts + self.timeframe
        if self.end_ts > connect_to_db(AGGTRADES_VALIDATOR_DB_TS).get_collection(TS).find_one()[TS]:
            LOG.error("No more trades to parse.")
            raise NoMoreParseableTrades("No more trades to parse.")

        for symbol in SP500_SYMBOLS_USDT_PAIRS:
            self.add_trades(symbol, query_db_col_between(PARSED_AGGTRADES_DB, symbol, self.start_ts, self.end_ts))

        self.ratios, self.fund_marketcap = coin_ratio_marketcap()

    def insert_in_db(self):
        tf_marketcap_quantity_as_list = []

        for marketcap_data in self.tf_marketcap_quantity.values():
            tf_marketcap_quantity_as_list.append({TS: marketcap_data.timestamp, MARKETCAP: marketcap_data.price,
                                                  QUANTITY: marketcap_data.quantity})

        insert_many_same_db_col(self.db_name, tf_marketcap_quantity_as_list)

        delete_db(self.validator_db)
        connect_to_db(self.validator_db).get_collection(TS).insert_one({TS: self.end_ts})

    def parse_trades(self):
        tf_range_to_parse = range(self.start_ts, self.end_ts, self.ms_parse_interval)
        self.tf_marketcap_quantity = {tf: TradeData(None, 0, 0, tf) for tf in tf_range_to_parse}

        for tf in range(self.start_ts, self.end_ts, self.ms_parse_interval):
            volume_traded, current_marketcap = 0, 0

            for symbol in self._symbols:
                try:
                    tf_trade = self.ts_data[symbol][tf]
                    current_marketcap += tf_trade.price * self.ratios[DEFAULT_SYMBOL_SEARCH][MARKETCAP] / self.ratios[DEFAULT_SYMBOL_SEARCH][PRICE]
                    volume_traded += tf_trade.price * tf_trade.quantity
                except KeyError:
                    continue

            self.tf_marketcap_quantity[tf].price = current_marketcap
            self.tf_marketcap_quantity[tf].quantity = volume_traded
