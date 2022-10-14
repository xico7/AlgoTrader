import logging
import time
from typing import Optional, List, Dict

import logs
from support.decorators_extenders import init_only_existing
from data_handling.data_helpers.vars_constants import PRICE, QUANTITY, SYMBOL, TS, DEFAULT_PARSE_INTERVAL, \
    PARSED_TRADES_BASE_DB, PARSED_AGGTRADES_DB, FUND_DB, MARKETCAP, AGGTRADES_DB, DEFAULT_TIMEFRAME_IN_MS, \
    END_TS_AGGTRADES_VALIDATOR_DB, DEFAULT_PARSE_INTERVAL_IN_MS, TEN_SECS_MS, FUND_DB_COL, START_TS_AGGTRADES_VALIDATOR_DB
from dataclasses import dataclass, asdict, field
from MongoDB.db_actions import insert_many_same_db_col, connect_to_db, insert_many_db, delete_db, query_db_col_timestamp_endpoint
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

        delete_db(END_TS_AGGTRADES_VALIDATOR_DB)
        connect_to_db(END_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).insert_one({TS: end_ts})

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

    def _pre_init__(self, *args, **kwargs):
        if 'marketcap' in kwargs:
            kwargs['price'] = kwargs['marketcap']
            kwargs.pop('marketcap')

        if kwargs and 'ID' not in kwargs:
            kwargs['ID'] = None

        return args, kwargs

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
        if self.one_percent:
            if (counter := int(((trade_price - self.min_price) // self.one_percent) + 1)) > 100:
                counter = 100
            return counter

        return -1

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
        self.symbols = symbols
        self.timeframe = DEFAULT_TIMEFRAME_IN_MS
        self.ms_parse_interval = DEFAULT_PARSE_INTERVAL * 1000
        self.db_name = PARSED_TRADES_BASE_DB

        for symbol in self.symbols:
            self.start_price[symbol] = 0
            self.end_price[symbol] = 0
            self.ts_data[symbol] = {}

    def add_trades(self, symbol, symbol_trades):
        for trade in symbol_trades.values():
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


class SymbolsTimeframeTrade(Trade):
    def __init__(self, start_ts: dict = None):
        self.start_ts = {}
        self.db_name = PARSED_TRADES_BASE_DB
        if start_ts:
            self.start_ts = start_ts
        else:
            for symbol in connect_to_db(PARSED_AGGTRADES_DB).list_collection_names():
                if endpoint := query_db_col_timestamp_endpoint(self.db_name, symbol, most_recent=True, ignore_empty=True):
                    self.start_ts[symbol] = endpoint + TEN_SECS_MS
                else:
                    self.start_ts[symbol] = connect_to_db(START_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).find_one()[TS]

        symbols = list(self.start_ts.keys())
        super().__init__(symbols)
        self.end_ts = {symbol: self.start_ts[symbol] + self.timeframe for symbol in symbols}

        if max(self.end_ts[symbol] for symbol in symbols) > connect_to_db(END_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).find_one()[TS]:
            LOG.info("No more trades to parse.")
            raise NoMoreParseableTrades("No more trades to parse.")

    def insert_in_db(self):
        for symbol in self.symbols:
            if trades := [asdict(v) for v in self.ts_data[symbol].values()]:
                connect_to_db(self.db_name).get_collection(symbol).insert_many(trades)
                time.sleep(0.1)  # Multiple connections socket saturation delay.


class FundTimeframeTrade(Trade):
    def __init__(self, start_ts: Optional[int] = None, ratio: dict = None):
        from data_handling.data_helpers.data_staging import coin_ratio_marketcap
        from data_handling.data_helpers.vars_constants import SP500_SYMBOLS_USDT_PAIRS
        from MongoDB.db_actions import query_db_col_between

        super().__init__(SP500_SYMBOLS_USDT_PAIRS)

        if start_ts:
            self.start_ts = start_ts
        elif endpoint := query_db_col_timestamp_endpoint(FUND_DB, FUND_DB_COL, most_recent=True, ignore_empty=True):
            self.start_ts = endpoint + TEN_SECS_MS
        else:
            self.start_ts = connect_to_db(START_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).find_one()[TS]

        self.end_ts = self.start_ts + self.timeframe
        self.tf_marketcap_quantity = {}

        if self.end_ts > connect_to_db(END_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).find_one()[TS]:
            LOG.error("No more trades to parse.")
            raise NoMoreParseableTrades("No more trades to parse.")

        for symbol in SP500_SYMBOLS_USDT_PAIRS:
            self.add_trades(symbol, query_db_col_between(PARSED_AGGTRADES_DB, symbol, self.start_ts, self.end_ts).trades)

        self.ratios = coin_ratio_marketcap() if not ratio else ratio

    def insert_in_db(self):
        insert_many_db(self.db_name, FUND_DB_COL, [{TS: elem.timestamp, MARKETCAP: elem.price, QUANTITY: elem.quantity}
                                                   for elem in self.tf_marketcap_quantity.values()])

    def parse_trades(self):
        self.tf_marketcap_quantity = {tf: TradeData(None, 0, 0, tf) for tf in range(self.start_ts, self.end_ts, self.ms_parse_interval)}

        for tf in range(self.start_ts, self.end_ts, self.ms_parse_interval):
            volume_traded, current_marketcap = 0, 0

            for symbol in self.symbols:
                try:
                    tf_trade = self.ts_data[symbol][tf]
                    current_marketcap += tf_trade.price * self.ratios[symbol][MARKETCAP] / self.ratios[symbol][PRICE]
                    volume_traded += tf_trade.price * tf_trade.quantity
                except KeyError:
                    continue

            self.tf_marketcap_quantity[tf].price = current_marketcap
            self.tf_marketcap_quantity[tf].quantity = volume_traded
