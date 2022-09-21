import contextlib
import logging
import time
from contextlib import suppress
from typing import Optional

import logs
from support.decorators_extenders import init_only_existing
from data_handling.data_helpers.vars_constants import PRICE, QUANTITY, SYMBOL, TS, DEFAULT_PARSE_INTERVAL, \
    PARSED_TRADES_BASE_DB, PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, FUND_DB, MARKETCAP, AGGTRADES_DB, \
    AGGTRADE_PYCACHE, ONE_HOUR_IN_MS, DEFAULT_TIMEFRAME_IN_MS
from dataclasses import dataclass, asdict
from MongoDB.db_actions import insert_many_same_db_col, query_starting_ts, connect_to_db, InvalidDataProvided, \
    query_db_col_between, insert_many_db
from data_handling.data_helpers.data_staging import round_last_ten_secs, current_milli_time


class InvalidParametersProvided(Exception): pass


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

    def insert_clear(self):
        if len(self) < AGGTRADE_PYCACHE:
            return False

        for symbol, trades in self.symbol_parsed_trades.items():
            insert_many_db(PARSED_AGGTRADES_DB, symbol, trades)

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
                kwargs['data'] = {PRICE: float(kwargs['p']), QUANTITY: float(kwargs['q']), TS: int(kwargs['E'])}
        except KeyError:
            kwargs['data'] = TradeData(float(kwargs['p']), float(kwargs['q']), int(kwargs['E']))
        return args, kwargs


@init_only_existing
@dataclass
class TradeData:
    price: float
    quantity: float
    timestamp: Optional[int]


# TODO: Improve this part..
class Trade:
    def __init__(self, symbols=None):
        self.ts_data = {}
        self.start_price = {}
        self.end_price = {}
        self._symbols = symbols
        self.timeframe = DEFAULT_TIMEFRAME_IN_MS
        self.ms_parse_interval = DEFAULT_PARSE_INTERVAL * 1000

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

            tf_trades = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)] = (TradeData(0, 0, round_last_ten_secs(trade.timestamp)))
            if not tf_trades.quantity:
                tf_trades.price += trade.quantity
            else:
                tf_trades.price += (trade.price - tf_trades.price) * trade.quantity / tf_trades.quantity
            tf_trades.quantity += trade.quantity
        return self


class SymbolsTimeframeTrade(Trade):
    def __init__(self, parse_interval_in_secs=DEFAULT_PARSE_INTERVAL, start_ts=None, end_ts=None, init_db=None):

        super().__init__(connect_to_db(PARSED_AGGTRADES_DB).list_collection_names())

        self.db_name = PARSED_TRADES_BASE_DB.format(parse_interval_in_secs)
        self._init_start_end_ts(start_ts, end_ts, init_db)

    def _init_start_end_ts(self, start_ts: [dict, float] = None, end_ts=None, init_db=None):
        if not start_ts:
            self.start_ts = {symbol: query_starting_ts(self.db_name, symbol, init_db=init_db) for symbol in self._symbols}
        elif isinstance(start_ts, dict):
            self.start_ts = {symbol: start_ts[symbol] for symbol in self._symbols}
        else:
            self.start_ts = {symbol: start_ts for symbol in self._symbols}

        self.end_ts = {}
        for symbol in self._symbols:
            possible_timeframe = self.start_ts[symbol] + self.timeframe - 1
            if end_ts:
                self.end_ts[symbol] = end_ts[symbol]
            elif possible_timeframe < current_milli_time():
                self.end_ts[symbol] = possible_timeframe
            else:
                self.end_ts[symbol] = current_milli_time()

    def insert_in_db(self):
        for symbol, symbol_ts_data in self.ts_data.items():
            if data := [asdict(v) for v in symbol_ts_data.values()]:
                connect_to_db(self.db_name).get_collection(symbol).insert_many(data)
            time.sleep(0.1)  # Multiple connections socket saturation delay.

    def reset_add_interval(self):
        for symbol in self._symbols:
            self.start_ts[symbol] += self.timeframe
            self.end_ts[symbol] += self.timeframe
            self.ts_data[symbol] = {}

        self._init_start_end_ts(self.start_ts, self.end_ts)


class FundTimeframeTrade(Trade):
    def __init__(self, start_ts=None):
        from data_handling.data_helpers.data_staging import coin_ratio_marketcap
        from data_handling.data_helpers.vars_constants import SP500_SYMBOLS_USDT_PAIRS
        from MongoDB.db_actions import query_db_col_between, db_col_names

        db_to_parse = PARSED_TRADES_BASE_DB.format(DEFAULT_PARSE_INTERVAL)
        if not db_col_names(db_to_parse):
            LOG.error("In order to parse Fund trades, '%s' needs to exist/have data.", db_to_parse)
            raise InvalidDataProvided("In order to parse Fund trades, '%s' needs to exist/have data.", db_to_parse)

        super().__init__(SP500_SYMBOLS_USDT_PAIRS)

        if not start_ts:
            #self.start_ts = 1663759910000 # TODO: max([query_starting_ts(db_to_parse, symbol) for symbol in SP500_SYMBOLS_USDT_PAIRS])
            self.start_ts = {symbol: 0 for symbol in parse_tf_trades._symbols} # TODO: Remove later
        self.end_ts = self.start_ts + DEFAULT_TIMEFRAME_IN_MS

        for symbol in SP500_SYMBOLS_USDT_PAIRS:
            self.add_trades(symbol, query_db_col_between(PARSED_AGGTRADES_DB, symbol, self.start_ts, self.end_ts))

        self.db_col_name = FUND_DB
        self.ratios, self.fund_marketcap = coin_ratio_marketcap()

    def insert_in_db(self):
        tf_marketcap_quantity_as_list = []

        for marketcap_data in self.tf_marketcap_quantity.values():
            tf_marketcap_quantity_as_list.append({TS: marketcap_data.timestamp, MARKETCAP: marketcap_data.price,
                                                  QUANTITY: marketcap_data.quantity})

        insert_many_same_db_col(self.db_col_name, tf_marketcap_quantity_as_list)

    def parse_trades(self):
        tf_range_to_parse = range(self.start_ts, self.end_ts, self.ms_parse_interval)
        self.tf_marketcap_quantity = {tf: TradeData(0, 0, tf) for tf in tf_range_to_parse}

        for tf in range(self.start_ts, self.end_ts, self.ms_parse_interval):
            volume_traded, current_marketcap = 0, 0

            for symbol in self._symbols:
                try:
                    tf_trade = self.ts_data[symbol][tf]
                    current_marketcap += tf_trade.price * self.ratios[DEFAULT_SYMBOL_SEARCH]['marketcap'] / self.ratios[DEFAULT_SYMBOL_SEARCH]['price']
                    volume_traded += tf_trade.price * tf_trade.quantity
                except KeyError:
                    continue

            self.tf_marketcap_quantity[tf].price = current_marketcap
            self.tf_marketcap_quantity[tf].quantity = volume_traded
