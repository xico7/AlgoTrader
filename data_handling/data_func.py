import logging
import time
from typing import Optional

import logs
from support.decorators_extenders import init_only_existing
from data_handling.data_helpers.vars_constants import PRICE, QUANTITY, SYMBOL, TS, DEFAULT_PARSE_INTERVAL, \
    PARSED_TRADES_BASE_DB, PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, FUND_DB, MARKETCAP, AGGTRADES_DB, \
    AGGTRADE_PYCACHE, ONE_HOUR_IN_MS
from dataclasses import dataclass, asdict
from MongoDB.db_actions import insert_many_same_db_col, query_starting_ts, connect_to_db, InvalidDataProvided, query_db_col_between
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
        aggtrade = Aggtrade(**trade)
        self.bundled_trades.append({aggtrade.symbol: aggtrade.data})
        self.symbol_parsed_trades.setdefault(aggtrade.symbol, []).append(aggtrade.data)

    def insert_clear(self):
        if len(self) < AGGTRADE_PYCACHE:
            return False

        for key in self.symbol_parsed_trades.keys():
            symbol_dict_trades = {}
            for trade in self.symbol_parsed_trades[key]:
                symbol_dict_trades.setdefault(key, []).append(asdict(trade))
            connect_to_db(PARSED_AGGTRADES_DB).get_collection(key).insert_many(symbol_dict_trades)

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
        kwargs['data'] = Trade(float(kwargs['p']), float(kwargs['q']), int(kwargs['E']))
        return args, kwargs


@init_only_existing
@dataclass
class Trade:
    price: float
    quantity: float
    timestamp: Optional[int]



# TODO: Improve this part..
class CopyTrade:

    def __init__(self, symbols=None, timeframe_in_ms=None, parse_interval_in_secs=None, copy_trade=None):

        if not copy_trade and not (symbols and timeframe_in_ms and parse_interval_in_secs):
            raise InvalidParametersProvided("Invalid parameters provided")

        self.start_ts = copy_trade.start_ts if copy_trade else {}
        self.end_ts = copy_trade.end_ts if copy_trade else {}
        self.ts_data = copy_trade.ts_data if copy_trade else {}
        self.start_price = copy_trade.start_price if copy_trade else {}
        self.end_price = copy_trade.end_price if copy_trade else {}
        self._symbols = copy_trade._symbols if copy_trade else symbols
        self.timeframe = copy_trade.timeframe if copy_trade else timeframe_in_ms
        self.ms_parse_interval = copy_trade.ms_parse_interval if copy_trade else parse_interval_in_secs * 1000


class SymbolsTimeframeTrade(CopyTrade):
    #TODO: timeframe_in_ms=ONE_HOUR_IN_MS
    def __init__(self, timeframe_in_ms=ONE_HOUR_IN_MS, db_name=PARSED_TRADES_BASE_DB,
                 parse_interval_in_secs=DEFAULT_PARSE_INTERVAL,
                 symbols=connect_to_db(PARSED_AGGTRADES_DB).list_collection_names(), start_ts=None, end_ts=None, init_db=None):

        super().__init__(symbols, timeframe_in_ms, parse_interval_in_secs)

        self.db_name = db_name.format(parse_interval_in_secs)
        self._init_start_end_ts(start_ts, end_ts, init_db)

        self.start_price = {symbol: 0 for symbol in self._symbols}
        self.end_price = {symbol: 0 for symbol in self._symbols}

    def __iadd__(self, trades):
        for symbol, symbol_trades in trades.items():
            if symbol_trades:
                for trade in symbol_trades:
                    trade_tf_data = self.ts_data[symbol][round_last_ten_secs(trade[TS])]
                    if t := trade[PRICE]:
                        self.end_price[symbol] = t
                        if not self.start_price[symbol]:
                            self.start_price[symbol] = t


                    trade_tf_data[PRICE] += (trade[PRICE] - trade_tf_data[PRICE]) * trade[QUANTITY] / \
                                            trade_tf_data[QUANTITY] if trade_tf_data[QUANTITY] else trade[PRICE]
                    trade_tf_data[QUANTITY] += trade[QUANTITY]
        return self

    def add_symbol_trades(self, symbol):
        if symbol_trades := query_db_col_between(PARSED_AGGTRADES_DB, symbol, self.start_ts[symbol], self.end_ts[symbol]):
            for trade in symbol_trades:
                trade_tf_data = self.ts_data[symbol][round_last_ten_secs(trade.timestamp)]
                if t := trade[PRICE]:
                    self.end_price[symbol] = t
                    if not self.start_price[symbol]:
                        self.start_price[symbol] = t
                trade_tf_data[PRICE] += (trade.price - trade_tf_data[PRICE]) * trade[QUANTITY] / \
                                        trade_tf_data[QUANTITY] if trade_tf_data[QUANTITY] else trade.price
                trade_tf_data[QUANTITY] += trade.quantity
            return self

    def _init_start_end_ts(self, start_ts: [dict, float] = None, end_ts=None, init_db=None):
        if not start_ts:
            self.start_ts = {symbol: query_starting_ts(self.db_name, symbol, init_db=init_db) for symbol in self._symbols}
        elif isinstance(start_ts, dict):
            self.start_ts = {symbol: start_ts[symbol] for symbol in self._symbols}
        else:
            self.start_ts = {symbol: start_ts for symbol in self._symbols}

        for symbol in self._symbols:
            possible_timeframe = self.start_ts[symbol] + self.timeframe - 1
            if end_ts:
                self.end_ts[symbol] = end_ts[symbol]
            elif possible_timeframe < current_milli_time():
                self.end_ts[symbol] = possible_timeframe
            else:
                self.end_ts[symbol] = current_milli_time()

        self.ts_data = {symbol: {} for symbol in self._symbols}

        for symbol in self._symbols:
            tf_range_to_parse = range(self.start_ts[symbol], self.end_ts[symbol], self.ms_parse_interval)
            self.ts_data[symbol] = {tf: Trade(0, 0, None) for tf in tf_range_to_parse}

    def insert_in_db(self):
        for symbol, symbol_ts_data in self.ts_data.items():
            connect_to_db(self.db_name).get_collection(symbol).insert_many([{TS: timeframe, **symbol_ts_data[timeframe]} for timeframe in symbol_ts_data])
            time.sleep(0.1)  # Multiple connections socket saturation delay.

    def reset_add_interval(self):
        for symbol in self._symbols:
            self.start_ts[symbol] += self.timeframe
            self.end_ts[symbol] += self.timeframe
        self._init_start_end_ts(self.start_ts, self.end_ts)


class FundTimeframeTrade(CopyTrade):
    def __init__(self, start_ts=None):
        from data_handling.data_helpers.data_staging import coin_ratio_marketcap
        from data_handling.data_helpers.vars_constants import SP500_SYMBOLS_USDT_PAIRS
        from MongoDB.db_actions import query_db_col_between, db_col_names

        db_to_parse = PARSED_TRADES_BASE_DB.format(DEFAULT_PARSE_INTERVAL)
        if not db_col_names(db_to_parse):
            LOG.error("In order to parse Fund trades, '%s' needs to exist/have data.", db_to_parse)
            raise InvalidDataProvided("In order to parse Fund trades, '%s' needs to exist/have data.", db_to_parse)

        if not start_ts:
            start_ts = max([query_starting_ts(db_to_parse, symbol) for symbol in SP500_SYMBOLS_USDT_PAIRS])

        parse_tf_trades = SymbolsTimeframeTrade(symbols=SP500_SYMBOLS_USDT_PAIRS, start_ts=start_ts)
        # parse_tf_trades.start_ts = {symbol: 0 for symbol in parse_tf_trades._symbols} # TODO: Remove later
        # parse_tf_trades.end_ts = {symbol: 16635566099999 for symbol in parse_tf_trades._symbols} # TODO: Remove later

        parse_tf_trades += {symbol: query_db_col_between(db_to_parse, symbol, parse_tf_trades.start_ts[symbol], parse_tf_trades.end_ts[symbol])
                            for symbol in SP500_SYMBOLS_USDT_PAIRS}

        #super().__init__(copy_trade=parse_tf_trades)
        self.db_col_name = FUND_DB
        self.ratios, self.fund_marketcap = coin_ratio_marketcap()
        self.tf_marketcap_quantity = {}
        tf_range_to_parse = range(self.start_ts[DEFAULT_SYMBOL_SEARCH], self.end_ts[DEFAULT_SYMBOL_SEARCH], self.ms_parse_interval)
        self.tf_marketcap_quantity = {tf: {MARKETCAP: 0, QUANTITY: 0} for tf in tf_range_to_parse}

    def insert_in_db(self):
        tf_marketcap_quantity_as_list = []

        for tf, marketcap_quantity in self.tf_marketcap_quantity.items():
            tf_marketcap_quantity_as_list.append({TS: tf,
                                                  MARKETCAP: marketcap_quantity[MARKETCAP],
                                                  QUANTITY: marketcap_quantity[QUANTITY]})

        insert_many_same_db_col(self.db_col_name, tf_marketcap_quantity_as_list)

    def reset_add_interval(self):
        pass
