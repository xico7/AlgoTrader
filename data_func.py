from support.decorators_extenders import init_only_existing
from vars_constants import PRICE, QUANTITY, SYMBOL, TS, DEFAULT_PARSE_INTERVAL, ONE_HOUR_IN_MS, \
    PARSED_TRADES_BASE_DB, PARSED_AGGTRADES_DB, DEFAULT_SYMBOL_SEARCH, FUND_DB, MARKETCAP, AGGTRADES_DB
from dataclasses import dataclass
from MongoDB.db_actions import insert_parsed_aggtrades, insert_many_db, query_starting_ts, query_existing_ws_trades, connect_to_db
from data_staging import round_last_ten_secs, current_milli_time


class InvalidParametersProvided(Exception): pass


class CacheAggtrades(dict):
    def __init__(self):
        super().__init__()
        self.bundled_trades = []
        self.symbol_parsed_trades = {}

    def __len__(self):
        return len(self.bundled_trades)

    def append(self, trade):
        self.bundled_trades.append(trade)
        trade_data = {k: v for k, v in trade.items() if not k == SYMBOL}
        try:
            self.symbol_parsed_trades[trade[SYMBOL]].append(trade_data)
        except KeyError:
            self.symbol_parsed_trades[trade[SYMBOL]] = [trade_data]

    def insert_clear(self):
        insert_parsed_aggtrades(self.symbol_parsed_trades)
        insert_many_db(AGGTRADES_DB, self.bundled_trades, same_col_db_name=True)
        self.symbol_parsed_trades.clear()
        self.bundled_trades.clear()


@init_only_existing
@dataclass
class Aggtrade:
    symbol: str
    timestamp: int
    price: float
    quantity: float


    def _pre_init__(self, *args, **kwargs):
        kwargs[SYMBOL] = kwargs['s']
        kwargs[TS] = int(kwargs['E'])
        kwargs[PRICE] = float(kwargs['p'])
        kwargs[QUANTITY] = float(kwargs['q'])

        return args, kwargs


# TODO: Improve this part..
class Trade:

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


class SymbolsTimeframeTrade(Trade):

    def __init__(self, timeframe_in_ms=ONE_HOUR_IN_MS, db_name=PARSED_TRADES_BASE_DB,
                 parse_interval_in_secs=DEFAULT_PARSE_INTERVAL,
                 symbols=connect_to_db(PARSED_AGGTRADES_DB).list_collection_names(), start_ts=None, end_ts=None):

        super().__init__(symbols=symbols, timeframe_in_ms=timeframe_in_ms, parse_interval_in_secs=parse_interval_in_secs)

        self.db_name = db_name.format(parse_interval_in_secs)
        self._init_start_end_ts(start_ts=start_ts, end_ts=end_ts)
        self._init_start_end_price()

    def __iadd__(self, trades):
        for symbol, symbol_trades in trades.items():
            for trade in symbol_trades:
                trade_tf_data = self.ts_data[symbol][round_last_ten_secs(trade[TS])]
                if t := trade[PRICE]:
                    self.end_price[symbol] = t
                    if not self.start_price[symbol]:
                        self.start_price[symbol] = t

                try:
                    trade_tf_data[PRICE] += (trade[PRICE] - trade_tf_data[PRICE]) * trade[QUANTITY] / \
                                            trade_tf_data[QUANTITY] if trade_tf_data[QUANTITY] else trade[PRICE]
                    trade_tf_data[QUANTITY] += trade[QUANTITY]
                except TypeError:
                    trade_tf_data[PRICE], trade_tf_data[QUANTITY] = trade[PRICE], trade[QUANTITY]
        return self

# TODO: Heisenbug here.. sometimes self.start_ts[symbol] is a dict.
    def _init_end_ts(self, symbol, end_ts):
        possible_timeframe = self.start_ts[symbol] + self.timeframe - 1
        if end_ts:
            self.end_ts[symbol] = end_ts
        elif possible_timeframe < current_milli_time():
            self.end_ts[symbol] = possible_timeframe
        else:
            self.end_ts[symbol] = current_milli_time()

    def _init_start_end_ts(self, start_ts=None, end_ts=None):
        for symbol in self._symbols:
            if not start_ts:
                self.start_ts[symbol] = query_starting_ts(self.db_name, symbol, init_db=PARSED_AGGTRADES_DB)
            else:
                self.start_ts[symbol] = start_ts

            self._init_end_ts(symbol, end_ts)

        existing_trades = query_existing_ws_trades(self.start_ts, self.end_ts, self.ms_parse_interval)

        for symbol in self._symbols:
            self.ts_data[symbol] = {}

            for tf in range(self.start_ts[symbol], self.end_ts[symbol], self.ms_parse_interval):
                self.ts_data[symbol][tf] = {PRICE: 0, QUANTITY: 0} if tf in existing_trades else {PRICE: None, QUANTITY: None}

    def _init_start_end_price(self):
        for symbol in self._symbols:
            self.start_price[symbol] = None
            self.end_price[symbol] = None

    def get_last_end_ts(self):
        last_ts = 0

        for ts in self.end_ts.values():
            last_ts = ts if ts > last_ts else last_ts

        return last_ts

    def insert_in_db(self):
        for symbol, symbol_ts_data in self.ts_data.items():
            insert_many_db(self.db_name,
                           [{TS: timeframe, **symbol_ts_data[timeframe]} for timeframe in symbol_ts_data], symbol)

    def reset_add_interval(self):
        for symbol in self._symbols:
            self.start_ts[symbol] += self.timeframe
            self.end_ts[symbol] += self.timeframe
        self._init_start_end_ts(self.start_ts, self.end_ts)


class FundTimeframeTrade(Trade):
    def __init__(self, start_ts=None):
        from data_staging import coin_ratio_marketcap
        from vars_constants import SP500_SYMBOLS_USDT_PAIRS
        from MongoDB.db_actions import query_parsed_aggtrade_multiple_timeframes

        if not start_ts:
            start_ts = 0
            for symbol in SP500_SYMBOLS_USDT_PAIRS:
                ts = query_starting_ts(PARSED_TRADES_BASE_DB.format(DEFAULT_PARSE_INTERVAL), symbol)
                if ts > start_ts:
                    start_ts = ts

        parse_tf_trades = SymbolsTimeframeTrade(symbols=SP500_SYMBOLS_USDT_PAIRS, start_ts=start_ts)
        parse_tf_trades += query_parsed_aggtrade_multiple_timeframes(
            SP500_SYMBOLS_USDT_PAIRS, parse_tf_trades.start_ts, parse_tf_trades.end_ts)

        super().__init__(copy_trade=parse_tf_trades)
        self.db_col_name = FUND_DB
        self.ratios, self.fund_marketcap = coin_ratio_marketcap()
        self.tf_marketcap_quantity = {}
        for tf in range(self.start_ts[DEFAULT_SYMBOL_SEARCH], self.end_ts[DEFAULT_SYMBOL_SEARCH], self.ms_parse_interval):
            self.tf_marketcap_quantity[tf] = {MARKETCAP: 0, QUANTITY: 0}

    def insert_in_db(self):
        tf_marketcap_quantity_as_list = []

        for tf, marketcap_quantity in self.tf_marketcap_quantity.items():
            tf_marketcap_quantity_as_list.append({TS: tf,
                                                  MARKETCAP: marketcap_quantity[MARKETCAP],
                                                  QUANTITY: marketcap_quantity[QUANTITY]})

        insert_many_db(self.db_col_name, tf_marketcap_quantity_as_list, same_col_db_name=True)

    def reset_add_interval(self):
        pass
