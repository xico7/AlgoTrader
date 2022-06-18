
from support.decorators_extenders import init_only_existing
from vars_constants import PRICE, QUANTITY, SYMBOL, DB_TS, millisecs_timeframe, default_parse_interval
from dataclasses import dataclass
from MongoDB.db_actions import insert_bundled_aggtrades, \
    insert_parsed_aggtrades, connect_to_parsed_aggtrade_db, parsed_trades_base_db, insert_many_db
from data_staging import query_db_col_newest_ts, round_last_n_secs, current_milli_time, query_existing_ws_trades


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
        insert_bundled_aggtrades(self.bundled_trades)
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
        kwargs[DB_TS] = int(kwargs['E'])
        kwargs[PRICE] = float(kwargs['p'])
        kwargs[QUANTITY] = float(kwargs['q'])

        return args, kwargs


class ParseAggtradeData:
    def __init__(self, parse_interval_in_secs=default_parse_interval, symbols=connect_to_parsed_aggtrade_db().list_collection_names()):
        self.ms_parse_interval = parse_interval_in_secs * 1000
        self.end_ts, self.start_ts, self.ts_data = {}, {}, {}
        self._symbols = symbols
        self._db_name = parsed_trades_base_db.format(default_parse_interval)
        self._timeframe = millisecs_timeframe
        self.init_price_volume()

    def __iadd__(self, trades):
        for symbol, symbol_trades in trades.items():
            for trade in symbol_trades:
                trade_tf_data = self.ts_data[symbol][round_last_n_secs(trade[DB_TS], self.ms_parse_interval / 1000)]

                try:
                    trade_tf_data[PRICE] += (trade[PRICE] - trade_tf_data[PRICE]) * trade[QUANTITY] / \
                                            trade_tf_data[QUANTITY] if trade_tf_data[QUANTITY] else trade[PRICE]
                    trade_tf_data[QUANTITY] += trade[QUANTITY]
                except TypeError:
                    trade_tf_data[PRICE], trade_tf_data[QUANTITY] = trade[PRICE], trade[QUANTITY]
        return self

    def init_price_volume(self, start_ts=None, end_ts=None):
        if not (start_ts and end_ts):
            for symbol in self._symbols:
                self.start_ts[symbol] = query_db_col_newest_ts(self._db_name, symbol, init_db='parsed_aggtrades')
                possible_timeframe = self.start_ts[symbol] + self._timeframe - 1
                self.end_ts[symbol] = possible_timeframe if possible_timeframe < current_milli_time() else current_milli_time()

        existing_trades = query_existing_ws_trades(min(list(self.start_ts.values())),
                                                   max(list(self.end_ts.values())),
                                                   self.ms_parse_interval)

        for symbol in self._symbols:
            self.ts_data[symbol] = {}
            for ts in range(self.start_ts[symbol], self.end_ts[symbol], self.ms_parse_interval):
                self.ts_data[symbol][ts] = {PRICE: 0, QUANTITY: 0} if ts in existing_trades else {PRICE: None, QUANTITY: None}

    def insert_in_db(self):
        for symbol, symbol_ts_data in self.ts_data.items():
            insert_many_db(self._db_name,
                           [{DB_TS: timeframe, **symbol_ts_data[timeframe]} for timeframe in symbol_ts_data], symbol)

    def reset_add_interval(self):
        for symbol in self._symbols:
            self.start_ts[symbol] += self._timeframe
            self.end_ts[symbol] += self._timeframe
        self.init_price_volume(self.start_ts, self.end_ts)