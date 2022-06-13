import copy
import time

from support.decorators_extenders import init_only_existing
from vars_constants import PRICE, QUANTITY, SYMBOL, DB_TS, millisecs_timeframe, default_parse_interval
from dataclasses import dataclass
from MongoDB.db_actions import connect_to_n_ms_parsed_trades_db, insert_bundled_aggtrades, \
    insert_parsed_aggtrades, connect_to_parsed_aggtrade_db, parsed_trades_base_db, insert_many_db
from data_staging import query_db_col_oldest_ts, round_last_n_secs, current_milli_time


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
    def __init__(self, start_ts=None, symbols=connect_to_parsed_aggtrade_db().list_collection_names(), parse_interval_in_secs=default_parse_interval):
        self.ms_parse_interval = parse_interval_in_secs * 1000
        self.start_ts = start_ts or {}
        self.end_ts, init_ts_data = {}, {}
        self._db_name = parsed_trades_base_db.format(default_parse_interval)
        self._timeframe = millisecs_timeframe

        for symbol in symbols:
            self.start_ts[symbol] = query_db_col_oldest_ts(self._db_name, symbol, init_db='parsed_aggtrades')

            possible_timeframe = self.start_ts[symbol] + self._timeframe
            self.end_ts[symbol] = possible_timeframe if possible_timeframe < current_milli_time() else current_milli_time()
            init_ts_data[symbol] = {ts: 0 for ts in range(self.start_ts[symbol], self.end_ts[symbol], self.ms_parse_interval)}

        self.price, self.volume = copy.deepcopy(init_ts_data), copy.deepcopy(init_ts_data)

    def parse_trades(self, trades, symbol):
        for trade in trades:
            key = round_last_n_secs(trade[DB_TS], self.ms_parse_interval / 1000)
            ts_volume, ts_price = self.volume[symbol][key], self.price[symbol][key]
            
            self.price[symbol][key] += (trade[PRICE] - ts_price) * trade[QUANTITY] / ts_volume if ts_volume else trade[PRICE]
            self.volume[symbol][key] += trade[QUANTITY]

    def insert_in_db(self):
        for symbol in self.price:
            insert_many_db(self._db_name, [{'timestamp': k, 'price': v} for k, v in self.price[symbol].items()], symbol)
        
    def clear_add_interval(self):
        print("here")



    # for interval in range(self.start_ts[symbol], self.end_ts[symbol], self.ms_parse_interval):
    #     leftover_trades, partial_trades_list = range_of_trades(trades, self.start_ts[symbol],
    #                                                            self.ms_parse_interval)
    #     trades = leftover_trades
    #     if partial_trades_list:
    #         price = (partial_trades_list[0][PRICE] + partial_trades_list[-1][PRICE]) / 2
    #         volume = sum([elem[PRICE] * elem[QUANTITY] for elem in partial_trades_list])
    #         self.price[from_seconds_n_to_n_plus][symbol] = price
    #         self.volume[from_seconds_n_to_n_plus][symbol] = volume
    #
    #         if symbol in SP500_SYMBOLS_USDT_PAIRS:
    #             self.price[from_seconds_n_to_n_plus][symbol] = price
    #             self.volume[from_seconds_n_to_n_plus][symbol] = volume
    #             # fund_symbol_ratio_prices[from_seconds_n_to_n_plus][symbol] = price * coin_ratios[symbol]
    #             # fund_symbol_volumes[from_seconds_n_to_n_plus][symbol] = volume
    # pass



# @dataclass
# class ParseSymbolsAggtradeData:
#     def __init__(self, timeframe_in_secs, symbols, range_in_secs=10, ts_begin=None):
#         from MongoDB.db_actions import connect_to_n_ms_parsed_trades_db
#         from data_staging import optional_add_secs_in_ms, query_db_col_oldest_ts, round_last_n_secs
#         from tasks.transform_trade_data import DEFAULT_COL_SEARCH
#         from MongoDB.db_actions import connect_to_bundled_aggtrade_db
#
#         #TODO Validate ts_begin.
#         if not ts_begin or not (ts_begin := optional_add_secs_in_ms((query_db_col_oldest_ts(connect_to_n_ms_parsed_trades_db(range_in_secs),
#                                                                          DEFAULT_COL_SEARCH)), range_in_secs)):
#                 ts_begin = round_last_n_secs(query_db_col_oldest_ts(connect_to_bundled_aggtrade_db(), DEFAULT_COL_SEARCH), range_in_secs)
#
#         iterations = range(0, timeframe_in_secs, range_in_secs)
#         self.starting_timestamp = ts_begin
#         self.timeframe_in_secs = timeframe_in_secs
#         self.seconds_interval = range_in_secs
#         self.price_data = {iteration: {symbols[i]: 0 for i, elem in enumerate(symbols)} for iteration in iterations}
#         self.volume_data = {iteration: {symbols[i]: 0 for i, elem in enumerate(symbols)} for iteration in iterations}
#
#     def parse_trades(self, trades):
#         def range_of_trades(trades, begin_ts, iteration):
#             leftover_trades, partial_trades_list = [], []
#             for i, trade in enumerate(trades):
#                 if begin_ts + iteration > list(trade.items())[1][1]:
#                     partial_trades_list.append(trade)
#                 else:
#                     leftover_trades.append(trade)
#
#                 return leftover_trades, partial_trades_list
#         for interval in range(0, self.timeframe_in_secs, self.seconds_interval):
#             leftover_trades, partial_trades_list = range_of_trades(trades, self.starting_timestamp, self.seconds_interval)
#             trades = leftover_trades
#             if partial_trades_list:
#                 price = (partial_trades_list[0][PRICE] + partial_trades_list[-1][PRICE]) / 2
#                 volume = sum([elem[PRICE] * elem[QUANTITY] for elem in partial_trades_list])
#                 self.price_data[from_seconds_n_to_n_plus][symbol] = price
#                 self.volume_data[from_seconds_n_to_n_plus][symbol] = volume
#
#                 if symbol in SP500_SYMBOLS_USDT_PAIRS:
#                     self.price_data[from_seconds_n_to_n_plus][symbol] = price
#                     self.volume_data[from_seconds_n_to_n_plus][symbol] = volume
#                     # fund_symbol_ratio_prices[from_seconds_n_to_n_plus][symbol] = price * coin_ratios[symbol]
#                     # fund_symbol_volumes[from_seconds_n_to_n_plus][symbol] = volume
#         pass
