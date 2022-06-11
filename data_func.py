import time

from MongoDB.db_actions import insert_many_aggtrades
from support.decorators_extenders import init_only_existing
from vars_constants import PRICE, QUANTITY, SYMBOL
from dataclasses import dataclass


class CacheAggtrades(dict):
    def __len__(self):
        length = 0
        for key in self.keys():
            length += len(self[key])
        return length

    def append(self, trade):
        trade_data = {k: v for k, v in trade.items() if not k == SYMBOL}
        try:
            self[trade[SYMBOL]].append(trade_data)
        except KeyError:
            self[trade[SYMBOL]] = [trade_data]

    def insert_clear(self):
        insert_many_aggtrades(self)
        self.clear()


@init_only_existing
@dataclass
class Aggtrade:
    symbol: str
    timestamp: float
    price: float
    quantity: float


    def _pre_init__(self, *args, **kwargs):
        kwargs['symbol'] = kwargs['s']
        kwargs['timestamp'] = kwargs['E']
        kwargs['price'] = kwargs['p']
        kwargs['quantity'] = kwargs['q']

        return args, kwargs


@dataclass
class ParseSymbolsAggtradeData:
    symbols: list

    def __post_init__(self):
        from MongoDB.db_actions import connect_to_n_ms_parsed_trades_db, connect_to_aggtrade_data_db
        from data_staging import optional_add_secs_in_ms, get_last_ts_from_db, round_to_last_n_secs
        from vars_constants import millisecs_timeframe, secs_parse_interval

        present_time = time.time()
        ms_parse_interval = secs_parse_interval * 1000
        self.start_ts, self.end_ts, iterations = {}, {}, {}

        for symbol in self.symbols:
            last_parsed_trade = optional_add_secs_in_ms((get_last_ts_from_db(connect_to_n_ms_parsed_trades_db(ms_parse_interval), symbol)), ms_parse_interval)
            self.start_ts[symbol] = round_to_last_n_secs(get_last_ts_from_db(connect_to_aggtrade_data_db(), symbol), secs_parse_interval) \
                if not last_parsed_trade else last_parsed_trade

            iterations[symbol] = range(0, millisecs_timeframe, ms_parse_interval) if \
                (self.start_ts[symbol] + millisecs_timeframe) / 1000 < present_time else range(0, 1)

            self.end_ts[symbol] = self.start_ts[symbol] + iterations[symbol].stop

        self.price_data = {iteration: {self.symbols[i]: 0 for i, elem in enumerate(self.symbols)} for iteration in iterations[symbol]}
        self.volume_data = {iteration: {self.symbols[i]: 0 for i, elem in enumerate(self.symbols)} for iteration in iterations[symbol]}

    def parse_trades(self, trades):
        def range_of_trades(trades, begin_ts, iteration):
            leftover_trades, partial_trades_list = [], []
            for i, trade in enumerate(trades):
                if begin_ts + iteration > list(trade.items())[1][1]:
                    partial_trades_list.append(trade)
                else:
                    leftover_trades.append(trade)

                return leftover_trades, partial_trades_list

        for interval in range(0, self.timeframe, self.seconds_interval):
            leftover_trades, partial_trades_list = range_of_trades(trades, self.starting_timestamp,
                                                                   self.seconds_interval)
            trades = leftover_trades
            if partial_trades_list:
                price = (partial_trades_list[0][PRICE] + partial_trades_list[-1][PRICE]) / 2
                volume = sum([elem[PRICE] * elem[QUANTITY] for elem in partial_trades_list])
                self.price_data[from_seconds_n_to_n_plus][symbol] = price
                self.volume_data[from_seconds_n_to_n_plus][symbol] = volume

                if symbol in SP500_SYMBOLS_USDT_PAIRS:
                    self.price_data[from_seconds_n_to_n_plus][symbol] = price
                    self.volume_data[from_seconds_n_to_n_plus][symbol] = volume
                    # fund_symbol_ratio_prices[from_seconds_n_to_n_plus][symbol] = price * coin_ratios[symbol]
                    # fund_symbol_volumes[from_seconds_n_to_n_plus][symbol] = volume
        pass



# @dataclass
# class ParseSymbolsAggtradeData:
#     def __init__(self, timeframe_in_secs, symbols, range_in_secs=10, ts_begin=None):
#         from MongoDB.db_actions import connect_to_n_ms_parsed_trades_db
#         from data_staging import optional_add_secs_in_ms, get_last_ts_from_db, round_to_last_n_secs
#         from tasks.transform_trade_data import DEFAULT_COL_SEARCH
#         from MongoDB.db_actions import connect_to_aggtrade_data_db
#
#         #TODO Validate ts_begin.
#         if not ts_begin or not (ts_begin := optional_add_secs_in_ms((get_last_ts_from_db(connect_to_n_ms_parsed_trades_db(range_in_secs),
#                                                                          DEFAULT_COL_SEARCH)), range_in_secs)):
#                 ts_begin = round_to_last_n_secs(get_last_ts_from_db(connect_to_aggtrade_data_db(), DEFAULT_COL_SEARCH), range_in_secs)
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
