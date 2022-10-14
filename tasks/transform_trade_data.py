import time
import logging
from dataclasses import asdict
from datetime import datetime
import logs
from data_handling.data_func import TradeData
from data_handling.data_helpers.vars_constants import TS, DEFAULT_SYMBOL_SEARCH, END_TS_AGGTRADES_VALIDATOR_DB, QUANTITY, \
    DEFAULT_PARSE_INTERVAL_IN_MS, PARSED_TRADES_BASE_DB, FUND_DB, FUND_DB_COL
from MongoDB.db_actions import trades_chart, query_starting_ts, connect_to_db, aggregate_symbols_filled_data, query_db_col_between, insert_many_db
from data_handling.data_helpers.data_staging import mins_to_ms


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidFinishValue(Exception): pass


def transform_trade_data(args):
    mins_to_add = mins_to_ms(30)
    start_ts = query_starting_ts(trades_chart.format(args['chart_minutes']), DEFAULT_SYMBOL_SEARCH, init_db=PARSED_TRADES_BASE_DB)
    end_ts = start_ts + mins_to_ms(args['chart_minutes'])
    symbols_timeframe_trades = {**aggregate_symbols_filled_data(PARSED_TRADES_BASE_DB, start_ts, end_ts),
                                **{FUND_DB_COL: query_db_col_between(FUND_DB, FUND_DB, start_ts, end_ts)}}
    symbols_thirty_mins_trades = {**aggregate_symbols_filled_data(PARSED_TRADES_BASE_DB, end_ts, end_ts + mins_to_add),
                                  **{FUND_DB_COL: query_db_col_between(FUND_DB, FUND_DB, end_ts, end_ts + mins_to_add)}}
    cache_symbols_timeframe_trades = {}

    for symbol, symbol_trade_info in symbols_timeframe_trades.items():
        symbol_trade_info.trades = {str(key): asdict(value) for key, value in symbol_trade_info.trades.items()}

    if finish_value := connect_to_db(END_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).find_one():
        while symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts < finish_value[TS]:
            LOG.info(f"Transforming data starting from {datetime.fromtimestamp(symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].start_ts / 1000)} "
                     f"to {datetime.fromtimestamp(symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts / 1000)}")

            for symbol, symbol_trade_info in symbols_timeframe_trades.items():
                try:
                    trade_to_add = symbols_thirty_mins_trades[symbol].trades[symbol_trade_info.end_ts]
                except KeyError:
                    db_to_query = PARSED_TRADES_BASE_DB if symbol != FUND_DB_COL else FUND_DB
                    if cached_trades := query_db_col_between(db_to_query, symbol, symbol_trade_info.end_ts, symbol_trade_info.end_ts + mins_to_add):
                        symbols_thirty_mins_trades[symbol] = cached_trades.fill_trades_tf()
                    else:
                        symbols_thirty_mins_trades[symbol] = cached_trades
                    trade_to_add = symbols_thirty_mins_trades[symbol].trades[symbol_trade_info.end_ts]

                trade_to_remove = TradeData(**symbol_trade_info.trades[str(symbol_trade_info.start_ts)])
                if not trade_to_add.price:
                    trade_to_add.price = trade_to_add.most_recent_price

                update_ranges = False
                if trade_to_add.price > symbol_trade_info.max_price:
                    symbol_trade_info.max_price = trade_to_add.price
                    update_ranges = True
                if trade_to_add.price < symbol_trade_info.min_price:
                    symbol_trade_info.min_price = trade_to_add.price
                    update_ranges = True

                if update_ranges:
                    symbol_trade_info.one_percent = (symbol_trade_info.max_price - symbol_trade_info.min_price) / 100
                    symbol_trade_info.price_range_percentage = (symbol_trade_info.max_price - symbol_trade_info.min_price) * 100 / symbol_trade_info.max_price
                    symbol_trade_info.range_price_volume = {str(i + 1): {'max': symbol_trade_info.min_price + (symbol_trade_info.one_percent * (i + 1)),
                                                                         'min': symbol_trade_info.min_price + (symbol_trade_info.one_percent * i), QUANTITY: 0} for i in range(100)}

                symbol_trade_info.total_volume += (trade_to_add.price * trade_to_add.quantity) - (trade_to_remove.price * trade_to_remove.quantity)
                symbol_trade_info.most_recent_price = trade_to_add.price
                symbol_trade_info.last_price_counter = symbol_trade_info.get_counter_factor_one_hundred(trade_to_add.price)
                symbol_trade_info.trades[str(symbol_trade_info.end_ts)] = asdict(symbols_thirty_mins_trades[symbol].trades[symbol_trade_info.end_ts])
                symbols_thirty_mins_trades[symbol].trades.pop(symbol_trade_info.end_ts)

                symbol_trade_info.start_ts += DEFAULT_PARSE_INTERVAL_IN_MS
                symbol_trade_info.end_ts += DEFAULT_PARSE_INTERVAL_IN_MS

            symbols = list(symbols_timeframe_trades.keys())
            if not cache_symbols_timeframe_trades:
                cache_symbols_timeframe_trades = {symbol: {'0': symbols_timeframe_trades[symbol].asdict_without_keys('trades')} for symbol in symbols}
            else:
                for symbol in symbols:
                    cache_symbols_timeframe_trades[symbol][str(len(cache_symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH]))] = symbols_timeframe_trades[symbol].asdict_without_keys('trades')

            if len(cache_symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH]) == 30:
                for symbol, symbol_trade_info in cache_symbols_timeframe_trades.items():
                    insert_many_db(trades_chart.format(args['chart_minutes']), symbol, list(symbol_trade_info.values()))
                    time.sleep(0.05)  # Multiple connections socket saturation delay.

    else:
        LOG.error("Finish value not initialized in DB.")
        raise InvalidFinishValue("Finish value not initialized in DB.")

    print("success.")
    exit(0)

