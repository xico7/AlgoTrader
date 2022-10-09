import time
import logging
from dataclasses import asdict
from datetime import datetime
import logs
from data_handling.data_func import TradeData
from data_handling.data_helpers.vars_constants import TS, DEFAULT_SYMBOL_SEARCH, AGGTRADES_VALIDATOR_DB_TS, QUANTITY, DEFAULT_PARSE_INTERVAL_IN_MS
from MongoDB.db_actions import trades_chart, query_starting_ts, connect_to_db, insert_one_db, \
    aggregate_symbols_filled_data, query_db_col_between
from data_handling.data_helpers.data_staging import mins_to_ms


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidFinishValue(Exception): pass


def transform_trade_data(args):
    transform_db_name = args['db_name']
    mins_to_add = mins_to_ms(30)
    start_ts = query_starting_ts(trades_chart.format(args['chart_minutes']), DEFAULT_SYMBOL_SEARCH, init_db=transform_db_name)
    end_ts = start_ts + mins_to_ms(args['chart_minutes'])
    symbols_timeframe_trades = aggregate_symbols_filled_data(transform_db_name, start_ts, end_ts)
    for symbol, symbol_trade_info in symbols_timeframe_trades.items():
        symbol_trade_info.trades = {str(key): asdict(value) for key, value in symbol_trade_info.trades.items()}

    symbols_thirty_mins_trades = aggregate_symbols_filled_data(transform_db_name, end_ts, end_ts + mins_to_add)

    LOG.info(f"Transforming data starting from {datetime.fromtimestamp(start_ts / 1000)} to {datetime.fromtimestamp(end_ts / 1000)}")

    if finish_value := connect_to_db(AGGTRADES_VALIDATOR_DB_TS).get_collection(TS).find_one():
        while end_ts < finish_value[TS]:
            for symbol, symbol_trade_info in symbols_timeframe_trades.items():
                try:
                    trade_to_add = symbols_thirty_mins_trades[symbol].trades[symbol_trade_info.end_ts]
                except KeyError:
                    end_ts = symbol_trade_info.end_ts
                    if cached_trades := query_db_col_between(transform_db_name, symbol, end_ts, end_ts + mins_to_add):
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

            for symbol, symbol_trade_info in symbols_timeframe_trades.items():
                insert_one_db(trades_chart.format(args['chart_minutes']), symbol, symbol_trade_info.asdict_without_keys('trades'))
                time.sleep(0.05)

    else:
        LOG.error("Finish value not initialized in DB.")
        raise InvalidFinishValue("Finish value not initialized in DB.")

    print("success.")
    exit(0)

