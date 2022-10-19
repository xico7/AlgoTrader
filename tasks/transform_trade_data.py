import time
import logging
from dataclasses import asdict
from datetime import datetime
import logs
from data_handling.data_func import TradeData
from data_handling.data_helpers.vars_constants import TS, DEFAULT_SYMBOL_SEARCH, END_TS_AGGTRADES_VALIDATOR_DB, QUANTITY, \
    DEFAULT_PARSE_INTERVAL_IN_MS, PARSED_TRADES_BASE_DB, FUND_DB_COL
from MongoDB.db_actions import trades_chart, connect_to_db, aggregate_symbols_filled_data, \
    query_trade_db_col_between, insert_many_db, query_db_col_timestamp_endpoint, query_chart_endpoint_ts
from data_handling.data_helpers.data_staging import mins_to_ms


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidFinishValue(Exception): pass
class InvalidFundData(Exception): pass

# TODO: Improve this code.. too many repetition, log.debug not working etc..

def transform_trade_data(args):
    mins_to_add = mins_to_ms(30)
    start_ts = query_chart_endpoint_ts(trades_chart.format(args['chart_minutes']), DEFAULT_SYMBOL_SEARCH, most_recent=True, init_db=PARSED_TRADES_BASE_DB)
    end_ts = start_ts + mins_to_ms(args['chart_minutes'])
    if query_db_col_timestamp_endpoint(PARSED_TRADES_BASE_DB, FUND_DB_COL, most_recent=True) < end_ts:
        LOG.error("Fund data needs to be parsed ahead of ten second data.")
        raise InvalidFundData("Fund data needs to be parsed ahead of ten second data.")

    symbols_timeframe_trades = {**aggregate_symbols_filled_data(PARSED_TRADES_BASE_DB, start_ts, end_ts),
                                **{FUND_DB_COL: query_trade_db_col_between(PARSED_TRADES_BASE_DB, FUND_DB_COL, start_ts, end_ts)}}
    for symbol, symbol_trade_info in symbols_timeframe_trades.items():
        symbol_trade_info.trades = {str(key): asdict(value) for key, value in symbol_trade_info.trades.items()}

    symbols_thirty_mins_trades = {**aggregate_symbols_filled_data(PARSED_TRADES_BASE_DB, end_ts, end_ts + mins_to_add),
                                  **{FUND_DB_COL: query_trade_db_col_between(PARSED_TRADES_BASE_DB, FUND_DB_COL, end_ts, end_ts + mins_to_add)}}

    cache_symbols_timeframe_trades = {}

    if finish_value := connect_to_db(END_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).find_one():
        while symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts < finish_value[TS]:
            for symbol, symbol_trade_info in symbols_timeframe_trades.items():
                try:
                    trade_to_add = symbols_thirty_mins_trades[symbol].trades[symbol_trade_info.end_ts]
                except KeyError:
                    if cached_trades := query_trade_db_col_between(PARSED_TRADES_BASE_DB, symbol, symbol_trade_info.end_ts, symbol_trade_info.end_ts + mins_to_add):
                        symbols_thirty_mins_trades[symbol] = cached_trades.fill_trades_tf()
                    else:
                        #LOG.debug(f"Symbol {symbol} does not contain valid data.")
                        continue
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
                    if symbol_trade_info:
                        insert_many_db(trades_chart.format(args['chart_minutes']), symbol, list(symbol_trade_info.values()))
                    time.sleep(0.15)  # Multiple connections socket saturation delay.
                else:
                    cache_symbols_timeframe_trades = {}

                LOG.info(f"Transformeddata starting from {(datetime.fromtimestamp(symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts / 1000 - 30000))} "
                         f"to {datetime.fromtimestamp(symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts / 1000)}")

    else:
        LOG.error("Finish value not initialized in DB.")
        raise InvalidFinishValue("Finish value not initialized in DB.")

    print("success.")
    exit(0)

