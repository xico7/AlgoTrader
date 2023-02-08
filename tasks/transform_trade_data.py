import logging
from datetime import datetime
import logs
from data_handling.data_structures import make_trade_data_group, CacheTradeData, TradesTAIndicators
from data_handling.data_helpers.vars_constants import TEN_SECS_PARSED_TRADES_DB, TRADE_DATA_CACHE_TIME_IN_MS, \
    TEN_SECONDS_IN_MS, BASE_TRADES_CHART_DB, DEFAULT_COL_SEARCH, UNUSED_CHART_TRADE_SYMBOLS
from MongoDB.db_actions import ValidatorDB, DB
from data_handling.data_helpers.data_staging import mins_to_ms


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidEndTS(Exception): pass


def transform_trade_data(args):
    symbols = [symbol for symbol in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if symbol not in UNUSED_CHART_TRADE_SYMBOLS]
    cache_db_insert = CacheTradeData(args['chart_minutes'], symbols)
    timeframe_in_ms = mins_to_ms(args['chart_minutes'])
    begin_ts = args['multithread_start_end_timeframe'][0]
    finish_ts = args['multithread_start_end_timeframe'][1] + timeframe_in_ms

    if ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts < finish_ts:
        err_msg = f"Trades have not yet been parsed for the provided end timestamp of {datetime.fromtimestamp(finish_ts / 1000)}, " \
                  f"parsing is currently at {datetime.fromtimestamp(ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts / 1000)}"
        LOG.error(err_msg)
        raise InvalidEndTS(err_msg)

    LOG.info("Starting to transform trade data for interval '%s' minutes, a start timestamp of '%s' and "
             "finish timestamp of '%s'.", args['chart_minutes'], datetime.fromtimestamp(begin_ts / 1000),
             datetime.fromtimestamp((finish_ts - timeframe_in_ms) / 1000))

    DB(BASE_TRADES_CHART_DB.format(args['chart_minutes'])).clear_collections_between(begin_ts, finish_ts)
    trade_data = make_trade_data_group(symbols, begin_ts - timeframe_in_ms + TEN_SECONDS_IN_MS, begin_ts, TEN_SECS_PARSED_TRADES_DB, filled=True)
    symbols_timeframe_trades = {symbol: TradesTAIndicators(**{'trades': getattr(trade_data, symbol)}) for symbol in symbols}

    cache_future_trades = make_trade_data_group(symbols, begin_ts + TEN_SECONDS_IN_MS, begin_ts + TRADE_DATA_CACHE_TIME_IN_MS,
                                                TEN_SECS_PARSED_TRADES_DB, filled=True)

    while symbols_timeframe_trades[DEFAULT_COL_SEARCH].end_ts <= finish_ts:
        cache_db_insert.append_update(symbols_timeframe_trades)
        for symbol, symbol_trade_info in symbols_timeframe_trades.items():
            symbols_timeframe_trades[symbol] += getattr(cache_future_trades, symbol)[0]
        cache_future_trades.del_update_cache()

    cache_db_insert.insert_in_db_clear()
    exit(0)

