import logging
import time
from datetime import datetime
import logs
from data_handling.data_func import get_trade_data_group, CacheTradeData, InvalidStartTimestamp
from data_handling.data_helpers.vars_constants import DEFAULT_SYMBOL_SEARCH, TEN_SECS_PARSED_TRADES_DB, \
    TRADE_DATA_CACHE_TIME_IN_MS, UNUSED_CHART_TRADE_SYMBOLS, TEN_SECONDS_IN_MS
from MongoDB.db_actions import trades_chart, ten_seconds_symbols_filled_data, DB, ValidatorDB
from data_handling.data_helpers.data_staging import mins_to_ms


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def transform_trade_data(args):
    chart_tf_db = trades_chart.format(args['chart_minutes'])
    chart_filtered_symbols = [symbol for symbol in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if symbol not in UNUSED_CHART_TRADE_SYMBOLS]
    cache_db_insert = CacheTradeData(chart_tf_db, chart_filtered_symbols)

    if args['multithread_start_end_timeframe']:
        start_ts = args['multithread_start_end_timeframe'][0]
        finish_ts = args['multithread_start_end_timeframe'][1] + mins_to_ms(args['chart_minutes'])
    else:
        finish_ts = 0
        if not (start_ts := ValidatorDB(chart_tf_db).finish_ts) and not (start_ts := ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts):
            LOG.error(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
            raise InvalidStartTimestamp(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")

        start_ts += TEN_SECONDS_IN_MS

    end_ts = start_ts + mins_to_ms(args['chart_minutes'])

    symbols_timeframe_trades = ten_seconds_symbols_filled_data(chart_filtered_symbols, start_ts, end_ts)
    cache_future_trades = get_trade_data_group(chart_filtered_symbols, end_ts + TEN_SECONDS_IN_MS,
                                               end_ts + TRADE_DATA_CACHE_TIME_IN_MS, TEN_SECS_PARSED_TRADES_DB,
                                               filled=True)

    def calc_finish_ts(finish_ts):
        if not finish_ts:
            finish_ts = ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts
        elif ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts < finish_ts + mins_to_ms(args['chart_minutes']):
            finish_ts = ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts - mins_to_ms(args['chart_minutes'])
        return finish_ts

    LOG.info("Starting to transform trade data for interval '%s' minutes, a start timestamp of '%s' and finish timestamp of '%s'.",
             args['chart_minutes'], datetime.fromtimestamp(start_ts / 1000),
             datetime.fromtimestamp((calc_finish_ts(finish_ts) - mins_to_ms(args['chart_minutes'])) / 1000))

    while symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts <= calc_finish_ts(finish_ts):
        cache_future_trades.del_update_cache(chart_filtered_symbols)
        cache_db_insert.append_update(symbols_timeframe_trades)

        for symbol, symbol_trade_info in symbols_timeframe_trades.items():
            symbols_timeframe_trades[symbol] += getattr(cache_future_trades, symbol)[0]
    else:
        cache_db_insert.insert_in_db_clear()

    if args['multithread_start_end_timeframe']:
        LOG.info(f"Parsed trade data from {datetime.fromtimestamp(args['multithread_start_end_timeframe'][0] / 1000) } to "
                 f"{datetime.fromtimestamp(args['multithread_start_end_timeframe'][1] / 1000) } for a {args['chart_minutes']} timeframe.")
        exit(0)
    else:
        LOG.info("Finished parsing trade data, waiting 20 minutes and trying again.")
        time.sleep(1200)
        transform_trade_data(args)
