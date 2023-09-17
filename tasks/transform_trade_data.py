import logging
from datetime import datetime
import logs
from data_handling.data_structures import CacheTradesChartData, TradeDataGroup
from data_handling.data_helpers.vars_constants import TEN_SECS_PARSED_TRADES_DB, TRADE_DATA_CACHE_TIME_IN_MS, \
    UNUSED_CHART_TRADE_SYMBOLS, DEFAULT_PARSE_INTERVAL_IN_MS, DEFAULT_COL_SEARCH
from MongoDB.db_actions import ValidatorDB, DB, TradesChartTimeframes, TradesChartTimeframeValuesAtomicity
from support.generic_helpers import mins_to_ms

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidEndTS(Exception): pass


def transform_trade_data(args):
    begin_ts, finish_ts = args['multithread_start_end_timeframe'][0], args['multithread_start_end_timeframe'][1]
    if ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts < finish_ts:
        err_msg = f"Trades have not yet been parsed for the provided end timestamp of {datetime.fromtimestamp(finish_ts / 1000)}, " \
                  f"parsing is currently at {datetime.fromtimestamp(ValidatorDB(TEN_SECS_PARSED_TRADES_DB).finish_ts / 1000)}"
        LOG.error(err_msg)
        raise InvalidEndTS(err_msg)

    #chart_timeframe, chart_timeframe_in_ms = args['chart_minutes'], mins_to_ms(args['chart_minutes'])
    #LOG.info("Starting to transform trade data for interval '%s' minutes, a start timestamp of '%s' and "
    #         "finish timestamp of '%s'.", chart_timeframe, datetime.fromtimestamp(begin_ts / 1000),
    #         datetime.fromtimestamp((finish_ts) / 1000))

    symbols = [symbol for symbol in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if symbol not in UNUSED_CHART_TRADE_SYMBOLS]

    cache_future_trades = {symbol: trade_chart_obj.trades for symbol, trade_chart_obj in
     TradeDataGroup(begin_ts + DEFAULT_PARSE_INTERVAL_IN_MS, begin_ts + int(TRADE_DATA_CACHE_TIME_IN_MS),
                    TEN_SECS_PARSED_TRADES_DB, True, symbols).symbols_data_group.items()}

    cache_db_insert = {}
    symbols_timeframe_trades = {}
    trades_chart_timeframes_values = [t.value for t in TradesChartTimeframes]

    for timeframe in trades_chart_timeframes_values:
        cache_db_insert[timeframe] = CacheTradesChartData(timeframe)

    for timeframe, atomicity in [(t.timeframe.value, t.atomicity) for t in
                                 [t.value for t in TradesChartTimeframeValuesAtomicity]]:
        symbols_timeframe_trades[timeframe] = TradeDataGroup(
            begin_ts - mins_to_ms(timeframe) + DEFAULT_PARSE_INTERVAL_IN_MS, begin_ts,
            TEN_SECS_PARSED_TRADES_DB, True, symbols, atomicity)

    finish_parsing_msg = "Finished parsing trade data, no more trades to parse."
    parsing_ts = begin_ts
    while parsing_ts <= finish_ts:
        for timeframe in trades_chart_timeframes_values:
            if not symbols_timeframe_trades[timeframe].start_ts % symbols_timeframe_trades[timeframe].atomicity:
                cache_db_insert[timeframe].append_update(symbols_timeframe_trades[timeframe])
                symbols_timeframe_trades[timeframe].add_trades_interval(cache_future_trades)

        parsing_ts += DEFAULT_PARSE_INTERVAL_IN_MS

        # Remove already used timestamp
        cache_future_trades = {symbol: trades[1:] for symbol, trades in cache_future_trades.items()}

        if not cache_future_trades[DEFAULT_COL_SEARCH]:
            print("SDWUDJHQWUDHQWUDHQWUDHQWUDHQWU")
            added_value = parsing_ts + int(TRADE_DATA_CACHE_TIME_IN_MS)
            if added_value > finish_ts:
                LOG.info(finish_parsing_msg)
                exit(0)
            cache_future_trades = {symbol: trade_chart_obj.trades for symbol, trade_chart_obj in
                                   TradeDataGroup(parsing_ts + DEFAULT_PARSE_INTERVAL_IN_MS, added_value,
                                                  TEN_SECS_PARSED_TRADES_DB, True, symbols).symbols_data_group.items()}

    else:
        LOG.info(finish_parsing_msg)
        exit(0)


