import logging
import time
from datetime import timedelta

import logs
from support.data_handling.data_structures import CacheTradesChartData, TradeDataGroup
from support.data_handling.data_helpers.vars_constants import TEN_SECS_PARSED_TRADES_DB, \
    TRADE_DATA_CACHE_TIME_IN_MINUTES, UNUSED_CHART_TRADE_SYMBOLS, DEFAULT_PARSE_INTERVAL_IN_MS, DEFAULT_COL_SEARCH, \
    DEFAULT_PARSE_INTERVAL_SECONDS, DEFAULT_PARSE_INTERVAL_TIMEDELTA
from MongoDB.db_actions import (DB, TRADES_CHART_TIMEFRAMES_VALUES, TRADES_CHART_TF_ATOMICITY,
                                TradesChartTimeframeValuesAtomicity)
from support.generic_helpers import mins_to_ms, date_from_timestamp_in_ms, round_last_ten_secs

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidEndTS(Exception): pass


TRANSFORM_TRADE_DATA_USED_SYMBOLS = [symbol for symbol in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if symbol not in UNUSED_CHART_TRADE_SYMBOLS]


def success_exit():
    LOG.info("Finished parsing trade data, no more trades to parse exiting..")
    exit(0)


def create_refresh_cache_future_trades(parsing_ts, symbols_to_parse) -> dict:
    cached_trades = {}
    trade_data_group = TradeDataGroup(TRADE_DATA_CACHE_TIME_IN_MINUTES,
                                      parsing_ts + timedelta(minutes=TRADE_DATA_CACHE_TIME_IN_MINUTES) + timedelta(seconds=DEFAULT_PARSE_INTERVAL_SECONDS),
                                      TEN_SECS_PARSED_TRADES_DB,
                                      True,
                                      symbols_to_parse)

    for symbol, trade_chart_obj in trade_data_group.symbols_data_group.items():
        cached_trades[symbol] = {trade.timestamp: trade for trade in trade_chart_obj.trades}

    return cached_trades


def transform_trade_data(args):
    LOG.info("These run arguments should only be started programatically through 'runner tasks' as some validations are done there beforehand.")

    begin_ts, finish_ts = round_last_ten_secs(args['start_end_timeframe'][0]), round_last_ten_secs(args['start_end_timeframe'][1])
    LOG.info(f"Starting to transform trade data from '{begin_ts}' until '{finish_ts}'.")

    # Below line for debug and testing.
    # TRANSFORM_TRADE_DATA_USED_SYMBOLS = ['BTCUSDT', 'ETHUSDT']

    cache_future_trades = create_refresh_cache_future_trades(begin_ts, TRANSFORM_TRADE_DATA_USED_SYMBOLS)

    symbols_timeframe_trades = {}
    cache_db_insert = {timeframe: CacheTradesChartData(timeframe) for timeframe in TRADES_CHART_TIMEFRAMES_VALUES}

    parsing_ts = begin_ts
    maximum_atomicity_timeframe = max([trade_chart_atomicity.value.atomicity for trade_chart_atomicity in TradesChartTimeframeValuesAtomicity])
    while parsing_ts <= finish_ts:
        if parsing_ts > list(cache_future_trades[DEFAULT_COL_SEARCH])[-1] - (maximum_atomicity_timeframe + DEFAULT_PARSE_INTERVAL_TIMEDELTA): #TODO: I think this is not ok..
            cache_future_trades = create_refresh_cache_future_trades(parsing_ts - DEFAULT_PARSE_INTERVAL_TIMEDELTA, TRANSFORM_TRADE_DATA_USED_SYMBOLS)

        for timeframe in TRADES_CHART_TIMEFRAMES_VALUES:
            if not int(parsing_ts.timestamp()) % TRADES_CHART_TF_ATOMICITY[timeframe].seconds:
                try:
                    symbols_timeframe_trades[timeframe].parse_trades_interval(cache_future_trades, parsing_ts)
                except KeyError:
                    symbols_timeframe_trades[timeframe] = TradeDataGroup(timeframe, parsing_ts,
                                                                         TEN_SECS_PARSED_TRADES_DB,
                                                                         True, TRANSFORM_TRADE_DATA_USED_SYMBOLS,
                                                                         TRADES_CHART_TF_ATOMICITY[timeframe])
                cache_db_insert[timeframe].append_update_insert_in_db(symbols_timeframe_trades[timeframe], parsing_ts)
        parsing_ts += DEFAULT_PARSE_INTERVAL_TIMEDELTA
    else:
        for timeframe in TRADES_CHART_TIMEFRAMES_VALUES:
            cache_db_insert[timeframe].insert_in_db_clear_cache()
        success_exit()
