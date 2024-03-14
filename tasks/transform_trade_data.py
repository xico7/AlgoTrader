import logging
import logs
from data_handling.data_structures import CacheTradesChartData, TradeDataGroup
from data_handling.data_helpers.vars_constants import TEN_SECS_PARSED_TRADES_DB, TRADE_DATA_CACHE_TIME_IN_MINUTES, \
    UNUSED_CHART_TRADE_SYMBOLS, DEFAULT_PARSE_INTERVAL_IN_MS, DEFAULT_COL_SEARCH
from MongoDB.db_actions import DB, TRADES_CHART_TIMEFRAMES_VALUES, TRADES_CHART_TF_ATOMICITY, \
    TradesChartTimeframeValuesAtomicity
from support.generic_helpers import mins_to_ms, date_from_timestamp_in_ms, round_last_ten_secs

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidEndTS(Exception): pass


TRANSFORM_TRADE_DATA_USED_SYMBOLS = [symbol for symbol in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if
                                     symbol not in UNUSED_CHART_TRADE_SYMBOLS]


def success_exit():
    LOG.info("Finished parsing trade data, no more trades to parse exiting..")
    exit(0)


def remove_refresh_cache_future_trades(cached_future_trades: dict, parsing_ts, finish_ts) -> dict:
    # Remove already used timestamps
    for index, timestamp in enumerate([trade.timestamp for trade in cached_future_trades[DEFAULT_COL_SEARCH]]):
        if timestamp == parsing_ts:
            break
    else:
        LOG.error("Invalid timestamps provided.")
        exit(1)
    new_cache_future_trades = {symbol: trades[index:] for symbol, trades in cached_future_trades.items()}

    if not new_cache_future_trades[DEFAULT_COL_SEARCH]:
        added_value = parsing_ts + int(TRADE_DATA_CACHE_TIME_IN_MINUTES)
        if added_value > finish_ts:
            success_exit()
        new_cache_future_trades = {symbol: trade_chart_obj.trades for symbol, trade_chart_obj in
                                   TradeDataGroup(
                                       parsing_ts + DEFAULT_PARSE_INTERVAL_IN_MS,
                                       added_value,
                                       TEN_SECS_PARSED_TRADES_DB,
                                       True,
                                       TRANSFORM_TRADE_DATA_USED_SYMBOLS).symbols_data_group.items()}

        return new_cache_future_trades


def transform_trade_data(args):
    LOG.info("These run arguments should only be started programatically through"
             " 'runner tasks' as some validations are done there beforehand.")

    begin_ts, finish_ts = round_last_ten_secs(args['start_end_timeframe'][0]), round_last_ten_secs(args['start_end_timeframe'][1])
    LOG.info(f"Starting to transform trade data from {date_from_timestamp_in_ms(begin_ts)} "
             f"until {date_from_timestamp_in_ms(finish_ts)}")

    # TODO: Remove below line for faster execution
    TRANSFORM_TRADE_DATA_USED_SYMBOLS = ['BTCUSDT']
    cache_future_trades = {symbol: trade_chart_obj.trades for symbol, trade_chart_obj in
                           TradeDataGroup(TRADE_DATA_CACHE_TIME_IN_MINUTES,
                                          begin_ts + mins_to_ms(TRADE_DATA_CACHE_TIME_IN_MINUTES) + DEFAULT_PARSE_INTERVAL_IN_MS,
                                          TEN_SECS_PARSED_TRADES_DB,
                                          True,
                                          TRANSFORM_TRADE_DATA_USED_SYMBOLS).symbols_data_group.items()}

    symbols_timeframe_trades = {}
    cache_db_insert = {timeframe: CacheTradesChartData(timeframe) for timeframe in TRADES_CHART_TIMEFRAMES_VALUES}

    for timeframe, atomicity in TRADES_CHART_TF_ATOMICITY:
        symbols_timeframe_trades[timeframe] = TradeDataGroup(
            timeframe,
            begin_ts,
            TEN_SECS_PARSED_TRADES_DB,
            True,
            TRANSFORM_TRADE_DATA_USED_SYMBOLS,
            atomicity
        )

    parsing_ts = begin_ts
    while parsing_ts <= finish_ts:
        for timeframe in TRADES_CHART_TIMEFRAMES_VALUES:
            if not parsing_ts % symbols_timeframe_trades[timeframe].atomicity:
                symbols_timeframe_trades[timeframe].add_trades_interval(cache_future_trades, parsing_ts)
                cache_db_insert[timeframe].append_update(symbols_timeframe_trades[timeframe], parsing_ts)

        parsing_ts += DEFAULT_PARSE_INTERVAL_IN_MS
        if not parsing_ts % max([trade_chart_atomicity.value.atomicity for trade_chart_atomicity in TradesChartTimeframeValuesAtomicity]):
            remove_refresh_cache_future_trades(cache_future_trades, parsing_ts, finish_ts)
    else:
        for timeframe in TRADES_CHART_TIMEFRAMES_VALUES:
            cache_db_insert[timeframe].clear_cache()
        success_exit()
