import dataclasses
import logging
from datetime import datetime
import logs
from data_handling.data_structures import make_trade_data_group, CacheTradesChartData, TradesChart, TradesChartGroup
from data_handling.data_helpers.vars_constants import TEN_SECS_PARSED_TRADES_DB, TRADE_DATA_CACHE_TIME_IN_MS, \
    TEN_SECONDS_IN_MS, BASE_TRADES_CHART_DB, UNUSED_CHART_TRADE_SYMBOLS
from MongoDB.db_actions import ValidatorDB, DB
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

    chart_timeframe, chart_timeframe_in_ms = args['chart_minutes'], mins_to_ms(args['chart_minutes'])
    LOG.info("Starting to transform trade data for interval '%s' minutes, a start timestamp of '%s' and "
             "finish timestamp of '%s'.", chart_timeframe, datetime.fromtimestamp(begin_ts / 1000),
             datetime.fromtimestamp((finish_ts) / 1000))

    parse_interval = DB(BASE_TRADES_CHART_DB.format(chart_timeframe)).atomicity

    symbols = [symbol for symbol in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if symbol not in UNUSED_CHART_TRADE_SYMBOLS]

    trade_data = make_trade_data_group(symbols, begin_ts - chart_timeframe_in_ms + TEN_SECONDS_IN_MS,
                                       begin_ts, TEN_SECS_PARSED_TRADES_DB, filled=True)

    trades = {symbol: TradesChart(
        **{'trades': getattr(trade_data, symbol)}) for symbol in [field.name for field in dataclasses.fields(trade_data)]}

    symbols_timeframe_trades = dataclasses.make_dataclass(
        'TradesChartGroup', [("symbols_data", dict)], bases=(TradesChartGroup,))\
            (**{'symbols_data': trades,
                'parse_interval': parse_interval})

    cache_future_trades = make_trade_data_group(symbols, begin_ts + TEN_SECONDS_IN_MS, begin_ts + TRADE_DATA_CACHE_TIME_IN_MS,
                                                TEN_SECS_PARSED_TRADES_DB, filled=True)

    cache_db_insert = CacheTradesChartData(chart_timeframe, symbols)
    while symbols_timeframe_trades.end_ts <= finish_ts:
        cache_db_insert.append_update(symbols_timeframe_trades)
        cache_future_trades.del_update_cache(parse_interval)
        symbols_timeframe_trades.add_trades_interval(cache_future_trades)
    else:
        cache_db_insert.insert_in_db_clear()
        exit(0)

