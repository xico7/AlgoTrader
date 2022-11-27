import logging
import logs
from data_handling.data_func import get_trade_data_group, CacheTradeData
from data_handling.data_helpers.vars_constants import DEFAULT_SYMBOL_SEARCH, TEN_SECS_PARSED_TRADES_DB, \
    TRADE_DATA_CACHE_TIME_IN_MS, UNUSED_CHART_TRADE_SYMBOLS, TEN_SECS_MS
from MongoDB.db_actions import trades_chart, ten_seconds_symbols_filled_data, DB, ValidatorDB, ATOMIC_TIMEFRAME_CHART_TRADES
from data_handling.data_helpers.data_staging import mins_to_ms


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class InvalidTimeframeProvided(Exception): pass


def transform_trade_data(args):
    if not args['chart_minutes']:
        args['chart_minutes'] = ATOMIC_TIMEFRAME_CHART_TRADES

    chart_tf_db = trades_chart.format(args['chart_minutes'])
    chart_filtered_symbols = [symbol for symbol in DB(TEN_SECS_PARSED_TRADES_DB).list_collection_names() if symbol not in UNUSED_CHART_TRADE_SYMBOLS]
    cache_db_insert = CacheTradeData(chart_tf_db, chart_filtered_symbols)

    if args['chart_minutes'] % ATOMIC_TIMEFRAME_CHART_TRADES != 0:
        LOG.error("Trades chart can only accept multiple of five minutes for trades chart parsing.")
        raise InvalidTimeframeProvided("Trades chart can only accept multiple of five minutes for trades chart parsing.")
    elif not (start_ts := ValidatorDB(chart_tf_db).end_ts) and not (start_ts := ValidatorDB(TEN_SECS_PARSED_TRADES_DB).start_ts):
                LOG.error(f"Trade data needs a valid {TEN_SECS_PARSED_TRADES_DB} db start timestamp entry.")
    else:
        if not args['multithread_start_end_timeframe']:
            end_ts = start_ts + mins_to_ms(args['chart_minutes'])

            symbols_timeframe_trades = ten_seconds_symbols_filled_data(chart_filtered_symbols, start_ts, end_ts)
            cache_future_trades = get_trade_data_group(chart_filtered_symbols, end_ts + TEN_SECS_MS,
                                                       end_ts + TRADE_DATA_CACHE_TIME_IN_MS, TEN_SECS_PARSED_TRADES_DB,
                                                       filled=True)
            while symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts < ValidatorDB(TEN_SECS_PARSED_TRADES_DB).end_ts:
                for symbol, symbol_trade_info in symbols_timeframe_trades.items():
                    symbols_timeframe_trades[symbol] += getattr(cache_future_trades, symbol)[0]

                cache_future_trades.del_update_cache(chart_filtered_symbols)
                cache_db_insert.append_update(symbols_timeframe_trades, True if args['chart_minutes'] > 5 else False)
        else:
            #TODO: Improve how this is done.. to many manual tasks in terminal.. good enough for now.
            start_ts, finish_ts = args['multithread_start_end_timeframe']
            start_ts_in_ms = start_ts * 1000
            finish_ts_in_ms = finish_ts * 1000
            end_ts = start_ts_in_ms + mins_to_ms(args['chart_minutes'])

            symbols_timeframe_trades = ten_seconds_symbols_filled_data(chart_filtered_symbols, start_ts_in_ms, end_ts)
            cache_future_trades = get_trade_data_group(chart_filtered_symbols, end_ts + TEN_SECS_MS,
                                                       end_ts + TRADE_DATA_CACHE_TIME_IN_MS, TEN_SECS_PARSED_TRADES_DB,
                                                       filled=True)

            while (symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts < ValidatorDB(TEN_SECS_PARSED_TRADES_DB).end_ts and
                symbols_timeframe_trades[DEFAULT_SYMBOL_SEARCH].end_ts < finish_ts_in_ms):
                for symbol, symbol_trade_info in symbols_timeframe_trades.items():
                    symbols_timeframe_trades[symbol] += getattr(cache_future_trades, symbol)[0]

                cache_future_trades.del_update_cache(chart_filtered_symbols)
                cache_db_insert.append_update(symbols_timeframe_trades, True if args['chart_minutes'] > 5 else False)

        LOG.info("Finished parsing trade data.")
        exit(0)
    # else:
    #     if not (start_ts := ValidatorDB(chart_tf_db).end_ts) and not (start_ts := ValidatorDB(trades_chart_base_db).start_ts):
    #             LOG.error(f"Trade data needs a valid {trades_chart_base_db} db start timestamp entry.")
    #
    #     trades_ta_indicators = {}
    #     while start_ts + mins_to_ms(args['chart_minutes']) < ValidatorDB(trades_chart_base_db).end_ts:
    #         for symbol in chart_filtered_symbols:
    #             trades = []
    #             for item in DBCol(trades_chart_base_db, symbol).column_between(
    #                     start_ts, start_ts + int(args['chart_minutes'] // ATOMIC_TIMEFRAME_CHART_TRADES) * mins_to_ms(ATOMIC_TIMEFRAME_CHART_TRADES),
    #                     'start_ts'):
    #                     trades.append(TradeData(**item))
    #             trades_ta_indicators[symbol] = TradesTAIndicators(**{'start_ts': trades[0].timestamp, 'end_ts': trades[-1].timestamp, 'trades': trades})
    #
    #         start_ts += mins_to_ms(args['chart_minutes'])
    #         cache_db_insert.append_update(trades_ta_indicators, True)

