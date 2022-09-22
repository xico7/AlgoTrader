import logging
from datetime import datetime
import logs
from data_handling.data_helpers.vars_constants import TS, FIVE_SECS_IN_MS, PARSED_AGGTRADES_DB, DEFAULT_COL_SEARCH
from MongoDB.db_actions import trades_chart, query_starting_ts, query_db_col_between, insert_many_same_db_col, db_col_names
from data_handling.data_helpers.data_staging import get_counter, get_current_second_in_ms, mins_to_ms


# TODO: Make cached_volume_chart insert many, maybe active sessions reduce.
# TODO: change logical sessions and refreshmillis parameter in mongodb
LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def transform_trade_data(args):

    transform_db_name = args['db_name']
    price_volume_chart, cache = {}, 0

    symbols_one_day_trades, symbols_append_trades, start_ts, end_ts, ts_data = {}, {}, {}, {}, {}
    btcusdt_start_ts = query_starting_ts(trades_chart.format(args['chart_minutes']), "BTCUSDT", init_db=transform_db_name)
    btcusdt_end_ts = btcusdt_start_ts + mins_to_ms(args['chart_minutes']) + FIVE_SECS_IN_MS
    for collection in db_col_names(transform_db_name):
        start_ts[collection] = query_starting_ts(trades_chart.format(args['chart_minutes']), collection, init_db=transform_db_name)
        end_ts[collection] = start_ts[collection] + mins_to_ms(args['chart_minutes']) + FIVE_SECS_IN_MS
        query_db_col_between(PARSED_AGGTRADES_DB, DEFAULT_COL_SEARCH, btcusdt_start_ts, btcusdt_end_ts, limit=1)

        symbols_one_day_trades[collection] = query_db_col_between(transform_db_name, collection, start_ts[collection], end_ts[collection])

    LOG.info(f"Transforming data starting from {datetime.fromtimestamp(min(start_ts.values()) / 1000)} to {datetime.fromtimestamp(max(end_ts.values())  / 1000)}")

    finish_ts = get_current_second_in_ms()
    while max(list(end_ts.values())) < finish_ts:
        if cache == 0:
            for symbol in symbols_one_day_trades.keys():
                symbols_append_trades[symbol] = query_db_col_between(transform_db_name, symbol, end_ts[symbol],
                                                                     end_ts[symbol] + 30 * 10000)

        for symbol, symbol_tf_values in symbols_one_day_trades.items():
            tf_values = [tf['price'] for tf in symbol_tf_values if tf['price']]
            max_tf_value = max(tf_values)
            min_tf_value = min(tf_values)
            price_range = ((max_tf_value - min_tf_value) * 100 / max_tf_value)
            most_recent_price = tf_values[-1]
            price_volume_chart.setdefault(symbol, []).append({
                'min': min_tf_value, 'max': max_tf_value, 'end_ts': symbols_one_day_trades[symbol][0][TS],
                'begin_ts': symbols_one_day_trades[symbol][-1][TS], 'last_price': most_recent_price,
                'range_percentage': price_range, 'total_volume': sum(tf_values), 'last_price_counter': get_counter(min_tf_value, price_range, most_recent_price)})

        for symbol, symbol_values in symbols_one_day_trades.items():
            symbol_values.pop(0)
            symbol_values.append(symbols_append_trades[symbol][0])
            symbols_append_trades[symbol].pop(0)

        cache += 1
        if cache == 30:
            insert_many_same_db_col(trades_chart.format(args['chart_minutes']), price_volume_chart)
            price_volume_chart = []
            cache = 0


    print("success.")
    exit(0)

