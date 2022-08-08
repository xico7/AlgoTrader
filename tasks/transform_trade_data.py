import logging

import logs
from vars_constants import DB_TS, FIVE_SECS_IN_MS
from MongoDB.db_actions import connect_to_db, trades_chart, query_starting_ts, query_db_col_between, insert_many_db
from data_staging import get_counter, get_current_second_in_ms, mins_to_ms


# TODO: Make cached_volume_chart insert many, maybe active sessions reduce.
# TODO: change logical sessions and refreshmillis parameter in mongodb
LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def transform_trade_data(args):

    LOG.info("Transforming data startfing from... asdasdsaas")
    finish_ts = get_current_second_in_ms()
    transform_db_name = args['transform_trade_data_db_name']
    price_volume_chart, cache = {}, 0

    symbols_one_day_trades, symbols_append_trades, start_ts, end_ts, ts_data = {}, {}, {}, {}, {}
    for collection in connect_to_db(transform_db_name).list_collection_names():
        start_ts[collection] = query_starting_ts(trades_chart.format(args['transform_trade_data_chart_minutes']), collection, init_db=transform_db_name)
        end_ts[collection] = start_ts[collection] + mins_to_ms(args['transform_trade_data_chart_minutes']) + FIVE_SECS_IN_MS
        symbols_one_day_trades[collection] = query_db_col_between(transform_db_name, collection, start_ts[collection], end_ts[collection])

    while max(list(end_ts.values())) < finish_ts:
        if cache == 0:
            for symbol in symbols_one_day_trades.keys():
                symbols_append_trades[symbol] = query_db_col_between(transform_db_name, symbol, end_ts[symbol],
                                                                     end_ts[symbol] + 30 * 10000)

        for symbol, symbol_values in symbols_one_day_trades.items():
            max_value = max([symbol['price'] for symbol in symbol_values if symbol['price']])
            min_value = min([symbol['price'] for symbol in symbol_values if symbol['price']])
            price_range = ((max_value - min_value) * 100 / max_value)
            most_recent_price = [trade['price'] for trade in symbol_values if trade['price']][-1]
            price_volume_chart[symbol].append({"min": min_value,
                                               "max": max_value,
                                               'end_ts': symbols_one_day_trades[symbol][0][DB_TS],
                                               'begin_ts': symbols_one_day_trades[symbol][-1][DB_TS],
                                               'last_price': most_recent_price,
                                               'range_percentage': price_range,
                                               'total_volume': sum([trade['quantity'] for trade in symbol_values if trade['quantity']]),
                                               'last_price_counter': get_counter(min_value, price_range, most_recent_price)})


        for symbol, symbol_values in symbols_one_day_trades.items():
            symbol_values.pop(0)
            symbol_values.append(symbols_append_trades[symbol][0])
            symbols_append_trades[symbol].pop(0)

        cache += 1
        if cache == 30:
            insert_many_db(trades_chart.format(args['transform_trade_data_chart_minutes']), price_volume_chart)
            price_volume_chart = []
            cache = 0


    print("success.")
    exit(0)
