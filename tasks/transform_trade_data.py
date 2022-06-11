import time



# TODO: Verify if data is being well inserted in parsed-trades, if all
#  collections have same size they should be, if they don't its probably not a big deal but harder to verify..

# TODO: Done for all symbols, do the same for fund.
# TODO: If the symbol disappears while this is running it will work wrongly,
#  thankfully that is rare ocurrence, it happened with luna because i deleted it manually..
# TODO: Get candles from binance to have a selfhealing ws-trades
from vars_constants import EVENT_TS, DEFAULT_COL_SEARCH, PRICE, QUANTITY, END_TS


def transform_trade_data(args):
    from MongoDB.db_actions import insert_many_to_db, connect_to_db, symbol_price_chart_db_name
    from data_staging import get_counter, get_last_ts_from_db, MongoDB, TEN_SECONDS_IN_MS, FIVE_SECS_IN_MS, ONE_DAY_IN_MS, \
        get_most_recent_price, get_first_ts_from_db, optional_add_secs_in_ms

    transform_db_name = args['transform_trade_data_db_name']
    db_cols = connect_to_db(transform_db_name).list_collection_names()

    symbol_price_chart_timeframe = TEN_SECONDS_IN_MS
    if not (first_element_timestamp := optional_add_secs_in_ms(get_last_ts_from_db(connect_to_db(symbol_price_chart_db_name), DEFAULT_COL_SEARCH,
                                                               timestamp_arg='end_timestamp'), symbol_price_chart_timeframe)):
        first_element_timestamp = get_first_ts_from_db(connect_to_db(transform_db_name), DEFAULT_COL_SEARCH)

    last_element_timestamp = first_element_timestamp + args['transform_trade_data_chart_milliseconds'] + FIVE_SECS_IN_MS

    symbols_one_day_trades = {}
    for col in db_cols:
        timeframe_data = list(connect_to_db(transform_db_name).get_collection(col).find(
            {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: first_element_timestamp}},
                           {EVENT_TS: {MongoDB.LOWER_EQ: last_element_timestamp}}]}))
        if len(timeframe_data) > (last_element_timestamp - first_element_timestamp) / 10000 - 1:
            symbols_one_day_trades[col] = timeframe_data

    symbols_append_trades = {}

    finish_ts = get_last_ts_from_db(connect_to_db(transform_db_name), DEFAULT_COL_SEARCH)
    cache = 0
    price_volume_chart = {}
    while True:
        if last_element_timestamp > finish_ts:
            print("success.")
            exit(0)
        time1 = time.time()
        if not symbols_append_trades or not symbols_append_trades[DEFAULT_COL_SEARCH]:
            for symbol in db_cols:
                symbols_append_trades[symbol] = list(connect_to_db(transform_db_name).get_collection(symbol).find(
                        {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: last_element_timestamp}},
                                       {EVENT_TS: {MongoDB.LOWER_EQ: last_element_timestamp + symbol_price_chart_timeframe * 300}}]}))

        #price_volume_chart.append({})
        for symbol, symbol_values in symbols_one_day_trades.items():
            symbol_volume = 0
            symbol_prices = []
            for elem in symbol_values:
                if elem[PRICE]:
                    symbol_prices.append(float(elem[PRICE]))
                    symbol_volume += float(elem[QUANTITY])

            if symbol_volume == 0:
                continue
            # TODO: Make cached_volume_chart insert many, maybe active sessions reduce.
            # TODO: change logical sessions and refreshmillis parameter in mongodb
            max_value = max(symbol_prices)
            min_value = min(symbol_prices)
            price_range = (max_value - min_value) / 10
            most_recent_price = get_most_recent_price(symbols_one_day_trades[symbol])
            #if not price_volume_chart[symbol]
            price_volume_chart[symbol].append({"last_price_counter": get_counter(min_value, price_range, most_recent_price)})
            price_volume_chart[symbol][cache]["last_price"] = most_recent_price
            price_volume_chart[symbol][cache][END_TS] = symbols_one_day_trades[symbol][0][EVENT_TS]
            price_volume_chart[symbol][cache]["begin_timestamp"] = symbols_one_day_trades[symbol][-1][EVENT_TS]
            price_volume_chart[symbol][cache]["range_percentage"] = (max_value - min_value) * 100 / max_value
            price_volume_chart[symbol][cache]["min"] = min_value
            price_volume_chart[symbol][cache]["max"] = max_value
            price_volume_chart[symbol][cache]["total_volume"] = symbol_volume

        last_element_timestamp += symbol_price_chart_timeframe
        cache += 1
        if cache == 30:
            insert_many_to_db(symbol_price_chart_db_name.format(args['transform_trade_data_chart_milliseconds']), price_volume_chart)
            price_volume_chart = [{}]
            cache = 0

        for symbol, symbol_values in symbols_one_day_trades.items():
            symbol_values.pop()
            symbol_values.insert(0, symbols_append_trades[symbol][-1])
            symbols_append_trades[symbol].pop()


        print(time.time() - time1)




