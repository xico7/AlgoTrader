import time

EVENT_TS = "E"
PRICE = "p"
QUANTITY = 'q'
END_TS = "end_timestamp"


def transform_trade_data(args):
    from MongoDB.db_actions import insert_many_to_db, connect_to_db, connect_to_ten_secs_parsed_trades_db
    from data_staging import get_counter, get_last_ts_from_db, query_db_last_minute, MongoDB, TEN_SECONDS_IN_MS, \
        sec_to_ms, debug_prints, sleep_until_time_match, ONE_DAY_IN_MS, FIVE_SECS_IN_MS

    db_type = args['transform_trade_data_db_name']
    #timeframe, time_interval = args["transform_trade_data_timeframe_in_secs"], args['transform_trade_data_interval_in_secs']
    # db_name = f"{db_type}_timeframe_{timeframe}_interval_{time_interval}"
    #
    # starting_execution_ts = query_db_last_minute(db_name)
    # timeframe_in_ms = sec_to_ms(timeframe)
    price_volume_chart = {}
    first_element_timestamp = {}
    last_ts = {}
    new_last_ts = {}
    symbols_one_day_trades = {}
    symbol_prices = {}
    symbol_volume = {}

    for collection in connect_to_db(db_type).list_collection_names():
        first_element_timestamp[collection] = get_last_ts_from_db(connect_to_db(db_type), collection)

    for collection in connect_to_db(db_type).list_collection_names():
        last_ts[collection] = first_element_timestamp[collection] + ONE_DAY_IN_MS + FIVE_SECS_IN_MS
        symbols_one_day_trades[collection] = list(connect_to_db(db_type).get_collection(collection).find(
                {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: first_element_timestamp[collection]}},
                               {EVENT_TS: {MongoDB.LOWER_EQ: last_ts[collection]}}]}))

    while True:
        time1 = time.time()
        for collection in connect_to_db(db_type).list_collection_names():
            symbols_one_day_trades[collection].pop(0)
            symbols_one_day_trades[collection].append(list(connect_to_db(db_type).get_collection(collection).find(
                    {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: last_ts[collection]}},
                                   {EVENT_TS: {MongoDB.LOWER_EQ: last_ts[collection] + TEN_SECONDS_IN_MS}}]})))
            last_ts[collection] += TEN_SECONDS_IN_MS

        timeit = time.time() - time1

        for symbol, symbol_values in symbols_one_day_trades.items():
            symbol_volume[symbol] = 0
            symbol_prices[symbol] = []
            for elem in symbol_values:
                #elem[PRICE]) não é floatable.... é str -.-
                symbol_prices[symbol].append(float(elem[PRICE]))
                symbol_volume[symbol] += float(elem[QUANTITY])

            if symbol_volume[symbol] == 0:
                continue

            max_value = max(symbol_prices[symbol])
            min_value = min(symbol_prices[symbol])
            price_range = (max_value - min_value) / 10

            most_recent_price = symbols_one_day_trades[symbol][-1]
            price_volume_chart[symbol] = {"last_price_counter": get_counter(min_value, price_range, most_recent_price)}
            price_volume_chart[symbol]["last_price"] = most_recent_price
            price_volume_chart[symbol][END_TS] = starting_execution_ts
            price_volume_chart[symbol]["begin_timestamp"] = starting_execution_ts - timeframe_in_ms
            price_volume_chart[symbol]["range_percentage"] = (max_value - min_value) * 100 / max_value
            price_volume_chart[symbol]["min"] = min_value
            price_volume_chart[symbol]["max"] = max_value
            price_volume_chart[symbol]["total_volume"] = symbol_volume[symbol]

        #starting_execution_ts += int(str(int(time_interval)) + "000")



        for collection in connect_to_db(db_type).list_collection_names():
            symbol_price = []
            total_volume = 0

            time1 = time.time()
            list_trades = list(connect_to_db(db_type).get_collection("BTCUSDT").find(
                    {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: starting_execution_ts - timeframe_in_ms}},
                                   {EVENT_TS: {MongoDB.LOWER_EQ: starting_execution_ts}}]}))
            timeit = time.time() - time1
            if not list_trades:
                continue

            for elem in list_trades:

                symbol_price.append(float(elem[PRICE]))
                total_volume += float(elem[QUANTITY])

            if total_volume == 0:
                continue

            max_value = max(symbol_price)
            min_value = min(symbol_price)
            price_range = (max_value - min_value) / 10

            most_recent_price = float(list_trades[0][PRICE])
            price_volume_chart[collection] = {"last_price_counter": get_counter(min_value, price_range, most_recent_price)}
            price_volume_chart[collection]["last_price"] = most_recent_price
            price_volume_chart[collection][END_TS] = starting_execution_ts
            price_volume_chart[collection]["begin_timestamp"] = starting_execution_ts - timeframe_in_ms
            price_volume_chart[collection]["range_percentage"] = (max_value - min_value) * 100 / max_value
            price_volume_chart[collection]["min"] = min_value
            price_volume_chart[collection]["max"] = max_value
            price_volume_chart[collection]["total_volume"] = total_volume

            for elem in list_trades:
                elem_counter = get_counter(min_value, price_range, float(elem[PRICE]))

                elem_volume_pct = (float(elem[QUANTITY]) / total_volume) * 100

                try:
                    add_to_volume = price_volume_chart[collection][elem_counter]
                except KeyError:
                    add_to_volume = 0
                price_volume_chart[collection][elem_counter] = add_to_volume + elem_volume_pct

        insert_many_to_db(db_name, price_volume_chart)

        debug_prints(starting_execution_ts)


