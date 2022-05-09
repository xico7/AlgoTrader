import time


EVENT_TS = "E"
PRICE = "p"
QUANTITY = 'q'
END_TS = "end_timestamp"


def transform_trade_data(args):
    from MongoDB.db_actions import insert_many_to_db, connect_to_db
    from data_staging import get_current_second_in_ms, get_counter, query_db_last_minute, MongoDB, sec_to_ms, debug_prints

    db_type = args['transform_trade_data_db_name']
    timeframe, time_interval = args["transform_trade_data_timeframe_in_secs"], args['transform_trade_data_interval_in_secs']
    db_name = f"{db_type}_timeframe_{timeframe}_interval_{time_interval}"

    starting_execution_ts = query_db_last_minute(db_name)
    timeframe_in_ms = sec_to_ms(timeframe)
    price_volume_chart = {}

    while True:
        for collection in connect_to_db(db_type).list_collection_names():
            symbol_price = []
            total_volume = 0

            list_trades = list(connect_to_db(db_type).get_collection(collection).find(
                    {MongoDB.AND: [{EVENT_TS: {MongoDB.HIGHER_EQ: starting_execution_ts - timeframe_in_ms}},
                                   {EVENT_TS: {MongoDB.LOWER_EQ: starting_execution_ts}}]}))

            if not list_trades:
                continue

            for elem in list_trades:
                symbol_price.append(float(elem[PRICE]))
                total_volume += float(elem[QUANTITY])

            max_value = max(symbol_price)
            min_value = min(symbol_price)
            price_range = (max_value - min_value) / 10

            price_volume_chart[collection] = {"last_price_counter": get_counter(min_value, price_range, float(list_trades[-1][PRICE]))}
            price_volume_chart[collection]["last_price"] = float(list_trades[-1][PRICE])
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
        starting_execution_ts += int(str(int(time_interval)) + "000")

        while starting_execution_ts > get_current_second_in_ms():
            time.sleep(1)

        time.sleep(30)
