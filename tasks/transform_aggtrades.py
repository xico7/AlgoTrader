import time

EVENT_TS = "E"
PRICE = "p"
QUANTITY = 'q'
END_TS = "End_timestamp"


def transform_ws_trades(args):
    from MongoDB.db_actions import connect_to_aggtrade_data_db, insert_many_to_timeframe_db
    from data_staging import get_current_time_ms, get_counter, get_timeframe_db_last_minute, MongoDB, sec_to_ms, debug_prints

    starting_execution_ts = get_timeframe_db_last_minute(args["transform_aggtrades_timeframe_in_secs"])
    timeframe_in_ms = sec_to_ms(args["transform_aggtrades_timeframe_in_secs"])
    begin_ts = starting_execution_ts - timeframe_in_ms
    symbols_volume_chart = {}

    while True:
        for symbol in connect_to_aggtrade_data_db().list_collection_names():
            symbol_price = []
            total_volume = 0

            list_trades = list(connect_to_aggtrade_data_db().get_collection(symbol).find(
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

            symbols_volume_chart[symbol] = {"last_price_counter": get_counter(min_value, price_range, float(list_trades[-1][PRICE]))}
            symbols_volume_chart[symbol]["last_price"] = float(list_trades[-1][PRICE])
            symbols_volume_chart[symbol][END_TS] = starting_execution_ts
            symbols_volume_chart[symbol]["Begin_timestamp"] = begin_ts
            symbols_volume_chart[symbol]["range_percentage"] = (max_value - min_value) * 100 / max_value
            symbols_volume_chart[symbol]["min"] = min_value
            symbols_volume_chart[symbol]["max"] = max_value

            for elem in list_trades:
                elem_counter = get_counter(min_value, price_range, float(elem[PRICE]))
                elem_volume_pct = (float(elem[QUANTITY]) / total_volume) * 100

                try:
                    add_to_volume = symbols_volume_chart[symbol][elem_counter]
                except KeyError:
                    add_to_volume = 0
                symbols_volume_chart[symbol][elem_counter] = add_to_volume + elem_volume_pct

        insert_many_to_timeframe_db(args["transform_aggtrades_timeframe_in_secs"], symbols_volume_chart)

        debug_prints(starting_execution_ts)
        starting_execution_ts += int(str(int(args["transform_aggtrades_interval_in_secs"])) + "000")

        while starting_execution_ts > get_current_time_ms():
            time.sleep(1)
