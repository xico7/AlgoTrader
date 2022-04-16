import traceback

async def execute_daemon_ta_analysis(alive_debug_secs):
    print("execute_daemon_ta_analysis")

    import time
    from pymongo.errors import ServerSelectionTimeoutError
    import MongoDB.db_actions as mongo
    import logging

    from data_staging import query_atr, create_last_day_rs_chart, query_rel_vol, get_current_time, ONE_HOUR_IN_SECS, \
        ONE_MIN_IN_SECS, TS, THIRTY_MIN_IN_SECS, SYMBOLS_VOLUME, non_existing_record, print_alive_if_passed_timestamp

    SET_START_TIME_PAST_N_HOURS = 1000
    TA_MINUTE_PERIODS = 14

    iteration_minute = 0

    starting_execution_ts = get_current_time()

    ta_analysis_db = mongo.connect_to_ta_analysis_db()
    five_min_ohlc_db = mongo.connect_to_5m_ohlc_db()
    one_day_ohlc_db = mongo.connect_to_1d_ohlc_db()

    execution_time = get_current_time() - (ONE_HOUR_IN_SECS * SET_START_TIME_PAST_N_HOURS)

    long_vol_col = ta_analysis_db.get_collection("long_relative_volume")
    short_vol_col = ta_analysis_db.get_collection("short_relative_volume")
    atrp_col = ta_analysis_db.get_collection("average_true_range_percentage")
    rs_chart_col = ta_analysis_db.get_collection("last_day_rs_chart")

    while True:
        try:
            execution_time += 1
            if execution_time > get_current_time():
                print("Sleeping for one minute..")
                time.sleep(ONE_MIN_IN_SECS)

            current_iteration_time = execution_time
            while current_iteration_time % ONE_MIN_IN_SECS != 0:
                current_iteration_time -= 1

            if iteration_minute != current_iteration_time:
                iteration_minute = int(current_iteration_time)

                if non_existing_record(long_vol_col, iteration_minute):
                    long_vol_col.insert_one(
                        {TS: iteration_minute, SYMBOLS_VOLUME: query_rel_vol(iteration_minute, one_day_ohlc_db, 7)})
                if non_existing_record(short_vol_col, iteration_minute):
                    short_vol_col.insert_one(
                        {TS: iteration_minute, SYMBOLS_VOLUME: query_rel_vol(iteration_minute, five_min_ohlc_db, 14)})
                if non_existing_record(atrp_col, iteration_minute):
                    atrp_col.insert_one(
                        {TS: iteration_minute, "atrp": query_atr(iteration_minute, five_min_ohlc_db, 14)})

                if iteration_minute % THIRTY_MIN_IN_SECS == 0:
                    if non_existing_record(rs_chart_col, iteration_minute):
                        rs_chart_col.insert_one({
                            TS: iteration_minute, "rs_chart": create_last_day_rs_chart(iteration_minute, mongo.connect_to_ta_lines_db())})

            if print_alive_if_passed_timestamp(starting_execution_ts + alive_debug_secs):
                starting_execution_ts += alive_debug_secs

        except ServerSelectionTimeoutError as e:
            if "localhost:27017" in e.args[0]:
                logging.exception("Cannot connect to mongo DB")
                raise
            else:
                logging.exception("Unexpected error")
                raise
        except Exception as e:
            traceback.print_exc()
            print(f"{e}")
            exit(1)





