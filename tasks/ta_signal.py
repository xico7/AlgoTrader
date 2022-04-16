import traceback
# CHANGEABLE PARAMETERS #
from data_staging import print_alive_if_passed_timestamp

RS_THRESHOLD = 0.0001
RS_SANITY_VALUE_THRESHOLD = 8
LONG_RVOL_THRESHOLD = 0.0001
SHORT_RVOL_THRESHOLD = 0.0001
ATRP_THRESHOLD = 0.0001
TA_MINUTE_PERIODS = 14
SET_START_TIME_PAST_N_HOURS = 200

#########################

iteration_minute = 0


async def execute_daemon_ta_signals(alive_debug_secs):
    import time
    from datetime import datetime
    import MongoDB.db_actions as mongo
    from pymongo.errors import ServerSelectionTimeoutError
    import logging

    from data_staging import get_current_time, ONE_HOUR_IN_SECS, ONE_MIN_IN_SECS, query_db_ta_value, SYMBOLS_VOLUME, \
        get_ta_indicator_when_rs_threshold, get_joined_signals, query_rs_signal_chart, TS, non_existing_record
    global iteration_minute

    rs_signal = None
    starting_execution_ts = get_current_time()


    print("execute_daemon_ta_signals")
    execution_time = get_current_time() - (ONE_HOUR_IN_SECS * SET_START_TIME_PAST_N_HOURS)

    while True:
        try:
            execution_time += 1

            if execution_time > get_current_time():
                print("Sleeping for one minute..")
                time.sleep(ONE_MIN_IN_SECS)

            current_iteration_time = execution_time
            while current_iteration_time % 60 != 0:
                current_iteration_time -= 1

            if iteration_minute != current_iteration_time:
                iteration_minute = int(current_iteration_time)

                if rs_signal:
                    long_rvol_signal = query_db_ta_value("long_relative_volume", iteration_minute,
                                                         LONG_RVOL_THRESHOLD, SYMBOLS_VOLUME)
                    short_rvol_signal = query_db_ta_value("short_relative_volume", iteration_minute,
                                                          SHORT_RVOL_THRESHOLD, SYMBOLS_VOLUME)
                    atrp_signal = query_db_ta_value("average_true_range_percentage", iteration_minute,
                                                        ATRP_THRESHOLD, "atrp")

                    atrp_with_rs = get_ta_indicator_when_rs_threshold(atrp_signal, rs_signal)
                    long_rvol_with_rs = get_ta_indicator_when_rs_threshold(long_rvol_signal, rs_signal)
                    short_rvol_with_rs = get_ta_indicator_when_rs_threshold(short_rvol_signal, rs_signal)

                    final_signal = get_joined_signals(rs_signal, long_rvol_with_rs, short_rvol_with_rs,
                                                          atrp_with_rs)

                    signal_db = mongo.connect_to_final_signal_db()

                    if final_signal and non_existing_record(signal_db.get_collection("signal_trade"), iteration_minute):
                        signal_db.get_collection("signal_trade").insert_one(
                            {TS: iteration_minute, 'final_signal': final_signal})

                if iteration_minute % 1800 == 0:

                    rs_signal = query_rs_signal_chart(iteration_minute)



                print(datetime.fromtimestamp(current_iteration_time))


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

