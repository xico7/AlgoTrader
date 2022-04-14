import time
import traceback
from datetime import datetime
import MongoDB.db_actions as mongo
from pymongo.errors import ServerSelectionTimeoutError
import asyncio
import logging

from data_staging import get_current_time, ONE_HOUR_IN_SECS, ONE_MIN_IN_SECS, query_db_ta_value, \
    get_ta_indicator_when_rs_threshold, get_joined_signals, query_rs_signal_chart, TS
from main import PRINT_RUNNING_EXECUTION_EACH_SECONDS
from tasks.parse_alpha import MONGO_DB_SYMBOLS_VOLUME


#########################
# CHANGEABLE PARAMETERS #

RS_THRESHOLD = 0.0001
RS_SANITY_VALUE_THRESHOLD = 8
LONG_RVOL_THRESHOLD = 0.0001
SHORT_RVOL_THRESHOLD = 0.0001
ATRP_THRESHOLD = 0.0001
TA_MINUTE_PERIODS = 14
SET_START_TIME_PAST_N_HOURS = 200

#########################

iteration_minute = 0





def non_existing_record(collection_feed):
    return not bool(collection_feed.find({TS: {'$eq': iteration_minute}}).count())


async def ta_analysis():
    global iteration_minute

    rs_signal = None
    debug_running_execution = get_current_time()


    print("Hello Here is my Alpha")
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
                                                             LONG_RVOL_THRESHOLD, MONGO_DB_SYMBOLS_VOLUME)
                    short_rvol_signal = query_db_ta_value("short_relative_volume", iteration_minute,
                                                              SHORT_RVOL_THRESHOLD, MONGO_DB_SYMBOLS_VOLUME)
                    atrp_signal = query_db_ta_value("average_true_range_percentage", iteration_minute,
                                                        ATRP_THRESHOLD, "atrp")

                    atrp_with_rs = get_ta_indicator_when_rs_threshold(atrp_signal, rs_signal)
                    long_rvol_with_rs = get_ta_indicator_when_rs_threshold(long_rvol_signal, rs_signal)
                    short_rvol_with_rs = get_ta_indicator_when_rs_threshold(short_rvol_signal, rs_signal)

                    final_signal = get_joined_signals(rs_signal, long_rvol_with_rs, short_rvol_with_rs,
                                                          atrp_with_rs)

                    signal_db = mongo.connect_to_final_signal_db()

                    if final_signal and non_existing_record(signal_db.get_collection("signal_trade")):
                        signal_db.get_collection("signal_trade").insert_one(
                            {TS: iteration_minute, 'final_signal': final_signal})

                if iteration_minute % 1800 == 0:

                    rs_signal = query_rs_signal_chart(iteration_minute)



                print(datetime.fromtimestamp(current_iteration_time))

            if get_current_time() > (debug_running_execution + PRINT_RUNNING_EXECUTION_EACH_SECONDS):
                print(get_current_time())
                debug_running_execution += PRINT_RUNNING_EXECUTION_EACH_SECONDS

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


async def main():
    while True:
        try:
            await ta_analysis()
        except Exception as e:
            traceback.print_exc()
            print(f"{e}")
            exit(1)


if __name__ == "__main__":
    asyncio.run(main())
