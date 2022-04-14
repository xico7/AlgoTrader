import time
import traceback
from datetime import datetime
from pymongo.errors import ServerSelectionTimeoutError
import MongoDB.db_actions as mongo
import asyncio
import logging

from data_staging import query_atr, create_last_day_rs_chart, query_rel_vol, get_current_time, ONE_HOUR_IN_SECS, \
    ONE_MIN_IN_SECS, TS, THIRTY_MIN_IN_SECS, SYMBOLS_VOLUME
from main import PRINT_RUNNING_EXECUTION_EACH_SECONDS


SET_START_TIME_PAST_N_HOURS = 1000
TA_MINUTE_PERIODS = 14

iteration_minute = 0


def non_existing_record(collection_feed):
    return not bool(collection_feed.find({TS: {'$eq': iteration_minute}}).count())


async def ta_analysis():
    global iteration_minute
    debug_running_execution = get_current_time()

    ta_analysis_db = mongo.connect_to_ta_analysis_db()
    five_min_ohlc_db = mongo.connect_to_5m_ohlc_db()
    one_day_ohlc_db = mongo.connect_to_1d_ohlc_db()

    print("Hello TA Analysis")
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

                if non_existing_record(long_vol_col):
                    long_vol_col.insert_one(
                        {TS: iteration_minute, SYMBOLS_VOLUME: query_rel_vol(iteration_minute, one_day_ohlc_db, 7)})
                if non_existing_record(short_vol_col):
                    short_vol_col.insert_one(
                        {TS: iteration_minute, SYMBOLS_VOLUME: query_rel_vol(iteration_minute, five_min_ohlc_db, 14)})
                if non_existing_record(atrp_col):
                    atrp_col.insert_one(
                        {TS: iteration_minute, "atrp": query_atr(iteration_minute, five_min_ohlc_db, 14)})

                if iteration_minute % THIRTY_MIN_IN_SECS == 0:
                    if non_existing_record(rs_chart_col):
                        rs_chart_col.insert_one({
                            TS: iteration_minute, "rs_chart": create_last_day_rs_chart(iteration_minute, mongo.connect_to_ta_lines_db())})

                print(datetime.fromtimestamp(iteration_minute))

            if get_current_time() > (debug_running_execution + PRINT_RUNNING_EXECUTION_EACH_SECONDS):
                print(datetime.fromtimestamp(get_current_time()))
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
