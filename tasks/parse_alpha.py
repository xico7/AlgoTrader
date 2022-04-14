import time
import traceback
from datetime import datetime

from pymongo.errors import ServerSelectionTimeoutError
import MongoDB.db_actions as mongo
import logging
from data_staging import get_minutes_after_ts, get_element_chart_percentage_line, MongoDB, TIME


### CONSTANTS ###


OHLC_CLOSE = 'c'
MONGO_DB_SYMBOLS_VOLUME = "symbols_volume"
ONE_MINUTE_IN_SECS = 60
ONE_HOUR = ONE_MINUTE_IN_SECS * 60
TWELVE_HOUR_TIMEFRAME = 43200
TS = 'timestamp'
RS = 'RS'



iteration_minute = 0
signal_db_collection = mongo.connect_to_signal_db().get_collection("signal_trade")
algo_alpha_db_collection = mongo.connect_to_algo_alpha_db().get_collection("alpha_runs")
one_min_db = mongo.connect_to_1m_ohlc_db()


def get_alpha():
    short_rvol_value = 0.818
    long_rvol_value = 0.3
    atr_value = 0.55
    rs_dif_value = 1.5
    average_rs_chart_threshold = 3

    blacklist = {}
    rs_chart_count = 0
    average_rs_chart_count = 0
    count_threshold_short_rvol = 0
    count_threshold_long_rvol = 0
    count_threshold_atr = 0
    count_threshold_rs_dif = 0
    count_keyerror, count_bought_value, count, count_threshold = 0, 0, 0, 0
    profit2 = 0
    number_of_trades = 0
    timestamp = 1649673780
    starting_from_timestamp = timestamp
    finish_run_timestamp = time.time()

    while timestamp < finish_run_timestamp:
        if (timestamp % TWELVE_HOUR_TIMEFRAME) == 0:
            print(f"{int((finish_run_timestamp - timestamp) / 3600)} hours remaining")

        timestamp += 60

        try:
            for signal in list(signal_db_collection.find({TS: {MongoDB.EQUAL: timestamp}})):
                for symbol, values in signal['final_signal'].items():
                    for list_element in values:
                        for signal_key, signal_value in list_element.items():
                            count += 1
                            try:

                                if signal_value['short_rvol'] < short_rvol_value or \
                                        signal_value['long_rvol'] < long_rvol_value or \
                                        signal_value['atr'] * 100 / signal_value['value'] < atr_value or \
                                        signal_value['rs_difference'] < rs_dif_value:

                                    if signal_value['short_rvol'] < short_rvol_value:
                                        count_threshold_short_rvol += 1
                                    if signal_value['long_rvol'] < long_rvol_value:
                                        count_threshold_long_rvol += 1
                                    if signal_value['atr'] < atr_value:
                                        count_threshold_atr += 1
                                    if signal_value['rs_difference'] < rs_dif_value:
                                        count_threshold_rs_dif += 1
                                    count_threshold += 1
                                    continue

                                try:
                                    if blacklist[symbol] > signal[TS]:
                                        continue
                                except KeyError:
                                    blacklist[symbol] = 0

                                blacklist[symbol] = signal[TS] + ONE_HOUR * 2

                                for minute_after_signal in get_minutes_after_ts(symbol, signal[TS]):
                                    minute_chart_element = get_element_chart_percentage_line(minute_after_signal[OHLC_CLOSE], signal_value['rs_chart'])

                                    if signal_value['rs_chart'][minute_chart_element]['average_rs'] > (minute_after_signal[RS] + average_rs_chart_threshold):
                                        average_rs_chart_count += 1
                                    rs_chart_count += 1

                                    sell_trigger = signal_value['rs_chart'][minute_chart_element]['average_rs'] > (minute_after_signal[RS] + average_rs_chart_threshold) or minute_after_signal[RS] < 0
                                    if sell_trigger:
                                        profit = 0
                                        number_of_trades += 1

                                        bought_value = get_minutes_after_ts(symbol, signal[TS])[0][OHLC_CLOSE]

                                        buy_amount = 100 / (bought_value * 1.0075)
                                        sell_amount = buy_amount * (minute_after_signal[OHLC_CLOSE] * 0.9925)

                                        profit += (sell_amount - 100)
                                        profit2 += (sell_amount - 100)

                                        print(f"{datetime.fromtimestamp(signal[TS])} Bought '{symbol}' at value '{bought_value}'and sold after "
                                              f"'{(minute_after_signal[TIME] - signal[TS]) / 60}' with profit '{float(profit)}'")

                                        break

                            except KeyError:
                                count_keyerror += 1
                                continue





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

    algo_alpha_db_collection.insert_one(
        {'start': datetime.fromtimestamp(starting_from_timestamp), 'finish': datetime.fromtimestamp(finish_run_timestamp),
         'start_ts': starting_from_timestamp, 'finish_ts': finish_run_timestamp, 'trades': number_of_trades, 'profit': int(profit2),
         'rs_difference': rs_dif_value, 'rs_chart_threshold_value': average_rs_chart_threshold,
         'long_rvol': long_rvol_value, 'short_rvol': short_rvol_value, 'atr': atr_value,
         'long_rvol_threshold': int(count_threshold_long_rvol / count_threshold + 100),
         'short_rvol_threshold': int(count_threshold_short_rvol / count_threshold + 100),
         'atr_threshold': int(count_threshold_atr / count_threshold + 100), 'rs_diff_threshold': int(count_threshold_rs_dif / count_threshold + 100),
         'rs_chart_trades': average_rs_chart_count})