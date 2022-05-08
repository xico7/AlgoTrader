from data_staging import ONE_DAY_IN_SECS, BEGIN_TIMESTAMP, MongoDB
from MongoDB.db_actions import connect_to_timeframe_db
import matplotlib.pyplot as plt


def query_all_ranges(timeframe):
    total_count = 0
    all_hits_count = 0

    for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
        for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
                {BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}})):
            if (element['last_price_counter'] == '2' or element['last_price_counter'] == '1') and element['range_percentage'] > 10 and \
                    (element['0'] + element['1']) < 7:
                all_hits_count += 1
                continue
            total_count += 1

    print("total hits", all_hits_count)
    print("total count", total_count)
    print("here")


def show_range_percentage_plot(timeframe):
    values = []
    for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
        for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
                {BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}})):
            values.append(element['range_percentage'])

        print("here")

    n, bins, patches = plt.hist(values)
    plt.show()


# def insert_fund_range(timeframe):
#     total_count = 0
#     all_hits_count = 0
#
#     for symbol in connect_to_timeframe_db(timeframe).list_collection_names():
#         for element in list(connect_to_timeframe_db(ONE_DAY_IN_SECS).get_collection(symbol).find(
#                 {BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}})):
#             if (element['last_price_counter'] == '2' or element['last_price_counter'] == '1') and element['range_percentage'] > 10 and \
#                     (element['0'] + element['1']) < 7:
#                 all_hits_count += 1
#                 continue
#             total_count += 1
#
#     print("total hits", all_hits_count)
#     print("total count", total_count)
#     print("here")



