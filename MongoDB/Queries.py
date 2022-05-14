from data_staging import ONE_DAY_IN_SECS, BEGIN_TIMESTAMP, MongoDB
from MongoDB.db_actions import connect_to_timeframe_db
import matplotlib.pyplot as plt


def query_all_ranges(db_name, range_pct, price_counters_values, price_counters_sum):
    total_count = 0
    all_hits_count = 0

    for col in connect_to_timeframe_db(db_name).list_collection_names():
        for element in list(connect_to_timeframe_db(db_name).get_collection(col).find(
                {BEGIN_TIMESTAMP: {MongoDB.HIGHER_EQ: 0}})):

            try:
                if (element['last_price_counter'] in price_counters_values) and element['range_percentage'] > range_pct and \
                        sum([element[x] for x in price_counters_values]) < price_counters_sum:
                    all_hits_count += 1
                    continue
            except KeyError:
                continue

            total_count += 1

    print("total hits", all_hits_count)
    print("total count", total_count)
    print("here")


def show_range_percentage_plot(db_name):
    values = []
    db = connect_to_timeframe_db(db_name)
    for col in db.list_collection_names():
        for element in list(db.get_collection(col).find(
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

#show_range_percentage_plot("sp500_data_timeframe_14400_interval_900")
query_all_ranges("sp500_data_timeframe_14400_interval_900", 9, ['10', '11'], 9)
query_all_ranges("aggtrade_data_timeframe_14400_interval_900", 16, ['0', '1'], 9)



