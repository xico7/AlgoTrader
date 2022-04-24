from pymongo import MongoClient
import MongoDB.db_actions as mongo


# TODO: insert_ohlc_1m takes 10-13 seconds, is or will this be na problem?
from data_staging import RS, TIME, VOLUME, PRICE, OHLC_OPEN, OHLC_HIGH, OHLC_LOW, OHLC_CLOSE, ONE_MIN_IN_SECS, \
    FIVE_MIN_IN_SECS, FIFTEEN_MIN_IN_SECS, ONE_HOUR_IN_SECS, FOUR_HOUR_IN_SECS, ONE_DAY_IN_SECS


def create_insert_ohlc_data(ohlc_open_timestamp, query_db, destination_db, ohlc_seconds,
                            ohlc_open=OHLC_OPEN, ohlc_close=OHLC_CLOSE, ohlc_high=OHLC_HIGH, ohlc_low=OHLC_LOW, debug=False):
    pairs_ohlcs = {}
    for collection in query_db.list_collection_names():
        trade_data = list(query_db.get_collection(collection).find({'$and': [
            {TIME: {'$gte': ohlc_open_timestamp}},
            {TIME: {'$lte': ohlc_open_timestamp + ohlc_seconds}}
        ]
        }).rewind())

        if trade_data:
            rs_sum = volume = high = 0
            low = 999999999999
            opening_value, closing_value = trade_data[0][ohlc_open], trade_data[-1][ohlc_close]

            for elem in trade_data:
                rs_sum += elem[RS]
                volume += elem[VOLUME]
                if elem[ohlc_high] > high:
                    high = elem[ohlc_high]
                if elem[ohlc_low] < low:
                    low = elem[ohlc_low]

            pairs_ohlcs[collection] = {TIME: ohlc_open_timestamp, OHLC_OPEN: opening_value, OHLC_HIGH: high, OHLC_LOW: low,
                                       OHLC_CLOSE: closing_value, RS: rs_sum / len(trade_data),
                                       VOLUME: volume}
    if debug:
        print(pairs_ohlcs)
        print(destination_db)
    mongo.insert_one_from_dict(destination_db, pairs_ohlcs)


# open timestamp is the last finished candle opening time,
# exactly what we want for one minute candle but not really whats
# needed for the other ones where we must add one minute.
def insert_ohlc_data(open_timestamp):
    #TODO:
    # if cur_time % 60 in [-1, 0, 1] and (
    #         latest_ohlc_open_timestamp := get_last_minute(cur_time - 3)) != done_timestamp:
    #     done_timestamp = latest_ohlc_open_timestamp
    #     insert_ohlc_data(latest_ohlc_open_timestamp)

    #TODO:
    # def calculate_relative_strength(coin_ohlc_data, cached_marketcap_ohlc_data) -> float:
    #     coin_change_percentage = ((float(coin_ohlc_data[len(coin_ohlc_data)][OHLC_OPEN]) / float(
    #         coin_ohlc_data[1][OHLC_OPEN])) - 1) * 100
    #     try:
    #         market_change_percentage = ((cached_marketcap_ohlc_data[len(coin_ohlc_data)][OHLC_OPEN] /
    #                                      cached_marketcap_ohlc_data[1][OHLC_OPEN]) - 1) * 100
    #     except ZeroDivisionError:
    #         return 0  # unlikely case, no better solution found.
    #
    #     return coin_change_percentage - market_change_percentage

    # TODO:
    # if symbol_pair in SP500_SYMBOLS_USDT_PAIRS:
    #     cache.marketcap_coins_value.update(
    #         {coin_symbol: (float(aggtrade_data[PRICE_P]) * coin_ratio[coin_symbol])})
    #     cache.marketcap_sum = sum(list(cache.marketcap_coins_value.values()))
    # coin_ratio = get_coin_fund_ratio(remove_usdt(SP500_SYMBOLS_USDT_PAIRS), requests.get(coingecko_marketcap_api_link).json())

    # TODO:
    # coin_ratio = get_coin_fund_ratio(remove_usdt(SP500_SYMBOLS_USDT_PAIRS),
    #                                  requests.get(coingecko_marketcap_api_link).json())
    # query = list(mongo.connect_to_aggtrade_data_db().get_collection("BTCUSDT").find({MongoDB.AND: [
    #     {"E": {MongoDB.HIGHER_EQ: 0}},
    #     {"E": {MongoDB.LOWER_EQ: get_current_time_ms()}}
    # ]
    # }))

    # TODO:
    # SP500_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
    #                             'DOGEUSDT',
    #                             'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT',
    #                             'BCHUSDT',
    #                             'ALGOUSDT',
    #                             'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT',
    #                             'ICPUSDT',
    #                             'FILUSDT',
    #                             'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
    #                             'NEARUSDT', 'BTTUSDTXTZUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
    #                             'LRCUSDT']
    ohlc_1m_db = mongo.connect_to_1m_ohlc_db()
    ohlc_5m_db = mongo.connect_to_5m_ohlc_db()
    ohlc_15m_db = mongo.connect_to_15m_ohlc_db()
    ohlc_1h_db = mongo.connect_to_1h_ohlc_db()
    ohlc_4h_db = mongo.connect_to_4h_ohlc_db()
    ohlc_1d_db = mongo.connect_to_1d_ohlc_db()

    aggtrade_data_client = mongo.CLIENT['aggtrade_data']

    if open_timestamp % ONE_MIN_IN_SECS == 0:
        create_insert_ohlc_data(open_timestamp, aggtrade_data_client, ohlc_1m_db, ONE_MIN_IN_SECS, PRICE, PRICE, PRICE, PRICE)
    if open_timestamp % FIVE_MIN_IN_SECS == 0:
        create_insert_ohlc_data((open_timestamp - FIVE_MIN_IN_SECS), aggtrade_data_client, ohlc_5m_db, FIVE_MIN_IN_SECS, PRICE, PRICE, PRICE, PRICE)
    if open_timestamp % FIFTEEN_MIN_IN_SECS == 0:
        create_insert_ohlc_data((open_timestamp - FIFTEEN_MIN_IN_SECS), aggtrade_data_client, ohlc_15m_db, FIFTEEN_MIN_IN_SECS, PRICE, PRICE, PRICE, PRICE)
    if open_timestamp % ONE_HOUR_IN_SECS == 0:
        create_insert_ohlc_data((open_timestamp - ONE_HOUR_IN_SECS), aggtrade_data_client, ohlc_1h_db, ONE_HOUR_IN_SECS, PRICE, PRICE, PRICE, PRICE)
    if open_timestamp % FOUR_HOUR_IN_SECS == 0:
        create_insert_ohlc_data((open_timestamp - FOUR_HOUR_IN_SECS), aggtrade_data_client, ohlc_4h_db, FOUR_HOUR_IN_SECS, PRICE, PRICE, PRICE, PRICE)
    if open_timestamp % ONE_DAY_IN_SECS == 0:
        create_insert_ohlc_data((open_timestamp - ONE_DAY_IN_SECS), aggtrade_data_client, ohlc_1d_db, ONE_DAY_IN_SECS, PRICE, PRICE, PRICE, PRICE)
