import contextlib
import logging
from datetime import datetime

from binance import AsyncClient, BinanceSocketManager, Client
import logs
from MongoDB.db_actions import connect_to_db, delete_db
from data_handling.data_func import CacheAggtrades
from data_handling.data_helpers.data_staging import usdt_with_bnb_symbols_aggtrades, usdt_with_bnb_symbols, \
    get_current_second_in_ms

# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.
from data_handling.data_helpers.secrets import BINANCE_API_KEY, BINANCE_API_SECRET
from data_handling.data_helpers.vars_constants import THIRTY_MINS_IN_MS, AGGTRADE_PYCACHE

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class QueueOverflow(Exception): pass


def execute_past_trades():
    if start_ts := connect_to_db('validator_db').get_collection('validated_timestamp').find_one():
        start_ts = start_ts['timestamp']
    else:
        start_ts = 1640955600000  # 31 December 2021

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    cache_symbols_parsed = CacheAggtrades()

    while start_ts < get_current_second_in_ms():
        end_ts = start_ts + THIRTY_MINS_IN_MS
        start_ts += 1000
        for symbol in usdt_with_bnb_symbols():
            for trade in client.get_aggregate_trades(**{'symbol': symbol, 'startTime': start_ts,
                                                        'endTime': end_ts, 'limit': 999999999}):
                cache_symbols_parsed.append({**trade, **{'s': symbol}})
        if cache_symbols_parsed.insert_clear():
            delete_db('validator_db')
            connect_to_db('validator_db').get_collection('validated_timestamp').insert_one({'timestamp': end_ts})
        LOG.info(f"{THIRTY_MINS_IN_MS / 1000} of aggtrades inserted from {datetime.fromtimestamp(start_ts / 1000)} to "
                 f"{datetime.fromtimestamp((start_ts + THIRTY_MINS_IN_MS) / 1000)}.")
        start_ts = end_ts + 1000

    LOG.info("Insertions ended.")


async def execute_ws_trades():
    from datetime import datetime
    import time
    print(datetime.fromtimestamp(time.time()))
    cache_symbols_parsed = CacheAggtrades()

    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket(usdt_with_bnb_symbols_aggtrades()) as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
            except Exception:
                with contextlib.suppress(KeyError):
                    if ws_trade['m'] == 'Queue overflow. Message not filled':
                        raise QueueOverflow("Queue Overflow error while trying to parse websocket trade with data: '%s'.", ws_trade)

                LOG.exception("Error while trying to parse websocket trade with data: '%s'.", ws_trade)
                exit(2)

            cache_symbols_parsed.append(ws_trade['data'])
            if len(cache_symbols_parsed) > AGGTRADE_PYCACHE:
                cache_symbols_parsed.insert_clear()





    # if not (query_valid_ts := connect_to_db('validator_db').get_collection('validated_timestamp').find_one()):
    #     validated_ts = round_last_ten_secs(query_starting_ts(PARSED_AGGTRADES_DB, DEFAULT_COL_SEARCH))
    #     connect_to_db('validator_db').get_collection('validated_timestamp').insert_one({'timestamp': validated_ts})
    # else:
    #     validated_ts = query_valid_ts['timestamp']
    #
    # def verify_ws_trades():
    #     more_than_min_distance_between_trades = []
    #     for ts in range(validated_ts, get_current_second_in_ms(), TEN_MIN_IN_MS):
    #         if trades := query_db_col_between(PARSED_AGGTRADES_DB, DEFAULT_COL_SEARCH, ts, ts + TEN_MIN_IN_MS, sort_value=pymongo.ASCENDING):
    #             ts_list = [trade.timestamp for trade in trades]
    #             for i, elem in enumerate(ts_list):
    #                 if i == 0:
    #                     continue
    #                 if (elem - ts_list[i-1]) > ONE_MIN_IN_MS:
    #                     more_than_min_distance_between_trades.append(ts_list[i-1])
    #      return more_than_min_distance_between_trades
    #
    # undone_list = verify_ws_trades()