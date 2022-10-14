import contextlib
import logging
import time
from datetime import datetime

from binance import AsyncClient, BinanceSocketManager, Client
import logs
from MongoDB.db_actions import connect_to_db
from data_handling.data_func import CacheAggtrades
from data_handling.data_helpers.data_staging import usdt_with_bnb_symbols
# TODO: e falta no ws_trades meter o start_db timestamp..
# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.
from data_handling.data_helpers.secrets import BINANCE_API_KEY, BINANCE_API_SECRET
from data_handling.data_helpers.vars_constants import THIRTY_MINS_IN_MS, AGGTRADE_PYCACHE, TS, END_TS_AGGTRADES_VALIDATOR_DB, ONE_SECONDS_IN_MS

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class QueueOverflow(Exception): pass


def execute_past_trades():
    if base_start_ts := connect_to_db(END_TS_AGGTRADES_VALIDATOR_DB).get_collection(TS).find_one():
        start_ts = base_start_ts[TS]
    else:
        start_ts = 1640955600000  # 31 December 2021

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    cache_symbols_parsed = CacheAggtrades()

    while start_ts < time.time() * 1000:
        end_ts = start_ts + THIRTY_MINS_IN_MS
        start_ts += ONE_SECONDS_IN_MS
        for symbol in usdt_with_bnb_symbols():
            for trade in client.get_aggregate_trades(**{'symbol': symbol, 'startTime': start_ts,
                                                        'endTime': end_ts, 'limit': 9999999999999999999}):
                cache_symbols_parsed.append({**trade, **{'s': symbol}})
        cache_symbols_parsed.insert_clear(end_ts)
        LOG.info(f"{(THIRTY_MINS_IN_MS / 1000) / 60} minutes of aggtrades inserted from {datetime.fromtimestamp(start_ts / 1000)} to "
                 f"{datetime.fromtimestamp((start_ts + THIRTY_MINS_IN_MS) / 1000)}.")
        start_ts = end_ts + ONE_SECONDS_IN_MS

    LOG.info("Insertions ended.")


async def execute_ws_trades():
    cache_symbols_parsed = CacheAggtrades()

    async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket([symbol.lower() + '@aggTrade' for symbol in usdt_with_bnb_symbols()]) as tscm:
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