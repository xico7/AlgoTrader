import contextlib
import logging
from binance import AsyncClient, BinanceSocketManager, Client
import logs
from MongoDB.db_actions import query_db_col_between, connect_to_db
from data_handling.data_func import CacheAggtrades
from data_handling.data_helpers.data_staging import usdt_with_bnb_symbols_aggtrades, get_current_second_in_ms, \
    usdt_with_bnb_symbols

# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.
from data_handling.data_helpers.secrets import BINANCE_API_KEY, BINANCE_API_SECRET
from data_handling.data_helpers.vars_constants import ONE_MIN_IN_MS

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


class QueueOverflow(Exception): pass


def execute_past_trades():
    from MongoDB.db_actions import query_starting_ts
    from data_handling.data_helpers.vars_constants import PARSED_AGGTRADES_DB, DEFAULT_COL_SEARCH

    if not (validated_ts := connect_to_db('validator_db').get_collection('validated_timestamp').find_one()):
        validated_ts = query_starting_ts(PARSED_AGGTRADES_DB, DEFAULT_COL_SEARCH)
        connect_to_db('validator_db').get_collection('validated_timestamp').insert_one({'timestamp': validated_ts})

    def verify_ws_trades():
        not_done_minute_list = []
        for ts in range(validated_ts, get_current_second_in_ms(), ONE_MIN_IN_MS):
            if not query_db_col_between(PARSED_AGGTRADES_DB, DEFAULT_COL_SEARCH, ts, ts + ONE_MIN_IN_MS, limit=1):
                not_done_minute_list.append(ts)

        return not_done_minute_list

    undone_list = verify_ws_trades()

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

    for symbol in usdt_with_bnb_symbols():

        midnight_day = 1663718400000
        THIRTY_MINS_IN_MS = 60 * 60 * 0.5 * 1000
        a = client.get_aggregate_trades(**{'symbol': DEFAULT_COL_SEARCH, 'startTime': midnight_day, 'endTime': midnight_day + THIRTY_MINS_IN_MS, 'limit': 999999999})

    print("here")


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
            cache_symbols_parsed.insert_clear()



