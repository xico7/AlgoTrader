import logging
import time
from datetime import datetime
import logs
from MongoDB.db_actions import DB
from data_handling.data_func import CacheAggtrades
from data_handling.data_helpers.data_staging import usdt_with_bnb_symbols, current_time_in_ms, mins_to_ms
from data_handling.data_helpers.vars_constants import ONE_SECONDS_IN_MS, PARSED_AGGTRADES_DB, binance_client

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def save_aggtrades():
    if not (start_ts := DB(PARSED_AGGTRADES_DB).end_ts):
        start_ts = 1640955600000

    parse_minutes = 60

    while start_ts < current_time_in_ms(time.time()):
        end_ts = start_ts + mins_to_ms(parse_minutes)
        cache_symbols_parsed = CacheAggtrades(start_ts, end_ts)

        for symbol in usdt_with_bnb_symbols():
            cache_symbols_parsed.append(symbol, binance_client.get_aggregate_trades(**{
                'symbol': symbol, 'startTime': start_ts, 'endTime': end_ts, 'limit': 9999999999999999999}))
        cache_symbols_parsed.insert_in_db_clear()
        LOG.info(f"{parse_minutes} minutes of aggtrades inserted from {datetime.fromtimestamp(start_ts / 1000)} to "
                 f"{datetime.fromtimestamp(end_ts / 1000)}.")

        start_ts = end_ts + ONE_SECONDS_IN_MS

    LOG.info("Insertions from past aggtrades ended.")

# import contextlib
# from binance import AsyncClient, BinanceSocketManager
# async def execute_ws_trades():
#     cache_symbols_parsed = CacheAggtrades()
#
#     async with BinanceSocketManager(await AsyncClient.create()).multiplex_socket([symbol.lower() + '@aggTrade' for symbol in usdt_with_bnb_symbols()]) as tscm:
#         while True:
#             try:
#                 ws_trade = await tscm.recv()
#             except Exception:
#                 with contextlib.suppress(KeyError):
#                     if ws_trade['m'] == 'Queue overflow. Message not filled':
#                         raise QueueOverflow("Queue Overflow error while trying to parse websocket trade with data: '%s'.", ws_trade)
#
#                 LOG.exception("Error while trying to parse websocket trade with data: '%s'.", ws_trade)
#                 exit(2)
#
#             cache_symbols_parsed.append(ws_trade['data'])
#             if len(cache_symbols_parsed) > AGGTRADE_PYCACHE:
#                 cache_symbols_parsed.insert_in_db_clear()
#class QueueOverflow(Exception): pass
