import logging
import itertools
from datetime import datetime
import requests
from requests import ReadTimeout
import logs
from support.data_handling.data_helpers.vars_constants import USDT, BNB
from support.data_handling.data_structures import CacheAggtrades
from binance import Client
from support.data_handling.data_helpers.secrets import BINANCE_API_KEY, BINANCE_API_SECRET

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)
binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, {"timeout": 90})


class InvalidTimestampProvided(Exception): pass


def get_next_parse_minutes_trades(symbol, start_ts, end_ts):
    while True:
        try:
            trades = binance_client.get_aggregate_trades(
                **{'symbol': symbol, 'startTime': start_ts * 1000, 'endTime': end_ts * 1000, 'limit': 1000})
            break
        except ReadTimeout as e:
                pass

    if len(trades) < 1000:
        return trades
    else:
        partial_interval = int((end_ts - start_ts) / 10)
        if partial_interval < 10:
            return trades  # Doesn't guarantee that all trades are parsed, but its good enough.
        else:
            divided_trades = []
            for tf in [*range(start_ts, end_ts, partial_interval)]:
                divided_trades.append(get_next_parse_minutes_trades(symbol, tf, tf + partial_interval))
            return [l for l in itertools.chain.from_iterable(divided_trades)]


def usdt_with_bnb_symbols() -> list:
    all_symbols = [symbol_data['symbol'] for symbol_data in requests.get("https://api.binance.com/api/v3/ticker/price").json()]
    usdt_symbols = [symbol for symbol in all_symbols if USDT in symbol]

    return [symbol for symbol in usdt_symbols if symbol.replace(USDT, BNB) in all_symbols
            or BNB + symbol.replace(USDT, '') in all_symbols or symbol == f'{BNB}{USDT}']


def save_aggtrades(args):
    start_ts, end_ts = args['start_ts'], args['end_ts']

    if len(str(start_ts)) != 10 and len(str(end_ts)) != 10:
        LOG.error("Invalid Start timestamp or End timestamp provided, needs to be timestamp in seconds.")
        raise InvalidTimestampProvided("Invalid Start timestamp or End timestamp provided, needs to be timestamp in seconds.")

    LOG.info(f"Starting to parse aggtrades from {datetime.fromtimestamp(start_ts)} to {datetime.fromtimestamp(end_ts)}.")
    cache_symbols_parsed = CacheAggtrades(start_ts, end_ts)

    for symbol in usdt_with_bnb_symbols():
        cache_symbols_parsed.append(symbol, get_next_parse_minutes_trades(symbol, start_ts, end_ts))
    cache_symbols_parsed.insert_in_db_clear()
    LOG.info(f"{(end_ts - start_ts) / 60} minutes of aggtrades inserted from {datetime.fromtimestamp(start_ts)} to "
             f"{datetime.fromtimestamp(end_ts / 1000)}, exiting.")
    exit(0)

