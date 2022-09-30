import logging
from datetime import datetime
import logs
from data_handling.data_helpers.vars_constants import TS, DEFAULT_SYMBOL_SEARCH, \
    AGGTRADES_VALIDATOR_DB_TS, QUANTITY
from MongoDB.db_actions import trades_chart, query_starting_ts, query_db_col_between, insert_many_same_db_col, \
    db_col_names, connect_to_db
from data_handling.data_helpers.data_staging import mins_to_ms, remove_none_values, query_trades_fill_empty

# TODO: Make cached_volume_chart insert many, maybe active sessions reduce.
# TODO: change logical sessions and refreshmillis parameter in mongodb
LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def transform_trade_data(args):
    transform_db_name = args['db_name']

    start_ts = query_starting_ts(trades_chart.format(args['chart_minutes']), DEFAULT_SYMBOL_SEARCH, init_db=transform_db_name)
    end_ts = start_ts + mins_to_ms(args['chart_minutes'])
    symbols_one_day_trades = remove_none_values({symbol: query_db_col_between(transform_db_name, symbol, start_ts, end_ts) for symbol in db_col_names(transform_db_name)})

    LOG.info(f"Transforming data starting from {datetime.fromtimestamp(start_ts / 1000)} to {datetime.fromtimestamp(end_ts / 1000)}")

    cache = 0
    aggregate_symbols_one_day_trades = []
    while end_ts < connect_to_db(AGGTRADES_VALIDATOR_DB_TS).get_collection(TS).find_one()[TS]:
        if cache == 0:
            symbols_thirty_mins_trades = remove_none_values({symbol: query_db_col_between(transform_db_name, symbol, end_ts, end_ts + mins_to_ms(30)) for symbol in db_col_names(transform_db_name)})
            symbols_thirty_mins_trades = {symbol: trade_group.fill_trades_tf() for symbol, trade_group in symbols_thirty_mins_trades.items()}

        aggregate_symbols_one_day_trades.append(symbols_one_day_trades)

        for symbol, symbol_trades in symbols_one_day_trades.items():
            symbol_trades.add_trade(symbols_thirty_mins_trades[symbol].trades[0])
            symbols_thirty_mins_trades[symbol].trades.pop(0)

        cache += 1
        if cache == 30:
            insert_many_same_db_col(trades_chart.format(args['chart_minutes']), price_volume_chart)
            price_volume_chart = []
            cache = 0


    print("success.")
    exit(0)

