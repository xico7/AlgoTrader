import logging
import time

import logs
from datetime import datetime
from data_handling.data_helpers.vars_constants import PARSED_AGGTRADES_DB
from data_handling.data_func import SymbolsTimeframeTrade, NoMoreParseableTrades
from MongoDB.db_actions import query_db_col_between

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():
    LOG.info("Beginning to parse ten seconds trades.")

    try:
        parse_aggtrade = None
        while True:
            if not parse_aggtrade:
                parse_aggtrade = SymbolsTimeframeTrade()

            for symbol in parse_aggtrade.symbols:
                if symbol_trades := query_db_col_between(PARSED_AGGTRADES_DB, symbol, parse_aggtrade.start_ts[symbol], parse_aggtrade.start_ts[symbol] + parse_aggtrade.timeframe):
                    parse_aggtrade.add_trades(symbol, symbol_trades.trades)

            parse_aggtrade.insert_in_db()
            LOG.info(f"Parsed symbol pairs from {datetime.fromtimestamp(min(parse_aggtrade.start_ts[symbol] for symbol in parse_aggtrade.symbols) / 1000)} to "
                     f"{datetime.fromtimestamp(max(parse_aggtrade.start_ts[symbol] for symbol in parse_aggtrade.symbols) / 1000)}.")
            parse_aggtrade.start_ts = {symbol: parse_aggtrade.start_ts[symbol] + parse_aggtrade.timeframe for symbol in parse_aggtrade.symbols}
            parse_aggtrade = SymbolsTimeframeTrade(parse_aggtrade.start_ts)
    except NoMoreParseableTrades:
        LOG.info("Finished parsing symbol pairs trades, sleeping for now.")
        time.sleep(60)
        parse_trades_ten_seconds()




