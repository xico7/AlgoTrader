import logging
import time

import logs
from datetime import datetime
from data_handling.data_helpers.vars_constants import PARSED_AGGTRADES_DB
from data_handling.data_func import SymbolsTimeframeTrade, NoMoreParseableTrades
from MongoDB.db_actions import connect_to_db, query_db_col_between

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():
    LOG.info("Beginning to parse ten seconds trades.")

    try:
        parse_aggtrade = None
        while True:
            if not parse_aggtrade:
                parse_aggtrade = SymbolsTimeframeTrade()

            for symbol in connect_to_db(PARSED_AGGTRADES_DB).list_collection_names():
                if symbol_trades := query_db_col_between(PARSED_AGGTRADES_DB, symbol, parse_aggtrade.start_ts, parse_aggtrade.start_ts + parse_aggtrade.timeframe):
                    parse_aggtrade.add_trades(symbol, symbol_trades)

            parse_aggtrade.insert_in_db()
            LOG.info(f"Parsed symbol pairs from {datetime.fromtimestamp(parse_aggtrade.start_ts / 1000)} to "
                     f"{datetime.fromtimestamp((parse_aggtrade.start_ts + parse_aggtrade.timeframe) / 1000)}.")

            parse_aggtrade = SymbolsTimeframeTrade((parse_aggtrade.start_ts + parse_aggtrade.timeframe))
    except NoMoreParseableTrades:
        LOG.info("Finished parsing symbol pairs trades, sleeping for now.")
        time.sleep(60)
        parse_trades_ten_seconds()




