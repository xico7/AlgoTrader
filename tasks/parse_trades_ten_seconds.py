import logging
import time

import logs
from datetime import datetime
from data_handling.data_helpers.vars_constants import ONE_MIN_IN_MS, PARSED_AGGTRADES_DB
from data_handling.data_func import SymbolsTimeframeTrade
from MongoDB.db_actions import connect_to_db, query_db_col_between
from data_handling.data_helpers.data_staging import current_milli_time

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_trades_ten_seconds():
    LOG.info("Beginning to parse ten seconds trades.")
    parse_aggtrade = SymbolsTimeframeTrade()

    while max(parse_aggtrade.end_ts.values()) < current_milli_time() - ONE_MIN_IN_MS:
        if max(parse_aggtrade.end_ts.values()) < connect_to_db('validator_db').get_collection('validated_timestamp').find_one()['timestamp']:
            for symbol in connect_to_db(PARSED_AGGTRADES_DB).list_collection_names():
                if symbol_trades := query_db_col_between(PARSED_AGGTRADES_DB, symbol, parse_aggtrade.start_ts[symbol], parse_aggtrade.end_ts[symbol]):
                    parse_aggtrade.add_trades(symbol, symbol_trades)

            parse_aggtrade.insert_in_db()
            LOG.info(f"Parsed symbol pairs from {datetime.fromtimestamp(min(parse_aggtrade.start_ts.values())/1000)} to "
                     f"{datetime.fromtimestamp(max(parse_aggtrade.end_ts.values())/1000)}.")
            parse_aggtrade.reset_add_interval()
        time.sleep(10)

    LOG.info("Finished parsing symbol pairs trades.")




