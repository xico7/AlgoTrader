import logging
import time

import logs
from data_handling.data_func import FundTimeframeTrade
from MongoDB.db_actions import connect_to_db
from data_handling.data_helpers.data_staging import current_milli_time
from data_handling.data_helpers.vars_constants import ONE_MIN_IN_MS

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_fund_trades_ten_seconds():
    LOG.info("Beginning to parse fund ten seconds trades.")

    fund_data = FundTimeframeTrade()

    while fund_data.end_ts < (current_milli_time() - ONE_MIN_IN_MS):
        if fund_data.end_ts < connect_to_db('validator_db').get_collection('validated_timestamp').find_one()['timestamp']:
            fund_data.parse_trades()
            fund_data.insert_in_db()
            fund_data = FundTimeframeTrade(start_ts=fund_data.end_ts)
        time.sleep(10)

        LOG.info("Finished parsing fund ten seconds trades.")
