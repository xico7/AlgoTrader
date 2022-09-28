import logging
import time
from datetime import datetime
import logs
from data_handling.data_func import FundTimeframeTrade, NoMoreParseableTrades

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def parse_fund_trades_ten_seconds():
    LOG.info("Beginning to parse fund ten seconds trades.")

    try:
        fund_data = None
        while True:
            if not fund_data:
                fund_data = FundTimeframeTrade()
            fund_data.parse_trades()
            fund_data.insert_in_db()

            LOG.info(f"Parsed symbol pairs from {datetime.fromtimestamp(fund_data.start_ts / 1000)} to "
                     f"{datetime.fromtimestamp((fund_data.start_ts + fund_data.timeframe) / 1000)}.")

            fund_data = FundTimeframeTrade((fund_data.start_ts + fund_data.timeframe))
    except NoMoreParseableTrades:
        LOG.info("Finished parsing fund ten seconds trades, sleeping for now.")
        time.sleep(60)
        parse_fund_trades_ten_seconds()

