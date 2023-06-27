import threading
import time

from MongoDB.db_actions import DBMapper, TechnicalIndicatorDBData
from support.threading_helpers import run_algotrader_process


def technical_indicators_runner():
    for db in DBMapper:
        if isinstance(db.value, TechnicalIndicatorDBData):
            threads = {db.name: threading.Thread(
                target=run_algotrader_process, args=('metrics-parser', ['--metric-db-mapper-name', db.name]))}

    for thread in threads.values():
        thread.start()
        time.sleep(5)
