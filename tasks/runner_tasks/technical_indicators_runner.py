import threading
import time

from MongoDB.db_actions import DBMapper, TechnicalIndicatorDetails
from support.threading_helpers import run_algotrader_process


def technical_indicators_runner():
    threads = {}
    for db in DBMapper:
        if isinstance(db.value, TechnicalIndicatorDetails):
            append_thread_number = []
            if db.value.threads_number != 1:
                append_thread_number = ['--threads-number', str(db.value.threads_number)]

            threads[db.name] = threading.Thread(target=run_algotrader_process, args=('metrics-parser', ['--metric-db-mapper-name', db.name] + append_thread_number))

    for thread in threads.values():
        thread.start()
        time.sleep(10)
