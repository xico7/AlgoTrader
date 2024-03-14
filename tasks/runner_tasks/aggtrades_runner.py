import threading
import time
from MongoDB.db_actions import DB, AggtradesValidatorDB
from data_handling.data_helpers.vars_constants import PARSED_AGGTRADES_DB
from support.generic_helpers import mins_to_ms
from support.threading_helpers import run_algotrader_process


def create_run_aggtrades_threads(threads_number, begin_ts):
    aggtrade_threads = []
    parse_minutes = 30

    for thread_index in range(0, threads_number):
        end_ts = begin_ts + mins_to_ms(parse_minutes)
        aggtrade_threads.append(threading.Thread(target=run_algotrader_process, args=(
            'save-aggtrades', ['--start-ts', f'{begin_ts}', '--end-ts', f'{end_ts}'])))
        begin_ts += mins_to_ms(parse_minutes)

    for thread in aggtrade_threads:
        thread.start()
        time.sleep(60)

    return aggtrade_threads


def aggtrades_runner(args):
    if not (begin_ts := DB(PARSED_AGGTRADES_DB).end_ts):
        begin_ts = 1620955600000
        AggtradesValidatorDB(PARSED_AGGTRADES_DB).set_start_ts(begin_ts)
    else:
        AggtradesValidatorDB(PARSED_AGGTRADES_DB).set_valid_timestamps()

    aggtrades_threads = create_run_aggtrades_threads(args['threads_number'], begin_ts)
    while True:
        if not any([thread.is_alive() for thread in aggtrades_threads]):
            AggtradesValidatorDB(PARSED_AGGTRADES_DB).set_valid_timestamps()
            aggtrades_threads = create_run_aggtrades_threads(args['threads_number'], DB(PARSED_AGGTRADES_DB).end_ts)
            time.sleep(20)
