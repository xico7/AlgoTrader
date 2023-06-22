import threading
from support.threading_helpers import run_algotrader_process


def parse_aggtrades_runner():
    threading.Thread(target=run_algotrader_process, args=('parse-trades-ten-seconds',)).start()