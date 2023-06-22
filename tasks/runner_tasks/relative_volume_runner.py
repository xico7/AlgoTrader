import threading
import time

from support.threading_helpers import run_algotrader_process


def relative_volume_runner():
    threads = {'relative-volume-60': threading.Thread(target=run_algotrader_process,
                                                      args=('relative-volume', ['--timeframe-in-minutes', '60'])),
               'relative-volume-120': threading.Thread(target=run_algotrader_process,
                                                       args=('relative-volume', ['--timeframe-in-minutes', '120'])),
               'relative-volume-240': threading.Thread(target=run_algotrader_process,
                                                       args=('relative-volume', ['--timeframe-in-minutes', '240'])),
               'relative-volume-480': threading.Thread(target=run_algotrader_process,
                                                       args=('relative-volume', ['--timeframe-in-minutes', '480'])),
               'relative-volume-1440': threading.Thread(target=run_algotrader_process,
                                                        args=('relative-volume', ['--timeframe-in-minutes', '1440'])),
               'relative-volume-2880': threading.Thread(target=run_algotrader_process,
                                                        args=('relative-volume', ['--timeframe-in-minutes', '2880'])),
               'relative-volume-5760': threading.Thread(target=run_algotrader_process,
                                                        args=('relative-volume', ['--timeframe-in-minutes', '5760'])),
               'relative-volume-11520': threading.Thread(target=run_algotrader_process,
                                                         args=('relative-volume', ['--timeframe-in-minutes', '11520']))}

    for thread in threads:
        thread.start()
        time.sleep(20)
