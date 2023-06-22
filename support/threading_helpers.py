import os
import subprocess
import sys


def run_algotrader_process(task_to_run, task_args=[]):
    use_shell = False
    if os.name == 'nt':
        use_shell = True
    subprocess.call([sys.executable, 'Algotrader.py', task_to_run] + task_args, shell=use_shell)