import os
import subprocess
import sys
import time


# Sometimes mongodb's port is being used by other process, replace that process with MongoDB
def run_mongodb_startup_process():
    subprocess.run(['powershell.exe', './ForceStartMongoDBWindows.ps1'], shell=True)
    time.sleep(20)


def run_algotrader_process(task_to_run, task_args=[]):
    use_shell = False
    if os.name == 'nt':
        use_shell = True
    subprocess.call([sys.executable, 'Algotrader.py', task_to_run] + task_args, shell=use_shell)