import os
import subprocess
import sys
import time

from support.data_handling.data_helpers.vars_constants import PACKAGED_PROGRAM_NAME


# Sometimes mongodb's port is being used by other process, replace that process with MongoDB
def run_mongodb_startup_process():
    if os.name == 'nt':
        subprocess_output = subprocess.run(['powershell.exe', 'Get-Service MongoDB'], capture_output=True, shell=True)
        if not 'Running' in subprocess_output.stdout.decode('utf-8'):
            subprocess.run(['powershell.exe', './ForceStartMongoDBWindows.ps1'], shell=True)
            time.sleep(20)


def run_algotrader_process(task_to_run, task_args=[]):
    use_shell = False
    if os.name == 'nt':
        use_shell = True
    subprocess.call([sys.executable, PACKAGED_PROGRAM_NAME, task_to_run] + task_args, shell=use_shell)