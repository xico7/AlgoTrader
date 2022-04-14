import asyncio
import argparse_func as argp
from tasks.parse_alpha import get_alpha
import tasks.ws_trades_mongodb as tasks_ws


PRINT_RUNNING_EXECUTION_EACH_SECONDS = 900

def main():
    parser_run_arguments = argp.algo_argparse()
    if parser_run_arguments.run_ws_trades_mongodb:
        print("here")
    elif parser_run_arguments.run_alpha_algo:
        asyncio.run(tasks_ws.main_test())
    get_alpha()


main()