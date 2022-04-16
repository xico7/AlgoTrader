import asyncio
import inspect
import traceback

import argparse_func as argp


def main():
    parser = argp.algo_argparse()
    call = argp.get_execute_function(parser)

    if inspect.iscoroutinefunction(call):
        asyncio.run(asyncmain(call, parser.debug_secs))
    else:
        call()


async def asyncmain(func, *args):
    while True:
        try:
            await func(*args)
        except Exception as e:
            traceback.print_exc()
            print(f"{e}")
            exit(1)


main()




