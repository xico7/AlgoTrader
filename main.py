import asyncio
import inspect
import traceback

import argparse_func as argp


def main():
    # TODO: Improve function args remove verbosity command etc.. use only related with task.
    # TODO: Improve how *args works.. [0] None não gosto.
    parser = argp.algo_argparse()
    function_to_call, function_args = argp.get_execute_function(parser)

    if inspect.iscoroutinefunction(function_to_call):
        asyncio.run(asyncmain(function_to_call, function_args))
    else:
        function_to_call(function_args) if function_args else function_to_call()


async def asyncmain(func, *args):
    while True:
        try:
            await func(*args) if args[0] else await func()
        except Exception as e:
            traceback.print_exc()
            print(f"{e}")
            exit(1)


main()




