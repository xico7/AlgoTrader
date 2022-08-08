import asyncio
import inspect
import argparse_func as argp
from support.helper_func import output_error


def main():
    parser = argp.algo_argparse()
    function_to_call, function_args = argp.get_execute_function(parser)

    if inspect.iscoroutinefunction(function_to_call):
        asyncio.run(asyncmain(function_to_call, function_args))
    else:
        function_to_call(function_args) if function_args else function_to_call()


async def asyncmain(func, args):
    while True:
        try:
            await func(*args) if args else await func()
        except Exception as e:
            output_error(e)

main()




