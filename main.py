import asyncio
import inspect
import argparse_func as argp
from support.helper_func import output_error


async def async_main(func, args):
    while True:
        try:
            await func(*args) if args else await func()
        except Exception as e:
            output_error(e)


def main():
    for function, function_args in argp.get_execute_functions(vars(argp.algo_argparse())):
        if inspect.iscoroutinefunction(function):
            asyncio.run(async_main(function, function_args))
        elif function_args:
            function(function_args)
        else:
            function()


main()




