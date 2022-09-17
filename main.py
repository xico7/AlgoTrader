import asyncio
import inspect
import argparse_func as argp


async def async_main(func, args):
    while True:
        await func(*args) if args else await func()


def main():
    for function, function_args in argp.get_execute_functions(vars(argp.algo_argparse())).items():
        if inspect.iscoroutinefunction(function):
            asyncio.run(async_main(function, function_args))
        elif function_args:
            function(function_args)
        else:
            function()


main()




