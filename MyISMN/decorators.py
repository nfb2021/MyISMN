from functools import wraps
import sys
import time


def sizeit(func):

    @wraps(func)
    def sizeit_wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        for arg in args:
            print(f'This variable "{str(arg)}" of type {type(arg)} \
                occupies {sys.getsizeof(arg) / 1e3} MB of memory')
        return result

    return sizeit_wrapper


def timeit(func):

    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"\n\n\tFunction {func.__name__}{args} {kwargs} \
                Took {total_time:.4f} seconds\n\n")
        return result

    return timeit_wrapper
