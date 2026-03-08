import time
from datetime import datetime
import functools
from utils.util_log import test_log as log

DEFAULT_FMT = '[{start_time}] [{elapsed:0.8f}s] {collection_name} {func_name} -> {res!r}'


def trace(fmt=DEFAULT_FMT, prefix='test', flag=True):
    def decorate(func):
        @functools.wraps(func)
        def inner_wrapper(*args, **kwargs):
            # args[0] is an instance of ApiCollectionWrapper class
            flag = args[0].active_trace
            if flag:
                start_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                t0 = time.perf_counter()
                res, result = func(*args, **kwargs)
                elapsed = time.perf_counter() - t0
                end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                func_name = func.__name__
                collection_name = args[0].collection.name
                # arg_lst = [repr(arg) for arg in args[1:]][:100]
                # arg_lst.extend(f'{k}={v!r}' for k, v in kwargs.items())
                # arg_str = ', '.join(arg_lst)[:200]

                log_str = f"[{prefix}]" + fmt.format(**locals())
                # TODO: add report function in this place, like uploading to influxdb
                # it is better a async way to do this, in case of blocking the request processing
                log.info(log_str)
                return res, result
            else:
                res, result = func(*args, **kwargs)
                return res, result

        return inner_wrapper

    return decorate


def counter(func):
    """ count func succ rate """
    def inner_wrapper(*args, **kwargs):
        """ inner wrapper """
        result, is_succ = func(*args, **kwargs)
        inner_wrapper.total += 1
        if is_succ:
            inner_wrapper.succ += 1
        else:
            inner_wrapper.fail += 1
        return result, is_succ

    inner_wrapper.name = func.__name__
    inner_wrapper.total = 0
    inner_wrapper.succ = 0
    inner_wrapper.fail = 0
    return inner_wrapper


if __name__ == '__main__':
    @trace()
    def snooze(seconds, name='snooze'):
        time.sleep(seconds)
        return name
        # print(f"name: {name}")


    for i in range(3):
        res = snooze(.123, name=i)
        print("res:", res)
