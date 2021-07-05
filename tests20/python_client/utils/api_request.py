import traceback
from utils.util_log import test_log as log


class Error:
    def __init__(self, error):
        self.code = getattr(error, 'code', -1)
        self.message = getattr(error, 'message', str(error))


log_row_length = 300


def api_request_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                log_res = str(res)[0:log_row_length] + '......' if len(str(res)) > log_row_length else str(res)
                log.debug("(api_response) : %s " % log_res)
                return res, True
            except Exception as e:
                log_e = str(e)[0:log_row_length] + '......' if len(str(e)) > log_row_length else str(e)
                log.error(traceback.format_exc())
                log.error("(api_response) : %s" % log_e)
                return Error(e), False
        return inner_wrapper
    return wrapper


@api_request_catch()
def api_request(_list, **kwargs):
    if isinstance(_list, list):
        func = _list[0]
        if callable(func):
            arg = []
            if len(_list) > 1:
                for a in _list[1:]:
                    arg.append(a)
            log_arg = str(arg)[0:log_row_length] + '......' if len(str(arg)) > log_row_length else str(arg)
            log.debug("(api_request)  : [%s] args: %s, kwargs: %s" % (func.__qualname__, log_arg, str(kwargs)))
            return func(*arg, **kwargs)
    return False, False

