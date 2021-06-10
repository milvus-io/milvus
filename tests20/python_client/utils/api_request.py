import traceback
from utils.util_log import test_log as log


class Error:
    def __init__(self, error):
        self.code = getattr(error, 'code', -1)
        self.message = getattr(error, 'message', str(error))


log_row_length = 150


def api_request_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                log.debug("(api_res) Response : %s " % str(res)[0:log_row_length])
                return res, True
            except Exception as e:
                log.error(traceback.format_exc())
                log.error("(api_res) [Milvus API Exception]%s: %s" % (str(func), str(e)[0:log_row_length]))
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
            log.info("(api_req)[%s] Parameters ars arg: %s, kwargs: %s" % (str(func), str(arg), str(kwargs)))
            return func(*arg, **kwargs)
    return False, False
