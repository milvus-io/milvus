import traceback
import copy
import os
from utils.util_log import test_log as log

# enable_traceback = os.getenv('ENABLE_TRACEBACK', "True")
# log.info(f"enable_traceback:{enable_traceback}")


class Error:
    def __init__(self, error):
        self.code = getattr(error, 'code', -1)
        self.message = getattr(error, 'message', str(error))

    def __str__(self):
        return f"Error(code={self.code}, message={self.message})"

    def __repr__(self):
        return f"Error(code={self.code}, message={self.message})"


log_row_length = 300


def api_request_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                _kwargs = copy.deepcopy(kwargs)
                if "enable_traceback" in _kwargs:
                    del _kwargs["enable_traceback"]
                res = func(*args, **_kwargs)
                # if enable_traceback == "True":
                if kwargs.get("enable_traceback", True):
                    res_str = str(res)
                    log_res = res_str[0:log_row_length] + '......' if len(res_str) > log_row_length else res_str
                    log.debug("(api_response) : %s " % log_res)

                return res, True
            except Exception as e:
                e_str = str(e)
                log_e = e_str[0:log_row_length] + '......' if len(e_str) > log_row_length else e_str
                # if enable_traceback == "True":
                if kwargs.get("enable_traceback", True):
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
            arg = _list[1:]
            arg_str = str(arg)
            log_arg = arg_str[0:log_row_length] + '......' if len(arg_str) > log_row_length else arg_str
            # if enable_traceback == "True":
            if kwargs.get("enable_traceback", True):
                log.debug("(api_request)  : [%s] args: %s, kwargs: %s" % (func.__qualname__, log_arg, str(kwargs)))
            return func(*arg, **kwargs)
    return False, False
