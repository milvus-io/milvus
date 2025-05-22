import sys
import traceback
import copy

from check.func_check import ResponseChecker, Error
from utils.util_log import test_log as log

# enable_traceback = os.getenv('ENABLE_TRACEBACK', "True")
# log.info(f"enable_traceback:{enable_traceback}")


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
            if kwargs.get("enable_traceback", True):
                arg = _list[1:]
                arg_str = str(arg)
                log_arg = arg_str[0:log_row_length] + '......' if len(arg_str) > log_row_length else arg_str
                log_kwargs = str(kwargs)[0:log_row_length] + '......' if len(str(kwargs)) > log_row_length else str(kwargs)
                log.debug("(api_request)  : [%s] args: %s, kwargs: %s" % (func.__qualname__, log_arg, log_kwargs))
            return func(*arg, **kwargs)
    return False, False


def logger_interceptor():
    def wrapper(func):
        def log_request(*arg, **kwargs):
            if kwargs.get("enable_traceback", True):
                arg = arg[1:]
                arg_str = str(arg)
                log_arg = arg_str[0:log_row_length] + '......' if len(arg_str) > log_row_length else arg_str
                log_kwargs = str(kwargs)[0:log_row_length] + '......' if len(str(kwargs)) > log_row_length else str(kwargs)
                log.debug("(api_request)  : [%s] args: %s, kwargs: %s" % (func.__name__, log_arg, log_kwargs))

        def log_response(res, **kwargs):
            if kwargs.get("enable_traceback", True):
                res_str = str(res)
                log_res = res_str[0:log_row_length] + '......' if len(res_str) > log_row_length else res_str
                log.debug("(api_response) : [%s] %s " % (func.__name__, log_res))
            return res, True

        async def handler(*args, **kwargs):
            _kwargs = copy.deepcopy(kwargs)
            _kwargs.pop("enable_traceback", None)
            check_task = kwargs.get("check_task", None)
            check_items = kwargs.get("check_items", None)
            try:
                # log request
                log_request(*args, **_kwargs)
                # exec func
                res = await func(*args, **_kwargs)
                # log response
                log_response(res, **_kwargs)
                # check_response
                check_res = ResponseChecker(res, sys._getframe().f_code.co_name, check_task, check_items, True).run()
                return res, check_res
            except Exception as e:
                log.error(str(e))
                e_str = str(e)
                log_e = e_str[0:log_row_length] + '......' if len(e_str) > log_row_length else e_str
                if kwargs.get("enable_traceback", True):
                    log.error(traceback.format_exc())
                    log.error("(api_response) : %s" % log_e)
                check_res = ResponseChecker(Error(e), sys._getframe().f_code.co_name, check_task,
                                            check_items, False).run()
                return Error(e), check_res

        return handler

    return wrapper
