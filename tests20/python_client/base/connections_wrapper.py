from pymilvus_orm import Connections
from pymilvus_orm.types import DataType
from pymilvus_orm.default_config import DefaultConfig
import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.util_log import test_log as log
from common.common_type import *


def connections_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                log.debug("(func_res) Response : %s " % str(res))
                return res, True
            except Exception as e:
                log.error("[Connections API Exception]%s: %s" % (str(func), str(e)))
                return e, False
        return inner_wrapper
    return wrapper


@connections_catch()
def func_req(_list, **kwargs):
    if isinstance(_list, list):
        func = _list[0]
        if callable(func):
            arg = []
            if len(_list) > 1:
                for a in _list[1:]:
                    arg.append(a)
            log.debug("(func_req)[%s] Parameters ars arg: %s, kwargs: %s" % (str(func), str(arg), str(kwargs)))
            return func(*arg, **kwargs)
    return False, False


class ApiConnectionsWrapper:
    def __init__(self):
        self.connection = Connections()

    def add_connection(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.connection.add_connection], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, **kwargs).run()
        return res, check_result

    def disconnect(self, alias, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.connection.disconnect, alias])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result

    def remove_connection(self, alias, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.connection.remove_connection, alias])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result

    def connect(self, alias=DefaultConfig.DEFAULT_USING, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.connection.connect, alias], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, alias=alias, **kwargs).run()
        return res, check_result

    def get_connection(self, alias=DefaultConfig.DEFAULT_USING, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.connection.get_connection, alias])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result

    def list_connections(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.connection.list_connections])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def get_connection_addr(self, alias, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.connection.get_connection_addr, alias])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result
