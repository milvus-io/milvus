from pymilvus_orm import Index
from pymilvus_orm.types import DataType
from pymilvus_orm.default_config import DefaultConfig
import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.util_log import test_log as log
from common.common_type import *


def index_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs), True
            except Exception as e:
                log.error("[Index API Exception]%s: %s" % (str(func), str(e)))
                return e, False
        return inner_wrapper
    return wrapper


@index_catch()
def func_req(_list, **kwargs):
    if isinstance(_list, list):
        func = _list[0]
        if callable(func):
            arg = []
            if len(_list) > 1:
                for a in _list[1:]:
                    arg.append(a)
            return func(*arg, **kwargs)
    return False, False


class ApiIndex:
    index = None

    def index_init(self, collection, field_name, index_params, name="", check_res=None, check_params=None, **kwargs):
        """ In order to distinguish the same name of index """
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([Index, collection, field_name, index_params, name], **kwargs)
        self.index = res if check is True else None
        check_result = CheckFunc(res, func_name, check_res, check_params, collection=collection, field_name=field_name,
                                 index_params=index_params, name=name, **kwargs).run()
        return res, check_result

    def name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.index.name])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def params(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.index.params])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def collection_name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.index.collection_name])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def field_name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.index.field_name])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def drop(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.index.drop], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, **kwargs).run()
        return res, check_result
