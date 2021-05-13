from pymilvus_orm import Partition
from pymilvus_orm.types import DataType
from pymilvus_orm.default_config import DefaultConfig
import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.util_log import test_log as log
from common.common_type import *


def partition_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs), True
            except Exception as e:
                log.error("[Partition API Exception]%s: %s" % (str(func), str(e)))
                return e, False
        return inner_wrapper
    return wrapper


@partition_catch()
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


class ApiPartition:
    partition = None

    def partition_init(self, collection, name, description="", check_res=None, check_params=None, **kwargs):
        """ In order to distinguish the same name of partition """
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([Partition, collection, name, description], **kwargs)
        self.partition = res if check is True else None
        check_result = CheckFunc(res, func_name, check_res, check_params, collection=collection, name=name, description=description,
                                 **kwargs).run()
        return res, check_result

    def description(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.description])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.name])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def is_empty(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.is_empty])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def num_entities(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.num_entities])
        check_result = CheckFunc(res, func_name, check_res, check_params).run()
        return res, check_result

    def drop(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.drop], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, **kwargs).run()
        return res, check_result

    def load(self, field_names=None, index_names=None, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.load, field_names, index_names], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, field_names=field_names, index_names=index_names,
                                 **kwargs).run()
        return res, check_result

    def release(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.release], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, **kwargs).run()
        return res, check_result

    def insert(self, data, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.insert, data], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, data=data, **kwargs).run()
        return res, check_result

    def search(self, data, anns_field, params, limit, expr=None, output_fields=None, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.partition.search, data, anns_field, params, limit, expr, output_fields], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, data=data, anns_field=anns_field, params=params,
                                 limit=limit, expr=expr, output_fields=output_fields, **kwargs).run()
        return res, check_result
