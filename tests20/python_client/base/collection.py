from pymilvus_orm import Collection
from pymilvus_orm.types import DataType
from pymilvus_orm.default_config import DefaultConfig
import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.util_log import test_log as log
from common.common_type import *


def collection_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs), True
            except Exception as e:
                log.error("[Collection API Exception]%s: %s" % (str(func), str(e)))
                return e, False
        return inner_wrapper
    return wrapper


@collection_catch()
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


class ApiCollection:
    collection = None

    def collection_init(self, name, data=None, schema=None, check_res=None, check_params=None, **kwargs):
        """ In order to distinguish the same name of collection """
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([Collection, name, data, schema], **kwargs)
        self.collection = res if check is True else None
        check_result = CheckFunc(res, func_name, check_res, check_params, check, name=name, data=data, schema=schema, **kwargs).run()
        return res, check_result

    def schema(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.schema])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def description(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.description])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.name])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def is_empty(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.is_empty])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def num_entities(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.num_entities])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def primary_field(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.primary_field])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def drop(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.drop], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, **kwargs).run()
        return res, check_result

    def load(self, field_names=None, index_names=None, partition_names=None, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.load, field_names, index_names, partition_names], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, field_names=field_names, index_names=index_names,
                                 partition_names=partition_names, **kwargs).run()
        return res, check_result

    def release(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.release], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, **kwargs).run()
        return res, check_result

    def insert(self, data, partition_name=None, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.insert, data, partition_name], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, dat=data, partition_name=partition_name, **kwargs).run()
        return res, check_result

    def search(self, data, anns_field, param, limit, expression, partition_names=None, output_fields=None, timeout=None,
               check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.search, data, anns_field, param, limit, expression, partition_names,
                               output_fields, timeout], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, data=data, anns_field=anns_field, param=param, limit=limit,
                                 expression=expression, partition_names=partition_names, output_fields=output_fields,
                                 timeout=timeout, **kwargs).run()
        return res, check_result

    def partitions(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.partitions])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def partition(self, partition_name, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.partition, partition_name])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, partition_name=partition_name).run()
        return res, check_result

    def has_partition(self, partition_name, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.has_partition, partition_name])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, partition_name=partition_name).run()
        return res, check_result

    def drop_partition(self, partition_name, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.drop_partition, partition_name], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, partition_name=partition_name, **kwargs).run()
        return res, check_result

    def indexes(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.indexes])
        check_result = CheckFunc(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def index(self, index_name="", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.index, index_name])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, index_name=index_name).run()
        return res, check_result

    def create_index(self, field_name, index_params, index_name="", check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.create_index, field_name, index_params, index_name], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, field_name=field_name, index_params=index_params,
                                 index_name=index_name, **kwargs).run()
        return res, check_result

    def has_index(self, index_name="", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.has_index, index_name])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, index_name=index_name).run()
        return res, check_result

    def drop_index(self, index_name="", check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.collection.drop_index, index_name], **kwargs)
        check_result = CheckFunc(res, func_name, check_res, check_params, check, index_name=index_name, **kwargs).run()
        return res, check_result
