from pymilvus_orm import utility
from pymilvus_orm.types import DataType
from pymilvus_orm.default_config import DefaultConfig
import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.util_log import test_log as log
from common.common_type import *


def utility_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                log.debug("(func_res) Response : %s " % str(res))
                return res, True
            except Exception as e:
                log.error("[Utility API Exception]%s: %s" % (str(func), str(e)))
                return e, False
        return inner_wrapper
    return wrapper


@utility_catch()
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


class ApiUtilityWrapper:
    """ Method of encapsulating utility files """

    ut = utility

    def loading_progress(self, collection_name, partition_names=[], using="default", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.ut.loading_progress, collection_name, partition_names, using])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, collection_name=collection_name,
                                 partition_names=partition_names,using=using).run()
        return res, check_result

    def wait_for_loading_complete(self, collection_name, partition_names=[], timeout=None, using="default", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.ut.wait_for_loading_complete, collection_name, partition_names, timeout, using])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, collection_name=collection_name,
                                 partition_names=partition_names, timeout=timeout, using=using).run()
        return res, check_result

    def index_building_progress(self, collection_name, index_name="", using="default", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.ut.index_building_progress, collection_name, index_name, using])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, collection_name=collection_name, index_name=index_name,
                                 using=using).run()
        return res, check_result

    def wait_for_index_building_complete(self, collection_name, index_name="", timeout=None, using="default", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.ut.wait_for_loading_complete, collection_name, index_name, timeout, using])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, collection_name=collection_name, index_name=index_name,
                                 timeout=timeout, using=using).run()
        return res, check_result

    def has_collection(self, collection_name, using="default", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.ut.has_collection, collection_name, using])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, collection_name=collection_name, using=using).run()
        return res, check_result

    def has_partition(self, collection_name, partition_name, using="default", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.ut.has_partition, collection_name, partition_name, using])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, collection_name=collection_name,
                                 partition_name=partition_name, using=using).run()
        return res, check_result

    def list_collections(self, timeout=None, using="default", check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = func_req([self.ut.list_collections, timeout, using])
        check_result = CheckFunc(res, func_name, check_res, check_params, check, timeout=timeout,  using=using).run()
        return res, check_result
