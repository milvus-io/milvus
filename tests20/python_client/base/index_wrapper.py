import sys
from pymilvus_orm import Index

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.api_request import api_request


class ApiIndexWrapper:
    index = None

    def init_index(self, collection, field_name, index_params, name="",check_task=None, check_items=None, **kwargs):
        """ In order to distinguish the same name of index """
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([Index, collection, field_name, index_params, name], **kwargs)
        self.index = res if is_succ is True else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ, collection=collection, field_name=field_name,
                                       index_params=index_params, name=name, **kwargs).run()
        return res, check_result

    def drop(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.index.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ, **kwargs).run()
        return res, check_result

    @property
    def name(self):
        return self.index.name

    @property
    def params(self):
        return self.index.params

    @property
    def collection_name(self):
        return self.index.collection_name

    @property
    def field_name(self):
        return self.index.field_name