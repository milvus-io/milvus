import sys
from pymilvus_orm import Index

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from base.api_request import api_request


class ApiIndexWrapper:
    index = None

    def index_init(self, collection, field_name, index_params, name="", check_res=None, check_params=None, **kwargs):
        """ In order to distinguish the same name of index """
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([Index, collection, field_name, index_params, name], **kwargs)
        self.index = res if check is True else None
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, collection=collection, field_name=field_name,
                                       index_params=index_params, name=name, **kwargs).run()
        return res, check_result

    def name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.index.name])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def params(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.index.params])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def collection_name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.index.collection_name])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def field_name(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.index.field_name])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def drop(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.index.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, **kwargs).run()
        return res, check_result
