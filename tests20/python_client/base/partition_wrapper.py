import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.api_request import api_request


class ApiPartitionWrapper:
    partition = None

    def init_partition(self, collection, name, description="",
                       check_task=None, check_items=None, **kwargs):
        """ In order to distinguish the same name of partition """
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([Partition, collection, name, description], **kwargs)
        self.partition = response if is_succ is True else None
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ,
                                       **kwargs).run()
        return response, check_result

    @property
    def description(self, check_task=None, check_items=None):
        return self.partition.description if self.partition else None
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.partition.description])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def name(self, check_task=None, check_items=None):
        return self.partition.name if self.partition else None
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.partition.name])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def is_empty(self, check_task=None, check_items=None):
        return self.partition.is_empty if self.partition else None
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.partition.is_empty])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def num_entities(self, check_task=None, check_items=None):
        return self.partition.num_entities if self.partition else None
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.partition.num_entities])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    def drop(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, succ, **kwargs).run()
        return res, check_result

    def load(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.load], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ=succ,
                                       **kwargs).run()
        return res, check_result

    def release(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.release], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ=succ,
                                       **kwargs).run()
        return res, check_result

    def insert(self, data, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.insert, data], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ=succ, data=data,
                                       **kwargs).run()
        return res, check_result

    def search(self, data, anns_field, params, limit, expr=None, output_fields=None,
               check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.search, data, anns_field, params,
                                 limit, expr, output_fields], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items,
                                       is_succ=succ, data=data, anns_field=anns_field,
                                       params=params, limit=limit, expr=expr,
                                       output_fields=output_fields, **kwargs).run()
        return res, check_result
