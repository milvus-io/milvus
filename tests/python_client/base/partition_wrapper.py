import sys

from pymilvus import Partition

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request


TIMEOUT = 20


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
    def description(self):
        return self.partition.description if self.partition else None

    @property
    def name(self):
        return self.partition.name if self.partition else None

    @property
    def is_empty(self):
        return self.partition.is_empty if self.partition else None

    @property
    def num_entities(self):
        return self.partition.num_entities if self.partition else None

    def drop(self, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.drop], **kwargs)
        check_result = ResponseChecker(res, func_name,
                                       check_task, check_items, succ, **kwargs).run()
        return res, check_result

    def load(self, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.load], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ=succ,
                                       **kwargs).run()
        return res, check_result

    def release(self, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.release], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ=succ,
                                       **kwargs).run()
        return res, check_result

    def insert(self, data, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.insert, data], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ=succ, data=data,
                                       **kwargs).run()
        return res, check_result

    def search(self, data, anns_field, params, limit, expr=None, output_fields=None,
               check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.partition.search, data, anns_field, params,
                                 limit, expr, output_fields], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items,
                                       is_succ=succ, data=data, anns_field=anns_field,
                                       params=params, limit=limit, expr=expr,
                                       output_fields=output_fields, **kwargs).run()
        return res, check_result
