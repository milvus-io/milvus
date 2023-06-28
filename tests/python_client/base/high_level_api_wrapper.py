import sys
import time
import timeout_decorator
from numpy import NaN

from pymilvus import Collection

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request
from utils.wrapper import trace
from utils.util_log import test_log as log
from pymilvus.orm.types import CONSISTENCY_STRONG
from common.common_func import param_info

TIMEOUT = 120
INDEX_NAME = ""


# keep small timeout for stability tests
# TIMEOUT = 5


class HighLevelApiWrapper:

    def __init__(self, active_trace=False):
        self.active_trace = active_trace

    @trace()
    def create_collection(self, client, collection_name, dimension, timeout=None, check_task=None,
                          check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_collection, collection_name, dimension], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, dimension=dimension,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def insert(self, client, collection_name, data, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.insert, collection_name, data], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, data=data,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def search(self, client, collection_name, data, limit=10, filter=None, output_fields=None, search_params=None,
               timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.search, collection_name, data, filter, limit,
                                  output_fields, search_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, data=data, limit=limit, filter=filter,
                                       output_fields=output_fields, search_params=search_params,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def query(self, client, collection_name, filter=None, output_fields=None,
              timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.query, collection_name, filter, output_fields], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, filter=filter,
                                       output_fields=output_fields,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def get(self, client, collection_name, ids, output_fields=None,
            timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.get, collection_name, ids, output_fields], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, ids=ids,
                                       output_fields=output_fields,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def num_entities(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.num_entities, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def delete(self, client, collection_name, pks, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.delete, collection_name, pks], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, pks=pks,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def flush(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.flush, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def describe_collection(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.describe_collection, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def list_collections(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_collections], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def drop_collection(self, client, collection_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_collection, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

