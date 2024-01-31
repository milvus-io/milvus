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
    def create_schema(self, client, timeout=None, check_task=None,
                      check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_schema], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       **kwargs).run()
        return res, check_result

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

    def has_collection(self, client, collection_name, timeout=None, check_task=None,
                       check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.has_collection, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
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
    def upsert(self, client, collection_name, data, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.upsert, collection_name, data], **kwargs)
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
    def query(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.query, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
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
    def delete(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.delete, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
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

    @trace()
    def list_partitions(self, client, collection_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_partitions, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def list_indexes(self, client, collection_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_indexes, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def get_load_state(self, client, collection_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.get_load_state, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def prepare_index_params(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.prepare_index_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def load_collection(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.load_collection, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name, **kwargs).run()
        return res, check_result

    @trace()
    def release_collection(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.release_collection, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name, **kwargs).run()
        return res, check_result

    @trace()
    def load_partitions(self, client, collection_name, partition_names, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.load_partitions, collection_name, partition_names], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       partition_names=partition_names,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def release_partitions(self, client, collection_name, partition_names, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.release_partitions, collection_name, partition_names], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       partition_names=partition_names,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def rename_collection(self, client, old_name, new_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.rename_collection, old_name, new_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       old_name=old_name,
                                       new_name=new_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def use_database(self, client, db_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.use_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       db_name=db_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def create_partition(self, client, collection_name, partition_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_partition, collection_name, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       partition_name=partition_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def list_partitions(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_partitions, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def drop_partition(self, client, collection_name, partition_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_partition, collection_name, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       partition_name=partition_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def has_partition(self, client, collection_name, partition_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.has_partition, collection_name, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       partition_name=partition_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def get_partition_stats(self, client, collection_name, partition_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.get_partition_stats, collection_name, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       partition_name=partition_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def prepare_index_params(self, client, check_task=None, check_items=None, **kwargs):

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.prepare_index_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def create_index(self, client, collection_name, index_params, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_index, collection_name, index_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       index_params=index_params,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def drop_index(self, client, collection_name, index_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_index, collection_name, index_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       index_name=index_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def describe_index(self, client, collection_name, index_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.describe_index, collection_name, index_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       index_name=index_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def list_indexes(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_indexes, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def create_alias(self, client, collection_name, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_alias, collection_name, alias], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       alias=alias,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def drop_alias(self, client, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_alias, alias], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       alias=alias,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def alter_alias(self, client, collection_name, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.alter_alias, collection_name, alias], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       collection_name=collection_name,
                                       alias=alias,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def describe_alias(self, client, alias, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.describe_alias, alias], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       alias=alias,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def list_aliases(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_aliases], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def using_database(self, client, db_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.using_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       db_name=db_name,
                                       **kwargs).run()
        return res, check_result