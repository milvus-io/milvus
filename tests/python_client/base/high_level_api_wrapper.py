import sys
import time
import timeout_decorator
from numpy import NaN

from pymilvus import Collection
from pymilvus import MilvusClient

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

    milvus_client = None

    def __init__(self, active_trace=False):
        self.active_trace = active_trace

    def init_milvus_client(self, uri, user="", password="", db_name="", token="", timeout=None,
                           check_task=None, check_items=None, active_trace=False, **kwargs):
        self.active_trace = active_trace
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([MilvusClient, uri, user, password, db_name, token, timeout], **kwargs)
        self.milvus_client = res if is_succ else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       uri=uri, user=user, password=password, db_name=db_name, token=token,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

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
    def create_collection(self, client, collection_name, dimension=None, primary_field_name='id',
                          id_type='int', vector_field_name='vector', metric_type='COSINE',
                          auto_id=False, schema=None, index_params=None, timeout=None, check_task=None,
                          check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_collection, collection_name, dimension, primary_field_name,
                                  id_type, vector_field_name, metric_type, auto_id, timeout, schema,
                                  index_params], **kwargs)
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
    def list_indexes(self, client, collection_name, field_name=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_indexes, collection_name, field_name], **kwargs)
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
    def list_aliases(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_aliases, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, collection_name=collection_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def using_database(self, client, db_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.using_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def create_user(self, user_name, password, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.create_user, user_name, password], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name,
                                       password=password, **kwargs).run()
        return res, check_result

    @trace()
    def drop_user(self, user_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.drop_user, user_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name, **kwargs).run()
        return res, check_result

    @trace()
    def update_password(self, user_name, old_password, new_password, reset_connection=False,
                        timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.update_password, user_name, old_password, new_password,
                                  reset_connection], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name, old_password=old_password,
                                       new_password=new_password, reset_connection=reset_connection,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def list_users(self, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.list_users], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def describe_user(self, user_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.describe_user, user_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name, **kwargs).run()
        return res, check_result

    @trace()
    def create_role(self, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.create_role, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def drop_role(self, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.drop_role, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def describe_role(self, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.describe_role, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def list_roles(self, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.list_roles], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def grant_role(self, user_name, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.grant_role, user_name, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       user_name=user_name, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def revoke_role(self, user_name, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.revoke_role, user_name, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       user_name=user_name, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def grant_privilege(self, role_name, object_type, privilege, object_name, db_name="",
                        timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.grant_privilege, role_name, object_type, privilege,
                                  object_name, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       role_name=role_name, object_type=object_type, privilege=privilege,
                                       object_name=object_name, db_name=db_name, **kwargs).run()
        return res, check_result

    @trace()
    def revoke_privilege(self, role_name, object_type, privilege, object_name, db_name="",
                         timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.milvus_client.revoke_privilege, role_name, object_type, privilege,
                                  object_name, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       role_name=role_name, object_type=object_type, privilege=privilege,
                                       object_name=object_name, db_name=db_name, **kwargs).run()
        return res, check_result

    @trace()
    def alter_index_properties(self, client, collection_name, index_name, properties, timeout=None,
                               check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.alter_index_properties, collection_name, index_name, properties],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def drop_index_properties(self, client, collection_name, index_name, property_keys, timeout=None,
                               check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_index_properties, collection_name, index_name, property_keys],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def alter_collection_properties(self, client, collection_name, properties, timeout=None,
                               check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.alter_collection_properties, collection_name, properties],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def drop_collection_properties(self, client, collection_name, property_keys, timeout=None,
                                    check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_collection_properties, collection_name, property_keys, timeout],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def alter_collection_field(self, client, collection_name, field_name, field_params, timeout=None,
                                    check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.alter_collection_field, collection_name, field_name, field_params, timeout],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def alter_database_properties(self, client, db_name, properties, timeout=None,
                               check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.alter_database_properties, db_name, properties], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def drop_database_properties(self, client, db_name, property_keys, timeout=None,
                                  check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_database_properties, db_name, property_keys], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def create_database(self, client, db_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def describe_database(self, client, db_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.describe_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def drop_database(self, client, db_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_database, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def list_databases(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_databases], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result





