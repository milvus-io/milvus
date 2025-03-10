import sys
from typing import Optional
from pymilvus import MilvusClient

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request
from utils.wrapper import trace
from utils.util_log import test_log as log
from common import common_func as cf
from base.client_base import Base

TIMEOUT = 120
INDEX_NAME = ""


class TestMilvusClientV2Base(Base):

    # milvus_client = None
    active_trace = False

    def init_async_milvus_client(self):
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        kwargs = {
            "uri": uri,
            "user": cf.param_info.param_user,
            "password": cf.param_info.param_password,
            "token": cf.param_info.param_token,
        }
        self.async_milvus_client_wrap.init_async_client(**kwargs)

    def _client(self, active_trace=False):
        """ return MilvusClient instance if connected successfully, otherwise return None"""
        if self.skip_connection:
            return None
        if cf.param_info.param_uri:
            uri = cf.param_info.param_uri
        else:
            uri = "http://" + cf.param_info.param_host + ":" + str(cf.param_info.param_port)
        res, is_succ = self.init_milvus_client(uri=uri, token=cf.param_info.param_token, active_trace=active_trace)
        if is_succ:
            # self.milvus_client = res
            log.info(f"server version: {res.get_server_version()}")
        return res

    def init_milvus_client(self, uri, user="", password="", db_name="", token="", timeout=None,
                           check_task=None, check_items=None, active_trace=False, **kwargs):
        self.active_trace = active_trace
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([MilvusClient, uri, user, password, db_name, token, timeout], **kwargs)
        # self.milvus_client = res if is_succ else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       uri=uri, user=user, password=password, db_name=db_name, token=token,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    @trace()
    def close(self, client, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([client.close])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ).run()
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
        consistency_level = kwargs.get("consistency_level", "Strong")
        kwargs.update({"consistency_level": consistency_level})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_collection, collection_name, dimension, primary_field_name,
                                  id_type, vector_field_name, metric_type, auto_id, timeout, schema,
                                  index_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, dimension=dimension,
                                       **kwargs).run()

        self.tear_down_collection_names.append(collection_name)
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
    def get_collection_stats(self, client, collection_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.get_collection_stats, collection_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, **kwargs).run()
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
    def hybrid_search(self, client, collection_name, reqs, ranker, limit=10, output_fields=None, timeout=None,
                      check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.hybrid_search, collection_name, reqs, ranker, limit,
                                  output_fields], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, reqs=reqs, ranker=ranker, limit=limit,
                                       output_fields=output_fields, **kwargs).run()
        return res, check_result

    @trace()
    def search_iterator(self, client, collection_name, data, batch_size, limit=-1, filter=None, output_fields=None,
                        search_params=None, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.search_iterator, collection_name, data, batch_size, filter, limit,
                                  output_fields, search_params], **kwargs)
        if any(k in kwargs for k in ['use_rbac_mul_db', 'use_mul_db']):
            self.using_database(client, kwargs.get('another_db'))
        if kwargs.get('use_alias', False) is True:
            alias = collection_name
            self.alter_alias(client, kwargs.get('another_collection'), alias)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, data=data, batch_size=batch_size, filter=filter,
                                       limit=limit, output_fields=output_fields, search_params=search_params,
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
        self.tear_down_collection_names.append(new_name)
        return res, check_result

    @trace()
    def create_database(self, client, db_name, properties: Optional[dict] = None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_database, db_name, properties], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check,
                                       db_name=db_name, properties=properties,
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

    def create_user(self, client, user_name, password, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_user, user_name, password], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name,
                                       password=password, **kwargs).run()
        return res, check_result

    @trace()
    def drop_user(self, client, user_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_user, user_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name, **kwargs).run()
        return res, check_result

    @trace()
    def update_password(self, client, user_name, old_password, new_password, reset_connection=False,
                        timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.update_password, user_name, old_password, new_password,
                                  reset_connection], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name, old_password=old_password,
                                       new_password=new_password, reset_connection=reset_connection,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def list_users(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_users], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def describe_user(self, client, user_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.describe_user, user_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, user_name=user_name, **kwargs).run()
        return res, check_result

    @trace()
    def create_role(self, client, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_role, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def drop_role(self, client, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_role, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def describe_role(self, client, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.describe_role, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def list_roles(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_roles], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def grant_role(self, client, user_name, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.grant_role, user_name, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       user_name=user_name, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def revoke_role(self, client, user_name, role_name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.revoke_role, user_name, role_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       user_name=user_name, role_name=role_name, **kwargs).run()
        return res, check_result

    @trace()
    def grant_privilege(self, client, role_name, object_type, privilege, object_name, db_name="",
                        timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.grant_privilege, role_name, object_type, privilege,
                                  object_name, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       role_name=role_name, object_type=object_type, privilege=privilege,
                                       object_name=object_name, db_name=db_name, **kwargs).run()
        return res, check_result

    @trace()
    def grant_privilege_v2(self, client, role_name, privilege, collection_name, db_name=None,
                           timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.grant_privilege_v2, role_name, privilege, collection_name,
                                  db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       role_name=role_name, privilege=privilege,
                                       collection_name=collection_name, db_name=db_name, **kwargs).run()
        return res, check_result

    @trace()
    def revoke_privilege(self, client, role_name, object_type, privilege, object_name, db_name="",
                         timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.revoke_privilege, role_name, object_type, privilege,
                                  object_name, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       role_name=role_name, object_type=object_type, privilege=privilege,
                                       object_name=object_name, db_name=db_name, **kwargs).run()
        return res, check_result

    @trace()
    def create_privilege_group(self, client, privilege_group: str, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_privilege_group, privilege_group], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       privilege_group=privilege_group, **kwargs).run()
        return res, check_result

    @trace()
    def drop_privilege_group(self, client, privilege_group: str, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.drop_privilege_group, privilege_group], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       privilege_group=privilege_group, **kwargs).run()
        return res, check_result

    @trace()
    def list_privilege_groups(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_privilege_groups], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def add_privileges_to_group(self, client, privilege_group: str, privileges: list, timeout=None,
                                check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.add_privileges_to_group, privilege_group, privileges], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       privilege_group=privilege_group, privileges=privileges, **kwargs).run()
        return res, check_result

    @trace()
    def remove_privileges_from_group(self, client, privilege_group: str, privileges: list, timeout=None,
                                     check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.remove_privileges_from_group, privilege_group, privileges],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       privilege_group=privilege_group, privileges=privileges, **kwargs).run()
        return res, check_result

    @trace()
    def grant_privilege_v2(self, client, role_name: str, privilege: str, collection_name: str, db_name=None, timeout=None,
                           check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.grant_privilege_v2, role_name, privilege, collection_name, db_name],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       role_name=role_name, privilege=privilege,
                                       collection_name=collection_name, db_name=db_name, **kwargs).run()
        return res, check_result

    @trace()
    def revoke_privilege_v2(self, client, role_name: str, privilege: str, collection_name: str, db_name=None,
                            timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.revoke_privilege_v2, role_name, privilege, collection_name, db_name],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       role_name=role_name, privilege=privilege,
                                       collection_name=collection_name, db_name=db_name, **kwargs).run()
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

    @trace()
    def run_analyzer(self, client, text, analyzer_params, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.run_analyzer, text, analyzer_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, text=text,
                                       analyzer_params=analyzer_params, **kwargs).run()
        return res, check_result

    def compact(self, client, collection_name, is_clustering=False, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.compact, collection_name, is_clustering], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       collection_name=collection_name, is_clustering=is_clustering, **kwargs).run()
        return res, check_result

    @trace()
    def get_compaction_state(self, client, job_id, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.get_compaction_state, job_id], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, job_id=job_id, **kwargs).run()
        return res, check_result

    @trace()
    def create_resource_group(self, client, name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.create_resource_group, name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       name=name, **kwargs).run()
        return res, check_result

    @trace()
    def update_resource_groups(self, client, configs, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.update_resource_groups, configs], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       configs=configs, **kwargs).run()
        return res, check_result

    @trace()
    def drop_resource_group(self, client, name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.update_resource_groups, name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       name=name, **kwargs).run()
        return res, check_result

    @trace()
    def describe_resource_group(self, client, name, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.describe_resource_group, name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       name=name, **kwargs).run()
        return res, check_result

    @trace()
    def list_resource_groups(self, client, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.list_resource_groups], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def transfer_replica(self, client, source_group, target_group, collection_name, num_replicas,
                         timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([client.transfer_replica, source_group, target_group, collection_name, num_replicas], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       source_group=source_group, target_group=target_group,
                                       collection_name=collection_name, num_replicas=num_replicas,
                                       **kwargs).run()
        return res, check_result