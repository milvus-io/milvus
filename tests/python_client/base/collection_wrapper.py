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

TIMEOUT = 180
INDEX_NAME = ""


# keep small timeout for stability tests
# TIMEOUT = 5


class ApiCollectionWrapper:
    collection = None

    def __init__(self, active_trace=False):
        self.active_trace = active_trace

    def init_collection(self, name, schema=None, using="default", check_task=None, check_items=None,
                        active_trace=False, **kwargs):
        self.active_trace = active_trace
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_STRONG)
        kwargs.update({"consistency_level": consistency_level})

        """ In order to distinguish the same name of collection """
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([Collection, name, schema, using], **kwargs)
        self.collection = res if is_succ else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       name=name, schema=schema, using=using, **kwargs).run()
        return res, check_result

    @property
    def schema(self):
        return self.collection.schema

    @property
    def description(self):
        return self.collection.description

    @property
    def name(self):
        return self.collection.name

    @property
    def is_empty(self):
        self.flush()
        return self.collection.is_empty

    @property
    def num_entities(self):
        self.flush()
        return self.collection.num_entities

    @property
    def num_shards(self):
        return self.collection.num_shards

    @property
    def num_entities_without_flush(self):
        return self.collection.num_entities

    @property
    def primary_field(self):
        return self.collection.primary_field

    @property
    def aliases(self):
        return self.collection.aliases

    @trace()
    def construct_from_dataframe(self, name, dataframe, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([Collection.construct_from_dataframe, name, dataframe], **kwargs)
        self.collection = res[0] if is_succ else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       name=name, dataframe=dataframe, **kwargs).run()
        return res, check_result

    @trace()
    def drop(self, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def load(self, partition_names=None, replica_number=NaN, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        replica_number = param_info.param_replica_num if replica_number is NaN else replica_number

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.load, partition_names, replica_number, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       partition_names=partition_names, **kwargs).run()
        return res, check_result

    @trace()
    def release(self, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.release], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def insert(self, data, partition_name=None, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.insert, data, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       data=data, partition_name=partition_name,
                                       **kwargs).run()
        return res, check_result

    # @trace()
    # def flush(self, check_task=None, check_items=None, **kwargs):
    #     #TODO:currently, flush is not supported by sdk in milvus
    #     timeout = kwargs.get("timeout", TIMEOUT)
    #
    #     @timeout_decorator.timeout(timeout, timeout_exception=TimeoutError)
    #     def _flush():
    #         res = self.collection.num_entities
    #         return res
    #     try:
    #         res = _flush()
    #         return res, True
    #     except TimeoutError as e:
    #         log.error(f"flush timeout error: {e}")
    #         res = None
    #         return res, False

    @trace()
    def flush(self, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.flush], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def search(self, data, anns_field, param, limit, expr=None,
               partition_names=None, output_fields=None, timeout=None, round_decimal=-1,
               check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.search, data, anns_field, param, limit,
                                  expr, partition_names, output_fields, timeout, round_decimal], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       data=data, anns_field=anns_field, param=param, limit=limit,
                                       expr=expr, partition_names=partition_names,
                                       output_fields=output_fields,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    @trace()
    def hybrid_search(self, reqs, rerank, limit, partition_names=None,
                      output_fields=None, timeout=None, round_decimal=-1,
                      check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.hybrid_search, reqs, rerank, limit, partition_names,
                                  output_fields, timeout, round_decimal], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       reqs=reqs, rerank=rerank, limit=limit,
                                       partition_names=partition_names,
                                       output_fields=output_fields,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    @trace()
    def search_iterator(self, data, anns_field, param, batch_size=1000, limit=-1, expr=None,
                        partition_names=None, output_fields=None, timeout=None, round_decimal=-1,
                        check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.search_iterator, data, anns_field, param, batch_size, limit,
                                  expr, partition_names, output_fields, timeout, round_decimal], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       data=data, anns_field=anns_field, param=param, limit=limit,
                                       expr=expr, partition_names=partition_names,
                                       output_fields=output_fields,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    @trace()
    def query(self, expr, output_fields=None, partition_names=None, timeout=None, check_task=None, check_items=None,
              **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.query, expr, output_fields, partition_names, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       expression=expr, partition_names=partition_names,
                                       output_fields=output_fields,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    @trace()
    def query_iterator(self, batch_size=1000, limit=-1, expr=None, output_fields=None, partition_names=None, timeout=None,
                       check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.query_iterator, batch_size, limit, expr, output_fields, partition_names,
                                  timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       batch_size=batch_size, limit=limit, expression=expr,
                                       output_fields=output_fields, partition_names=partition_names,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    @property
    def partitions(self):
        return self.collection.partitions

    @trace()
    def partition(self, partition_name, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.collection.partition, partition_name])
        check_result = ResponseChecker(res, func_name, check_task, check_items,
                                       succ, partition_name=partition_name).run()
        return res, check_result

    @trace()
    def has_partition(self, partition_name, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.collection.has_partition, partition_name])
        check_result = ResponseChecker(res, func_name, check_task, check_items,
                                       succ, partition_name=partition_name).run()
        return res, check_result

    @trace()
    def drop_partition(self, partition_name, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        kwargs.update({"timeout": timeout})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.drop_partition, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, partition_name=partition_name,
                                       **kwargs).run()
        return res, check_result

    @trace()
    def create_partition(self, partition_name, check_task=None, check_items=None, description=""):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.create_partition, partition_name, description])
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       partition_name=partition_name).run()
        return res, check_result

    @property
    def indexes(self):
        return self.collection.indexes

    @trace()
    def index(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        if "index_name" in kwargs:
            index_name = kwargs.get('index_name')
            kwargs.update({"index_name": index_name})
        res, check = api_request([self.collection.index], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def create_index(self, field_name, index_params=None, index_name=None, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = 1200 if timeout is None else timeout
        index_name = INDEX_NAME if index_name is None else index_name
        index_name = kwargs.get("index_name", index_name)
        kwargs.update({"index_name": index_name})
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.create_index, field_name, index_params, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def has_index(self, index_name=None, check_task=None, check_items=None, **kwargs):
        index_name = INDEX_NAME if index_name is None else index_name
        index_name = kwargs.get("index_name", index_name)
        kwargs.update({"index_name": index_name})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.has_index], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def drop_index(self, index_name=None, check_task=None, check_items=None, **kwargs):
        timeout = kwargs.get("timeout", TIMEOUT)
        index_name = INDEX_NAME if index_name is None else index_name
        index_name = kwargs.get("index_name", index_name)
        kwargs.update({"timeout": timeout, "index_name": index_name})

        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.drop_index], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def delete(self, expr, partition_name=None, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.delete, expr, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def upsert(self, data, partition_name=None, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.upsert, data, partition_name, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def compact(self, is_clustering=False, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.compact, is_clustering, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def get_compaction_state(self, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.get_compaction_state, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def get_compaction_plans(self, timeout=None, check_task=None, check_items={}, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.get_compaction_plans, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def wait_for_compaction_completed(self, timeout=None, **kwargs):
        timeout = TIMEOUT * 3 if timeout is None else timeout
        res = self.collection.wait_for_compaction_completed(timeout, **kwargs)
        # log.debug(res)
        return res

    @trace()
    def get_replicas(self, timeout=None, check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.get_replicas, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def describe(self, timeout=None, check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.describe, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, check).run()
        return res, check_result

    @trace()
    def alter_index(self, index_name, extra_params={}, timeout=None,  check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.alter_index, index_name, extra_params, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @trace()
    def set_properties(self, extra_params={}, timeout=None,  check_task=None, check_items=None, **kwargs):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.set_properties, extra_params, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result