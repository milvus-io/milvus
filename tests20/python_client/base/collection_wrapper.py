import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from utils.api_request import api_request


class ApiCollectionWrapper:
    collection = None

    def init_collection(self, name, schema=None, check_task=None, check_items=None, **kwargs):
        """ In order to distinguish the same name of collection """
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([Collection, name, schema], **kwargs)
        self.collection = res if is_succ else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       name=name, schema=schema, **kwargs).run()
        return res, check_result

    @property
    def schema(self, check_task=None, check_items=None):
        return self.collection.schema
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.schema])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def description(self, check_task=None, check_items=None):
        return self.collection.description
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.description])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def name(self, check_task=None, check_items=None):
        return self.collection.name
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.name])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def is_empty(self, check_task=None, check_items=None):
        return self.collection.is_empty
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.is_empty])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def num_entities(self, check_task=None, check_items=None):
        return self.collection.num_entities
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.num_entities])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    @property
    def primary_field(self, check_task=None, check_items=None):
        return self.collection.primary_field
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.primary_field])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    def drop(self, check_task=None, check_items=None, **kwargs):
        log.info("Dropping collection")
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        if check_result:
            log.info("Dropped collection")
        else:
            log.info("Dropping collection failed")
        return res, check_result

    def load(self, partition_names=None, check_task=None, check_items=None, **kwargs):
        log.info("loading data")
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.load, partition_names], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       partition_names=partition_names, **kwargs).run()
        if check_result:
            log.info("loaded data")
        else:
            log.info("loading failed")
        return res, check_result

    def release(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.release], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, check, **kwargs).run()
        return res, check_result

    def insert(self, data, partition_name=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.insert, data, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       dat=data, partition_name=partition_name,
                                       **kwargs).run()
        return res, check_result

    def search(self, data, anns_field, param, limit, expression,
               partition_names=None, output_fields=None, timeout=None,
               check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.search, data, anns_field, param, limit, expression, partition_names,
                               output_fields, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       data=data, anns_field=anns_field, param=param, limit=limit,
                                       expression=expression, partition_names=partition_names,
                                       output_fields=output_fields,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    @property
    def partitions(self, check_task=None, check_items=None):
        return self.collection.partitions
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.partitions])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    def partition(self, partition_name, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.collection.partition, partition_name])
        check_result = ResponseChecker(res, func_name, check_task, check_items,
                                       succ, partition_name=partition_name).run()
        return res, check_result

    def has_partition(self, partition_name, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, succ = api_request([self.collection.has_partition, partition_name])
        check_result = ResponseChecker(res, func_name, check_task, check_items,
                                       succ, partition_name=partition_name).run()
        return res, check_result

    def drop_partition(self, partition_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.drop_partition, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, partition_name=partition_name, **kwargs).run()
        return res, check_result

    def create_partition(self, partition_name, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.create_partition, partition_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       partition_name=partition_name, **kwargs).run()
        return res, check_result

    @property
    def indexes(self, check_task=None, check_items=None):
        return self.collection.indexes
        # func_name = sys._getframe().f_code.co_name
        # res, check = func_req([self.collection.indexes])
        # check_result = CheckFunc(res, func_name, check_task, check_items, check).run()
        # return res, check_result

    def index(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.index])
        check_result = ResponseChecker(res, func_name, check_task, check_items, check).run()
        return res, check_result

    def create_index(self, field_name, index_params, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.create_index, field_name, index_params], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       field_name=field_name, index_params=index_params, **kwargs).run()
        return res, check_result

    def has_index(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.has_index])
        check_result = ResponseChecker(res, func_name, check_task, check_items, check).run()
        return res, check_result

    def drop_index(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.collection.drop_index], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result
