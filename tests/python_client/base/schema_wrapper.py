import sys

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request
from pymilvus import CollectionSchema, FieldSchema


class ApiCollectionSchemaWrapper:
    collection_schema = None

    def init_collection_schema(self, fields, description="", check_task=None, check_items=None, **kwargs):
        """In order to distinguish the same name of CollectionSchema"""
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([CollectionSchema, fields, description], **kwargs)
        self.collection_schema = response if is_succ else None
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ=is_succ, fields=fields,
                                       description=description, **kwargs).run()
        return response, check_result

    @property
    def primary_field(self):
        return self.collection_schema.primary_field if self.collection_schema else None

    @property
    def fields(self):
        return self.collection_schema.fields if self.collection_schema else None

    @property
    def description(self):
        return self.collection_schema.description if self.collection_schema else None

    @property
    def auto_id(self):
        return self.collection_schema.auto_id if self.collection_schema else None


class ApiFieldSchemaWrapper:
    field_schema = None

    def init_field_schema(self, name, dtype, description="", check_task=None, check_items=None, **kwargs):
        """In order to distinguish the same name of FieldSchema"""
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([FieldSchema, name, dtype, description], **kwargs)
        self.field_schema = response if is_succ else None
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ, name=name, dtype=dtype,
                                       description=description, **kwargs).run()
        return response, check_result

    @property
    def description(self):
        return self.field_schema.description if self.field_schema else None

    @property
    def params(self):
        return self.field_schema.params if self.field_schema else None

    @property
    def dtype(self):
        return self.field_schema.dtype if self.field_schema else None
