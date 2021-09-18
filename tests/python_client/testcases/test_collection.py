import numpy
import pandas as pd
import pytest
from pymilvus import DataType

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.utils import *
from common.constants import *

prefix = "collection"
exp_name = "name"
exp_schema = "schema"
exp_num = "num_entities"
exp_primary = "primary"
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()
uid_count = "collection_count"
tag = "collection_count_tag"
uid_stats = "get_collection_stats"
uid_create = "create_collection"
uid_describe = "describe_collection"
uid_drop = "drop_collection"
uid_has = "has_collection"
uid_list = "list_collections"
uid_load = "load_collection"
field_name = default_float_vec_field_name
default_single_query = {
    "bool": {
        "must": [
            {"vector": {field_name: {"topk": default_top_k, "query": gen_vectors(1, default_dim), "metric_type": "L2",
                                     "params": {"nprobe": 10}}}}
        ]
    }
}


class TestCollectionParams(TestcaseBase):
    """ Test case of collection interface """

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_none_removed_invalid_strings(self, request):
        if request.param is None:
            pytest.skip("None schema is valid")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_type_fields(self, request):
        if isinstance(request.param, list):
            pytest.skip("list is valid fields")
        yield request.param

    @pytest.fixture(scope="function", params=cf.gen_all_type_fields())
    def get_unsupported_primary_field(self, request):
        if request.param.dtype == DataType.INT64:
            pytest.skip("int64 type is valid primary key")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_dim(self, request):
        if request.param == 1:
            pytest.skip("1 is valid dim")
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection(self):
        """
        target: test collection with default schema
        method: create collection with default schema
        expected: assert collection property
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.collection_wrap.init_collection(c_name, schema=default_schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema, exp_num: 0,
                                                          exp_primary: ct.default_int64_field_name})
        assert c_name, _ in self.utility_wrap.list_collections()

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="exception not MilvusException")
    def test_collection_empty_name(self):
        """
        target: test collection with empty name
        method: create collection with a empty name
        expected: raise exception
        """
        self._connect()
        c_name = ""
        error = {ct.err_code: 1, ct.err_msg: f'`collection_name` value is illegal'}
        self.collection_wrap.init_collection(c_name, schema=default_schema, check_task=CheckTasks.err_res,
                                             check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not MilvusException")
    @pytest.mark.parametrize("name", [[], 1, [1, "2", 3], (1,), {1: 1}, None])
    def test_collection_illegal_name(self, name):
        """
        target: test collection with illegal name
        method: create collection with illegal name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 1, ct.err_msg: "`collection_name` value {} is illegal".format(name)}
        self.collection_wrap.init_collection(name, schema=default_schema, check_task=CheckTasks.err_res,
                                             check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_collection_invalid_name(self, name):
        """
        target: test collection with invalid name
        method: create collection with invalid name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 1, ct.err_msg: "Invalid collection name: {}".format(name)}
        self.collection_wrap.init_collection(name, schema=default_schema, check_task=CheckTasks.err_res,
                                             check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_dup_name(self):
        """
        target: test collection with dup name
        method: create collection with dup name and none schema and data
        expected: collection properties consistent
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        self.collection_wrap.init_collection(collection_w.name)
        assert collection_w.name == self.collection_wrap.name
        assert collection_w.schema == self.collection_wrap.schema
        assert collection_w.num_entities == self.collection_wrap.num_entities
        assert collection_w.name, _ in self.utility_wrap.list_collections()[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_with_desc(self):
        """
        target: test collection with dup name
        method: 1. default schema with desc 2. dup name collection
        expected: desc consistent
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(description=ct.collection_desc)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: schema})
        self.collection_wrap.init_collection(c_name,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})
        assert collection_w.description == self.collection_wrap.description

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_new_schema(self):
        """
        target: test collection with dup name and new schema
        method: 1.create collection with default schema
                2. collection with dup name and new schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                  check_items={exp_name: c_name, exp_schema: default_schema})
        fields = [cf.gen_int64_field(is_primary=True)]
        schema = cf.gen_collection_schema(fields=fields)
        error = {ct.err_code: 0, ct.err_msg: "The collection already exist, but the schema is not the same as the "
                                             "schema passed in."}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_new_primary(self):
        """
        target: test collection with dup name and new primary_field schema
        method: 1.collection with default schema
                2. collection with same fields and new primary_field schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field_one = cf.gen_int64_field()
        int_field_two = cf.gen_int64_field(name="int2")
        fields = [int_field_one, int_field_two, cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields, primary_field=int_field_one.name)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: schema,
                                                              exp_primary: int_field_one.name})
        new_schema = cf.gen_collection_schema(fields, primary_field=int_field_two.name)
        error = {ct.err_code: 0, ct.err_msg: "The collection already exist, but the schema is not the same as the "
                                             "schema passed in."}
        self.collection_wrap.init_collection(c_name, schema=new_schema, check_task=CheckTasks.err_res,
                                             check_items=error)
        assert collection_w.primary_field.name == int_field_one.name

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_new_dim(self):
        """
        target: test collection with dup name and new dim schema
        method: 1. default schema 2. schema with new dim
        expected: raise exception
        """
        self._connect()
        new_dim = 120
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        schema = cf.gen_default_collection_schema()
        new_fields = cf.gen_float_vec_field(dim=new_dim)
        schema.fields[-1] = new_fields
        error = {ct.err_code: 0, ct.err_msg: "The collection already exist, but the schema is not the same as the "
                                             "schema passed in."}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)
        dim = collection_w.schema.fields[-1].params['dim']
        assert dim == ct.default_dim

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_dup_name_invalid_schema_type(self, get_none_removed_invalid_strings):
        """
        target: test collection with dup name and invalid schema
        method: 1. default schema 2. invalid schema
        expected: raise exception and
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        error = {ct.err_code: 0, ct.err_msg: "Schema type must be schema.CollectionSchema"}
        schema = get_none_removed_invalid_strings
        self.collection_wrap.init_collection(collection_w.name, schema=schema,
                                             check_task=CheckTasks.err_res, check_items=error)
        assert collection_w.name == c_name

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_same_schema(self):
        """
        target: test collection with dup name and same schema
        method: dup name and same schema
        expected: two collection object is available
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        self.collection_wrap.init_collection(name=c_name, schema=default_schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema})
        assert collection_w.name == self.collection_wrap.name

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_none_schema(self):
        """
        target: test collection with none schema
        method: create collection with none schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Should be passed into the schema"}
        self.collection_wrap.init_collection(c_name, schema=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_invalid_type_schema(self, get_none_removed_invalid_strings):
        """
        target: test collection with invalid schema
        method: create collection with non-CollectionSchema type schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Schema type must be schema.CollectionSchema"}
        self.collection_wrap.init_collection(c_name, schema=get_none_removed_invalid_strings,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_invalid_type_fields(self, get_invalid_type_fields):
        """
        target: test collection with invalid fields type, non-list
        method: create collection schema with non-list invalid fields
        expected: exception
        """
        self._connect()
        fields = get_invalid_type_fields
        error = {ct.err_code: 0, ct.err_msg: "The fields of schema must be type list"}
        self.collection_schema_wrap.init_collection_schema(fields=fields,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_with_unknown_type(self):
        """
        target: test collection with unknown type
        method: create with DataType.UNKNOWN
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 0, ct.err_msg: "Field dtype must be of DataType"}
        self.field_schema_wrap.init_field_schema(name="unknown", dtype=DataType.UNKNOWN,
                                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not MilvusException")
    @pytest.mark.parametrize("name", [[], 1, (1,), {1: 1}, "12-s"])
    def test_collection_invalid_type_field(self, name):
        """
        target: test collection with invalid field name
        method: invalid string name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=name, dtype=5, is_primary=True)
        vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[field, vec_field])
        error = {ct.err_code: 1, ct.err_msg: "expected one of: bytes, unicode"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_collection_invalid_field_name(self, name):
        """
        target: test collection with invalid field name
        method: invalid string name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=name, dtype=DataType.INT64, is_primary=True)
        vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[field, vec_field])
        error = {ct.err_code: 1, ct.err_msg: "Invalid field name"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not MilvusException")
    def test_collection_none_field_name(self):
        """
        target: test field schema with None name
        method: None field name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=None, dtype=DataType.INT64, is_primary=True)
        schema = cf.gen_collection_schema(fields=[field, cf.gen_float_vec_field()])
        error = {ct.err_code: 1, ct.err_msg: "You should specify the name of field"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [6, [[]], {}, (), "", "a"])
    def test_collection_invalid_field_type(self, dtype):
        """
        target: test collection with invalid field type
        method: invalid DataType
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 0, ct.err_msg: "Field dtype must be of DataType"}
        self.field_schema_wrap.init_field_schema(name="test", dtype=dtype, is_primary=True,
                                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="exception not MilvusException")
    def test_collection_field_dtype_float_value(self):
        """
        target: test collection with float type
        method: create field with float type
        expected:
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=5.0,
                                                            is_primary=True)
        schema = cf.gen_collection_schema(fields=[field, cf.gen_float_vec_field()])
        error = {ct.err_code: 0, ct.err_msg: "Field type must be of DataType!"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_empty_fields(self):
        """
        target: test collection with empty fields
        method: create collection with fields = []
        expected: exception
        """
        self._connect()
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}
        self.collection_schema_wrap.init_collection_schema(fields=[], primary_field=ct.default_int64_field_name,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_field(self):
        """
        target: test collection with dup field name
        method: Two FieldSchema have same name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field_one = cf.gen_int64_field(is_primary=True)
        field_two = cf.gen_int64_field()
        schema = cf.gen_collection_schema(fields=[field_one, field_two, cf.gen_float_vec_field()])
        error = {ct.err_code: 1, ct.err_msg: "duplicated field name"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)
        assert not self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("field", [cf.gen_float_vec_field(), cf.gen_binary_vec_field()])
    def test_collection_only_vector_field(self, field):
        """
        target: test collection just with vec field
        method: create with float-vec fields
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe"}
        self.collection_schema_wrap.init_collection_schema([field], check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_multi_float_vectors(self):
        """
        target: test collection with multi float vectors
        method: create collection with two float-vec fields
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_vec_field(), cf.gen_float_vec_field(name="tmp")]
        schema = cf.gen_collection_schema(fields=fields, auto_id=True)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_mix_vectors(self):
        """
        target: test collection with mix vectors
        method: create with float and binary vec
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_vec_field(), cf.gen_binary_vec_field()]
        schema = cf.gen_collection_schema(fields=fields, auto_id=True)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_without_vectors(self):
        """
        target: test collection without vectors
        method: create collection only with int field
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_collection_schema([cf.gen_int64_field(is_primary=True)])
        error = {ct.err_code: 0, ct.err_msg: "No vector field is found."}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_without_primary_field(self):
        """
        target: test collection without primary field
        method: no primary field specified in collection schema and fields
        expected: raise exception
        """
        self._connect()
        int_fields, _ = self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=DataType.INT64)
        vec_fields, _ = self.field_schema_wrap.init_field_schema(name=ct.default_float_vec_field_name,
                                                                 dtype=DataType.FLOAT_VECTOR, dim=ct.default_dim)
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}
        self.collection_schema_wrap.init_collection_schema([int_fields, vec_fields],
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_is_primary_false(self):
        """
        target: test collection with all is_primary false
        method: set all fields if_primary false
        expected: raise exception
        """
        self._connect()
        fields = [cf.gen_int64_field(is_primary=False), cf.gen_float_field(is_primary=False),
                  cf.gen_float_vec_field(is_primary=False)]
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}
        self.collection_schema_wrap.init_collection_schema(fields, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("is_primary", ct.get_invalid_strs)
    def test_collection_invalid_is_primary(self, is_primary):
        """
        target: test collection with invalid primary
        method: define field with is_primary=non-bool
        expected: raise exception
        """
        self._connect()
        name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Param is_primary must be bool type"}
        self.field_schema_wrap.init_field_schema(name=name, dtype=DataType.INT64, is_primary=is_primary,
                                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_collection_invalid_primary_field(self, primary_field):
        """
        target: test collection with invalid primary_field
        method: specify invalid string primary_field in collection schema
        expected: raise exception
        """
        self._connect()
        fields = [cf.gen_int64_field(), cf.gen_float_vec_field()]
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}
        self.collection_schema_wrap.init_collection_schema(fields=fields, primary_field=primary_field,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [[], 1, [1, "2", 3], (1,), {1: 1}, None])
    def test_collection_non_string_primary_field(self, primary_field):
        """
        target: test collection with non-string primary_field
        method: primary_field type is not string
        expected: raise exception
        """
        self._connect()
        fields = [cf.gen_int64_field(), cf.gen_float_vec_field()]
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}
        self.collection_schema_wrap.init_collection_schema(fields, primary_field=primary_field,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_not_existed_primary_field(self):
        """
        target: test collection with not exist primary field
        method: specify not existed field as primary_field
        expected: raise exception
        """
        self._connect()
        fake_field = cf.gen_unique_str()
        fields = [cf.gen_int64_field(), cf.gen_float_vec_field()]
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}

        self.collection_schema_wrap.init_collection_schema(fields, primary_field=fake_field,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_primary_in_schema(self):
        """
        target: test collection with primary field
        method: specify primary field in CollectionSchema
        expected: collection.primary_field
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(primary_field=ct.default_int64_field_name)
        self.collection_wrap.init_collection(c_name, schema=schema)
        assert self.collection_wrap.primary_field.name == ct.default_int64_field_name

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_primary_in_field(self):
        """
        target: test collection with primary field
        method: specify primary field in FieldSchema
        expected: collection.primary_field
        """
        self._connect()
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(), cf.gen_float_vec_field()]
        schema, _ = self.collection_schema_wrap.init_collection_schema(fields)
        self.collection_wrap.init_collection(cf.gen_unique_str(prefix), schema=schema)
        assert self.collection_wrap.primary_field.name == ct.default_int64_field_name

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not MilvusException")
    def test_collection_unsupported_primary_field(self, get_unsupported_primary_field):
        """
        target: test collection with unsupported primary field type
        method: specify non-int64 as primary field
        expected: raise exception
        """
        self._connect()
        field = get_unsupported_primary_field
        vec_field = cf.gen_float_vec_field(name="vec")
        error = {ct.err_code: 1, ct.err_msg: "Primary key type must be DataType.INT64."}
        self.collection_schema_wrap.init_collection_schema(fields=[field, vec_field], primary_field=field.name,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_multi_primary_fields(self):
        """
        target: test collection with multi primary
        method: collection with two primary fields
        expected: raise exception
        """
        self._connect()
        int_field_one = cf.gen_int64_field(is_primary=True)
        int_field_two = cf.gen_int64_field(name="int2", is_primary=True)
        error = {ct.err_code: 0, ct.err_msg: "Primary key field can only be one."}
        self.collection_schema_wrap.init_collection_schema(
            fields=[int_field_one, int_field_two, cf.gen_float_vec_field()],
            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_primary_inconsistent(self):
        """
        target: test collection with different primary field setting
        method: 1. set A field is_primary 2. set primary_field is B
        expected: raise exception
        """
        self._connect()
        int_field_one = cf.gen_int64_field(is_primary=True)
        int_field_two = cf.gen_int64_field(name="int2")
        fields = [int_field_one, int_field_two, cf.gen_float_vec_field()]
        error = {ct.err_code: 0, ct.err_msg: "Primary key field can only be one"}
        self.collection_schema_wrap.init_collection_schema(fields, primary_field=int_field_two.name,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_primary_consistent(self):
        """
        target: test collection with both collection schema and field schema
        method: 1. set A field is_primary 2.set primary_field is A
        expected: verify primary field
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field_one = cf.gen_int64_field(is_primary=True)
        schema = cf.gen_collection_schema(fields=[int_field_one, cf.gen_float_vec_field()],
                                          primary_field=int_field_one.name)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_collection_auto_id_in_field_schema(self, auto_id):
        """
        target: test collection with auto_id in field schema
        method: specify auto_id True in field schema
        expected: verify schema's auto_id
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(is_primary=True, auto_id=auto_id)
        vec_field = cf.gen_float_vec_field(name='vec')
        schema, _ = self.collection_schema_wrap.init_collection_schema([int_field, vec_field])
        assert schema.auto_id == auto_id
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_collection_auto_id_in_collection_schema(self, auto_id):
        """
        target: test collection with auto_id in collection schema
        method: specify auto_id True in collection schema
        expected: verify schema auto_id and collection schema
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field(name='vec')
        schema, _ = self.collection_schema_wrap.init_collection_schema([int_field, vec_field], auto_id=auto_id)
        assert schema.auto_id == auto_id
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_auto_id_non_primary_field(self):
        """
        target: test collection set auto_id in non-primary field
        method: set auto_id=True in non-primary field
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 0, ct.err_msg: "auto_id can only be specified on the primary key field"}
        self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=DataType.INT64, auto_id=True,
                                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_auto_id_false_non_primary(self):
        """
        target: test collection set auto_id in non-primary field
        method: set auto_id=True in non-primary field
        expected: verify schema auto_id is False
        """
        self._connect()
        int_field_one = cf.gen_int64_field(is_primary=True)
        int_field_two = cf.gen_int64_field(name='int2', auto_id=False)
        fields = [int_field_one, int_field_two, cf.gen_float_vec_field()]
        schema, _ = self.collection_schema_wrap.init_collection_schema(fields)
        assert not schema.auto_id

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_auto_id_inconsistent(self):
        """
        target: test collection auto_id with both collection schema and field schema
        method: 1.set primary field auto_id=True in field schema 2.set auto_id=False in collection schema
        expected: raise exception
        """
        self._connect()
        int_field = cf.gen_int64_field(is_primary=True, auto_id=True)
        vec_field = cf.gen_float_vec_field(name='vec')
        error = {ct.err_code: 0, ct.err_msg: "The auto_id of the collection is inconsistent with "
                                             "the auto_id of the primary key field"}
        self.collection_schema_wrap.init_collection_schema([int_field, vec_field], auto_id=False,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_collection_auto_id_consistent(self, auto_id):
        """
        target: test collection auto_id with both collection schema and field schema
        method: set auto_id=True/False both field and schema
        expected: verify auto_id
        """
        self._connect()
        int_field = cf.gen_int64_field(is_primary=True, auto_id=auto_id)
        vec_field = cf.gen_float_vec_field(name='vec')
        schema, _ = self.collection_schema_wrap.init_collection_schema([int_field, vec_field], auto_id=auto_id)
        assert schema.auto_id == auto_id

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_auto_id_none_in_field(self):
        """
        target: test collection with auto_id is None
        method: set auto_id=None
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 0, ct.err_msg: "Param auto_id must be bool type"}
        self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=DataType.INT64,
                                                 is_primary=True,
                                                 auto_id=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", ct.get_invalid_strs)
    def test_collection_invalid_auto_id(self, auto_id):
        """
        target: test collection with invalid auto_id
        method: define field with auto_id=non-bool
        expected: raise exception
        """
        self._connect()
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field(name='vec')
        error = {ct.err_code: 0, ct.err_msg: "Param auto_id must be bool type"}
        self.collection_schema_wrap.init_collection_schema([int_field, vec_field], auto_id=auto_id,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_multi_fields_auto_id(self):
        """
        target: test collection auto_id with multi fields
        method: specify auto_id=True for multi int64 fields
        expected: todo raise exception
        """
        self._connect()
        error = {ct.err_code: 0, ct.err_msg: "auto_id can only be specified on the primary key field"}
        cf.gen_int64_field(is_primary=True, auto_id=True)
        self.field_schema_wrap.init_field_schema(name="int", dtype=DataType.INT64, auto_id=True,
                                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR])
    def test_collection_vector_without_dim(self, dtype):
        """
        target: test collection without dimension
        method: define vector field without dim
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        float_vec_field, _ = self.field_schema_wrap.init_field_schema(name="vec", dtype=dtype)
        schema = cf.gen_collection_schema(fields=[cf.gen_int64_field(is_primary=True), float_vec_field])
        error = {ct.err_code: 1, ct.err_msg: "dimension is not defined in field type params"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not MilvusException")
    def test_collection_vector_invalid_dim(self, get_invalid_dim):
        """
        target: test collection with invalid dimension
        method: define float-vec field with invalid dimension
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        float_vec_field = cf.gen_float_vec_field(dim=get_invalid_dim)
        schema = cf.gen_collection_schema(fields=[cf.gen_int64_field(is_primary=True), float_vec_field])
        error = {ct.err_code: 1, ct.err_msg: f'invalid dim: {get_invalid_dim}'}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [-1, 0, 32769])
    def test_collection_vector_out_bounds_dim(self, dim):
        """
        target: test collection with out of bounds dim
        method: invalid dim -1 and 32759
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        float_vec_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[cf.gen_int64_field(is_primary=True), float_vec_field])
        error = {ct.err_code: 1, ct.err_msg: "invalid dimension: {}. should be in range 1 ~ 32768".format(dim)}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_non_vector_field_dim(self):
        """
        target: test collection with dim for non-vector field
        method: define int64 field with dim
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field, _ = self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=DataType.INT64,
                                                                dim=ct.default_dim)
        float_vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[int_field, float_vec_field],
                                          primary_field=ct.default_int64_field_name)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_desc(self):
        """
        target: test collection with description
        method: create with description
        expected: assert default description
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(description=ct.collection_desc)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not MilvusException")
    def test_collection_none_desc(self):
        """
        target: test collection with none description
        method: create with none description
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(description=None)
        error = {ct.err_code: 1, ct.err_msg: "None has type NoneType, but expected one of: bytes, unicode"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_long_desc(self):
        """
        target: test collection with long desc
        method: create with long desc
        expected:
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        desc = "a".join("a" for _ in range(256))
        schema = cf.gen_default_collection_schema(description=desc)
        self.collection_wrap.init_collection(c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_binary(self):
        """
        target: test collection with binary-vec
        method: create collection with binary field
        expected: assert binary field
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.collection_wrap.init_collection(c_name, schema=default_binary_schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_binary_schema})
        assert c_name, _ in self.utility_wrap.list_collections()


class TestCollectionOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test collection interface operations
    ******************************************************************
    """

    # def teardown_method(self):
    #     if self.self.collection_wrap is not None and self.self.collection_wrap.collection is not None:
    #         self.self.collection_wrap.drop()

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_without_connection(self):
        """
        target: test collection without connection
        method: 1.create collection after connection removed
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first'}
        self.collection_wrap.init_collection(c_name, schema=default_schema,
                                             check_task=CheckTasks.err_res, check_items=error)
        assert self.collection_wrap.collection is None

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_multi_create_drop(self):
        """
        target: test cycle creation and deletion of multiple collections
        method: in a loop, collections are created and deleted sequentially
        expected: no exception
        """
        self._connect()
        c_num = 20
        for _ in range(c_num):
            c_name = cf.gen_unique_str(prefix)
            self.collection_wrap.init_collection(c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
            self.collection_wrap.drop()
            assert c_name, _ not in self.utility_wrap.list_collections()

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_drop(self):
        """
        target: test collection with dup name, and drop
        method: 1. two dup name collection object
                2. one object drop collection
        expected: collection dropped
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        self.collection_wrap.init_collection(c_name, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema})
        self.collection_wrap.drop()
        assert not self.utility_wrap.has_collection(c_name)[0]
        error = {ct.err_code: 1, ct.err_msg: f'HasPartition failed: can\'t find collection: {c_name}'}
        collection_w.has_partition("p", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_after_drop(self):
        """
        target: test create collection after create and drop
        method: 1. create a 2. drop a 3, re-create a
        expected: no exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        collection_w.drop()
        assert not self.utility_wrap.has_collection(collection_w.name)[0]
        self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                  check_items={exp_name: c_name, exp_schema: default_schema})
        assert self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_all_datatype_fields(self):
        """
        target: test create collection with all dataType fields
        method: create collection with all dataType schema
        expected: create successfully
        """
        self._connect()
        fields = []
        for k, v in DataType.__members__.items():
            if v and v != DataType.UNKNOWN and v != DataType.FLOAT_VECTOR and v != DataType.BINARY_VECTOR:
                field, _ = self.field_schema_wrap.init_field_schema(name=k.lower(), dtype=v)
                fields.append(field)
        fields.append(cf.gen_float_vec_field())
        schema, _ = self.collection_schema_wrap.init_collection_schema(fields,
                                                                       primary_field=ct.default_int64_field_name)
        c_name = cf.gen_unique_str(prefix)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})


class TestCollectionDataframe(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test construct_from_dataframe
    ******************************************************************
    """

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_non_df(self, request):
        if request.param is None:
            pytest.skip("skip None")
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_construct_from_dataframe(self):
        """
        target: test collection with dataframe data
        method: create collection and insert with dataframe
        expected: collection num entities equal to nb
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_schema})
        conn.flush([c_name])
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_construct_from_binary_dataframe(self):
        """
        target: test binary collection with dataframe
        method: create binary collection with dataframe
        expected: collection num entities equal to nb
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df, _ = cf.gen_default_binary_dataframe_data(nb=ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_binary_schema})
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_none_dataframe(self):
        """
        target: test create collection by empty dataframe
        method: invalid dataframe type create collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Dataframe can not be None."}
        self.collection_wrap.construct_from_dataframe(c_name, None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_dataframe_only_column(self):
        """
        target: test collection with dataframe only columns
        method: dataframe only has columns
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame(columns=[ct.default_int64_field_name, ct.default_float_vec_field_name])
        error = {ct.err_code: 0, ct.err_msg: "Cannot infer schema from empty dataframe"}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_inconsistent_dataframe(self):
        """
        target: test collection with data inconsistent
        method: create and insert with inconsistent data
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        # one field different type df
        mix_data = [(1, 2., [0.1, 0.2]), (2, 3., 4)]
        df = pd.DataFrame(data=mix_data, columns=list("ABC"))
        error = {ct.err_code: 0, ct.err_msg: "The data in the same column must be of the same type"}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field='A', check_task=CheckTasks.err_res,
                                                      check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_non_dataframe(self, get_non_df):
        """
        target: test create collection by invalid dataframe
        method: non-dataframe type create collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Data type must be pandas.DataFrame."}
        df = get_non_df
        self.collection_wrap.construct_from_dataframe(c_name, df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_data_type_dataframe(self):
        """
        target: test collection with invalid dataframe
        method: create with invalid dataframe
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame({"date": pd.date_range('20210101', periods=3), ct.default_int64_field_name: [1, 2, 3]})
        error = {ct.err_code: 0, ct.err_msg: "Cannot infer schema from empty dataframe."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_invalid_field_name(self):
        """
        target: test collection with invalid field name
        method: create with invalid field name dataframe
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame({'%$#': cf.gen_vectors(3, 2), ct.default_int64_field_name: [1, 2, 3]})
        error = {ct.err_code: 1, ct.err_msg: "Invalid field name"}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_none_primary_field(self):
        """
        target: test collection with none primary field
        method: primary_field is none
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        error = {ct.err_code: 0, ct.err_msg: "Schema must have a primary key field."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=None,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_not_existed_primary_field(self):
        """
        target: test collection with not existed primary field
        method: primary field not existed
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=c_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_with_none_auto_id(self):
        """
        target: test construct with non-int64 as primary field
        method: non-int64 as primary field
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        error = {ct.err_code: 0, ct.err_msg: "Param auto_id must be bool type"}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_true_insert(self):
        """
        target: test construct with true auto_id
        method: auto_id=True and insert values
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(nb=100)
        error = {ct.err_code: 0, ct.err_msg: "Auto_id is True, primary field should not have data."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=True, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_true_no_insert(self):
        """
        target: test construct with true auto_id
        method: auto_id=True and not insert ids(primary fields all values are None)
        expected: verify num entities
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        # df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        df[ct.default_int64_field_name] = None
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=True)
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_none_value_auto_id_true(self):
        """
        target: test construct with none value, auto_id
        method: df primary field with none value, auto_id=true
        expected: todo
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        df.iloc[:, 0] = numpy.NaN
        res, _ = self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                               primary_field=ct.default_int64_field_name, auto_id=True)
        mutation_res = res[1]
        assert cf._check_primary_keys(mutation_res.primary_keys, 100)
        assert self.collection_wrap.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_false(self):
        """
        target: test construct with false auto_id
        method: auto_id=False, primary_field correct
        expected: verify auto_id
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=False)
        assert not self.collection_wrap.schema.auto_id
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_none_value_auto_id_false(self):
        """
        target: test construct with none value, auto_id
        method: df primary field with none value, auto_id=false
        expected: raise exception
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        df.iloc[:, 0] = numpy.NaN
        error = {ct.err_code: 0, ct.err_msg: "Primary key type must be DataType.INT64"}
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name, auto_id=False,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_false_same_values(self):
        """
        target: test construct with false auto_id and same value
        method: auto_id=False, primary field same values
        expected: verify num entities
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        df.iloc[1:, 0] = 1
        res, _ = self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                               primary_field=ct.default_int64_field_name, auto_id=False)
        collection_w = res[0]
        assert collection_w.num_entities == nb
        mutation_res = res[1]
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_false_negative_values(self):
        """
        target: test construct with negative values
        method: auto_id=False, primary field values is negative
        expected: verify num entities
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        new_values = pd.Series(data=[i for i in range(0, -nb, -1)])
        df[ct.default_int64_field_name] = new_values
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name, auto_id=False)
        assert self.collection_wrap.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_dataframe_dup_name(self):
        """
        target: test collection with dup name and insert dataframe
        method: create collection with dup name, none schema, dataframe
        expected: two collection object is correct
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, primary_field=ct.default_int64_field_name,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_schema})
        conn.flush([collection_w.name])
        assert collection_w.num_entities == ct.default_nb
        assert collection_w.num_entities == self.collection_wrap.num_entities


class TestCollectionCount:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            1000,
            2001
        ],
    )
    def insert_count(self, request):
        yield request.param

    """
    generate valid create_index params
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        return request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_without_connection(self, collection, dis_connect):
        '''
        target: test count_entities, without connection
        method: calling count_entities with correct params, with a disconnected instance
        expected: count_entities raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.count_entities(collection)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_count_no_vectors(self, connect, collection):
        '''
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
            assert the value returned by count_entities method is equal to 0
        expected: the count is equal to 0
        '''
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == 0


class TestCollectionCountIP:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            1000,
            2001
        ],
    )
    def insert_count(self, request):
        yield request.param

    """
    generate valid create_index params
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        request.param.update({"metric_type": "IP"})
        return request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_after_index_created(self, connect, collection, get_simple_index, insert_count):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params
        expected: count_entities raise exception
        '''
        entities = gen_entities(insert_count)
        connect.insert(collection, entities)
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count


class TestCollectionCountBinary:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            1000,
            2001
        ],
    )
    def insert_count(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_jaccard_index(self, request, connect):
        request.param["metric_type"] = "JACCARD"
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_hamming_index(self, request, connect):
        request.param["metric_type"] = "HAMMING"
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_substructure_index(self, request, connect):
        request.param["metric_type"] = "SUBSTRUCTURE"
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_superstructure_index(self, request, connect):
        request.param["metric_type"] = "SUPERSTRUCTURE"
        return request.param

    # TODO: need to update and enable
    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_after_index_created_A(self, connect, binary_collection, get_hamming_index, insert_count):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params
        expected: count_entities raise exception
        '''
        raw_vectors, entities = gen_binary_entities(insert_count)
        connect.insert(binary_collection, entities)
        connect.flush([binary_collection])
        # connect.load_collection(binary_collection)
        connect.create_index(binary_collection, default_binary_vec_field_name, get_hamming_index)
        stats = connect.get_collection_stats(binary_collection)
        assert stats[row_count] == insert_count

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_no_entities(self, connect, binary_collection):
        '''
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
            assert the value returned by count_entities method is equal to 0
        expected: the count is equal to 0
        '''
        stats = connect.get_collection_stats(binary_collection)
        assert stats[row_count] == 0


class TestCollectionMultiCollections:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            1000,
            2001
        ],
    )
    def insert_count(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_count_multi_collections_l2(self, connect, insert_count):
        '''
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        entities = gen_entities(insert_count)
        collection_list = []
        collection_num = 20
        for i in range(collection_num):
            collection_name = gen_unique_str(uid_count)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            connect.insert(collection_name, entities)
        connect.flush(collection_list)
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats[row_count] == insert_count
            connect.drop_collection(collection_list[i])

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_multi_collections_binary(self, connect, binary_collection, insert_count):
        '''
        target: test collection rows_count is correct or not with multiple collections of JACCARD
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        raw_vectors, entities = gen_binary_entities(insert_count)
        connect.insert(binary_collection, entities)
        collection_list = []
        collection_num = 20
        for i in range(collection_num):
            collection_name = gen_unique_str(uid_count)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_binary_fields)
            connect.insert(collection_name, entities)
        connect.flush(collection_list)
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats[row_count] == insert_count
            connect.drop_collection(collection_list[i])

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_multi_collections_mix(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of JACCARD
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        collection_list = []
        collection_num = 20
        for i in range(0, int(collection_num / 2)):
            collection_name = gen_unique_str(uid_count)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            connect.insert(collection_name, default_entities)
        for i in range(int(collection_num / 2), collection_num):
            collection_name = gen_unique_str(uid_count)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_binary_fields)
            res = connect.insert(collection_name, default_binary_entities)
        connect.flush(collection_list)
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats[row_count] == default_nb
            connect.drop_collection(collection_list[i])


class TestGetCollectionStats:
    """
    ******************************************************************
      The following cases are used to test `collection_stats` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_jaccard_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            request.param["metric_type"] = "JACCARD"
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.fixture(
        scope="function",
        params=[
            1,
            1000,
            2001
        ],
    )
    def insert_count(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_name_not_existed(self, connect, collection):
        '''
        target: get collection stats where collection name does not exist
        method: call collection_stats with a random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str(uid_stats)
        with pytest.raises(Exception) as e:
            connect.get_collection_stats(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_name_invalid(self, connect, get_invalid_collection_name):
        '''
        target: get collection stats where collection name is invalid
        method: call collection_stats with invalid collection_name
        expected: status not ok
        '''
        collection_name = get_invalid_collection_name
        with pytest.raises(Exception) as e:
            connect.get_collection_stats(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_empty(self, connect, collection):
        '''
        target: get collection stats where no entity in collection
        method: call collection_stats in empty collection
        expected: segment = []
        '''
        stats = connect.get_collection_stats(collection)
        connect.flush([collection])
        assert stats[row_count] == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_without_connection(self, collection, dis_connect):
        '''
        target: test count_entities, without connection
        method: calling count_entities with correct params, with a disconnected instance
        expected: count_entities raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.get_collection_stats(collection)

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_batch(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add entities, check count in collection info
        expected: count as expected
        '''
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert int(stats[row_count]) == default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_single(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add entity one by one, check count in collection info
        expected: count as expected
        '''
        nb = 10
        for i in range(nb):
            connect.insert(collection, default_entity)
            connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == nb

    @pytest.mark.tags(CaseLabel.L2)
    def _test_get_collection_stats_after_delete(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete entities, check count in collection info
        expected: status ok, count as expected
        '''
        ids = connect.insert(collection, default_entities)
        status = connect.flush([collection])
        delete_ids = [ids[0], ids[-1]]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == default_nb - 2
        assert stats["partitions"][0]["row_count"] == default_nb - 2
        assert stats["partitions"][0]["segments"][0]["data_size"] > 0

    # TODO: enable
    @pytest.mark.tags(CaseLabel.L2)
    def _test_get_collection_stats_after_compact_parts(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete entities, and compact collection, check count in collection info
        expected: status ok, count as expected
        '''
        delete_length = 1000
        ids = connect.insert(collection, default_entities)
        status = connect.flush([collection])
        delete_ids = ids[:delete_length]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        assert stats["row_count"] == default_nb - delete_length
        compact_before = stats["partitions"][0]["segments"][0]["data_size"]
        connect.compact(collection)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_after = stats["partitions"][0]["segments"][0]["data_size"]
        assert compact_before == compact_after

    @pytest.mark.tags(CaseLabel.L2)
    def _test_get_collection_stats_after_compact_delete_one(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete one entity, and compact collection, check count in collection info
        expected: status ok, count as expected
        '''
        ids = connect.insert(collection, default_entities)
        status = connect.flush([collection])
        delete_ids = ids[:1]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_before = stats["partitions"][0]["row_count"]
        connect.compact(collection)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_after = stats["partitions"][0]["row_count"]
        # pdb.set_trace()
        assert compact_before == compact_after

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partition(self, connect, collection):
        '''
        target: get partition info in a collection
        method: call collection_stats after partition created and check partition_stats
        expected: status ok, vectors added to partition
        '''
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_partitions(self, connect, collection):
        '''
        target: get partition info in a collection
        method: create two partitions, add vectors in one of the partitions, call collection_stats and check
        expected: status ok, vectors added to one partition but not the other
        '''
        new_tag = "new_tag"
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb
        connect.insert(collection, default_entities, partition_name=new_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb * 2
        connect.insert(collection, default_entities)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb * 3

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partitions_A(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partitions_B(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in one of the partitions,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities, partition_name=default_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_partitions_C(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in one of the partitions,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities)
        connect.insert(collection, entities, partition_name=default_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count * 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partitions_D(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in one of the partitions,
            assert the value returned by count_entities method is equal to length of entities
        expected: the collection count is equal to the length of entities
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities, partition_name=default_tag)
        connect.insert(collection, entities, partition_name=new_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count * 2

    # TODO: assert metric type in stats response
    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_after_index_created(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created
        method: create collection, add vectors, create index and call collection_stats
        expected: status ok, index created and shown in segments
        '''
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb

    # TODO: assert metric type in stats response
    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_after_index_created_ip(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created
        method: create collection, add vectors, create index and call collection_stats
        expected: status ok, index created and shown in segments
        '''
        get_simple_index["metric_type"] = "IP"
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        get_simple_index.update({"metric_type": "IP"})
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb

    # TODO: assert metric type in stats response
    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_after_index_created_jac(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test collection info after index created
        method: create collection, add binary entities, create index and call collection_stats
        expected: status ok, index created and shown in segments
        '''
        ids = connect.insert(binary_collection, default_binary_entities)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, default_binary_vec_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        assert stats[row_count] == default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_collection_stats_after_create_different_index(self, connect, collection):
        '''
        target: test collection info after index created repeatedly
        method: create collection, add vectors, create index and call collection_stats multiple times
        expected: status ok, index info shown in segments
        '''
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        for index_type in ["IVF_FLAT", "IVF_SQ8"]:
            connect.create_index(collection, default_float_vec_field_name,
                                 {"index_type": index_type, "params": {"nlist": 1024}, "metric_type": "L2"})
            stats = connect.get_collection_stats(collection)
            assert stats[row_count] == default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_multi_collections_indexed(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: row count in segments
        '''
        collection_list = []
        collection_num = 10
        for i in range(collection_num):
            collection_name = gen_unique_str(uid_stats)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            res = connect.insert(collection_name, default_entities)
            connect.flush(collection_list)
            index_1 = {"index_type": "IVF_SQ8", "params": {"nlist": 1024}, "metric_type": "L2"}
            index_2 = {"index_type": "IVF_FLAT", "params": {"nlist": 1024}, "metric_type": "L2"}
            if i % 2:
                connect.create_index(collection_name, default_float_vec_field_name, index_1)
            else:
                connect.create_index(collection_name, default_float_vec_field_name, index_2)
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats[row_count] == default_nb
            index = connect.describe_index(collection_list[i], "")
            if i % 2:
                create_target_index(index_1, default_float_vec_field_name)
                assert index == index_1
            else:
                create_target_index(index_2, default_float_vec_field_name)
                assert index == index_2
                # break
            connect.drop_collection(collection_list[i])


class TestCreateCollection:
    """
    ******************************************************************
      The following cases are used to test `create_collection` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_single_filter_fields()
    )
    def get_filter_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_single_vector_fields()
    )
    def get_vector_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_segment_row_limits()
    )
    def get_segment_row_limit(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def _test_create_collection_segment_row_limit(self, connect, get_segment_row_limit):
        '''
        target: test create normal collection with different fields
        method: create collection with diff segment_row_limit
        expected: no exception raised
        '''
        collection_name = gen_unique_str(uid_create)
        fields = copy.deepcopy(default_fields)
        # fields["segment_row_limit"] = get_segment_row_limit
        connect.create_collection(collection_name, fields)
        assert connect.has_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_collection_after_insert(self, connect, collection):
        '''
        target: test insert vector, then create collection again
        method: insert vector and create collection
        expected: error raised
        '''
        # pdb.set_trace()
        connect.insert(collection, default_entity)

        try:
            connect.create_collection(collection, default_fields)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "Create collection failed: meta table add collection failed," \
                              "error = collection %s exist" % collection

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_collection_after_insert_flush(self, connect, collection):
        '''
        target: test insert vector, then create collection again
        method: insert vector and create collection
        expected: error raised
        '''
        connect.insert(collection, default_entity)
        connect.flush([collection])
        try:
            connect.create_collection(collection, default_fields)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "Create collection failed: meta table add collection failed," \
                              "error = collection %s exist" % collection

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_multithread(self, connect):
        '''
        target: test create collection with multithread
        method: create collection using multithread, 
        expected: collections are created
        '''
        threads_num = 8
        threads = []
        collection_names = []

        def create():
            collection_name = gen_unique_str(uid_create)
            collection_names.append(collection_name)
            connect.create_collection(collection_name, default_fields)

        for i in range(threads_num):
            t = MyThread(target=create, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

        for item in collection_names:
            assert item in connect.list_collections()
            connect.drop_collection(item)


class TestCreateCollectionInvalid(object):
    """
    Test creating collections with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_metric_types()
    )
    def get_metric_type(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_segment_row_limit(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_dim(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_invalid_string(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_field_types()
    )
    def get_field_type(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def _test_create_collection_with_invalid_segment_row_limit(self, connect, get_segment_row_limit):
        collection_name = gen_unique_str()
        fields = copy.deepcopy(default_fields)
        fields["segment_row_limit"] = get_segment_row_limit
        with pytest.raises(Exception) as e:
            connect.create_collection(collection_name, fields)

    @pytest.mark.tags(CaseLabel.L2)
    def _test_create_collection_no_segment_row_limit(self, connect):
        '''
        target: test create collection with no segment_row_limit params
        method: create collection with correct params
        expected: use default default_segment_row_limit
        '''
        collection_name = gen_unique_str(uid_create)
        fields = copy.deepcopy(default_fields)
        fields.pop("segment_row_limit")
        connect.create_collection(collection_name, fields)
        res = connect.get_collection_info(collection_name)
        logging.getLogger().info(res)
        assert res["segment_row_limit"] == default_server_segment_row_limit

    # TODO: assert exception
    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_limit_fields(self, connect):
        collection_name = gen_unique_str(uid_create)
        limit_num = 64
        fields = copy.deepcopy(default_fields)
        for i in range(limit_num):
            field_name = gen_unique_str("field_name")
            field = {"name": field_name, "type": DataType.INT64}
            fields["fields"].append(field)

        try:
            connect.create_collection(collection_name, fields)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "maximum field's number should be limited to 64"


class TestDescribeCollection:

    @pytest.fixture(
        scope="function",
        params=gen_single_filter_fields()
    )
    def get_filter_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_single_vector_fields()
    )
    def get_vector_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        logging.getLogger().info(request.param)
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in CPU mode")
        return request.param

    """
    ******************************************************************
      The following cases are used to test `describe_collection` function, no data in collection
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_fields(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, check info returned
        method: create collection with diff fields: metric/field_type/..., calling `describe_collection`
        expected: no exception raised, and value returned correct
        '''
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str(uid_describe)
        fields = {
            "fields": [gen_primary_field(), filter_field, vector_field],
            # "segment_row_limit": default_segment_row_limit
        }
        connect.create_collection(collection_name, fields)
        res = connect.describe_collection(collection_name)
        # assert res['segment_row_limit'] == default_segment_row_limit
        assert len(res["fields"]) == len(fields.get("fields"))
        for field in res["fields"]:
            if field["type"] == filter_field:
                assert field["name"] == filter_field["name"]
            elif field["type"] == vector_field:
                assert field["name"] == vector_field["name"]
                assert field["params"] == vector_field["params"]

    @pytest.mark.tags(CaseLabel.L0)
    def test_describe_collection_after_index_created(self, connect, collection, get_simple_index):
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            assert index["index_type"] == get_simple_index["index_type"]
            assert index["metric_type"] == get_simple_index["metric_type"]
            assert index["params"] == get_simple_index["params"]

    @pytest.mark.tags(CaseLabel.L2)
    def test_describe_collection_without_connection(self, collection, dis_connect):
        '''
        target: test get collection info, without connection
        method: calling get collection info with correct params, with a disconnected instance
        expected: get collection info raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.describe_collection(collection)

    @pytest.mark.tags(CaseLabel.L0)
    def test_describe_collection_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, create this collection then drop it,
            assert the value returned by describe_collection method
        expected: False
        '''
        collection_name = gen_unique_str(uid_describe)
        connect.create_collection(collection_name, default_fields)
        connect.describe_collection(collection_name)
        connect.drop_collection(collection_name)
        try:
            connect.describe_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.L2)
    def test_describe_collection_multithread(self, connect):
        '''
        target: test create collection with multithread
        method: create collection using multithread,
        expected: collections are created
        '''
        threads_num = 4
        threads = []
        collection_name = gen_unique_str(uid_describe)
        connect.create_collection(collection_name, default_fields)

        def get_info():
            connect.describe_collection(collection_name)

        for i in range(threads_num):
            t = MyThread(target=get_info)
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    """
    ******************************************************************
      The following cases are used to test `describe_collection` function, and insert data in collection
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_describe_collection_fields_after_insert(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, check info returned
        method: create collection with diff fields: metric/field_type/..., calling `describe_collection`
        expected: no exception raised, and value returned correct
        '''
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str(uid_describe)
        fields = {
            "fields": [gen_primary_field(), filter_field, vector_field],
            # "segment_row_limit": default_segment_row_limit
        }
        connect.create_collection(collection_name, fields)
        entities = gen_entities_by_fields(fields["fields"], default_nb, vector_field["params"]["dim"])
        res_ids = connect.insert(collection_name, entities)
        connect.flush([collection_name])
        res = connect.describe_collection(collection_name)
        # assert res['segment_row_limit'] == default_segment_row_limit
        assert len(res["fields"]) == len(fields.get("fields"))
        for field in res["fields"]:
            if field["type"] == filter_field:
                assert field["name"] == filter_field["name"]
            elif field["type"] == vector_field:
                assert field["name"] == vector_field["name"]
                assert field["params"] == vector_field["params"]


class TestDescribeCollectionInvalid(object):
    """
    Test describe collection with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_describe_collection_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.describe_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ('', None))
    def test_describe_collection_with_empty_or_None_collection_name(self, connect, collection_name):
        with pytest.raises(Exception) as e:
            connect.describe_collection(collection_name)


class TestDropCollection:
    """
    ******************************************************************
      The following cases are used to test `drop_collection` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_collection_A(self, connect, collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        connect.drop_collection(collection)
        time.sleep(2)
        assert not connect.has_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_without_connection(self, collection, dis_connect):
        '''
        target: test describe collection, without connection
        method: drop collection with correct params, with a disconnected instance
        expected: drop raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.drop_collection(collection)

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_collection_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, 
            assert the exception raised returned by drp_collection method
        expected: False
        '''
        collection_name = gen_unique_str(uid_drop)
        try:
            connect.drop_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_drop_collection_multithread(self, connect):
        '''
        target: test create and drop collection with multithread
        method: create and drop collection using multithread, 
        expected: collections are created, and dropped
        '''
        threads_num = 8
        threads = []
        collection_names = []

        def create():
            collection_name = gen_unique_str(uid_drop)
            collection_names.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            connect.drop_collection(collection_name)

        for i in range(threads_num):
            t = MyThread(target=create, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

        for item in collection_names:
            assert not connect.has_collection(item)


class TestDropCollectionInvalid(object):
    """
    Test has collection with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ('', None))
    def test_drop_collection_with_empty_or_None_collection_name(self, connect, collection_name):
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)


class TestHasCollection:
    """
    ******************************************************************
      The following cases are used to test `has_collection` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_without_connection(self, collection, dis_connect):
        '''
        target: test has collection, without connection
        method: calling has collection with correct params, with a disconnected instance
        expected: has collection raise exception
        '''
        with pytest.raises(Exception) as e:
            assert dis_connect.has_collection(collection)

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_collection_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, create this collection then drop it,
            assert the value returned by has_collection method
        expected: False
        '''
        collection_name = gen_unique_str(uid_has)
        connect.create_collection(collection_name, default_fields)
        assert connect.has_collection(collection_name)
        connect.drop_collection(collection_name)
        assert not connect.has_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_multithread(self, connect):
        '''
        target: test create collection with multithread
        method: create collection using multithread,
        expected: collections are created
        '''
        threads_num = 4
        threads = []
        collection_name = gen_unique_str(uid_has)
        connect.create_collection(collection_name, default_fields)

        def has():
            assert connect.has_collection(collection_name)
            # assert not assert_collection(connect, collection_name)

        for i in range(threads_num):
            t = MyThread(target=has, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()


class TestHasCollectionInvalid(object):
    """
    Test has collection with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_with_empty_collection_name(self, connect):
        collection_name = ''
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_with_none_collection_name(self, connect):
        collection_name = None
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)


class TestListCollections:
    """
    ******************************************************************
      The following cases are used to test `list_collections` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_collections_multi_collections(self, connect):
        '''
        target: test list collections
        method: create collection, assert the value returned by list_collections method
        expected: True
        '''
        collection_num = 50
        collection_names = []
        for i in range(collection_num):
            collection_name = gen_unique_str(uid_list)
            collection_names.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            assert collection_name in connect.list_collections()
        for i in range(collection_num):
            connect.drop_collection(collection_names[i])

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_without_connection(self, dis_connect):
        '''
        target: test list collections, without connection
        method: calling list collections with correct params, with a disconnected instance
        expected: list collections raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.list_collections()

    # TODO: make sure to run this case in the end
    @pytest.mark.skip("r0.3-test")
    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_no_collection(self, connect):
        '''
        target: test show collections is correct or not, if no collection in db
        method: delete all collections,
            assert the value returned by list_collections method is equal to []
        expected: the status is ok, and the result is equal to []      
        '''
        result = connect.list_collections()
        if result:
            for collection_name in result:
                assert connect.has_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_multithread(self, connect):
        '''
        target: test list collection with multithread
        method: list collection using multithread,
        expected: list collections correctly
        '''
        threads_num = 10
        threads = []
        collection_name = gen_unique_str(uid_list)
        connect.create_collection(collection_name, default_fields)

        def _list():
            assert collection_name in connect.list_collections()

        for i in range(threads_num):
            t = MyThread(target=_list)
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()


class TestLoadCollection:
    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request, connect):
        return request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_collection_after_index(self, connect, collection, get_simple_index):
        '''
        target: test load collection, after index created
        method: insert and create index, load collection with correct params
        expected: no error raised
        '''
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        connect.load_collection(collection)
        connect.release_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_index_binary(self, connect, binary_collection, get_binary_index):
        '''
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        '''
        result = connect.insert(binary_collection, default_binary_entities)
        assert len(result.primary_keys) == default_nb
        connect.flush([binary_collection])
        for metric_type in binary_metrics():
            get_binary_index["metric_type"] = metric_type
            connect.drop_index(binary_collection, default_binary_vec_field_name)
            if get_binary_index["index_type"] == "BIN_IVF_FLAT" and metric_type in structure_metrics():
                with pytest.raises(Exception) as e:
                    connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
            else:
                connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
                index = connect.describe_index(binary_collection, "")
                create_target_index(get_binary_index, default_binary_vec_field_name)
                assert index == get_binary_index
            connect.load_collection(binary_collection)
            connect.release_collection(binary_collection)

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_empty_collection(self, connect, collection):
        '''
        target: test load collection
        method: no entities in collection, load collection with correct params
        expected: load success
        '''
        connect.load_collection(collection)
        connect.release_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_dis_connect(self, dis_connect, collection):
        '''
        target: test load collection, without connection
        method: load collection with correct params, with a disconnected instance
        expected: load raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.load_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_collection_dis_connect(self, dis_connect, collection):
        '''
        target: test release collection, without connection
        method: release collection with correct params, with a disconnected instance
        expected: release raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.release_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_not_existed(self, connect, collection):
        collection_name = gen_unique_str(uid_load)
        try:
            connect.load_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_collection_not_existed(self, connect, collection):
        collection_name = gen_unique_str(uid_load)
        try:
            connect.release_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_collection_not_load(self, connect, collection):
        """
        target: test release collection without load
        method:
        expected: raise exception
        """
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.release_collection(collection)

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_collection_after_load_release(self, connect, collection):
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.release_collection(collection)
        connect.load_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_repeatedly(self, connect, collection):
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.load_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_collection(self, connect, collection):
        collection_name = gen_unique_str(uid_load)
        connect.create_collection(collection_name, default_fields)
        connect.insert(collection_name, default_entities)
        connect.flush([collection_name])
        connect.load_collection(collection_name)
        connect.release_collection(collection_name)
        connect.drop_collection(collection_name)
        try:
            connect.load_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

        try:
            connect.release_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_collection_after_drop(self, connect, collection):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.drop_collection(collection)
        try:
            connect.release_collection(collection)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_collection_without_flush(self, connect, collection):
        """
        target: test load collection without flush
        method: insert entities without flush, then load collection
        expected: load collection failed
        """
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.load_collection(collection)

    # TODO
    @pytest.mark.tags(CaseLabel.L2)
    def _test_load_collection_larger_than_memory(self):
        """
        target: test load collection when memory less than collection size
        method: i don't know
        expected: raise exception
        """

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_collection_release_part_partitions(self, connect, collection):
        """
        target: test release part partitions after load collection
        method: load collection and release part partitions
        expected: released partitions search empty
        """
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.release_partitions(collection, [default_tag])
        with pytest.raises(Exception) as e:
            connect.search(collection, default_single_query, partition_names=[default_tag])
        res = connect.search(collection, default_single_query, partition_names=[default_partition_name])
        assert len(res[0]) == default_top_k

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_all_partitions(self, connect, collection):
        """
        target: test release all partitions after load collection
        method: load collection and release all partitions
        expected: search empty
        """
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.release_partitions(collection, [default_partition_name, default_tag])
        res = connect.search(collection, default_single_query)
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_partitions_release_collection(self, connect, collection):
        """
        target: test release collection after load partitions
        method: insert entities into partitions, search empty after load partitions and release collection
        expected: search result empty
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)
        # assert len(res[0]) == 0


class TestReleaseAdvanced:

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_collection_during_searching(self, connect, collection):
        """
        target: test release collection during searching
        method: insert entities into collection, flush and load collection, release collection during searching
        expected:
        """
        nq = 1000
        top_k = 1
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.load_collection(collection)
        query, _ = gen_query_vectors(field_name, default_entities, top_k, nq)
        future = connect.search(collection, query, _async=True)
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partition_during_searching(self, connect, collection):
        """
        target: test release partition during searching
        method: insert entities into partition, flush and load partition, release partition during searching
        expected:
        """
        nq = 1000
        top_k = 1
        connect.create_partition(collection, default_tag)
        query, _ = gen_query_vectors(field_name, default_entities, top_k, nq)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        res = connect.search(collection, query, _async=True)
        connect.release_partitions(collection, [default_tag])
        with pytest.raises(Exception) as e:
            res = connect.search(collection, default_single_query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_collection_during_searching_A(self, connect, collection):
        """
        target: test release collection during searching
        method: insert entities into partition, flush and load partition, release collection during searching
        expected:
        """
        nq = 1000
        top_k = 1
        connect.create_partition(collection, default_tag)
        query, _ = gen_query_vectors(field_name, default_entities, top_k, nq)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        res = connect.search(collection, query, _async=True)
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)

    def _test_release_collection_during_loading(self, connect, collection):
        """
        target: test release collection during loading
        method: insert entities into collection, flush, release collection during loading
        expected:
        """
        connect.insert(collection, default_entities)
        connect.flush([collection])

        def load():
            connect.load_collection(collection)

        t = threading.Thread(target=load, args=())
        t.start()
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)

    def _test_release_partition_during_loading(self, connect, collection):
        """
        target: test release partition during loading
        method: insert entities into partition, flush, release partition during loading
        expected:
        """
        connect.create_partition(collection, default_tag)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])

        def load():
            connect.load_collection(collection)

        t = threading.Thread(target=load, args=())
        t.start()
        connect.release_partitions(collection, [default_tag])
        res = connect.search(collection, default_single_query)
        assert len(res[0]) == 0

    def _test_release_collection_during_inserting(self, connect, collection):
        """
        target: test release collection during inserting
        method: load collection, do release collection during inserting
        expected:
        """
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.load_collection(collection)

        def insert():
            connect.insert(collection, default_entities)

        t = threading.Thread(target=insert, args=())
        t.start()
        connect.release_collection(collection)
        with pytest.raises(Exception):
            res = connect.search(collection, default_single_query)
        # assert len(res[0]) == 0

    def _test_release_collection_during_indexing(self, connect, collection):
        """
        target: test release collection during building index
        method: insert and flush, load collection, do release collection during creating index
        expected:
        """
        pass

    def _test_release_collection_during_droping_index(self, connect, collection):
        """
        target: test release collection during droping index
        method: insert, create index and flush, load collection, do release collection during droping index
        expected:
        """
        pass


class TestLoadCollectionInvalid(object):
    """
    Test load collection with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.load_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_collection_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.release_collection(collection_name)


class TestLoadPartition:
    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in cpu mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_partition_after_index_binary(self, connect, binary_collection, get_binary_index):
        '''
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        '''
        connect.create_partition(binary_collection, default_tag)
        result = connect.insert(binary_collection, default_binary_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([binary_collection])
        for metric_type in binary_metrics():
            logging.getLogger().info(metric_type)
            get_binary_index["metric_type"] = metric_type
            if get_binary_index["index_type"] == "BIN_IVF_FLAT" and metric_type in structure_metrics():
                with pytest.raises(Exception) as e:
                    connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
            else:
                connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
            connect.load_partitions(binary_collection, [default_tag])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_dis_connect(self, connect, dis_connect, collection):
        '''
        target: test load collection, without connection
        method: load collection with correct params, with a disconnected instance
        expected: load raise exception
        '''
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            dis_connect.load_partitions(collection, [default_tag])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partition_dis_connect(self, connect, dis_connect, collection):
        '''
        target: test release collection, without connection
        method: release collection with correct params, with a disconnected instance
        expected: release raise exception
        '''
        connect.create_partition(collection, default_tag)
        connect.load_partitions(collection, [default_tag])
        with pytest.raises(Exception) as e:
            dis_connect.release_partitions(collection, [default_tag])

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_partition_not_existed(self, connect, collection):
        """
        target: test load partition for invalid scenario
        method: load not existed partition
        expected: raise exception and report the error
        """
        partition_name = gen_unique_str(uid_load)
        try:
            connect.load_partitions(collection, [partition_name])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % partition_name

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_partition_not_load(self, connect, collection):
        """
        target: test release partition without load
        method: release partition without load
        expected: raise exception
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.release_partitions(collection, [default_tag])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_after_drop(self, connect, collection):
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.release_partitions(collection, [default_tag])
        connect.drop_partition(collection, default_tag)
        try:
            connect.load_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % default_tag

        try:
            connect.release_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % default_tag

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_partition_after_drop(self, connect, collection):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.drop_partition(collection, default_tag)
        try:
            connect.load_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % default_tag

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_release_after_collection_drop(self, connect, collection):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.release_partitions(collection, [default_tag])
        connect.drop_collection(collection)
        try:
            connect.load_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection

        try:
            connect.release_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection


class TestLoadPartitionInvalid(object):
    """
    Test load collection with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_partition_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_with_invalid_partition_name(self, connect, collection, get_partition_name):
        partition_name = get_partition_name
        with pytest.raises(Exception) as e:
            connect.load_partitions(collection, [partition_name])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partition_with_invalid_partition_name(self, connect, collection, get_partition_name):
        partition_name = get_partition_name
        with pytest.raises(Exception) as e:
            connect.load_partitions(collection, [partition_name])
