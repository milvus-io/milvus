import random

import numpy
import pandas as pd
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from utils.util_log import test_log as log

prefix = "collection"
exp_name = "name"
exp_schema = "schema"
exp_num = "num_entities"
exp_primary = "primary"
exp_shards_num = "shards_num"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()
default_shards_num = 1
uid_count = "collection_count"
tag = "collection_count_tag"
uid_stats = "get_collection_stats"
uid_create = "create_collection"
uid_describe = "describe_collection"
uid_drop = "drop_collection"
uid_has = "has_collection"
uid_list = "list_collections"
uid_load = "load_collection"
partition1 = 'partition1'
partition2 = 'partition2'
field_name = default_float_vec_field_name
default_single_query = {
    "data": gen_vectors(1, default_dim),
    "anns_field": default_float_vec_field_name,
    "param": {"metric_type": "L2", "params": {"nprobe": 10}},
    "limit": default_top_k,
}

default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
default_binary_index_params = {"index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD", "params": {"nlist": 64}}
default_nq = ct.default_nq
default_search_exp = "int64 >= 0"
default_limit = ct.default_limit
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params


class TestCollectionParams(TestcaseBase):
    """ Test case of collection interface """

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_none_removed_invalid_strings(self, request):
        if request.param is None:
            pytest.skip("None schema is valid")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_type_fields)
    def get_invalid_type_fields(self, request):
        if isinstance(request.param, list):
            pytest.skip("list is valid fields")
        yield request.param

    @pytest.fixture(scope="function", params=cf.gen_all_type_fields())
    def get_unsupported_primary_field(self, request):
        if request.param.dtype == DataType.INT64 or request.param.dtype == DataType.VARCHAR:
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
        assert c_name in self.utility_wrap.list_collections()[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_empty_name(self):
        """
        target: test collection with empty name
        method: create collection with an empty name
        expected: raise exception
        """
        self._connect()
        c_name = ""
        error = {ct.err_code: 1, ct.err_msg: f'`collection_name` value is illegal'}
        self.collection_wrap.init_collection(c_name, schema=default_schema, check_task=CheckTasks.err_res,
                                             check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("name", [[], 1, [1, "2", 3], (1,), {1: 1}, "qw$_o90", "1ns_", None])
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("name", ["_co11ection", "co11_ection"])
    def test_collection_naming_rules(self, name):
        """
        target: test collection with valid name
        method: 1. connect milvus
                2. Create a field with a name which uses all the supported elements in the naming rules
                3. Create a collection with a name which uses all the supported elements in the naming rules
        expected: Collection created successfully
        """
        self._connect()
        fields = [cf.gen_int64_field(), cf.gen_int64_field("_1nt"), cf.gen_float_vec_field("f10at_")]
        schema = cf.gen_collection_schema(fields=fields, primary_field=ct.default_int64_field_name)
        self.collection_wrap.init_collection(name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#", "".join("a" for i in range(ct.max_name_length + 1))])
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
        assert collection_w.name in self.utility_wrap.list_collections()[0]

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
        expected: raise exception
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_none_schema(self):
        """
        target: test collection with none schema
        method: create collection with none schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: "Collection '%s' not exist, or you can pass in schema to create one."}
        self.collection_wrap.init_collection(c_name, schema=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_with_unknown_type(self):
        """
        target: test collection with unknown type
        method: create with DataType.UNKNOWN
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 1, ct.err_msg: "Field dtype must be of DataType"}
        self.field_schema_wrap.init_field_schema(name="unknown", dtype=DataType.UNKNOWN,
                                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: f"bad argument type for built-in"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
    @pytest.mark.xfail(reason="issue #19334")
    def test_collection_field_dtype_float_value(self):
        """
        target: test collection with float type
        method: create field with float type
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=5.0,
                                                            is_primary=True)
        schema = cf.gen_collection_schema(fields=[field, cf.gen_float_vec_field()])
        error = {ct.err_code: 0, ct.err_msg: "Field type must be of DataType!"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_multi_float_vectors(self):
        """
        target: test collection with multi float vectors
        method: create collection with two float-vec fields
        expected: raise exception (not supported yet)
        """
        # 1. connect
        self._connect()
        # 2. create collection with multiple vectors
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_float_vec_field(dim=default_dim), cf.gen_float_vec_field(name="tmp", dim=default_dim)]
        schema = cf.gen_collection_schema(fields=fields)
        err_msg = "multiple vector fields is not supported"
        self.collection_wrap.init_collection(c_name, schema=schema,
                                             check_task=CheckTasks.err_res,
                                             check_items={"err_code": 1, "err_msg": err_msg})

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
        err_msg = "multiple vector fields is not supported"
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res,
                                             check_items={"err_code": 1, "err_msg": err_msg})

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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [[], 1, [1, "2", 3], (1,), {1: 1}, None])
    def test_collection_non_string_primary_field(self, primary_field):
        """
        target: test collection with non-string primary_field
        method: primary_field type is not string
        expected: raise exception
        """
        self._connect()
        fields = [cf.gen_int64_field(), cf.gen_float_vec_field()]
        error = {ct.err_code: 1, ct.err_msg: "Param primary_field must be str type."}
        self.collection_schema_wrap.init_collection_schema(fields, primary_field=primary_field,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_unsupported_primary_field(self, get_unsupported_primary_field):
        """
        target: test collection with unsupported primary field type
        method: specify non-int64 as primary field
        expected: raise exception
        """
        self._connect()
        field = get_unsupported_primary_field
        vec_field = cf.gen_float_vec_field(name="vec")
        error = {ct.err_code: 1, ct.err_msg: "Primary key type must be DataType.INT64 or DataType.VARCHAR."}
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
        error = {ct.err_code: 0, ct.err_msg: "Expected only one primary key field"}
        self.collection_schema_wrap.init_collection_schema(
            fields=[int_field_one, int_field_two, cf.gen_float_vec_field()],
            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: "Expected only one primary key field"}
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 24578")
    def test_collection_auto_id_inconsistent(self):
        """
        target: test collection auto_id with both collection schema and field schema
        method: 1.set primary field auto_id=True in field schema 2.set auto_id=False in collection schema
        expected: raise exception
        """
        self._connect()
        int_field = cf.gen_int64_field(is_primary=True, auto_id=True)
        vec_field = cf.gen_float_vec_field(name='vec')

        schema, _ = self.collection_schema_wrap.init_collection_schema([int_field, vec_field], auto_id=False)
        assert schema.auto_id

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 24578")
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
        assert c_name in self.utility_wrap.list_collections()[0]

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_shards_num_with_default_value(self):
        """
        target:test collection with shards_num
        method:create collection with shards_num
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.collection_wrap.init_collection(c_name, schema=default_schema, shards_num=default_shards_num,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_shards_num: default_shards_num})
        assert c_name in self.utility_wrap.list_collections()[0]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shards_num", [-256, 0, ct.max_shards_num // 2, ct.max_shards_num])
    def test_collection_shards_num_with_not_default_value(self, shards_num):
        """
        target:test collection with shards_num
        method:create collection with not default shards_num
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.collection_wrap.init_collection(c_name, schema=default_schema, shards_num=shards_num,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_shards_num: shards_num})
        assert c_name in self.utility_wrap.list_collections()[0]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("shards_num", [ct.max_shards_num + 1, 257])
    def test_collection_shards_num_invalid(self, shards_num):
        """
        target:test collection with invalid shards_num
        method:create collection with shards_num out of [1, 16]
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: f"maximum shards's number should be limited to {ct.max_shards_num}"}
        self.collection_wrap.init_collection(c_name, schema=default_schema, shards_num=shards_num,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("error_type_shards_num", [1.0, "2"])
    def test_collection_shards_num_with_error_type(self, error_type_shards_num):
        """
        target:test collection with error type shards_num
        method:create collection with error type shards_num
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: f"expected one of: int, long"}
        self.collection_wrap.init_collection(c_name, schema=default_schema, shards_num=error_type_shards_num,
                                             check_task=CheckTasks.err_res,
                                             check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_maximum_fields(self):
        """
        target: test create collection with maximum fields
        method: create collection with maximum field number
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        limit_num = ct.max_field_num - 2
        for i in range(limit_num):
            int_field_name = cf.gen_unique_str("field_name")
            field = cf.gen_int64_field(name=int_field_name)
            int_fields.append(field)
        int_fields.append(cf.gen_float_vec_field())
        int_fields.append(cf.gen_int64_field(is_primary=True))
        schema = cf.gen_collection_schema(fields=int_fields)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_over_maximum_fields(self):
        """
        target: Test create collection with more than the maximum fields
        method: create collection with more than the maximum field number
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        limit_num = ct.max_field_num
        for i in range(limit_num):
            int_field_name = cf.gen_unique_str("field_name")
            field = cf.gen_int64_field(name=int_field_name)
            int_fields.append(field)
        int_fields.append(cf.gen_float_vec_field())
        int_fields.append(cf.gen_int64_field(is_primary=True))
        schema = cf.gen_collection_schema(fields=int_fields)
        error = {ct.err_code: 1, ct.err_msg: "maximum field's number should be limited to 64"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)


class TestCollectionOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test collection interface operations
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L1)
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
            assert c_name not in self.utility_wrap.list_collections()[0]

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

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_all_datatype_fields(self):
        """
        target: test create collection with all dataType fields
        method: create collection with all dataType schema
        expected: create successfully
        """
        self._connect()
        fields = []
        for k, v in DataType.__members__.items():
            if v and v != DataType.UNKNOWN and v != DataType.STRING and v != DataType.VARCHAR and v != DataType.FLOAT_VECTOR and v != DataType.BINARY_VECTOR:
                field, _ = self.field_schema_wrap.init_field_schema(name=k.lower(), dtype=v)
                fields.append(field)
        fields.append(cf.gen_float_vec_field())
        schema, _ = self.collection_schema_wrap.init_collection_schema(fields,
                                                                       primary_field=ct.default_int64_field_name)
        c_name = cf.gen_unique_str(prefix)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_partition(self):
        """
        target: test release the partition after load collection
        method: load collection and load the partition
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w1.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        partition_w1.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_partition(self):
        """
        target: test release the partition after load collection
        method: load collection and release the partition
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w1.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        partition_w1.release()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_release_collection(self):
        """
        target: test release the collection after load collection
        method: load collection and release the collection
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.release()


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
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_schema})
        # flush
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
        df = cf.gen_default_dataframe_data()
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

    @pytest.mark.tags(CaseLabel.L2)
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
        collection_w.flush()
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
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, primary_field=ct.default_int64_field_name,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_schema})
        # flush
        assert collection_w.num_entities == ct.default_nb
        assert collection_w.num_entities == self.collection_wrap.num_entities


class TestCollectionCount(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_no_vectors(self):
        """
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
                assert the value returned by num_entities attribute is equal to 0
        expected: the count is equal to 0
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        assert collection_w.num_entities == 0


class TestCollectionCountIP(TestcaseBase):
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_count_after_index_created(self, insert_count):
        """
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling num_entities with correct params
        expected: count_entities raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()

        data = cf.gen_default_list_data(insert_count, ct.default_dim)
        collection_w.insert(data)

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        assert collection_w.num_entities == insert_count


class TestCollectionCountBinary(TestcaseBase):
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

    # TODO: need to update and enable
    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_count_after_index_created_binary(self, insert_count):
        """
        target: test num_entities, after index have been created
        method: add vectors in db, and create binary index, then calling num_entities with correct params
        expected: num_entities equals entities count just inserted
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(insert_count)
        mutation_res, _ = collection_w.insert(data=df)
        collection_w.create_index(ct.default_binary_vec_field_name, default_binary_index_params)
        assert collection_w.num_entities == insert_count

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_binary_collection_with_min_dim(self, auto_id):
        """
        target: test binary collection when dim=1
        method: creat collection and set dim=1
        expected: check error message successfully
        """
        self._connect()
        dim = 1
        c_schema = cf.gen_default_binary_collection_schema(auto_id=auto_id, dim=dim)
        collection_w = self.init_collection_wrap(schema=c_schema,
                                                 check_task=CheckTasks.err_res,
                                                 check_items={"err_code": 1,
                                                              "err_msg": f"invalid dimension: {dim}. should be multiple of 8."})

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_no_entities(self):
        """
        target: test collection num_entities is correct or not, if collection is empty
        method: create collection and no vectors in it,
                assert the value returned by num_entities method is equal to 0
        expected: the count is equal to 0
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        assert collection_w.num_entities == 0


class TestCollectionMultiCollections(TestcaseBase):
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
    def test_collection_count_multi_collections_l2(self, insert_count):
        """
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add entities in it,
                assert the value returned by num_entities is equal to length of entities
        expected: the count is equal to the length of entities
        """
        self._connect()
        data = cf.gen_default_list_data(insert_count)
        collection_list = []
        collection_num = 20
        for i in range(collection_num):
            collection_name = gen_unique_str(uid_count)
            collection_w = self.init_collection_wrap(name=collection_name)
            collection_w.insert(data)
            collection_list.append(collection_name)
        for i in range(collection_num):
            res, _ = self.collection_wrap.init_collection(collection_list[i])
            assert self.collection_wrap.num_entities == insert_count

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_multi_collections_binary(self, insert_count):
        """
        target: test collection rows_count is correct or not with multiple collections of JACCARD
        method: create collection and add entities in it,
                assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        """
        self._connect()
        df, _ = cf.gen_default_binary_dataframe_data(insert_count)
        collection_list = []
        collection_num = 20
        for i in range(collection_num):
            c_name = cf.gen_unique_str(prefix)
            collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
            mutation_res, _ = collection_w.insert(data=df)
            collection_list.append(c_name)
        for i in range(collection_num):
            res, _ = self.collection_wrap.init_collection(collection_list[i])
            assert self.collection_wrap.num_entities == insert_count

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_multi_collections_mix(self):
        """
        target: test collection rows_count is correct or not with multiple collections of
        method: create collection and add entities in it,
                assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        """
        self._connect()
        collection_list = []
        collection_num = 20
        data = cf.gen_default_list_data()
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        for i in range(0, int(collection_num / 2)):
            collection_name = gen_unique_str(uid_count)
            collection_w = self.init_collection_wrap(name=collection_name)
            collection_w.insert(data)
            collection_list.append(collection_name)
        for i in range(int(collection_num / 2), collection_num):
            c_name = cf.gen_unique_str(prefix)
            collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
            mutation_res, _ = collection_w.insert(data=df)
            collection_list.append(c_name)
        for i in range(collection_num):
            res, _ = self.collection_wrap.init_collection(collection_list[i])
            assert self.collection_wrap.num_entities == ct.default_nb


class TestCreateCollection(TestcaseBase):

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_multithread(self):
        """
        target: test create collection with multi-thread
        method: create collection using multi-thread,
        expected: collections are created
        """
        self._connect()
        threads_num = 8
        threads = []
        collection_names = []

        def create():
            collection_name = gen_unique_str(uid_create)
            collection_names.append(collection_name)
            self.init_collection_wrap(name=collection_name)

        for i in range(threads_num):
            t = MyThread(target=create, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

        for item in collection_names:
            assert item in self.utility_wrap.list_collections()[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_using_default_value(self, auto_id):
        """
        target: test create collection with default_value
        method: create a schema with all fields using default value
        expected: collections are created
        """
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_int8_field(default_value=numpy.int8(8)),
            cf.gen_int16_field(default_value=numpy.int16(16)),
            cf.gen_int32_field(default_value=numpy.int32(32)),
            cf.gen_int64_field(default_value=numpy.int64(64)),
            cf.gen_float_field(default_value=numpy.float32(3.14)),
            cf.gen_double_field(default_value=numpy.double(3.1415)),
            cf.gen_bool_field(default_value=False),
            cf.gen_string_field(default_value="abc")
        ]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        self.init_collection_wrap(schema=schema,
                                  check_task=CheckTasks.check_collection_property,
                                  check_items={"schema": schema})


class TestCreateCollectionInvalid(TestcaseBase):
    """
    Test creating collections with invalid params
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_limit_fields(self):
        """
        target: test create collection with maximum fields
        method: create collection with maximum field number
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        limit_num = ct.max_field_num
        field_schema_list = []
        field_pr = cf.gen_int64_field(ct.default_int64_field_name, is_primary=True)
        field_v = cf.gen_float_vec_field(ct.default_float_vec_field_name)
        field_schema_list.append(field_pr)
        field_schema_list.append(field_v)

        for i in range(limit_num):
            field_name_tmp = gen_unique_str("field_name")
            field_schema_temp = cf.gen_int64_field(field_name_tmp)
            field_schema_list.append(field_schema_temp)
        error = {ct.err_code: 1, ct.err_msg: "'maximum field\'s number should be limited to 64'"}
        schema, _ = self.collection_schema_wrap.init_collection_schema(fields=field_schema_list)
        self.init_collection_wrap(name=c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", ["abc"])
    @pytest.mark.skip(reason="issue #24634")
    def test_create_collection_with_invalid_default_value_string(self, default_value):
        """
        target: test create collection with maximum fields
        method: create collection with maximum field number
        expected: raise exception
        """
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_string_field(max_length=2, default_value=default_value)
        ]
        schema = cf.gen_collection_schema(fields)
        self.init_collection_wrap(schema=schema,
                                  check_task=CheckTasks.check_collection_property,
                                  check_items={"schema": schema})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", ["abc", 9.09, 1, False])
    def test_create_collection_with_invalid_default_value_float(self, default_value):
        """
        target: test create collection with maximum fields
        method: create collection with maximum field number
        expected: raise exception
        """
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_float_field(default_value=default_value)
        ]
        schema = cf.gen_collection_schema(fields)
        self.init_collection_wrap(schema=schema, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "default value type mismatches field schema type"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", ["abc", 9.09, 1, False])
    def test_create_collection_with_invalid_default_value_int8(self, default_value):
        """
        target: test create collection with maximum fields
        method: create collection with maximum field number
        expected: raise exception
        """
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_int8_field(default_value=default_value)
        ]
        schema = cf.gen_collection_schema(fields)
        self.init_collection_wrap(schema=schema, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "default value type mismatches field schema type"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_with_pk_field_using_default_value(self):
        """
        target: test create collection with pk field using default value
        method: create a pk field and set default value
        expected: report error
        """
        # 1. pk int64
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True, default_value=np.int64(1)),
            cf.gen_float_vec_field(), cf.gen_string_field(max_length=2)
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        collection_w.insert([[], [vectors[0]], ["a"]],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "pk field schema can not set default value"})
        # 2. pk string
        fields = [
            cf.gen_string_field(name='pk', is_primary=True, default_value="a"),
            cf.gen_float_vec_field(), cf.gen_string_field(max_length=2)
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        collection_w.insert([[], [vectors[0]], ["a"]],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "pk field schema can not set default value"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_with_json_field_using_default_value(self):
        """
        target: test create collection with json field using default value
        method: create a json field and set default value
        expected: report error
        """
        json_default_value = {"number": 1, "float": 2.0, "string": "abc", "bool": True,
                              "list": [i for i in range(5)]}
        cf.gen_json_field(default_value=json_default_value,
                          check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 1,
                                       ct.err_msg: "Default value unsupported data type: 999"})


class TestDropCollection(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `drop_collection` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_collection_A(self):
        """
        target: test delete collection created with correct params
        method: create collection and then delete,
                assert the value returned by delete method
        expected: status ok, and no collection in collections
        """
        self._connect()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.drop()
        assert not self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_without_connection(self):
        """
        target: test describe collection, without connection
        method: drop collection with correct params, with a disconnected instance
        expected: drop raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_wr = self.init_collection_wrap(c_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first'}
        collection_wr.drop(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_collection_not_existed(self):
        """
        target: test if collection not created
        method: random a collection name, which not existed in db,
                assert the exception raised returned by drp_collection method
        expected: False
        """
        self._connect()
        c_name = cf.gen_unique_str()
        self.init_collection_wrap(name=c_name)
        c_name_2 = cf.gen_unique_str()
        # error = {ct.err_code: 0, ct.err_msg: 'DescribeCollection failed: can\'t find collection: %s' % c_name_2}
        # self.utility_wrap.drop_collection(c_name_2, check_task=CheckTasks.err_res, check_items=error)
        # @longjiquan: dropping collection should be idempotent.
        self.utility_wrap.drop_collection(c_name_2)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_drop_collection_multithread(self):
        """
        target: test create and drop collection with multi-thread
        method: create and drop collection using multi-thread,
        expected: collections are created, and dropped
        """
        self._connect()
        threads_num = 8
        threads = []
        collection_names = []

        def create():
            c_name = cf.gen_unique_str()
            collection_names.append(c_name)
            collection_wr = self.init_collection_wrap(name=c_name)
            collection_wr.drop()

        for i in range(threads_num):
            t = MyThread(target=create, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

        for item in collection_names:
            assert not self.utility_wrap.has_collection(item)[0]


class TestDropCollectionInvalid(TestcaseBase):
    """
    Test drop collection with invalid params
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_drop_collection_with_invalid_collection_name(self, name):
        """
        target: test drop invalid collection
        method: drop collection with invalid collection name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 1, ct.err_msg: "Invalid collection name: {}".format(name)}
        self.utility_wrap.drop_collection(name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_with_empty_or_None_collection_name(self):
        """
        target: test drop invalid collection
        method: drop collection with empty or None collection name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: -1, ct.err_msg: '`collection_name` value  is illegal'}
        self.utility_wrap.drop_collection('', check_task=CheckTasks.err_res, check_items=error)
        error_none = {ct.err_code: -1, ct.err_msg: '`collection_name` value None is illegal'}
        self.utility_wrap.drop_collection(None, check_task=CheckTasks.err_res, check_items=error_none)


class TestHasCollection(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `has_collection` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_without_connection(self):
        """
        target: test has collection, without connection
        method: calling has collection with correct params, with a disconnected instance
        expected: has collection raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(c_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first'}
        self.utility_wrap.has_collection(c_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_not_existed(self):
        """
        target: test if collection not created
        method: random a collection name, create this collection then drop it,
                assert the value returned by has_collection method
        expected: False
        """
        self._connect()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.drop()
        assert not self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_multithread(self):
        """
        target: test create collection with multi-thread
        method: create collection using multi-thread,
        expected: collections are created
        """
        self._connect()
        threads_num = 4
        threads = []
        c_name = cf.gen_unique_str()
        self.init_collection_wrap(name=c_name)

        def has():
            assert self.utility_wrap.has_collection(c_name)
            # assert not assert_collection(connect, collection_name)

        for i in range(threads_num):
            t = MyThread(target=has, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()


class TestHasCollectionInvalid(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_has_collection_with_invalid_collection_name(self, name):
        """
        target: test list collections with invalid scenario
        method: show collection with invalid collection name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 1, ct.err_msg: "Invalid collection name: {}".format(name)}
        self.utility_wrap.has_collection(name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_with_empty_collection_name(self):
        """
        target: test list collections with invalid scenario
        method: show collection with empty collection name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: -1, ct.err_msg: '`collection_name` value  is illegal'}
        self.utility_wrap.has_collection('', check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_with_none_collection_name(self):
        """
        target: test list collections with invalid scenario
        method: show collection with no collection name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: -1, ct.err_msg: '`collection_name` value None is illegal'}
        self.utility_wrap.has_collection(None, check_task=CheckTasks.err_res, check_items=error)


class TestListCollections(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `utility.list_collections()` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_collections_multi_collections(self):
        """
        target: test list collections
        method: create collection, assert the value returned by list_collections method
        expected: True
        """
        self._connect()
        collection_num = 50
        collection_names = []
        for i in range(collection_num):
            collection_name = cf.gen_unique_str()
            collection_names.append(collection_name)
            self.init_collection_wrap(name=collection_name)
        for i in range(collection_num):
            assert collection_names[i] in self.utility_wrap.list_collections()[0]
            self.utility_wrap.drop_collection(collection_names[i])

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_without_connection(self):
        """
        target: test list collections, without connection
        method: calling list collections with correct params, with a disconnected instance
        expected: list collections raise exception
        """
        self._connect()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first'}
        self.utility_wrap.list_collections(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_multithread(self):
        """
        target: test list collection with multi-threads
        method: list collection using multi-threads
        expected: list collections correctly
        """
        self._connect()
        threads_num = 10
        threads = []
        collection_name = cf.gen_unique_str()
        self.init_collection_wrap(name=collection_name)

        def _list():
            assert collection_name in self.utility_wrap.list_collections()[0]

        for i in range(threads_num):
            t = MyThread(target=_list)
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()


class TestLoadCollection(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `collection.load()` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_collection_after_index(self):
        """
        target: test load collection, after index created
        method: insert and create index, load collection with correct params
        expected: no error raised
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        data = cf.gen_default_list_data()
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        collection_w.load()
        collection_w.release()

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_after_index_binary(self):
        """
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=df)
        collection_w.create_index(ct.default_binary_vec_field_name, default_binary_index_params)
        collection_w.load()
        collection_w.release()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_empty_collection(self):
        """
        target: test load an empty collection with no data inserted
        method: no entities in collection, load and release the collection
        expected: load and release successfully
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.release()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_dis_connect(self):
        """
        target: test load collection, without connection
        method: load collection with correct params, with a disconnected instance
        expected: load raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_wr = self.init_collection_wrap(c_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first'}
        collection_wr.load(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_collection_dis_connect(self):
        """
        target: test release collection, without connection
        method: release collection with correct params, with a disconnected instance
        expected: release raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_wr = self.init_collection_wrap(c_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first'}
        collection_wr.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_not_existed(self):
        """
        target: test load invalid collection
        method: load not existed collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.drop()
        error = {ct.err_code: 1,
                 ct.err_msg: "DescribeCollection failed: can't find collection: %s" % c_name}
        collection_wr.load(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_collection_not_existed(self):
        """
        target: test release a not existed collection
        method: release with a not existed collection name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.drop()
        error = {ct.err_code: 1,
                 ct.err_msg: "DescribeCollection failed: can't find collection: %s" % c_name}
        collection_wr.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_collection_not_load(self):
        """
        target: test release collection without load
        method: release collection without load
        expected: release successfully
        """
        self._connect()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.release()

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_collection_after_load_release(self):
        """
        target: test load collection after load and release
        method: 1.load and release collection after entities flushed
                2.re-load collection
        expected: No exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        insert_data = cf.gen_default_list_data()
        collection_w.insert(data=insert_data)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.release()
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_repeatedly(self):
        """
        target: test load collection repeatedly
        method: load collection twice
        expected: No exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        insert_data = cf.gen_default_list_data()
        collection_w.insert(data=insert_data)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partitions_after_load_collection(self):
        """
        target: test load partitions after load collection
        method: 1. load collection
                2. load partitions
                3. search on one partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        self.init_partition_wrap(collection_w, partition1)
        self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        collection_w.load(partition_names=[partition1, partition2])
        res = collection_w.search(vectors, default_search_field, default_search_params,
                                  default_limit, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_load_release_collection(self):
        """
        target: test load partitions after load release collection
        method: 1. load collection
                2. release collection
                3. load partitions
                4. search on one partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        self.init_partition_wrap(collection_w, partition1)
        self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        collection_w.release()
        collection_w.load(partition_names=[partition1, partition2])
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_release_collection_partition(self):
        """
        target: test load collection after release collection and partition
        method: 1. load collection
                2. release collection
                3. release one partition
                4. load collection
                5. search on the partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w = self.init_partition_wrap(collection_w, partition1)
        self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        collection_w.release()
        partition_w.release()
        collection_w.load()
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_release_collection_partition(self):
        """
        target: test load partitions after release collection and partition
        method: 1. load collection
                2. release collection
                3. release partition
                4. search on the partition and report error
                5. load partitions
                6. search on the partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        collection_w.release()
        partition_w1.release()
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "not loaded"})
        partition_w1.load()
        partition_w2.load()
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_release_partition(self):
        """
        target: test load collection after load collection and release partition
        method: 1. load collection
                2. release one partition
                3. search on the released partition and report error
                4. search on the non-released partition and raise no exception
                3. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "not loaded"})
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition2])
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_release_partition(self):
        """
        target: test load collection after release partition and load partitions
        method: 1. load collection
                2. release partition
                3. search on the released partition and report error
                4. load partitions
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "not loaded"})
        partition_w1.load()
        partition_w2.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_release_partition_collection(self):
        """
        target: test load collection after release partition and collection
        method: 1. load collection
                2. release partition
                3. query on the released partition and report error
                3. release collection
                4. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w = self.init_partition_wrap(collection_w, partition1)
        self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w.release()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition1],
                           check_task=CheckTasks.err_res, check_items=error)
        collection_w.release()
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_release_partition_collection(self):
        """
        target: test load partitions after release partition and collection
        method: 1. load collection
                2. release partition
                3. release collection
                4. load one partition
                5. query on the other partition and raise error
                6. load the other partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        collection_w.release()
        partition_w1.load()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w2.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_release_partitions(self):
        """
        target: test load collection after release partitions
        method: 1. load collection
                2. release partitions
                3. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        partition_w2.release()
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_release_partitions(self):
        """
        target: test load partitions after release partitions
        method: 1. load collection
                2. release partitions
                3. load partitions
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        partition_w2.release()
        partition_w1.load()
        partition_w2.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_drop_partition_and_release_another(self):
        """
        target: test load collection after drop a partition and release another
        method: 1. load collection
                2. drop a partition
                3. release left partition
                4. query on the left partition
                5. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.release()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_after_drop_partition_and_release_another(self):
        """
        target: test load partition after drop a partition and release another
        method: 1. load collection
                2. drop a partition
                3. release left partition
                4. load partition
                5. query on the partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.release()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_another_partition_after_drop_one_partition(self):
        """
        target: test load another partition after drop a partition
        method: 1. load collection
                2. drop a partition
                3. load another partition
                4. query on the partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_drop_one_partition(self):
        """
        target: test load collection after drop a partition
        method: 1. load collection
                2. drop a partition
                3. load collection
                4. query on the partition
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        collection_w.load()
        partition_w1.release()
        partition_w1.drop()
        collection_w.load()
        collection_w.query(default_term_expr, partition_names=[partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_collection(self):
        """
        target: test load, release non-exist collection
        method: 1. load, release and drop collection
                2. load and release dropped collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_wr.load()
        collection_wr.release()
        collection_wr.drop()
        error = {ct.err_code: 1,
                 ct.err_msg: "DescribeCollection failed: can't find collection: %s" % c_name}
        collection_wr.load(check_task=CheckTasks.err_res, check_items=error)
        collection_wr.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_collection_after_drop(self):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_wr.load()
        collection_wr.drop()
        error = {ct.err_code: 0,
                 ct.err_msg: "can't find collection"}
        collection_wr.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_names_empty(self):
        """
        target: test query another partition
        method: 1. insert entities into two partitions
                2.query on one partition and query result empty
        expected: query result is empty
        """
        self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        # insert [0, half) into partition_w
        half = ct.default_nb // 2
        df_partition = cf.gen_default_dataframe_data(nb=half)
        partition_w.insert(df_partition)
        # insert [half, nb) into _default
        df_default = cf.gen_default_dataframe_data(nb=half, start=half)
        collection_w.insert(df_default)
        # flush
        collection_w.num_entities
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        # load
        error = {ct.err_code: 0, ct.err_msg: "due to no partition specified"}
        collection_w.load(partition_names=[], check_task=CheckTasks.err_res, check_items=error)

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_non_number_replicas(self, request):
        if request.param == 1:
            pytest.skip("1 is valid replica number")
        if request.param is None:
            pytest.skip("None is valid replica number")
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue #21618")
    def test_load_replica_non_number(self, get_non_number_replicas):
        """
        target: test load collection with non-number replicas
        method: load with non-number replicas
        expected: raise exceptions
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        # load with non-number replicas
        error = {ct.err_code: 0, ct.err_msg: f"but expected one of: int, long"}
        collection_w.load(replica_number=get_non_number_replicas, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("replicas", [-1, 0])
    def test_load_replica_invalid_number(self, replicas):
        """
        target: test load partition with invalid replica number
        method: load with invalid replica number
        expected: load successfully as replica = 1
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        collection_w.load(replica_number=replicas)
        replicas = collection_w.get_replicas()[0]
        groups = replicas.groups
        assert len(groups) == 1
        assert len(groups[0].shards) == 1

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("replicas", [None])
    def test_load_replica_number_none(self, replicas):
        """
        target: test load partition with replica number none
        method: load with replica number=None
        expected: raise exceptions
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        collection_w.load(replica_number=replicas,
                          check_task=CheckTasks.err_res,
                          check_items={"err_code": 1,
                                       "err_msg": "`replica_number` value None is illegal"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_replica_greater_than_querynodes(self):
        """
        target: test load with replicas that greater than querynodes
        method: load with 3 replicas (2 querynode)
        expected: Raise exception
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        error = {ct.err_code: 1, ct.err_msg: f"no enough nodes to create replicas"}
        collection_w.load(replica_number=3, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_change(self):
        """
        target: test load replica change
        method: 1.load with replica 1
                2.load with a new replica number
                3.release collection
                4.load with a new replica
                5.create index is a must because get_query_segment_info could
                  only return indexed and loaded segment
        expected: The second time successfully loaded with a new replica number
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load(replica_number=1)
        for seg in self.utility_wrap.get_query_segment_info(collection_w.name)[0]:
            assert len(seg.nodeIds) == 1

        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]")
        loading_progress, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert loading_progress == {'loading_progress': '100%'}

        # verify load different replicas thrown an exception
        error = {ct.err_code: 5, ct.err_msg: f"Should release first then reload with the new number of replicas"}
        collection_w.load(replica_number=2, check_task=CheckTasks.err_res, check_items=error)
        one_replica, _ = collection_w.get_replicas()
        assert len(one_replica.groups) == 1

        collection_w.release()
        collection_w.load(replica_number=2)
        # replicas is not yet reflected in loading progress
        loading_progress, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert loading_progress == {'loading_progress': '100%'}
        two_replicas, _ = collection_w.get_replicas()
        assert len(two_replicas.groups) == 2
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]", check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}]})

        # verify loaded segments included 2 replicas and twice num entities
        seg_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
        num_entities = 0
        for seg in seg_info:
            assert len(seg.nodeIds) == 2
            num_entities += seg.num_rows
        assert num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_multi(self):
        """
        target: test load with multiple replicas
        method: 1.create collection with one shards
                2.insert multiple segments
                3.load with multiple replicas
                4.query and search
        expected: Query and search successfully
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        tmp_nb = 1000
        replica_number = 2
        for i in range(replica_number):
            df = cf.gen_default_dataframe_data(nb=tmp_nb, start=i * tmp_nb)
            insert_res, _ = collection_w.insert(df)
            assert collection_w.num_entities == (i + 1) * tmp_nb

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(replica_number=replica_number)
        replicas = collection_w.get_replicas()[0]
        assert len(replicas.groups) == replica_number

        for seg in self.utility_wrap.get_query_segment_info(collection_w.name)[0]:
            assert len(seg.nodeIds) == replica_number

        query_res, _ = collection_w.query(expr=f"{ct.default_int64_field_name} in [0, {tmp_nb}]")
        assert len(query_res) == 2
        search_res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(search_res[0]) == ct.default_limit

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_partitions(self):
        """
        target: test load replica with partitions
        method: 1.Create collection and one partition
                2.Insert data into collection and partition
                3.Load multi replicas with partition
                4.Query
        expected: Verify query result
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df_1 = cf.gen_default_dataframe_data(nb=default_nb)
        df_2 = cf.gen_default_dataframe_data(nb=default_nb, start=default_nb)

        collection_w.insert(df_1)
        partition_w = self.init_partition_wrap(collection_w, ct.default_tag)
        partition_w.insert(df_2)
        assert collection_w.num_entities == ct.default_nb * 2
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        collection_w.load([partition_w.name], replica_number=2)
        for seg in self.utility_wrap.get_query_segment_info(collection_w.name)[0]:
            assert len(seg.nodeIds) == 2
        # default tag query 0 empty
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]", partition_names=[ct.default_tag],
                           check_tasks=CheckTasks.check_query_empty)
        # default query 0 empty
        collection_w.query(expr=f"{ct.default_int64_field_name} in [3000]",
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': df_2.iloc[:1, :1].to_dict('records')})

        error = {ct.err_code: 1, ct.err_msg: f"not loaded into memory"}
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]",
                           partition_names=[ct.default_partition_name, ct.default_tag],
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_replica_non_shard_leader(self):
        """
        target: test replica groups which one of QN is not shard leader
        method: 1.deploy cluster with 5 QNs
                2.create collection with 2 shards
                3.insert and flush
                4.load with 2 replica number
                5.insert growing data
                6.search and query
        expected: Verify search and query results
        """
        # create and insert entities
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=2)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        # load with multi replica and insert growing data
        collection_w.load(replica_number=2)
        df_growing = cf.gen_default_dataframe_data(100, start=ct.default_nb)
        collection_w.insert(df_growing)

        replicas = collection_w.get_replicas()[0]
        # verify there are 2 groups (2 replicas)
        assert len(replicas.groups) == 2
        log.debug(replicas)
        all_group_nodes = []
        for group in replicas.groups:
            # verify each group have 3 shards
            assert len(group.shards) == 2
            all_group_nodes.extend(group.group_nodes)
        # verify all groups has 5 querynodes
        assert len(all_group_nodes) == 5

        # Verify 2 replicas segments loaded
        seg_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        for seg in seg_info:
            assert len(seg.nodeIds) == 2

        # verify search successfully
        res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(res[0]) == ct.default_limit

        # verify query sealed and growing data successfully
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0, {ct.default_nb}]",
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}, {'int64': 3000}]})

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_replica_multiple_shard_leader(self):
        """
        target: test replica groups which one of QN is shard leader of multiple shards
        method: 1.deploy cluster with 5 QNs
                2.create collection with 3 shards
                3.insert and flush
                4.load with 2 replica number
                5.insert growng data
                6.search and query
        expected: Verify search and query results
        """
        # craete and insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=3)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        # load with multi replicas and insert growing data
        collection_w.load(replica_number=2)
        df_growing = cf.gen_default_dataframe_data(100, start=ct.default_nb)
        collection_w.insert(df_growing)

        # verify replica infos
        replicas, _ = collection_w.get_replicas()
        log.debug(replicas)
        assert len(replicas.groups) == 2
        all_group_nodes = []
        for group in replicas.groups:
            # verify each group have 3 shards
            assert len(group.shards) == 3
            all_group_nodes.extend(group.group_nodes)
        # verify all groups has 5 querynodes
        assert len(all_group_nodes) == 5

        # Verify 2 replicas segments loaded
        seg_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        for seg in seg_info:
            assert len(seg.nodeIds) == 2

        # Verify search successfully
        res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(res[0]) == ct.default_limit

        # Verify query sealed and growing entities successfully
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0, {ct.default_nb}]",
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}, {'int64': 3000}]})

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_replica_sq_count_balance(self):
        """
        target: test load with multi replicas, and sq request load balance cross replicas
        method: 1.Deploy milvus with multi querynodes
                2.Insert entities and load with replicas
                3.Do query req many times
                4.Verify the querynode sq_req_count metrics
        expected: Infer whether the query request is load balanced.
        """
        from utils.util_k8s import get_metrics_querynode_sq_req_count
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=5000)
        mutation_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == 5000
        total_sq_count = 20
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        collection_w.load(replica_number=3)
        for i in range(total_sq_count):
            ids = [random.randint(0, 100) for _ in range(5)]
            collection_w.query(f"{ct.default_int64_field_name} in {ids}")

        replicas, _ = collection_w.get_replicas()
        log.debug(replicas)
        sq_req_count = get_metrics_querynode_sq_req_count()
        for group in replicas.groups:
            group_nodes = group.group_nodes
            group_sq_req_count = 0
            for node in group_nodes:
                group_sq_req_count += sq_req_count[node]
            log.debug(f"Group nodes {group_nodes} with total sq_req_count {group_sq_req_count}")

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_replicas_not_loaded(self):
        """
        target: test get replicas of not loaded collection
        method: not loaded collection and get replicas
        expected: raise an exception
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

        collection_w.get_replicas(check_task=CheckTasks.err_res,
                                  check_items={"err_code": 15,
                                               "err_msg": "collection not found, maybe not loaded"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_count_multi_replicas(self):
        """
        target: test count multi replicas
        method: 1. load data with multi replicas
                2. count
        expected: verify count
        """
        # create -> insert -> flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        collection_w.flush()

        # index -> load replicas
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(replica_number=2)

        # count
        collection_w.query(expr=f'{ct.default_int64_field_name} >= 0', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{"count(*)": ct.default_nb}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_without_creating_index(self):
        """
        target: test drop index after load without release
        method: create a collection without index, then load
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.load(check_task=CheckTasks.err_res,
                          check_items={"err_code": 1,
                                       "err_msg": "index not exist"})


class TestDescribeCollection(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `collection.describe` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 24493")
    def test_collection_describe(self):
        """
        target: test describe collection
        method: create a collection and check its information when describe
        expected: return correct information
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        description = {'collection_name': c_name, 'auto_id': False, 'num_shards': ct.default_shards_num, 'description': '',
                       'fields': [{'field_id': 100, 'name': 'int64', 'description': '', 'type': 5,
                                   'params': {}, 'is_primary': True, 'auto_id': False,
                                   'is_partition_key': False, 'default_value': None, 'is_dynamic': False},
                                  {'field_id': 101, 'name': 'float', 'description': '', 'type': 10,
                                   'params': {}, 'is_primary': False, 'auto_id': False,
                                   'is_partition_key': False, 'default_value': None, 'is_dynamic': False},
                                  {'field_id': 102, 'name': 'varchar', 'description': '', 'type': 21,
                                   'params': {'max_length': 65535}, 'is_primary': False, 'auto_id': False,
                                   'is_partition_key': False, 'default_value': None, 'is_dynamic': False},
                                  {'field_id': 103, 'name': 'json_field', 'description': '', 'type': 23,
                                   'params': {}, 'is_primary': False, 'auto_id': False,
                                   'is_partition_key': False, 'default_value': None, 'is_dynamic': False},
                                  {'field_id': 104, 'name': 'float_vector', 'description': '', 'type': 101,
                                   'params': {'dim': 128}, 'is_primary': False, 'auto_id': False,
                                   'is_partition_key': False, 'default_value': None, 'is_dynamic': False}],
                       'aliases': [], 'consistency_level': 2, 'properties': [], 'num_partitions': 0,
                       'enable_dynamic_field': False}
        res = collection_w.describe()[0]
        del res['collection_id']
        log.info(res)
        assert description['fields'] == res['fields'], description['aliases'] == res['aliases']
        del description['fields'], res['fields'], description['aliases'], res['aliases']
        del description['properties'], res['properties']
        assert description == res


class TestReleaseAdvanced(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_release_collection_during_searching(self):
        """
        target: test release collection during searching
        method: insert entities into collection, flush and load collection, release collection during searching
        expected: raise exception
        """
        self._connect()
        data = cf.gen_default_list_data()
        c_name = cf.gen_unique_str()
        collection_wr = self.init_collection_wrap(name=c_name)
        collection_wr.insert(data=data)
        assert collection_wr.num_entities == ct.default_nb
        collection_wr.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_wr.load()
        search_res, _ = collection_wr.search(vectors, default_search_field, default_search_params,
                                             default_limit, _async=True)
        collection_wr.release()
        error = {ct.err_code: 1, ct.err_msg: 'collection %s was not loaded into memory' % c_name}
        collection_wr.search(vectors, default_search_field, default_search_params, default_limit,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partition_during_searching(self):
        """
        target: test release partition during searching
        method: insert entities into partition, flush and load partition, release partition during searching
        expected: raise exception
        """
        self._connect()
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 10, partition_num, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        par = collection_w.partitions
        par_name = par[partition_num].name
        par[partition_num].load()
        limit = 10
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name])
        par[partition_num].release()
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "partition has been released"})

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_indexed_collection_during_searching(self):
        """
        target: test release indexed collection during searching
        method: insert entities into partition, flush and load partition, release collection during searching
        expected: raise exception
        """
        self._connect()
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 10, partition_num, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        par = collection_w.partitions
        par_name = par[partition_num].name
        par[partition_num].load()
        limit = 10
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name], _async=True)
        collection_w.release()
        error = {ct.err_code: 1, ct.err_msg: 'collection %s was not loaded into memory' % collection_w.name}
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name],
                            check_task=CheckTasks.err_res,
                            check_items=error)


class TestLoadPartition(TestcaseBase):
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
    def get_binary_index(self, request):
        log.info(request.param)
        if request.param["index_type"] in ct.binary_support:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize('binary_index', gen_binary_index())
    @pytest.mark.parametrize('metric_type', ct.binary_metrics)
    def test_load_partition_after_index_binary(self, binary_index, metric_type):
        """
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        """
        self._connect()
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, ct.default_nb, partition_num,
                                                    is_binary=True, is_index=False)[0]

        # for metric_type in ct.binary_metrics:
        binary_index["metric_type"] = metric_type
        if binary_index["index_type"] == "BIN_IVF_FLAT" and metric_type in ct.structure_metrics:
            error = {ct.err_code: 1, ct.err_msg: 'Invalid metric_type: SUBSTRUCTURE, '
                                                 'which does not match the index type: BIN_IVF_FLAT'}
            collection_w.create_index(ct.default_binary_vec_field_name, binary_index,
                                      check_task=CheckTasks.err_res, check_items=error)
            collection_w.create_index(ct.default_binary_vec_field_name, ct.default_bin_flat_index)
        else:
            collection_w.create_index(ct.default_binary_vec_field_name, binary_index)
        par = collection_w.partitions
        par[partition_num].load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_dis_connect(self):
        """
        target: test load partition, without connection
        method: load partition with correct params, with a disconnected instance
        expected: load raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first.'}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partition_dis_connect(self):
        """
        target: test release collection, without connection
        method: release collection with correct params, with a disconnected instance
        expected: release raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first.'}
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_not_existed(self):
        """
        target: test load partition for invalid scenario
        method: load not existed partition
        expected: raise exception and report the error
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.drop()
        error = {ct.err_code: 1, ct.err_msg: 'partitionID of partitionName:%s can not be find' % partition_name}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_partition_not_load(self):
        """
        target: test release partition without load
        method: release partition without load
        expected: release success
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        partition_w.release()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_after_drop(self):
        """
        target: test load and release partition after drop
        method: drop partition and then load and release it
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        partition_w.drop()
        error = {ct.err_code: 1, ct.err_msg: 'Partition %s not exist.' % partition_name}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_partition_after_drop(self):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        partition_w.drop()
        error = {ct.err_code: 1, ct.err_msg: 'partitionID of partitionName:%s can not be find' % partition_name}
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_release_after_collection_drop(self):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        name = collection_w.name
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        collection_w.drop()
        error = {ct.err_code: 0, ct.err_msg: "can\'t find collection"}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_loaded_partition(self):
        """
        target: test load partition after load partition
        method: 1. load partition
                2. load the partition again
                3. query on the non-loaded partition
                4. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.load()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_unloaded_partition(self):
        """
        target: test load partition after load an unloaded partition
        method: 1. load partition
                2. load another partition
                3. query on the collection
                4. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr)
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_one_partition(self):
        """
        target: test load partition after load partition
        method: 1. load partition
                2. load collection
                3. query on the partitions
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        collection_w.load()
        collection_w.query(default_term_expr, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_partitions_release_collection(self):
        """
        target: test release collection after load partitions
        method: 1. load partition
                2. release collection
                3. query on the partition
                4. load partitions
                5. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        collection_w.release()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition1],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_collection(self):
        """
        target: test load collection after load partitions
        method: 1. load partition
                2. release collection
                3. load collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        collection_w.release()
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_load_release_partition(self):
        """
        target: test load partitions after load and release partition
        method: 1. load partition
                2. release partition
                3. query on the partition
                4. load partitions(include released partition and non-released partition)
                5. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition1],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_release_partition(self):
        """
        target: test load collection after load and release partition
        method: 1. load partition
                2. release partition
                3. load collection
                4. search on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        collection_w.load()
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_load_partition_release_partitions(self):
        """
        target: test load partitions after load partition and release partitions
        method: 1. load partition
                2. release partitions
                3. load partitions
                4. query on the partitions
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w2.release()
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_partition_release_partitions(self):
        """
        target: test load collection after load partition and release partitions
        method: 1. load partition
                2. release partitions
                3. query on the partitions
                4. load collection
                5. query on the partitions
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w2.release()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition1, partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        collection_w.load()
        collection_w.query(default_term_expr, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_after_load_drop_partition(self):
        """
        target: test load partition after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. load the left partition
                4. query on the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_drop_partition(self):
        """
        target: test load collection after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. query on the partition
                4. drop another partition
                5. load collection
                6. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        error = {ct.err_code: 1, ct.err_msg: 'name not found'}
        collection_w.query(default_term_expr, partition_names=[partition1, partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w2.drop()
        collection_w.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_partition_after_load_drop_partition(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. release another partition
                4. load the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.release()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_collection_after_load_drop_partition(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. release another partition
                4. load collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.release()
        collection_w.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_another_partition_after_load_drop_partition(self):
        """
        target: test load another collection after load and drop one partition
        method: 1. load partition
                2. drop the unloaded partition
                3. load the partition again
                4. query on the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.drop()
        partition_w1.load()
        collection_w.query(default_term_expr, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_partition_after_load_partition_drop_another(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the unloaded partition
                3. release the loaded partition
                4. query on the released partition
                5. reload the partition
                6. query on the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.drop()
        partition_w1.release()
        error = {ct.err_code: 1, ct.err_msg: 'not loaded into memory'}
        collection_w.query(default_term_expr, partition_names=[partition1],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w1.load()
        collection_w.query(default_term_expr, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_collection_after_load_partition_drop_another(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the unloaded partition
                3. release the loaded partition
                4. load collection
                5. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.drop()
        partition_w1.release()
        collection_w.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_unloaded_partition(self):
        """
        target: test load collection after load and drop partition
        method: 1. load partition
                2. release the other partition
                3. query on the first partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.release()
        collection_w.query(default_term_expr, partition_names=[partition1])


class TestCollectionString(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test about string 
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_string_field_is_primary(self):
        """
        target: test create collection with string field
        method: 1. create collection with string field and vector field
                2. set string fields is_primary=True
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_string_pk_default_collection_schema()
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_with_muti_string_fields(self):
        """
        target: test create collection with muti string fields
        method: 1. create collection with primary string field and not primary string field
                2. string fields is_primary=True
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field()
        vec_field = cf.gen_float_vec_field()
        string_field_1 = cf.gen_string_field(is_primary=True)
        string_field_2 = cf.gen_string_field(name=c_name)
        schema = cf.gen_collection_schema(fields=[int_field, string_field_1, string_field_2, vec_field])
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_only_string_field(self):
        """
        target: test create collection with one string field
        method: create collection with only string field
        expected: Raise exception
        """
        self._connect()
        string_field = cf.gen_string_field(is_primary=True)
        schema = cf.gen_collection_schema([string_field])
        error = {ct.err_code: 0, ct.err_msg: "No vector field is found"}
        self.collection_wrap.init_collection(name=cf.gen_unique_str(prefix), schema=schema,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_string_field_with_exceed_max_len(self):
        """
        target: test create collection with string field
        method: 1. create collection with string field
                2. String field max_length exceeds maximum
        expected: Raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        max_length = 100000
        string_field = cf.gen_string_field(max_length=max_length)
        schema = cf.gen_collection_schema([int_field, string_field, vec_field])
        error = {ct.err_code: 1, ct.err_msg: "invalid max_length: %s" % max_length}
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_invalid_string_field_dtype(self):
        """
        target: test create collection with string field
        method: create collection with string field, the string field datatype is invaild
        expected: Raise exception
        """
        self._connect()
        string_field = self.field_schema_wrap.init_field_schema(name="string", dtype=DataType.STRING)[0]
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[int_field, string_field, vec_field])
        error = {ct.err_code: 0, ct.err_msg: "string data type not supported yet, please use VarChar type instead"}
        self.collection_wrap.init_collection(name=cf.gen_unique_str(prefix), schema=schema,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_string_field_is_primary_and_auto_id(self):
        """
        target: test create collection with string field 
        method: create collection with string field, the string field primary and auto id are true
        expected: Create collection successfully
        """
        self._connect()
        int_field = cf.gen_int64_field()
        vec_field = cf.gen_float_vec_field()
        string_field = cf.gen_string_field(is_primary=True, auto_id=True)
        fields = [int_field, string_field, vec_field]
        error = {ct.err_code: 0, ct.err_msg: "The auto_id can only be specified on field with DataType.INT64"}
        self.collection_schema_wrap.init_collection_schema(fields=fields,
                                                           check_task=CheckTasks.err_res, check_items=error)
        
class TestCollectionJSON(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test about string 
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_collection_json_field_as_primary_key(self, auto_id):
        """
        target: test create collection with JSON field as primary key
        method: 1. create collection with one JSON field, and vector field
                2. set json field is_primary=true
                3. set auto_id as true
        expected: Raise exception (not supported)
        """
        self._connect()
        int_field = cf.gen_int64_field()
        vec_field = cf.gen_float_vec_field()
        string_field = cf.gen_string_field()
        # 1. create json field as primary key through field schema api
        error = {ct.err_code: 1, ct.err_msg: "Primary key type must be DataType.INT64 or DataType.VARCHAR"}
        json_field = cf.gen_json_field(is_primary=True, auto_id=auto_id)
        fields = [int_field, string_field, json_field, vec_field]
        self.collection_schema_wrap.init_collection_schema(fields=fields,
                                                           check_task=CheckTasks.err_res, check_items=error)
        # 2. create json field as primary key through collection schema api
        json_field = cf.gen_json_field()
        fields = [int_field, string_field, json_field, vec_field]
        self.collection_schema_wrap.init_collection_schema(fields=fields, primary_field=ct.default_json_field_name,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_float_field_name, ct.default_json_field_name])
    def test_collection_json_field_partition_key(self, primary_field):
        """
        target: test create collection with multiple JSON fields
        method: 1. create collection with multiple JSON fields, primary key field and vector field
                2. set json field is_primary=false
        expected: Raise exception
        """
        self._connect()
        cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: "Partition key field type must be DataType.INT64 or DataType.VARCHAR."}
        cf.gen_json_default_collection_schema(primary_field=primary_field, is_partition_key=True,
                                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_collection_json_field_supported_primary_key(self, primary_field):
        """
        target: test create collection with one JSON field
        method: 1. create collection with one JSON field, primary key field and vector field
                2. set json field is_primary=false
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_json_default_collection_schema(primary_field=primary_field)
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})
        
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_collection_multiple_json_fields_supported_primary_key(self, primary_field):
        """
        target: test create collection with multiple JSON fields
        method: 1. create collection with multiple JSON fields, primary key field and vector field
                2. set json field is_primary=false
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_multiple_json_default_collection_schema(primary_field=primary_field)
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})


