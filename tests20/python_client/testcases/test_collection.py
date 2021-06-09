import pandas as pd
import pytest
from pymilvus import DataType

from base.client_base import TestcaseBase
# from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "collection"
exp_name = "name"
exp_schema = "schema"
exp_num = "num_entities"
exp_primary = "primary"
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()


class TestCollectionParams(TestcaseBase):
    """ Test case of collection interface """

    '''
    def teardown_method(self):
        if self.self.collection_wrap is not None and self.self.collection_wrap.collection is not None:
            self.self.collection_wrap.drop()
    '''

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_type_schema(self, request):
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
            request.param = 0
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
        self.collection_wrap.init_collection(c_name, data=None, schema=default_schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema})
        assert c_name, _ in self.utility_wrap.list_collections()

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_empty_name(self):
        """
        target: test collection with empty name
        method: create collection with a empty name
        expected: raise exception
        """
        self._connect()
        c_name = ""
        error = {ct.err_code: 1, ct.err_msg: "value  is illegal"}
        self.collection_wrap.init_collection(c_name, schema=default_schema, check_task=CheckTasks.err_res,
                                             check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
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
        fields = [cf.gen_int64_field()]
        schema = cf.gen_collection_schema(fields=fields)
        error = {ct.err_code: 1, ct.err_msg: "The collection already exist, but the schema isnot the same as the "
                                             "passed in"}
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
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        schema = cf.gen_default_collection_schema(primary_field=ct.default_int64_field_name)
        error = {ct.err_code: 1, ct.err_msg: "The collection already exist, but the schema isnot the same as the "
                                             "passed in"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)
        assert collection_w.primary_field is None

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
        error = {ct.err_code: 1, ct.err_msg: "The collection already exist, but the schema isnot the same as the "
                                             "passed in"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)
        assert collection_w.primary_field is None

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_invalid_schema_type(self, get_invalid_type_schema):
        """
        target: test collection with dup name and invalid schema
        method: 1. default schema 2. invalid schema
        expected: raise exception and
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        error = {ct.err_code: 1, ct.err_msg: "schema type must be schema.CollectionSchema"}
        schema = get_invalid_type_schema
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_none_schema_dataframe(self):
        """
        target: test collection with dup name and insert dataframe
        method: create collection with dup name, none schema, dataframe
        expected: two collection object is correct
        """
        conn = self._connect()
        nb = ct.default_nb
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        df = cf.gen_default_dataframe_data(nb)
        self.collection_wrap.init_collection(c_name, schema=None, data=df,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema})
        conn.flush([collection_w.name])
        assert collection_w.num_entities == nb
        assert collection_w.num_entities == self.collection_wrap.num_entities

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_none_schema_data_list(self):
        """
        target: test collection with dup name and insert data (list-like)
        method: create collection with dup name, none schema, data (list-like)
        expected: two collection object is correct
        """
        conn = self._connect()
        nb = ct.default_nb
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        data = cf.gen_default_dataframe_data(nb)
        self.collection_wrap.init_collection(c_name, schema=None, data=data,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema})
        conn.flush([collection_w.name])
        assert collection_w.num_entities == nb
        assert collection_w.num_entities == self.collection_wrap.num_entities

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_none_schema(self):
        """
        target: test collection with none schema
        method: create collection with none schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: "Collection missing schema"}
        self.collection_wrap.init_collection(c_name, schema=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_invalid_type_schema(self, get_invalid_type_schema):
        """
        target: test collection with invalid schema
        method: create collection with non-CollectionSchema type schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: "schema type must be schema.CollectionSchema"}
        self.collection_wrap.init_collection(c_name, schema=get_invalid_type_schema,
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
        error = {ct.err_code: 1, ct.err_msg: "The fields of schema must be type list"}
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
        error = {ct.err_code: 0, ct.err_msg: "Field type not support <DataType.UNKNOWN: 999"}
        self.field_schema_wrap.init_field_schema(name="unknown", dtype=DataType.UNKNOWN,
                                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", [[], 1, (1,), {1: 1}, "12-s"])
    def test_collection_invalid_type_field(self, name):
        """
        target: test collection with invalid field name
        method: invalid string name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=name, dtype=5)
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
        field, _ = self.field_schema_wrap.init_field_schema(name=name, dtype=5)
        vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[field, vec_field])
        error = {ct.err_code: 1, ct.err_msg: "Invalid field name"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_none_field_name(self):
        """
        target: test field schema with None name
        method: None field name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=None, dtype=5)
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
        error = {ct.err_code: 0, ct.err_msg: "Field type must be of DataType"}
        self.field_schema_wrap.init_field_schema(name="test", dtype=dtype,
                                                 check_task=CheckTasks.err_res, check_items=error)

    def test_collection_field_float_type(self):
        """
        target: test collection with float type
        method: create field with float type
        expected:
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=5.0)
        schema = cf.gen_collection_schema(fields=[field, cf.gen_float_vec_field()])
        error = {ct.err_code: 0, ct.err_msg: "Field type must be of DataType"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_empty_fields(self):
        """
        target: test collection with empty fields
        method: create collection with fields = []
        expected: exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_collection_schema(fields=[])
        error = {ct.err_code: 0, ct.err_msg: "The field of the schema cannot be empty"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_field(self):
        """
        target: test collection with dup field name
        method: Two FieldSchema have same name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field_one = cf.gen_int64_field()
        field_two = cf.gen_int64_field()
        schema = cf.gen_collection_schema(fields=[field_one, field_two])
        error = {ct.err_code: 0, ct.err_msg: "duplicated field name"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)
        assert not self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("field", [cf.gen_float_vec_field(), cf.gen_binary_vec_field()])
    def test_collection_only_vector(self, field):
        """
        target: test collection just with vec field
        method: create with float-vec fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_collection_schema(fields=[field])
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_multi_float_vectors(self):
        """
        target: test collection with multi float vectors
        method: create collection with two float-vec fields
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_float_vec_field(), cf.gen_float_vec_field(name="tmp")]
        schema = cf.gen_collection_schema(fields=fields)
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
        fields = [cf.gen_float_vec_field(), cf.gen_binary_vec_field()]
        schema = cf.gen_collection_schema(fields=fields)
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
        schema = cf.gen_collection_schema([cf.gen_int64_field()])
        error = {ct.err_code: 0, ct.err_msg: "The schema must have vector column"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_primary_field(self):
        """
        target: test collection with primary field
        method: specify primary field
        expected: collection.primary_field
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(primary_field=ct.default_int64_field_name)
        self.collection_wrap.init_collection(c_name, schema=schema)
        assert self.collection_wrap.primary_field.name == ct.default_int64_field_name

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_unsupported_primary_field(self, get_unsupported_primary_field):
        """
        target: test collection with unsupported primary field type
        method: specify non-int64 as primary field
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field = get_unsupported_primary_field
        vec_field = cf.gen_float_vec_field(name="vec")
        schema = cf.gen_collection_schema(fields=[field, vec_field], primary_field=field.name)
        error = {ct.err_code: 1, ct.err_msg: "the data type of primary key should be int64"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_multi_primary_fields(self):
        """
        target: test collection with multi primary
        method: collection with two primary fields
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(is_primary=True)
        float_vec_field = cf.gen_float_vec_field(is_primary=True)
        schema = cf.gen_collection_schema(fields=[int_field, float_vec_field])
        error = {ct.err_code: 0, ct.err_msg: "there are more than one primary key"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_primary_inconsistent(self):
        """
        target: test collection with different primary field setting
        method: 1. set A field is_primary 2. set primary_field is B
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(name="int", is_primary=True)
        float_vec_field = cf.gen_float_vec_field(name="vec")
        schema = cf.gen_collection_schema(fields=[int_field, float_vec_field], primary_field="vec")
        error = {ct.err_code: 0, ct.err_msg: "there are more than one primary key"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_field_primary_false(self):
        """
        target: test collection with primary false
        method: define field with is_primary false
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(name="int")
        float_vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[int_field, float_vec_field])
        self.collection_wrap.init_collection(c_name, schema=schema)
        assert self.collection_wrap.primary_field is None
        assert self.collection_wrap.schema.auto_id

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("is_primary", ct.get_invalid_strs)
    def test_collection_field_invalid_primary(self, is_primary):
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

    @pytest.mark.tags(CaseLabel.L0)
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
        schema = cf.gen_collection_schema(fields=[float_vec_field])
        error = {ct.err_code: 0, ct.err_msg: "dimension is not defined in field type params"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_vector_invalid_dim(self, get_invalid_dim):
        """
        target: test collection with invalid dimension
        method: define float-vec field with invalid dimension
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        float_vec_field = cf.gen_float_vec_field(dim=get_invalid_dim)
        schema = cf.gen_collection_schema(fields=[float_vec_field])
        error = {ct.err_code: 0, ct.err_msg: "dim must be of int"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [-1, 32769])
    def test_collection_vector_out_bounds_dim(self, dim):
        """
        target: test collection with out of bounds dim
        method: invalid dim -1 and 32759
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        float_vec_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[float_vec_field])
        error = {ct.err_code: 0, ct.err_msg: "invalid dimension: {}. should be in range 1 ~ 32768".format(dim)}
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
        int_field, _ = self.field_schema_wrap.init_field_schema(name="int", dtype=DataType.INT64, dim=ct.default_dim)
        float_vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[int_field, float_vec_field])
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
    def test_collection_none_desc(self):
        """
        target: test collection with none description
        method: create with none description
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(description=None)
        error = {ct.err_code: 0, ct.err_msg: "expected one of: bytes, unicode"}
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
    def test_collection_with_dataframe(self):
        """
        target: test collection with dataframe data
        method: create collection and insert with dataframe
        expected: collection num entities equal to nb
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        data = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.init_collection(c_name, schema=default_schema, data=data,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema})
        conn.flush([c_name])
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_with_data_list(self):
        """
        target: test collection with data (list-like)
        method: create collection with data (list-like)
        expected: collection num entities equal to nb
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        data = cf.gen_default_list_data(ct.default_nb)
        self.collection_wrap.init_collection(c_name, schema=default_schema, data=data,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_schema})
        conn.flush([c_name])
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5667")
    def test_collection_binary(self):
        """
        target: test collection with binary-vec
        method: create collection with binary field
        expected: assert binary field
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.collection_wrap.init_collection(c_name, data=None, schema=default_binary_schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_binary_schema})
        assert c_name, _ in self.utility_wrap.list_collections()

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_binary_with_dataframe(self):
        """
        target: test binary collection with dataframe
        method: create binary collection with dataframe
        expected: collection num entities equal to nb
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        df, _ = cf.gen_default_binary_dataframe_data(nb=ct.default_nb)
        self.collection_wrap.init_collection(c_name, schema=default_binary_schema, data=df,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_binary_schema})
        conn.flush([c_name])
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_binary_with_data_list(self):
        """
        target: test collection with data (list-like)
        method: create binary collection with data (list-like)
        expected: collection num entities equal to nb
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        data, _ = cf.gen_default_binary_list_data(ct.default_nb)
        self.collection_wrap.init_collection(c_name, schema=default_binary_schema, data=data,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: default_binary_schema})
        conn.flush([c_name])
        assert self.collection_wrap.num_entities == ct.default_nb


class TestCollectionOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test collection interface operations
    ******************************************************************
    """

    # def teardown_method(self):
    #     if self.self.collection_wrap is not None and self.self.collection_wrap.collection is not None:
    #         self.self.collection_wrap.drop()

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_non_df(self, request):
        if request.param is None:
            pytest.skip("skip None")
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5671")
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
        error = {ct.err_code: 0, ct.err_msg: "There is no connection with alias '{}'".format(ct.default_alias)}
        self.collection_wrap.init_collection(c_name, schema=default_schema,
                                             check_task=CheckTasks.err_res, check_items=error)
        assert self.collection_wrap.collection is None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5667")
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
        error = {ct.err_code: 0, ct.err_msg: "can't find collection"}
        collection_w.has_partition("p", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_created_by_dataframe(self):
        """
        target: test collection with dataframe
        method: create collection with dataframe
        expected: create successfully
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(nb=ct.default_nb)
        schema = cf.gen_default_collection_schema()
        self.collection_wrap.init_collection(name=c_name, data=df, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})
        conn.flush([c_name])
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_created_by_empty_dataframe(self):
        """
        target: test create collection by empty dataframe
        method: invalid dataframe type create collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        data = pd.DataFrame()
        error = {ct.err_code: 0, ct.err_msg: "The field of the schema cannot be empty"}
        self.collection_wrap.init_collection(name=c_name, schema=None, data=data,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("df", [pd.DataFrame({"date": pd.date_range('20210101', periods=3)}),
                                    pd.DataFrame({'%$#': cf.gen_vectors(3, 2)})])
    def test_collection_created_by_invalid_dataframe(self, df):
        """
        target: test collection with invalid dataframe
        method: create with invalid dataframe
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Invalid field name"}
        self.collection_wrap.init_collection(name=c_name, schema=None, data=df,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_construct_only_column_dataframe(self):
        """
        target: test collection with dataframe only columns
        method: dataframe only has columns
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame(columns=[ct.default_int64_field_name, ct.default_float_vec_field_name])
        error = {ct.err_code: 0, ct.err_msg: "Cannot infer schema from empty dataframe"}
        self.collection_wrap.init_collection(name=c_name, schema=None, data=df,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_construct_no_column_dataframe(self):
        """
        target: test collection with dataframe without columns
        method: dataframe without columns
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame({' ': cf.gen_vectors(3, 2)})
        error = {ct.err_code: 0, ct.err_msg: "Field name should not be empty"}
        self.collection_wrap.init_collection(name=c_name, schema=None, data=df,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_created_by_inconsistent_dataframe(self):
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
        self.collection_wrap.init_collection(name=c_name, schema=None, data=df,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_created_by_non_dataframe(self, get_non_df):
        """
        target: test create collection by invalid dataframe
        method: non-dataframe type create collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Data of not pandas.DataFrame type should bepassed into the schema"}
        self.collection_wrap.init_collection(name=c_name, schema=None, data=get_non_df,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_created_by_data_list(self):
        """
        target: test create collection by data list
        method: data type is list-like
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        data = cf.gen_default_list_data(nb=100)
        error = {ct.err_code: 0, ct.err_msg: "Data of not pandas.DataFrame type should bepassed into the schema"}
        self.collection_wrap.init_collection(name=c_name, schema=None, data=data,
                                             check_task=CheckTasks.err_res, check_items=error)

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
    @pytest.mark.xfail(reason="issue #5675")
    def test_collection_binary_created_by_dataframe(self):
        """
        target: test collection with dataframe
        method: create collection with dataframe
        expected: create successfully
        """
        conn = self._connect()
        c_name = cf.gen_unique_str(prefix)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        schema = cf.gen_default_binary_collection_schema()
        self.collection_wrap.init_collection(name=c_name, data=df, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})
        conn.flush([c_name])

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_binary_created_by_data_list(self):
        """
        target: test create collection by data list
        method: data type is list-like
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        data, _ = cf.gen_default_binary_list_data(nb=100)
        error = {ct.err_code: 0, ct.err_msg: "Data of not pandas.DataFrame type should bepassed into the schema"}
        self.collection_wrap.init_collection(name=c_name, schema=None, data=data,
                                             check_task=CheckTasks.err_res, check_items=error)
