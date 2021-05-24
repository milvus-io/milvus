import pytest
from milvus import DataType
from pymilvus_orm import FieldSchema

from base.client_request import ApiReq
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel

prefix = "collection"
default_schema = cf.gen_default_collection_schema()


def assert_default_collection(collection, exp_name=None, exp_schema=default_schema, exp_num=0, exp_primary=None):
    if exp_name:
        assert collection.name == exp_name
    assert collection.description == exp_schema.description
    assert collection.schema == exp_schema
    if exp_num == 0:
        assert collection.is_empty
    assert collection.num_entities == exp_num
    if exp_primary is None:
        assert collection.primary_field is None
    else:
        assert collection.primary_field == exp_primary


class TestCollectionParams(ApiReq):
    """ Test case of collection interface """

    def teardown_method(self):
        if self.collection is not None and self.collection.collection is not None:
            self.collection.drop()

    def setup_method(self):
        pass

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_string(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_type_schema(self, request):
        if request.param is None:
            pytest.skip("None schema is valid")
        yield request.param

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_type_fields(self, request):
        skip_param = []
        if request.param == skip_param:
            pytest.skip("skip []")
        yield request.param

    @pytest.fixture(
        scope="function",
        params=cf.gen_invalid_field_types()
    )
    def get_invalid_field_type(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=cf.gen_all_type_fields()
    )
    def get_unsupported_primary_field(self, request):
        if request.param.dtype == DataType.INT64:
            pytest.skip("int64 type is valid primary key")
        yield request.param

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_dim(self, request):
        if request.param == 1:
            request.param = 0
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5224")
    def test_collection(self):
        """
        target: test collection with default schema
        method: create collection with default schema
        expected: assert collection property
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection, _ = self.collection.collection_init(c_name, data=None, schema=default_schema)
        assert_default_collection(collection, c_name)
        assert c_name in self.utility.list_collections()

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_empty_name(self):
        """
        target: test collection with empty name
        method: create collection with a empty name
        expected: raise exception
        """
        self._connect()
        c_name = ""
        ex, check = self.collection.collection_init(c_name, schema=default_schema)
        assert "value  is illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_invalid_name(self, get_invalid_string):
        """
        target: test collection with invalid name
        method: create collection with invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_string
        ex, check = self.collection.collection_init(c_name, schema=default_schema)
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5241 #5367")
    def test_collection_dup_name(self):
        """
        target: test collection with dup name
        method: create collection with dup name and none schema and data
        expected: collection properties consistent
        """
        self._connect()
        collection = self._collection()
        c_name = collection.name
        assert_default_collection(collection)
        dup_collection, _ = self.collection.collection_init(c_name)
        assert collection.name == dup_collection.name
        assert collection.schema == dup_collection.schema
        assert collection.num_entities == dup_collection.num_entities
        assert id(collection) == id(dup_collection)
        assert collection.name in self.utility.list_collections()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5231")
    def test_collection_dup_name_with_desc(self):
        """
        target: test collection with dup name
        method: 1. default schema with desc 2. dup name collection
        expected: desc consistent
        """
        self._connect()
        schema = cf.gen_default_collection_schema(description=ct.collection_desc)
        collection = self._collection(schema=schema)
        assert_default_collection(collection, exp_schema=schema)
        c_name = collection.name
        dup_collection, _ = self.collection.collection_init(c_name)
        assert_default_collection(dup_collection, c_name, exp_schema=schema)
        assert collection.description == dup_collection.description

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_new_schema(self):
        """
        target: test collection with dup name and new schema
        method: 1.create collection with default schema
                2. collection with dup name and new schema
        expected: raise exception
        """
        self._connect()
        collection = self._collection()
        c_name = collection.name
        assert_default_collection(collection)
        fields = [cf.gen_int64_field()]
        schema = cf.gen_collection_schema(fields=fields)
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "The collection already exist, but the schema isnot the same as the passed in" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_new_primary(self):
        """
        target: test collection with dup name and new primary_field schema
        method: 1.collection with default schema
                2. collection with same fields and new primary_field schema
        expected: raise exception
        """
        self._connect()
        collection = self._collection()
        c_name = collection.name
        assert_default_collection(collection)
        schema = cf.gen_default_collection_schema(primary_field=ct.default_int64_field)
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "The collection already exist, but the schema isnot the same as the passed in" in str(ex)
        assert collection.primary_field is None

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_new_dim(self):
        """
        target: test collection with dup name and new dim schema
        method: 1. default schema 2. schema with new dim
        expected: raise exception
        """
        self._connect()
        new_dim = 120
        collection = self._collection()
        c_name = collection.name
        assert_default_collection(collection)
        schema = cf.gen_default_collection_schema()
        new_fields = cf.gen_float_vec_field(dim=new_dim)
        schema.fields[-1] = new_fields
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "The collection already exist, but the schema isnot the same as the passed in" in str(ex)
        assert collection.primary_field is None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5304")
    def test_collection_dup_name_invalid_schema_type(self, get_invalid_type_schema):
        """
        target: test collection with dup name and invalid schema
        method: 1. default schema 2. invalid schema
        expected: raise exception and
        """
        self._connect()
        collection = self._collection()
        c_name = collection.name
        assert_default_collection(collection)
        ex, _ = self.collection.collection_init(c_name, schema=get_invalid_type_schema)
        assert "schema type must be schema.CollectionSchema" in str(ex)
        assert_default_collection(collection, c_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5241 #5367")
    def test_collection_dup_name_same_schema(self):
        """
        target: test collection with dup name and same schema
        method: dup name and same schema
        expected: two collection object is available
        """
        self._connect()
        collection = self._collection(schema=default_schema)
        c_name = collection.name
        assert_default_collection(collection)
        dup_collection, _ = self.collection.collection_init(c_name, schema=default_schema)
        assert_default_collection(dup_collection, c_name)
        assert id(collection) == id(dup_collection)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_collection_dup_name_none_schema_dataframe(self):
        """
        target: test collection with dup name and insert dataframe
        method: create collection with dup name, none schema, dataframe
        expected: two collection object is correct
        """
        self._connect()
        nb = ct.default_nb
        collection = self._collection()
        c_name = collection.name
        assert_default_collection(collection)
        df = cf.gen_default_dataframe_data(nb)
        dup_collection, _ = self.collection.collection_init(c_name, schema=None, data=df)
        assert_default_collection(dup_collection, c_name, exp_num=nb)
        assert collection.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_collection_dup_name_none_schema_data_list(self):
        """
        target: test collection with dup name and insert data (list-like)
        method: create collection with dup name, none schema, data (list-like)
        expected: two collection object is correct
        """
        self._connect()
        nb = ct.default_nb
        collection = self._collection()
        c_name = collection.name
        assert_default_collection(collection)
        data = cf.gen_default_dataframe_data(nb)
        dup_collection, _ = self.collection.collection_init(c_name, schema=None, data=data)
        assert_default_collection(dup_collection, c_name, exp_num=nb)
        assert collection.num_entities == nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_none_schema(self):
        """
        target: test collection with none schema
        method: create collection with none schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        ex, _ = self.collection.collection_init(c_name, schema=None)
        assert "Collection missing schema" in str(ex)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_invalid_type_schema(self, get_invalid_type_schema):
        """
        target: test collection with invalid schema
        method: create collection with non-CollectionSchema type schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        ex, _ = self.collection.collection_init(c_name, schema=get_invalid_type_schema)
        assert "schema type must be schema.CollectionSchema" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5331")
    def test_collection_invalid_type_fields(self, get_invalid_type_fields):
        """
        target: test collection with invalid fields type, non-list
        method: create collection schema with non-list invalid fields
        expected: exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = get_invalid_type_fields
        schema = cf.gen_collection_schema(fields=fields)
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        log.error(str(ex))

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_invalid_field_name(self, get_invalid_string):
        """
        target: test collection with invalid field name
        method: invalid string name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field = FieldSchema(name=get_invalid_string, dtype=5)
        schema = cf.gen_collection_schema(fields=[field])
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        message_one = "but expected one of: bytes, unicode"
        message_two = "You should specify the name of field"
        message_three = "Invalid field name"
        assert message_one or message_two or message_three in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5317")
    def test_collection_invalid_field_type(self, get_invalid_field_type):
        """
        target: test collection with invalid field type
        method: invalid DataType
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field = FieldSchema(name="test", dtype=get_invalid_field_type)
        schema = cf.gen_collection_schema(fields=[field])
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "Field type must be of DataType" in str(ex)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5312")
    def test_collection_empty_fields(self):
        """
        target: test collection with empty fields
        method: create collection with fields = []
        expected: exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_collection_schema(fields=[])
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        # TODO assert

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
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "duplicated field name" in str(ex)
        has, _ = self.utility.has_collection(c_name)
        assert not has

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_only_float_vector(self):
        """
        target: test collection just with float-vec field
        method: create with float-vec fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_collection_schema(fields=[cf.gen_float_vec_field()])
        collection, _ = self.collection.collection_init(c_name, schema=schema)
        assert_default_collection(collection, c_name, exp_schema=schema)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5345")
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
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        log.debug(ex)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5345")
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
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        log.debug(ex)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5285")
    def test_collection_without_vectors(self):
        """
        target: test collection without vectors
        method: create collection only with int field
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_collection_schema([cf.gen_int64_field()])
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "must" in str(ex)

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_primary_field(self):
        """
        target: test collection with primary field
        method: specify primary field
        expected: collection.primary_field
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(primary_field=ct.default_int64_field)
        collection, _ = self.collection.collection_init(c_name, schema=schema)
        assert collection.primary_field.name == ct.default_int64_field

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_unsupported_primary_field(self, get_unsupported_primary_field):
        """
        target: test collection with unsupported parimary field type
        method: specify non-int64 as primary field
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field = get_unsupported_primary_field
        schema = cf.gen_collection_schema(fields=[field], primary_field=field.name)
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "the data type of primary key should be int64" in str(ex)

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
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "there are more than one primary key" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5349")
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
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        log.info(schema)
        log.info(ex.primary_field.name)
        assert "there are more than one primary key" in str(ex)

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
        collection, _ = self.collection.collection_init(c_name, schema=schema)
        assert collection.primary_field is None
        assert collection.schema.auto_id

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5350")
    def test_collection_field_invalid_primary(self, get_invalid_string):
        """
        target: test collection with invalid primary
        method: define field with is_primary=non-bool
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(name="int", is_primary=get_invalid_string)
        float_vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[int_field, float_vec_field])
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        log.info(str(ex))

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_vector_without_dim(self):
        """
        target: test collection without dimension
        method: define vector field without dim
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        float_vec_field = FieldSchema(name=ct.default_float_vec_field_name, dtype=DataType.FLOAT_VECTOR)
        schema = cf.gen_collection_schema(fields=[float_vec_field])
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "dimension is not defined in field type params" in str(ex)

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
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "dim must be of int" in str(ex)

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
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "invalid dimension: {}. should be in range 1 ~ 32768".format(dim) in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_non_vector_field_dim(self):
        """
        target: test collection with dim for non-vector field
        method: define int64 field with dim
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = FieldSchema(name="int", dtype=DataType.INT64, dim=ct.default_dim)
        float_vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[int_field, float_vec_field])
        collection, _ = self.collection.collection_init(c_name, schema=schema)
        assert_default_collection(collection, c_name, exp_schema=schema)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("desc", [None, ct.collection_desc])
    def test_collection_desc(self, desc):
        """
        target: test collection with none description
        method: create with none description
        expected: assert default description
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(description=desc)
        collection, _ = self.collection.collection_init(c_name, schema=schema)
        assert_default_collection(collection, c_name, exp_schema=schema)

    # TODO
    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_long_desc(self):
        """
        target: test collection with long desc
        method: create with long desc
        expected:
        """
        pass

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5302")
    def test_collection_with_dataframe(self):
        """
        target: test collection with dataframe data
        method: create collection and insert with dataframe
        expected: collection num entities equal to nb
        """
        self._connect()
        nb = ct.default_nb
        c_name = cf.gen_unique_str(prefix)
        data = cf.gen_default_dataframe_data(nb)
        collection, _ = self.collection.collection_init(c_name, schema=default_schema, data=data)
        assert_default_collection(collection, c_name, exp_num=nb)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5302")
    def test_collection_with_data_list(self):
        """
        target: test collection with data (list-like)
        method: create collection with data (list-like)
        expected: collection num entities equal to nb
        """
        self._connect()
        nb = ct.default_nb
        c_name = cf.gen_unique_str(prefix)
        data = cf.gen_default_list_data(nb)
        collection, _ = self.collection.collection_init(c_name, schema=default_schema, data=data)
        assert_default_collection(collection, c_name, exp_num=nb)


class TestCollectionOperation(ApiReq):
    """
    ******************************************************************
      The following cases are used to test collection interface operations
    ******************************************************************
    """

    def teardown_method(self):
        if self.collection is not None and self.collection.collection is not None:
            self.collection.drop()

    def setup_method(self):
        pass

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_non_df(self, request):
        if request.param is None:
            pytest.skip("skip None")
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_without_connection(self):
        """
        target: test collection without connection
        method: 1.create collection after connection removed
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.connection.remove_connection(ct.default_alias)
        res_list = self.connection.list_connections()
        assert ct.default_alias not in res_list
        ex, check = self.collection.collection_init(c_name, schema=default_schema)
        assert "There is no connection with alias '{}'".format(ct.default_alias) in str(ex)
        assert self.collection.collection is None

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
            collection, _ = self.collection.collection_init(c_name, schema=default_schema)
            assert_default_collection(collection, c_name)
            collection.drop()
            assert c_name not in self.utility.list_collections()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #xxx")
    def test_collection_dup_name_drop(self):
        """
        target: test collection with dup name, and drop
        method: 1. two dup name collection object
                2. one object drop collection
        expected: collection dropped
        """
        self._connect()
        collection = self._collection()
        assert_default_collection(collection)
        log.info(collection.schema)
        dup_collection, _ = self.collection.collection_init(collection.name)
        assert_default_collection(dup_collection, collection.name)
        dup_collection.drop()
        has, _ = self.utility.has_collection(collection.name)
        assert not has
        with pytest.raises(Exception, match="can't find collection"):
            collection.num_entities

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5302")
    def test_collection_schema_insert_dataframe(self):
        """
        target: test collection create and insert dataframe
        method: 1. create by schema 2. insert dataframe
        expected: assert num_entities
        """
        self._connect()
        nb = ct.default_nb
        collection = self._collection()
        assert_default_collection(collection)
        df = cf.gen_default_dataframe_data(nb)
        self.collection.insert(data=df)
        assert collection.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_collection_created_by_dataframe(self):
        """
        target: test collection with dataframe
        method: create collection with dataframe
        expected: create successfully
        """
        self._connect()
        nb = ct.default_nb
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(nb)
        schema = cf.gen_default_collection_schema()
        collection, _ = self.collection.collection_init(name=c_name, data=df)
        assert_default_collection(collection, exp_name=c_name, exp_num=nb, exp_schema=schema)

    # TODO
    @pytest.mark.tags(CaseLabel.L0)
    def _test_collection_created_by_invalid_dataframe(self, get_invalid_df):
        """
        target: test create collection by invalid dataframe
        method: invalid dataframe type create collection
        expected: raise exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_collection_created_by_non_dataframe(self, get_non_df):
        """
        target: test create collection by invalid dataframe
        method: non-dataframe type create collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        ex, _ = self.collection.collection_init(name=c_name, schema=None, data=get_non_df)
        assert "Data of not pandas.DataFrame type should bepassed into the schema" in str(ex)

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
        ex, _ = self.collection.collection_init(name=c_name, schema=None, data=data)
        assert "Data of not pandas.DataFrame type should bepassed into the schema" in str(ex)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5302")
    def test_collection_schema_insert_data(self):
        """
        target: test collection create and insert list-like data
        method: 1. create by schema 2. insert data
        expected: assert num_entities
        """
        self._connect()
        nb = ct.default_nb
        collection = self._collection()
        assert_default_collection(collection)
        data = cf.gen_default_list_data(nb)
        self.collection.insert(data=data)
        assert collection.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_after_drop(self):
        """
        target: test create collection after create and drop
        method: 1. create a 2. drop a 3, re-create a
        expected: no exception
        """
        collection = self._collection()
        assert_default_collection(collection)
        c_name = collection.name
        collection.drop()
        assert not self.utility.has_collection(c_name)[0]
        re_collection = self._collection(name=c_name)
        assert_default_collection(re_collection, c_name)
        assert self.utility.has_collection(c_name)[0]