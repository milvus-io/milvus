import pytest
from base.client_request import ApiReq
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel

prefix = "collection"
default_schema = cf.gen_default_collection_schema()


def assert_default_collection(collection, exp_name, exp_schema=default_schema, exp_num=0, exp_primary=None):
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
    def get_invalid_schema_type(self, request):
        if request.param is None:
            pytest.skip("None schema is valid")
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
    @pytest.mark.xfail(reason="issue #5231 #5241")
    def test_collection_dup_name(self):
        """
        target: test collection with dup name
        method: create collection with dup name and none schema and data
        expected: collection properties consistent
        """
        self._connect()
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
        dup_collection, _ = self.collection.collection_init(c_name)
        assert_default_collection(dup_collection, c_name)
        assert collection.name == dup_collection.name
        assert collection.name in self.utility.list_collections()
        assert collection.schema == dup_collection.schema
        assert id(collection) == id(dup_collection)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_dup_name_new_schema(self):
        """
        target: test collection with dup name and new schema
        method: 1.create collection with default schema
                2. collection with dup name and new schema
        expected: raise exception
        """
        self._connect()
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
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
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
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
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
        schema = cf.gen_default_collection_schema()
        new_fields = cf.gen_float_vec_field(dim=new_dim)
        schema.fields[-1] = new_fields
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "The collection already exist, but the schema isnot the same as the passed in" in str(ex)
        assert collection.primary_field is None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5304")
    def test_collection_dup_name_invalid_schema_type(self, get_invalid_schema_type):
        """
        target: test collection with dup name and invalid schema
        method: 1. default schema 2. invalid schema
        expected: raise exception and
        """
        self._connect()
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
        ex, _ = self.collection.collection_init(c_name, schema=get_invalid_schema_type)
        assert "schema type must be schema.CollectionSchema" in str(ex)
        assert_default_collection(collection, c_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5231, #5241")
    def test_collection_dup_name_same_schema(self):
        """
        target: test collection with dup name and same schema
        method: dup name and same schema
        expected: two collection object is available
        """
        self._connect()
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
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
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
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
        c_name, collection = self._collection()
        assert_default_collection(collection, c_name)
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
    def test_collection_invalid_schema_type(self, get_invalid_schema_type):
        """
        target: test collection with invalid schema
        method: create collection with non-CollectionSchema type schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        ex, _ = self.collection.collection_init(c_name, schema=get_invalid_schema_type)
        assert "schema type must be schema.CollectionSchema" in str(ex)

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

    # #5237
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
