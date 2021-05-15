import pytest
from base.client_request import ApiReq
from utils.util_log import test_log as log
from common.common_type import *
from common.common_func import *

default_schema = gen_default_collection_schema()


class TestCollectionParams(ApiReq):
    """ Test case of collection interface """

    def teardown_method(self):
        if self.collection.collection is not None:
            self.collection.drop()

    @pytest.fixture(
        scope="function",
        params=get_invalid_strs
    )
    def get_invalid_string(self, request):
        yield request.param

    # #5224
    @pytest.mark.tags(CaseLabel.L3)
    def test_collection(self):
        """
        target: test collection with default schema
        method: create collection with default schema
        expected: assert collection property
        """
        self._connect()
        c_name = get_unique_str
        collection, _ = self.collection.collection_init(c_name, data=None, schema=default_schema)
        assert collection.name == c_name
        assert collection.description == default_schema.description
        assert collection.schema == default_schema
        assert collection.is_empty
        assert collection.num_entities == 0
        assert collection.primary_field is None
        assert c_name in self.utility.list_collections()

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

    # #5231 TODO
    def test_collection_dup_name(self):
        """
        target: test collection with dup name
        method: create collection with dup name and none schema and data
        expected: collection properties consistent
        """
        self._connect()
        c_name = get_unique_str
        collection, _ = self.collection.collection_init(c_name, data=None, schema=default_schema)
        assert collection.name == c_name
        dup_collection, _ = self.collection.collection_init(c_name)
        assert c_name, c_name in self.utility.list_collections()
        assert collection.name == dup_collection.name
        # log.debug(collection.schema)
        # log.debug(dup_collection.schema)
        # assert collection.schema == dup_collection.schema

    def test_collection_dup_name_new_schema(self):
        """
        target: test collection with dup name and new schema
        method: 1.create collection with default schema 2. collection with dup name and new schema
        expected: raise exception
        """
        self._connect()
        c_name = get_unique_str
        collection, _ = self.collection.collection_init(c_name, data=None, schema=default_schema)
        assert collection.name == c_name
        fields = [gen_int64_field()]
        schema = gen_collection_schema(fields=fields)
        ex, _ = self.collection.collection_init(c_name, schema=schema)
        assert "The collection already exist, but the schema isnot the same as the passed in" in str(ex)


class TestCollectionOperation(ApiReq):
    """
    ******************************************************************
      The following cases are used to test collection interface operations
    ******************************************************************
    """

    def test_collection_without_connection(self):
        """
        target: test collection without connection
        method: 1.create collection after connection removed
        expected: raise exception
        """
        self._connect()
        self.connection.remove_connection(default_alias)
        res_list = self.connection.list_connections()
        assert len(res_list) == 0
        c_name = get_unique_str
        ex, check = self.collection.collection_init(c_name, schema=default_schema)
        assert "no connection" in str(ex)
        assert self.collection is None
