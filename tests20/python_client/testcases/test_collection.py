import pytest
from base.client_request import ApiReq
from utils.util_log import test_log as log
from common.common_type import *
from common.common_func import *


class TestCollectionBase(ApiReq):
    """ Test case of collection interface """

    @pytest.mark.tags(CaseLabel.L3)
    def test_collection(self):
        conn = self._connect()
        schema = get_default_collection_schema()
        c_name = get_unique_str
        collection, _ = self.collection.collection_init(c_name, schema=schema, check_res=collection_property_check)
        assert collection.is_empty
        assert collection.num_entities == 0
        assert collection.primary_field is None

    def test_collection_without_connection(self):
        """
        target: test collection without connection
        method: 1.create collection after connection removed
        expected: raise exception
        """
        self.connection.remove_connection(default_alias)
        schema = get_default_collection_schema()
        c_name = get_unique_str
        with pytest.raises(Exception, match=r"* no connection .*") as e:
            collection, check_res = self.collection.collection_init(c_name, schema=schema, check_res='')

    def test_collection_empty_name(self):
        """
        target: test collection with empty name
        method: create collection with a empty name
        expected: raise exception
        """
        pass

    def test_collection_invalid_name(self):
        """
        target: test collection with invalid name
        method: create collection with invalid name
        expected: raise exception
        """