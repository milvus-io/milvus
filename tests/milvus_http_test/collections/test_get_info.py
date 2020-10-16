import logging

import pytest
import time
import copy
from utils import *
from constants import *

uid = "info_collection"


class TestInfoBase:
    """
    ******************************************************************
      The following cases are used to test `get_collection_info` function, no data in collection
    ******************************************************************
    """

    def test_get_collection_info(self, client, collection):
        """
        target: test get collection info with normal collection
        method: create collection with default fields and get collection info
        expected: no exception raised, and value returned correct
        """
        info = client.info_collection(collection)
        assert info['count'] == 0
        assert info['auto_id'] == True
        assert info['segment_row_limit'] == default_segment_row_limit
        assert len(info["fields"]) == 3
        for field in info['fields']:
            if field['type'] == 'INT64':
                assert field['name'] == default_int_field_name
            if field['type'] == 'FLOAT':
                assert field['name'] == default_float_field_name
            if field['type'] == 'VECTOR_FLOAT':
                assert field['name'] == default_float_vec_field_name

    def test_get_collection_info_segment_row_limit(self, client, collection):
        """
        target: test get collection info with non-default segment row limit
        method: create collection with non-default segment row limit and get collection info
        expected: no exception raised
        """
        segment_row_limit = 4096
        collection_name = gen_unique_str(uid)
        fields = copy.deepcopy(default_fields)
        fields["segment_row_limit"] = segment_row_limit
        client.create_collection(collection_name, fields)
        client.insert(collection, default_entities)
        client.flush([collection])
        info = client.info_collection(collection_name)
        assert info['segment_row_limit'] == segment_row_limit

    def test_get_collection_info_id_collection(self, client, id_collection):
        """
        target: test get collection info with id collection
        method: create id collection with auto_id=False and get collection info
        expected: no exception raised
        """
        info = client.info_collection(id_collection)
        assert info['count'] == 0
        assert info['auto_id'] == False
        assert info['segment_row_limit'] == default_segment_row_limit
        assert len(info["fields"]) == 3

    def test_get_collection_info_with_collection_not_existed(self, client):
        """
        target: test get collection info with not existed collection
        method: call get collection info with random collection name which not in db
        expected: not ok
        """
        collection_name = gen_unique_str(uid)
        assert not client.info_collection(collection_name)

    @pytest.fixture(
        scope="function",
        params=[
            1,
            "12-s",
            " ",
            "12 s",
            " siede ",
            "(mn)",
            "中文",
            "a".join("a" for i in range(256))
        ]
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    def test_get_collection_info_collection_name_invalid(self, client, get_invalid_collection_name):
        collection_name = get_invalid_collection_name
        assert not client.info_collection(collection_name)

    def test_row_count_after_insert(self, client, collection):
        """
        target: test the change of collection row count after insert data
        method: insert entities to collection and get collection info
        expected: row count increase
        """
        info = client.info_collection(collection)
        assert info['count'] == 0
        assert client.insert(collection, default_entities)
        client.flush([collection])
        info = client.info_collection(collection)
        assert info['count'] == default_nb

    def test_get_collection_info_after_index_created(self, client, collection):
        """
        target: test index of collection info after index created
        method: create index and get collection info
        expected: no exception raised
        """
        index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        res = client.create_index(collection, default_float_vec_field_name, index)
        info = client.info_collection(collection)
        for field in info['fields']:
            if field['name'] == default_float_vec_field_name:
                assert field['index_params'] == index
