import pytest
import logging
import time
from utils import *
from constants import *

uid = "collection_info"


class TestInfoBase:

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

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        logging.getLogger().info(request.param)
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("sq8h not support in CPU mode")
        return request.param

    """
    ******************************************************************
      The following cases are used to test `get_collection_info` function, no data in collection
    ******************************************************************
    """

    def test_info_collection_fields(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, check info returned
        method: create collection with diff fields: metric/field_type/..., calling `get_collection_info`
        expected: no exception raised, and value returned correct
        '''
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str(uid)
        fields = {
            "fields": [filter_field, vector_field],
            "segment_row_limit": default_segment_row_limit
        }
        connect.create_collection(collection_name, fields)
        res = connect.get_collection_info(collection_name)
        assert res['auto_id'] == True
        assert res['segment_row_limit'] == default_segment_row_limit
        assert len(res["fields"]) == 2
        for field in res["fields"]:
            if field["type"] == filter_field:
                assert field["name"] == filter_field["name"]
            elif field["type"] == vector_field:
                assert field["name"] == vector_field["name"]
                assert field["params"] == vector_field["params"]

    def test_create_collection_segment_row_limit(self, connect, get_segment_row_limit):
        '''
        target: test create normal collection with different fields
        method: create collection with diff segment_row_limit
        expected: no exception raised
        '''
        collection_name = gen_unique_str(uid)
        fields = copy.deepcopy(default_fields)
        fields["segment_row_limit"] = get_segment_row_limit
        connect.create_collection(collection_name, fields)
        # assert segment row count
        res = connect.get_collection_info(collection_name)
        assert res['segment_row_limit'] == get_segment_row_limit

    def test_get_collection_info_after_index_created(self, connect, collection, get_simple_index):
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        res = connect.get_collection_info(collection)
        for field in res["fields"]:
            if field["name"] == default_float_vec_field_name:
                index = field["indexes"][0]
                assert index["index_type"] == get_simple_index["index_type"]
                assert index["metric_type"] == get_simple_index["metric_type"]

    @pytest.mark.level(2)
    def test_get_collection_info_without_connection(self, collection, dis_connect):
        '''
        target: test get collection info, without connection
        method: calling get collection info with correct params, with a disconnected instance
        expected: get collection info raise exception
        '''
        with pytest.raises(Exception) as e:
            assert connect.get_collection_info(dis_connect, collection)

    def test_get_collection_info_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, 
            assert the value returned by get_collection_info method
        expected: False
        '''
        collection_name = gen_unique_str(uid)
        with pytest.raises(Exception) as e:
            res = connect.get_collection_info(connect, collection_name)

    @pytest.mark.level(2)
    def test_get_collection_info_multithread(self, connect):
        '''
        target: test create collection with multithread
        method: create collection using multithread,
        expected: collections are created
        '''
        threads_num = 4
        threads = []
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)

        def get_info():
            connect.get_collection_info(collection_name)

        for i in range(threads_num):
            t = TestThread(target=get_info)
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    """
    ******************************************************************
      The following cases are used to test `get_collection_info` function, and insert data in collection
    ******************************************************************
    """

    def test_info_collection_fields_after_insert(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, check info returned
        method: create collection with diff fields: metric/field_type/..., calling `get_collection_info`
        expected: no exception raised, and value returned correct
        '''
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str(uid)
        fields = {
            "fields": [filter_field, vector_field],
            "segment_row_limit": default_segment_row_limit
        }
        connect.create_collection(collection_name, fields)
        entities = gen_entities_by_fields(fields["fields"], default_nb, vector_field["params"]["dim"])
        res_ids = connect.bulk_insert(collection_name, entities)
        connect.flush([collection_name])
        res = connect.get_collection_info(collection_name)
        assert res['auto_id'] == True
        assert res['segment_row_limit'] == default_segment_row_limit
        assert len(res["fields"]) == 2
        for field in res["fields"]:
            if field["type"] == filter_field:
                assert field["name"] == filter_field["name"]
            elif field["type"] == vector_field:
                assert field["name"] == vector_field["name"]
                assert field["params"] == vector_field["params"]

    def test_create_collection_segment_row_limit_after_insert(self, connect, get_segment_row_limit):
        '''
        target: test create normal collection with different fields
        method: create collection with diff segment_row_limit
        expected: no exception raised
        '''
        collection_name = gen_unique_str(uid)
        fields = copy.deepcopy(default_fields)
        fields["segment_row_limit"] = get_segment_row_limit
        connect.create_collection(collection_name, fields)
        entities = gen_entities_by_fields(fields["fields"], default_nb, fields["fields"][-1]["params"]["dim"])
        res_ids = connect.bulk_insert(collection_name, entities)
        connect.flush([collection_name])
        res = connect.get_collection_info(collection_name)
        assert res['auto_id'] == True
        assert res['segment_row_limit'] == get_segment_row_limit


class TestInfoInvalid(object):
    """
    Test get collection info with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_get_collection_info_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.get_collection_info(collection_name)

    @pytest.mark.level(2)
    def test_get_collection_info_with_empty_collectionname(self, connect):
        collection_name = ''
        with pytest.raises(Exception) as e:
            connect.get_collection_info(collection_name)

    @pytest.mark.level(2)
    def test_get_collection_info_with_none_collectionname(self, connect):
        collection_name = None
        with pytest.raises(Exception) as e:
            connect.get_collection_info(collection_name)

    def test_get_collection_info_None(self, connect):
        '''
        target: test create collection but the collection name is None
        method: create collection, param collection_name is None
        expected: create raise error
        '''
        with pytest.raises(Exception) as e:
            connect.get_collection_info(None)
