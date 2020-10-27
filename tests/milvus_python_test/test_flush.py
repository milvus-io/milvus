import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

DELETE_TIMEOUT = 60
default_single_query = {
    "bool": {
        "must": [
            {"vector": {default_float_vec_field_name: {"topk": 10, "query": gen_vectors(1, default_dim),
                                                       "metric_type": "L2", "params": {"nprobe": 10}}}}
        ]
    }
}

class TestFlushBase:
    """
    ******************************************************************
      The following cases are used to test `flush` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] not in ivf():
                pytest.skip("Only support index_type: idmap/flat")
        return request.param

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

    def test_flush_collection_not_existed(self, connect, collection):
        '''
        target: test flush, params collection_name not existed
        method: flush, with collection not existed
        expected: error raised
        '''
        collection_new = gen_unique_str("test_flush_1")
        with pytest.raises(Exception) as e:
            connect.flush([collection_new])

    def test_flush_empty_collection(self, connect, collection):
        '''
        method: flush collection with no vectors
        expected: no error raised
        '''
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        connect.flush([collection])
        res = connect.count_entities(collection)
        assert 0 == res
        # with pytest.raises(Exception) as e:
        #     connect.flush([collection])

    def test_add_partition_flush(self, connect, id_collection):
        '''
        method: add entities into partition in collection, flush serveral times
        expected: the length of ids and the collection row count
        '''
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        ids = connect.insert(id_collection, default_entities, ids)
        connect.flush([id_collection])
        res_count = connect.count_entities(id_collection)
        assert res_count == default_nb
        ids = connect.insert(id_collection, default_entities, ids, partition_tag=default_tag)
        assert len(ids) == default_nb
        connect.flush([id_collection])
        res_count = connect.count_entities(id_collection)
        assert res_count == default_nb * 2

    def test_add_partitions_flush(self, connect, id_collection):
        '''
        method: add entities into partitions in collection, flush one
        expected: the length of ids and the collection row count
        '''
        tag_new = gen_unique_str()
        connect.create_partition(id_collection, default_tag)
        connect.create_partition(id_collection, tag_new)
        ids = [i for i in range(default_nb)]
        ids = connect.insert(id_collection, default_entities, ids, partition_tag=default_tag)
        connect.flush([id_collection])
        ids = connect.insert(id_collection, default_entities, ids, partition_tag=tag_new)
        connect.flush([id_collection])
        res = connect.count_entities(id_collection)
        assert res == 2 * default_nb

    def test_add_collections_flush(self, connect, id_collection):
        '''
        method: add entities into collections, flush one
        expected: the length of ids and the collection row count
        '''
        collection_new = gen_unique_str()
        default_fields = gen_default_fields(False)
        connect.create_collection(collection_new, default_fields)
        connect.create_partition(id_collection, default_tag)
        connect.create_partition(collection_new, default_tag)
        ids = [i for i in range(default_nb)]
        ids = connect.insert(id_collection, default_entities, ids, partition_tag=default_tag)
        ids = connect.insert(collection_new, default_entities, ids, partition_tag=default_tag)
        connect.flush([id_collection])
        connect.flush([collection_new])
        res = connect.count_entities(id_collection)
        assert res == default_nb
        res = connect.count_entities(collection_new)
        assert res == default_nb

    def test_add_collections_fields_flush(self, connect, id_collection, get_filter_field, get_vector_field):
        '''
        method: create collection with different fields, and add entities into collections, flush one
        expected: the length of ids and the collection row count
        '''
        nb_new = 5
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_new = gen_unique_str("test_flush")
        fields = {
            "fields": [filter_field, vector_field],
            "segment_row_limit": default_segment_row_limit,
            "auto_id": False
        }
        connect.create_collection(collection_new, fields)
        connect.create_partition(id_collection, default_tag)
        connect.create_partition(collection_new, default_tag)
        entities_new = gen_entities_by_fields(fields["fields"], nb_new, default_dim)
        ids = [i for i in range(default_nb)]
        ids_new = [i for i in range(nb_new)]
        ids = connect.insert(id_collection, default_entities, ids, partition_tag=default_tag)
        ids = connect.insert(collection_new, entities_new, ids_new, partition_tag=default_tag)
        connect.flush([id_collection])
        connect.flush([collection_new])
        res = connect.count_entities(id_collection)
        assert res == default_nb
        res = connect.count_entities(collection_new)
        assert res == nb_new

    def test_add_flush_multiable_times(self, connect, collection):
        '''
        method: add entities, flush serveral times
        expected: no error raised
        '''
        ids = connect.insert(collection, default_entities)
        for i in range(10):
            connect.flush([collection])
        res = connect.count_entities(collection)
        assert res == len(ids)
        # query_vecs = [vectors[0], vectors[1], vectors[-1]]
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        assert res

    def test_add_flush_auto(self, connect, id_collection):
        '''
        method: add entities
        expected: no error raised
        '''
        ids = [i for i in range(default_nb)]
        ids = connect.insert(id_collection, default_entities, ids)
        timeout = 20
        start_time = time.time()
        while (time.time() - start_time < timeout):
            time.sleep(1)
            res = connect.count_entities(id_collection)
            if res == default_nb:
                break
        if time.time() - start_time > timeout:
            assert False

    @pytest.fixture(
        scope="function",
        params=[
            1,
            100
        ],
    )
    def same_ids(self, request):
        yield request.param

    def test_add_flush_same_ids(self, connect, id_collection, same_ids):
        '''
        method: add entities, with same ids, count(same ids) < 15, > 15
        expected: the length of ids and the collection row count
        '''
        ids = [i for i in range(default_nb)]
        for i, item in enumerate(ids):
            if item <= same_ids:
                ids[i] = 0
        ids = connect.insert(id_collection, default_entities, ids)
        connect.flush([id_collection])
        res = connect.count_entities(id_collection)
        assert res == default_nb

    def test_delete_flush_multiable_times(self, connect, collection):
        '''
        method: delete entities, flush serveral times
        expected: no error raised
        '''
        ids = connect.insert(collection, default_entities)
        status = connect.delete_entity_by_id(collection, [ids[-1]])
        assert status.OK()
        for i in range(10):
            connect.flush([collection])
        # query_vecs = [vectors[0], vectors[1], vectors[-1]]
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        assert res

    # TODO: unable to set config 
    @pytest.mark.level(2)
    def _test_collection_count_during_flush(self, connect, collection, args):
        '''
        method: flush collection at background, call `count_entities`
        expected: no timeout
        '''
        ids = []
        for i in range(5):
            tmp_ids = connect.insert(collection, default_entities)
            connect.flush([collection])
            ids.extend(tmp_ids)
        disable_flush(connect)
        status = connect.delete_entity_by_id(collection, ids)
        def flush():
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            logging.error("start flush")
            milvus.flush([collection])
            logging.error("end flush")
    
        p = threading.Thread(target=flush, args=())
        p.start()
        time.sleep(0.2)
        logging.error("start count")
        res = connect.count_entities(collection, timeout = 10)
        p.join()
        res = connect.count_entities(collection)
        assert res == 0


class TestFlushAsync:
    @pytest.fixture(scope="function", autouse=True)
    def skip_http_check(self, args):
        if args["handler"] == "HTTP":
            pytest.skip("skip in http mode")

    """
    ******************************************************************
      The following cases are used to test `flush` function
    ******************************************************************
    """

    def check_status(self):
        logging.getLogger().info("In callback check status")

    def test_flush_empty_collection(self, connect, collection):
        '''
        method: flush collection with no vectors
        expected: status ok
        '''
        future = connect.flush([collection], _async=True)
        status = future.result()

    def test_flush_async_long(self, connect, collection):
        ids = connect.insert(collection, default_entities)
        future = connect.flush([collection], _async=True)
        status = future.result()

    def test_flush_async_long_drop_collection(self, connect, collection):
        for i in range(5):
            ids = connect.insert(collection, default_entities)
        future = connect.flush([collection], _async=True)
        logging.getLogger().info("DROP")
        connect.drop_collection(collection)

    def test_flush_async(self, connect, collection):
        connect.insert(collection, default_entities)
        logging.getLogger().info("before")
        future = connect.flush([collection], _async=True, _callback=self.check_status)
        logging.getLogger().info("after")
        future.done()
        status = future.result()


class TestCollectionNameInvalid(object):
    """
    Test adding vectors with invalid collection names
    """

    @pytest.fixture(
        scope="function",
        # params=gen_invalid_collection_names()
        params=gen_invalid_strs()
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_flush_with_invalid_collection_name(self, connect, get_invalid_collection_name):
        collection_name = get_invalid_collection_name
        if collection_name is None or not collection_name:
            pytest.skip("while collection_name is None, then flush all collections")
        with pytest.raises(Exception) as e:
            connect.flush(collection_name)
