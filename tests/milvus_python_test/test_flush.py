import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils import *

dim = 128
segment_row_count = 5000
index_file_size = 10
collection_id = "test_flush"
DELETE_TIMEOUT = 60
nprobe = 1
tag = "1970-01-01"
top_k = 1
nb = 6000
tag = "partition_tag"
field_name = "float_vector"
entity = gen_entities(1)
entities = gen_entities(nb)
raw_vector, binary_entity = gen_binary_entities(1)
raw_vectors, binary_entities = gen_binary_entities(nb)
default_fields = gen_default_fields()
default_single_query = {
    "bool": {
        "must": [
            {"vector": {field_name: {"topk": 10, "query": gen_vectors(1, dim),
                                     "params": {"nprobe": 10}}}}
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
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        res = connect.count_entities(collection)
        assert 0 == res
        # with pytest.raises(Exception) as e:
        #     connect.flush([collection])

    # TODO
    @pytest.mark.level(2)
    def test_add_partition_flush(self, connect, id_collection):
        '''
        method: add entities into partition in collection, flush serveral times
        expected: the length of ids and the collection row count
        '''
        # vector = gen_vector(nb, dim)
        connect.create_partition(id_collection, tag)
        # vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        ids = connect.insert(id_collection, entities, ids)
        connect.flush([id_collection])
        res_count = connect.count_entities(id_collection)
        assert res_count == nb
        ids = connect.insert(id_collection, entities, ids, partition_tag=tag)
        assert len(ids) == nb
        connect.flush([id_collection])
        res_count = connect.count_entities(id_collection)
        assert res_count == nb * 2

    # TODO
    @pytest.mark.level(2)
    def test_add_partitions_flush(self, connect, collection):
        '''
        method: add entities into partitions in collection, flush one
        expected: the length of ids and the collection row count
        '''
        # vectors = gen_vectors(nb, dim)
        tag_new = gen_unique_str()
        connect.create_partition(collection, tag)
        connect.create_partition(collection, tag_new)
        ids = [i for i in range(nb)]
        ids = connect.insert(collection, entities, ids, partition_tag=tag)
        connect.flush([collection])
        ids = connect.insert(collection, entities, ids, partition_tag=tag_new)
        connect.flush([collection])
        res = connect.count_entities(collection)
        assert res == 2 * nb

    # TODO
    @pytest.mark.level(2)
    def test_add_collections_flush(self, connect, collection):
        '''
        method: add entities into collections, flush one
        expected: the length of ids and the collection row count
        '''
        collection_new = gen_unique_str()
        connect.create_collection(collection_new, default_fields)
        connect.create_partition(collection, tag)
        connect.create_partition(collection_new, tag)
        # vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        ids = connect.insert(collection, entities, ids, partition_tag=tag)
        ids = connect.insert(collection_new, entities, ids, partition_tag=tag)
        connect.flush([collection])
        connect.flush([collection_new])
        res = connect.count_entities(collection)
        assert res == nb
        res = connect.count_entities(collection_new)
        assert res == nb

    # TODO
    @pytest.mark.level(2)
    def test_add_collections_fields_flush(self, connect, collection, get_filter_field, get_vector_field):
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
            "segment_row_count": segment_row_count
        }
        connect.create_collection(collection_new, fields)
        connect.create_partition(collection, tag)
        connect.create_partition(collection_new, tag)
        # vectors = gen_vectors(nb, dim)
        entities_new = gen_entities_by_fields(fields["fields"], nb_new, dim)
        ids = [i for i in range(nb)]
        ids_new = [i for i in range(nb_new)]
        ids = connect.insert(collection, entities, ids, partition_tag=tag)
        ids = connect.insert(collection_new, entities_new, ids_new, partition_tag=tag)
        connect.flush([collection])
        connect.flush([collection_new])
        res = connect.count_entities(collection)
        assert res == nb
        res = connect.count_entities(collection_new)
        assert res == nb_new

    @pytest.mark.skip(reason="search not support yet")
    def test_add_flush_multiable_times(self, connect, collection):
        '''
        method: add entities, flush serveral times
        expected: no error raised
        '''
        # vectors = gen_vectors(nb, dim)
        ids = connect.insert(collection, entities)
        for i in range(10):
            connect.flush([collection])
        res = connect.count_entities(collection)
        assert res == len(ids)
        # query_vecs = [vectors[0], vectors[1], vectors[-1]]
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        assert res

    # TODO
    @pytest.mark.level(2)
    # TODO: stable case
    def test_add_flush_auto(self, connect, id_collection):
        '''
        method: add entities
        expected: no error raised
        '''
        # vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        ids = connect.insert(id_collection, entities, ids)
        timeout = 10
        start_time = time.time()
        while (time.time() - start_time < timeout):
            time.sleep(1)
            res = connect.count_entities(id_collection)
            if res == nb:
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

    # TODO
    @pytest.mark.level(2)
    def test_add_flush_same_ids(self, connect, id_collection, same_ids):
        '''
        method: add entities, with same ids, count(same ids) < 15, > 15
        expected: the length of ids and the collection row count
        '''
        # vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        for i, item in enumerate(ids):
            if item <= same_ids:
                ids[i] = 0
        ids = connect.insert(id_collection, entities, ids)
        connect.flush([id_collection])
        res = connect.count_entities(id_collection)
        assert res == nb

    @pytest.mark.skip(reason="search not support yet")
    def test_delete_flush_multiable_times(self, connect, collection):
        '''
        method: delete entities, flush serveral times
        expected: no error raised
        '''
        # vectors = gen_vectors(nb, dim)
        ids = connect.insert(collection, entities)
        status = connect.delete_entity_by_id(collection, [ids[-1]])
        assert status.OK()
        for i in range(10):
            connect.flush([collection])
        # query_vecs = [vectors[0], vectors[1], vectors[-1]]
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        assert res

    # TODO: CI fail, LOCAL pass
    def _test_collection_count_during_flush(self, connect, args):
        '''
        method: flush collection at background, call `count_entities`
        expected: status ok
        '''
        collection = gen_unique_str("test_flush")
        # param = {'collection_name': collection,
        #          'dimension': dim,
        #          'index_file_size': index_file_size,
        #          'metric_type': MetricType.L2}
        milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
        milvus.create_collection(collection, default_fields)
        # vectors = gen_vector(nb, dim)
        ids = milvus.insert(collection, entities, ids=[i for i in range(nb)])

        def flush(collection_name):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            status = milvus.delete_entity_by_id(collection_name, [i for i in range(nb)])
            with pytest.raises(Exception) as e:
                milvus.flush([collection_name])


        p = Process(target=flush, args=(collection,))
        p.start()
        res = milvus.count_entities(collection)
        assert res == nb
        p.join()
        res = milvus.count_entities(collection)
        assert res == nb
        logging.getLogger().info(res)
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
        # vectors = gen_vectors(nb, dim)
        ids = connect.insert(collection, entities)
        future = connect.flush([collection], _async=True)
        status = future.result()

    # TODO:
    def _test_flush_async(self, connect, collection):
        nb = 100000
        vectors = gen_vectors(nb, dim)
        connect.insert(collection, entities)
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
