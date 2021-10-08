import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils.utils import *
from common.constants import *
from common.common_type import CaseLabel

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
        # if str(connect._cmd("mode")[1]) == "GPU":
        #     if request.param["index_type"] not in ivf():
        #         pytest.skip("Only support index_type: idmap/flat")
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

    @pytest.mark.tags(CaseLabel.L0)
    def test_flush_collection_not_existed(self, connect, collection):
        """
        target: test flush, params collection_name not existed
        method: flush, with collection not existed
        expected: error raised
        """
        collection_new = gen_unique_str("test_flush_1")
        try:
            connect.flush([collection_new])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_new

    @pytest.mark.tags(CaseLabel.L0)
    def test_flush_empty_collection(self, connect, collection):
        """
        method: flush collection with no vectors
        expected: no error raised
        """
        connect.flush([collection])
        results = connect.insert(collection, default_entities)
        assert len(results.primary_keys) == default_nb
        # status = connect.delete_entity_by_id(collection, ids)
        # assert status.OK()
        connect.flush([collection])
        res = connect.get_collection_stats(collection)
        assert res["row_count"] == default_nb
        connect.flush([collection])
        # with pytest.raises(Exception) as e:
        #     connect.flush([collection])

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_partition_flush(self, connect, id_collection):
        """
        method: add entities into partition in collection, flush several times
        expected: the length of ids and the collection row count
        """
        connect.create_partition(id_collection, default_tag)
        result = connect.insert(id_collection, default_entities)
        connect.flush([id_collection])
        res_count = connect.get_collection_stats(id_collection)
        assert res_count["row_count"] == default_nb
        result = connect.insert(id_collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        connect.flush([id_collection])
        res_count = connect.get_collection_stats(id_collection)
        assert res_count["row_count"] == default_nb * 2

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_partitions_flush(self, connect, id_collection):
        """
        method: add entities into partitions in collection, flush one
        expected: the length of ids and the collection row count
        """
        tag_new = gen_unique_str()
        connect.create_partition(id_collection, default_tag)
        connect.create_partition(id_collection, tag_new)
        ids = [i for i in range(default_nb)]
        connect.insert(id_collection, default_entities, partition_name=default_tag)
        connect.flush([id_collection])
        connect.insert(id_collection, default_entities, partition_name=tag_new)
        connect.flush([id_collection])
        res = connect.get_collection_stats(id_collection)
        assert res["row_count"] == 2 * default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_collections_flush(self, connect, id_collection):
        """
        method: add entities into collections, flush one
        expected: the length of ids and the collection row count
        """
        collection_new = gen_unique_str()
        default_fields = gen_default_fields(False)
        connect.create_collection(collection_new, default_fields)
        connect.create_partition(id_collection, default_tag)
        connect.create_partition(collection_new, default_tag)
        ids = [i for i in range(default_nb)]
        # ids = connect.insert(id_collection, default_entities, ids, partition_name=default_tag)
        # ids = connect.insert(collection_new, default_entities, ids, partition_name=default_tag)
        connect.insert(id_collection, default_entities, partition_name=default_tag)
        connect.insert(collection_new, default_entities, partition_name=default_tag)
        connect.flush([id_collection])
        connect.flush([collection_new])
        res = connect.get_collection_stats(id_collection)
        assert res["row_count"] == default_nb
        res = connect.get_collection_stats(collection_new)
        assert res["row_count"] == default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_collections_fields_flush(self, connect, id_collection, get_filter_field, get_vector_field):
        """
        method: create collection with different fields, and add entities into collections, flush one
        expected: the length of ids and the collection row count
        """
        nb_new = 5
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_new = gen_unique_str("test_flush")
        fields = {
            "fields": [gen_primary_field(), filter_field, vector_field],
            "segment_row_limit": default_segment_row_limit,
            "auto_id": False
        }
        connect.create_collection(collection_new, fields)
        connect.create_partition(id_collection, default_tag)
        connect.create_partition(collection_new, default_tag)
        entities_new = gen_entities_by_fields(fields["fields"], nb_new, default_dim)
        connect.insert(id_collection, default_entities, partition_name=default_tag)
        connect.insert(collection_new, entities_new, partition_name=default_tag)
        connect.flush([id_collection])
        connect.flush([collection_new])
        res = connect.get_collection_stats(id_collection)
        assert res["row_count"] == default_nb
        res = connect.get_collection_stats(collection_new)
        assert res["row_count"] == nb_new

    # TODO ci failed
    @pytest.mark.tags(CaseLabel.L0)
    def test_add_flush_multiable_times(self, connect, collection):
        """
        method: add entities, flush several times
        expected: no error raised
        """
        result = connect.insert(collection, default_entities)
        for i in range(10):
            connect.flush([collection])
        res = connect.get_collection_stats(collection)
        assert res["row_count"] == len(result.primary_keys)
        # query_vecs = [vectors[0], vectors[1], vectors[-1]]
        connect.load_collection(collection)
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        assert len(res) == 1
        assert len(res[0].ids) == 10
        assert len(res[0].distances) == 10

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_flush_auto(self, connect, id_collection):
        """
        method: add entities
        expected: no error raised
        """
        ids = [i for i in range(default_nb)]
        result = connect.insert(id_collection, default_entities)
        # add flush
        connect.flush([id_collection])
        timeout = 20
        start_time = time.time()
        while (time.time() - start_time < timeout):
            time.sleep(1)
            res = connect.get_collection_stats(id_collection)
            if res["row_count"] == default_nb:
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

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_flush_same_ids(self, connect, id_collection, same_ids):
        """
        method: add entities, with same ids, count(same ids) < 15, > 15
        expected: the length of ids and the collection row count
        """
        ids = [i for i in range(default_nb)]
        for i, item in enumerate(ids):
            if item <= same_ids:
                ids[i] = 0
        result = connect.insert(id_collection, default_entities)
        connect.flush([id_collection])
        res = connect.get_collection_stats(id_collection)
        assert res["row_count"] == default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_delete_flush_multiable_times(self, connect, collection):
        """
        method: delete entities, flush several times
        expected: no error raised
        """
        result = connect.insert(collection, default_entities)
        # status = connect.delete_entity_by_id(collection, [ids[-1]])
        # assert status.OK()
        for i in range(10):
            connect.flush([collection])
        # query_vecs = [vectors[0], vectors[1], vectors[-1]]
        connect.load_collection(collection)
        res = connect.search(collection, default_single_query)
        assert len(res) == 1
        assert len(res[0].ids) == 10
        assert len(res[0].distances) == 10
        logging.getLogger().debug(res)
        # assert res

    # TODO: unable to set config 
    @pytest.mark.tags(CaseLabel.L2)
    def _test_collection_count_during_flush(self, connect, collection, args):
        """
        method: flush collection at background, call `get_collection_stats`
        expected: no timeout
        """
        ids = []
        for i in range(5):
            tmp_ids = connect.insert(collection, default_entities)
            connect.flush([collection])
            ids.extend(tmp_ids)
        disable_flush(connect)
        # status = connect.delete_entity_by_id(collection, ids)

        def flush():
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            logging.error("start flush")
            milvus.flush([collection])
            logging.error("end flush")

        p = MyThread(target=flush, args=())
        p.start()
        time.sleep(0.2)
        logging.error("start count")
        res = connect.get_collection_stats(collection, timeout=10)
        p.join()
        res = connect.get_collection_stats(collection)
        assert res["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_flush_during_search(self, connect, collection, args):
        """
        method: search at background, call `delete and flush`
        expected: no timeout
        """
        ids = []
        loops = 5
        for i in range(loops):
            tmp = connect.insert(collection, default_entities)
            connect.flush([collection])
            ids.extend(tmp.primary_keys)
        nq = 10000
        query, query_vecs = gen_query_vectors(default_float_vec_field_name, default_entities, default_top_k, nq)
        time.sleep(0.1)
        connect.load_collection(collection)
        future = connect.search(collection, query, _async=True)
        res = future.result()
        assert res
        delete_ids = [ids[0], ids[-1]]
        connect.flush([collection])
        res_count = connect.get_collection_stats(collection, timeout=120)
        assert res_count["row_count"] == loops * default_nb


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

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_empty_collection(self, connect, collection):
        """
        method: flush collection with no vectors
        expected: status ok
        """
        future = connect.flush([collection], _async=True)
        status = future.result()
        assert status is None

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_async_long(self, connect, collection):
        """
        target: test async flush
        method: async flush collection
        expected: status ok
        """
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        future = connect.flush([collection], _async=True)
        status = future.result()
        assert status is None

    @pytest.mark.tags(CaseLabel.L0)
    def test_flush_async_long_drop_collection(self, connect, collection):
        """
        target: test drop collection after async flush
        method: drop collection after async flush collection
        expected: status ok
        """
        for i in range(5):
            result = connect.insert(collection, default_entities)
            assert len(result.primary_keys) == default_nb
        future = connect.flush([collection], _async=True)
        assert future.result() is None
        logging.getLogger().info("DROP")
        res = connect.drop_collection(collection)
        assert res is None

    @pytest.mark.tags(CaseLabel.L0)
    def test_flush_async(self, connect, collection):
        """
        target: test async flush
        method: async flush collection with callback
        expected: status ok
        """
        connect.insert(collection, default_entities)
        logging.getLogger().info("before")
        future = connect.flush([collection], _async=True, _callback=self.check_status)
        logging.getLogger().info("after")
        future.done()
        status = future.result()
        assert status is None


class TestCollectionNameInvalid(object):
    """
    Test adding vectors with invalid collection names
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_with_invalid_collection_name(self, connect, get_invalid_collection_name):
        """
        target: test flush when collection is invalid
        method: flush collection with invalid name
        expected: raise exception
        """
        collection_name = get_invalid_collection_name
        if collection_name is None or not collection_name:
            pytest.skip("while collection_name is None, then flush all collections")
        with pytest.raises(Exception) as e:
            connect.flush(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_flush_empty(self, connect, collection):
        """
        target: test flush with empty collection list
        method: flush with empty collection params
        expected: raise exception
        """
        result = connect.insert(collection, default_entities)
        assert len(result.primary_keys) == default_nb
        try:
            connect.flush()
        except Exception as e:
            assert e.args[0] == "Collection name list can not be None or empty"
