import pdb
import pytest
from utils import *
from constants import *

uid = "load_collection"
field_name = default_float_vec_field_name
default_single_query = {
    "bool": {
        "must": [
            {"vector": {field_name: {"topk": default_top_k, "query": gen_vectors(1, default_dim), "metric_type": "L2",
                                     "params": {"nprobe": 10}}}}
        ]
    }
}


class TestLoadCollection:
    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request, connect):
        return request.param

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_collection_after_index(self, connect, collection, get_simple_index):
        '''
        target: test load collection, after index created
        method: insert and create index, load collection with correct params
        expected: no error raised
        '''
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        connect.load_collection(collection)
        connect.release_collection(collection)

    @pytest.mark.level(2)
    def test_load_collection_after_index_binary(self, connect, binary_collection, get_binary_index):
        '''
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        '''
        ids = connect.insert(binary_collection, default_binary_entities)
        assert len(ids) == default_nb
        connect.flush([binary_collection])
        for metric_type in binary_metrics():
            get_binary_index["metric_type"] = metric_type
            connect.drop_index(binary_collection, default_binary_vec_field_name)
            if get_binary_index["index_type"] == "BIN_IVF_FLAT" and metric_type in structure_metrics():
                with pytest.raises(Exception) as e:
                    connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
            else:
                connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
                index = connect.describe_index(binary_collection, "")
                create_target_index(get_binary_index, default_binary_vec_field_name)
                assert index == get_binary_index
            connect.load_collection(binary_collection)
            connect.release_collection(binary_collection)

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_empty_collection(self, connect, collection):
        '''
        target: test load collection
        method: no entities in collection, load collection with correct params
        expected: load success
        '''
        connect.load_collection(collection)
        connect.release_collection(collection)

    @pytest.mark.level(2)
    def test_load_collection_dis_connect(self, dis_connect, collection):
        '''
        target: test load collection, without connection
        method: load collection with correct params, with a disconnected instance
        expected: load raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.load_collection(collection)

    @pytest.mark.level(2)
    def test_release_collection_dis_connect(self, dis_connect, collection):
        '''
        target: test release collection, without connection
        method: release collection with correct params, with a disconnected instance
        expected: release raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.release_collection(collection)

    @pytest.mark.level(2)
    def test_load_collection_not_existed(self, connect, collection):
        collection_name = gen_unique_str(uid)
        try:
            connect.load_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.level(2)
    def test_release_collection_not_existed(self, connect, collection):
        collection_name = gen_unique_str(uid)
        try:
            connect.release_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_release_collection_not_load(self, connect, collection):
        """
        target: test release collection without load
        method:
        expected: raise exception
        """
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.release_collection(collection)

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_collection_after_load_release(self, connect, collection):
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.release_collection(collection)
        connect.load_collection(collection)

    def test_load_collection_repeatedly(self, connect, collection):
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.load_collection(collection)

    @pytest.mark.level(2)
    def test_load_release_collection(self, connect, collection):
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        connect.insert(collection_name, default_entities)
        connect.flush([collection_name])
        connect.load_collection(collection_name)
        connect.release_collection(collection_name)
        connect.drop_collection(collection_name)
        try:
            connect.load_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

        try:
            connect.release_collection(collection_name)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_release_collection_after_drop(self, connect, collection):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.drop_collection(collection)
        try:
            connect.release_collection(collection)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection

#     @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_collection_without_flush(self, connect, collection):
        """
        target: test load collection without flush
        method: insert entities without flush, then load collection
        expected: load collection failed
        """
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.load_collection(collection)

    # TODO
    def _test_load_collection_larger_than_memory(self):
        """
        target: test load collection when memory less than collection size
        method: i don't know
        expected: raise exception
        """

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_collection_release_part_partitions(self, connect, collection):
        """
        target: test release part partitions after load collection
        method: load collection and release part partitions
        expected: released partitions search empty
        """
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.release_partitions(collection, [default_tag])
        with pytest.raises(Exception) as e:
            connect.search(collection, default_single_query, partition_names=[default_tag])
        res = connect.search(collection, default_single_query, partition_names=[default_partition_name])
        assert len(res[0]) == default_top_k

    def test_load_collection_release_all_partitions(self, connect, collection):
        """
        target: test release all partitions after load collection
        method: load collection and release all partitions
        expected: search empty
        """
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_collection(collection)
        connect.release_partitions(collection, [default_partition_name, default_tag])
        with pytest.raises(Exception) as e:
            connect.search(collection, default_single_query)

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_partitions_release_collection(self, connect, collection):
        """
        target: test release collection after load partitions
        method: insert entities into partitions, search empty after load partitions and release collection
        expected: search result empty
        """
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)
        # assert len(res[0]) == 0


class TestReleaseAdvanced:

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_release_collection_during_searching(self, connect, collection):
        """
        target: test release collection during searching
        method: insert entities into collection, flush and load collection, release collection during searching
        expected:
        """
        nq = 1000
        top_k = 1
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.load_collection(collection)
        query, _ = gen_query_vectors(field_name, default_entities, top_k, nq)
        future = connect.search(collection, query, _async=True)
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)

    def test_release_partition_during_searching(self, connect, collection):
        """
        target: test release partition during searching
        method: insert entities into partition, flush and load partition, release partition during searching
        expected:
        """
        nq = 1000
        top_k = 1
        connect.create_partition(collection, default_tag)
        query, _ = gen_query_vectors(field_name, default_entities, top_k, nq)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        res = connect.search(collection, query, _async=True)
        connect.release_partitions(collection, [default_tag])
        with pytest.raises(Exception):
            res = connect.search(collection, default_single_query)

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_release_collection_during_searching_A(self, connect, collection):
        """
        target: test release collection during searching
        method: insert entities into partition, flush and load partition, release collection during searching
        expected:
        """
        nq = 1000
        top_k = 1
        connect.create_partition(collection, default_tag)
        query, _ = gen_query_vectors(field_name, default_entities, top_k, nq)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        res = connect.search(collection, query, _async=True)
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)

    def _test_release_collection_during_loading(self, connect, collection):
        """
        target: test release collection during loading
        method: insert entities into collection, flush, release collection during loading
        expected:
        """
        connect.insert(collection, default_entities)
        connect.flush([collection])

        def load():
            connect.load_collection(collection)

        t = threading.Thread(target=load, args=())
        t.start()
        connect.release_collection(collection)
        with pytest.raises(Exception):
            connect.search(collection, default_single_query)

    def _test_release_partition_during_loading(self, connect, collection):
        """
        target: test release partition during loading
        method: insert entities into partition, flush, release partition during loading
        expected:
        """
        connect.create_partition(collection, default_tag)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])

        def load():
            connect.load_collection(collection)

        t = threading.Thread(target=load, args=())
        t.start()
        connect.release_partitions(collection, [default_tag])
        res = connect.search(collection, default_single_query)
        assert len(res[0]) == 0

    def _test_release_collection_during_inserting(self, connect, collection):
        """
        target: test release collection during inserting
        method: load collection, do release collection during inserting
        expected:
        """
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.load_collection(collection)

        def insert():
            connect.insert(collection, default_entities)

        t = threading.Thread(target=insert, args=())
        t.start()
        connect.release_collection(collection)
        with pytest.raises(Exception):
            res = connect.search(collection, default_single_query)
        # assert len(res[0]) == 0

    def _test_release_collection_during_indexing(self, connect, collection):
        """
        target: test release collection during building index
        method: insert and flush, load collection, do release collection during creating index
        expected:
        """
        pass

    def _test_release_collection_during_droping_index(self, connect, collection):
        """
        target: test release collection during droping index
        method: insert, create index and flush, load collection, do release collection during droping index
        expected:
        """
        pass


class TestLoadCollectionInvalid(object):
    """
    Test load collection with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_load_collection_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.load_collection(collection_name)

    @pytest.mark.level(2)
    def test_release_collection_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.release_collection(collection_name)


class TestLoadPartition:
    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in cpu mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_partition_after_index(self, connect, collection, get_simple_index):
        '''
        target: test load collection, after index created
        method: insert and create index, load collection with correct params
        expected: no error raised
        '''
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        search_param = get_search_param(get_simple_index["index_type"])
        query, vecs = gen_query_vectors(field_name, default_entities, default_top_k, nq=1, search_params=search_param)
        connect.load_partitions(collection, [default_tag])
        res = connect.search(collection, query, partition_names=[default_tag])
        assert len(res[0]) == default_top_k

    @pytest.mark.level(2)
    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_partition_after_index_binary(self, connect, binary_collection, get_binary_index):
        '''
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        '''
        connect.create_partition(binary_collection, default_tag)
        ids = connect.insert(binary_collection, default_binary_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([binary_collection])
        for metric_type in binary_metrics():
            logging.getLogger().info(metric_type)
            get_binary_index["metric_type"] = metric_type
            if get_binary_index["index_type"] == "BIN_IVF_FLAT" and metric_type in structure_metrics():
                with pytest.raises(Exception) as e:
                    connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
            else:
                connect.create_index(binary_collection, default_binary_vec_field_name, get_binary_index)
            connect.load_partitions(binary_collection, [default_tag])

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_empty_partition(self, connect, collection):
        '''
        target: test load collection
        method: no entities in collection, load collection with correct params
        expected: load success
        '''
        connect.create_partition(collection, default_tag)
        connect.load_partitions(collection, [default_tag])
        res = connect.search(collection, default_single_query)
        assert len(res[0]) == 0

    @pytest.mark.level(2)
    def test_load_collection_dis_connect(self, connect, dis_connect, collection):
        '''
        target: test load collection, without connection
        method: load collection with correct params, with a disconnected instance
        expected: load raise exception
        '''
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            dis_connect.load_partitions(collection, [default_tag])

    @pytest.mark.level(2)
    def test_release_partition_dis_connect(self, connect, dis_connect, collection):
        '''
        target: test release collection, without connection
        method: release collection with correct params, with a disconnected instance
        expected: release raise exception
        '''
        connect.create_partition(collection, default_tag)
        connect.load_partitions(collection, [default_tag])
        with pytest.raises(Exception) as e:
            dis_connect.release_partitions(collection, [default_tag])

    @pytest.mark.level(2)
    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_partition_not_existed(self, connect, collection):
        partition_name = gen_unique_str(uid)
        try:
            connect.load_partitions(collection, [partition_name])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % partition_name

    @pytest.mark.level(2)
    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_release_partition_not_existed(self, connect, collection):
        partition_name = gen_unique_str(uid)
        try:
            connect.release_partitions(collection, [partition_name])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % partition_name

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_release_partition_not_load(self, connect, collection):
        """
        target: test release collection without load
        method:
        expected: raise exception
        """
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.release_partitions(collection, [default_tag])

    @pytest.mark.level(2)
    def test_load_release_after_drop(self, connect, collection):
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.release_partitions(collection, [default_tag])
        connect.drop_partition(collection, default_tag)
        try:
            connect.load_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % default_tag

        try:
            connect.release_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % default_tag

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_release_partition_after_drop(self, connect, collection):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.drop_partition(collection, default_tag)
        try:
            connect.load_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % default_tag

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_load_release_after_collection_drop(self, connect, collection):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.load_partitions(collection, [default_tag])
        connect.release_partitions(collection, [default_tag])
        connect.drop_collection(collection)
        try:
            connect.load_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection

        try:
            connect.release_partitions(collection, [default_tag])
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "describe collection failed: can't find collection: %s" % collection


class TestLoadPartitionInvalid(object):
    """
    Test load collection with invalid params
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_partition_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_load_partition_with_invalid_partition_name(self, connect, collection, get_partition_name):
        partition_name = get_partition_name
        with pytest.raises(Exception) as e:
            connect.load_partitions(collection, [partition_name])

    @pytest.mark.level(2)
    def test_release_partition_with_invalid_partition_name(self, connect, collection, get_partition_name):
        partition_name = get_partition_name
        with pytest.raises(Exception) as e:
            connect.load_partitions(collection, [partition_name])
