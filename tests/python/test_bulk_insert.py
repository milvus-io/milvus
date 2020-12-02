import pytest
from .utils import *
from .constants import *

ADD_TIMEOUT = 600
uid = "test_insert"
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
default_single_query = {
    "bool": {
        "must": [
            {"vector": {field_name: {"topk": 10, "query": gen_vectors(1, default_dim), "metric_type": "L2",
                                     "params": {"nprobe": 10}}}}
        ]
    }
}


class TestInsertBase:
    """
    ******************************************************************
      The following cases are used to test `insert` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("CPU not support index_type: ivf_sq8h")
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

    def test_add_vector_with_empty_vector(self, connect, collection):
        '''
        target: test add vectors with empty vectors list
        method: set empty vectors list as add method params
        expected: raises a Exception
        '''
        vector = []
        with pytest.raises(Exception) as e:
            status, ids = connect.bulk_insert(collection, vector)

    def test_add_vector_with_None(self, connect, collection):
        '''
        target: test add vectors with None
        method: set None as add method params
        expected: raises a Exception
        '''
        vector = None
        with pytest.raises(Exception) as e:
            status, ids = connect.bulk_insert(collection, vector)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_collection_not_existed(self, connect):
        '''
        target: test insert, with collection not existed
        method: insert entity into a random named collection
        expected: error raised 
        '''
        collection_name = gen_unique_str(uid)
        with pytest.raises(Exception) as e:
            connect.bulk_insert(collection_name, default_entities)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_drop_collection(self, connect, collection):
        '''
        target: test delete collection after insert vector
        method: insert vector and delete collection
        expected: no error raised
        '''
        ids = connect.bulk_insert(collection, default_entity)
        assert len(ids) == 1
        connect.drop_collection(collection)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_sleep_drop_collection(self, connect, collection):
        '''
        target: test delete collection after insert vector for a while
        method: insert vector, sleep, and delete collection
        expected: no error raised 
        '''
        ids = connect.bulk_insert(collection, default_entity)
        assert len(ids) == 1
        connect.flush([collection])
        connect.drop_collection(collection)

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_create_index(self, connect, collection, get_simple_index):
        '''
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        '''
        ids = connect.bulk_insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        info = connect.get_collection_info(collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == field_name:
                assert field["indexes"][0] == get_simple_index

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_create_index_new(self, connect, collection, get_simple_index):
        '''
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        '''
        ids = connect.bulk_insert(collection, default_entities_new)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        info = connect.get_collection_info(collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == field_name:
                assert field["indexes"][0] == get_simple_index

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_after_create_index(self, connect, collection, get_simple_index):
        '''
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        '''
        connect.create_index(collection, field_name, get_simple_index)
        ids = connect.bulk_insert(collection, default_entities)
        assert len(ids) == default_nb
        info = connect.get_collection_info(collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == field_name:
                assert field["indexes"][0] == get_simple_index

    @pytest.mark.skip(" later ")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_search(self, connect, collection):
        '''
        target: test search vector after insert vector after a while
        method: insert vector, sleep, and search collection
        expected: no error raised 
        '''
        ids = connect.bulk_insert(collection, default_entities)
        connect.flush([collection])
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        assert res
    
    @pytest.mark.skip
    def test_insert_segment_row_count(self, connect, collection):
        nb = default_segment_row_limit + 1
        res_ids = connect.bulk_insert(collection, gen_entities(nb))
        connect.flush([collection])
        assert len(res_ids) == nb
        stats = connect.get_collection_stats(collection)
        assert len(stats['partitions'][0]['segments']) == 2
        for segment in stats['partitions'][0]['segments']:
            assert segment['row_count'] in [default_segment_row_limit, 1]

    @pytest.fixture(
        scope="function",
        params=[
            1,
            2000
        ],
    )
    def insert_count(self, request):
        yield request.param

    @pytest.mark.skip(" laster need custom ids")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_ids(self, connect, id_collection, insert_count):
        '''
        target: test insert vectors in collection, use customize ids
        method: create collection and insert vectors in it, check the ids returned and the collection length after vectors inserted
        expected: the length of ids and the collection row count
        '''
        nb = insert_count
        ids = [i for i in range(nb)]
        res_ids = connect.bulk_insert(id_collection, gen_entities(nb), ids)
        connect.flush([id_collection])
        assert len(res_ids) == nb
        assert res_ids == ids
        res_count = connect.count_entities(id_collection)
        assert res_count == nb

    @pytest.mark.skip(" laster need custom ids")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_the_same_ids(self, connect, id_collection, insert_count):
        '''
        target: test insert vectors in collection, use customize the same ids
        method: create collection and insert vectors in it, check the ids returned and the collection length after vectors inserted
        expected: the length of ids and the collection row count
        '''
        nb = insert_count
        ids = [1 for i in range(nb)]
        res_ids = connect.bulk_insert(id_collection, gen_entities(nb), ids)
        connect.flush([id_collection])
        assert len(res_ids) == nb
        assert res_ids == ids
        res_count = connect.count_entities(id_collection)
        assert res_count == nb

    @pytest.mark.skip(" not support count_entites")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_ids_fields(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, insert entities into id with ids
        method: create collection with diff fields: metric/field_type/..., insert, and count
        expected: row count correct
        '''
        nb = 5
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str("test_collection")
        fields = {
            "fields": [filter_field, vector_field],
            "segment_row_limit": default_segment_row_limit,
            "auto_id": True
        }
        connect.create_collection(collection_name, fields)
        ids = [i for i in range(nb)]
        entities = gen_entities_by_fields(fields["fields"], nb, default_dim)
        res_ids = connect.bulk_insert(collection_name, entities, ids)
        assert res_ids == ids
        connect.flush([collection_name])
        res_count = connect.count_entities(collection_name)
        assert res_count == nb

    # TODO: assert exception && enable
    @pytest.mark.skip(" todo support custom id")
    @pytest.mark.level(2)
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_twice_ids_no_ids(self, connect, id_collection):
        '''
        target: check the result of insert, with params ids and no ids
        method: test insert vectors twice, use customize ids first, and then use no ids
        expected:  error raised
        '''
        ids = [i for i in range(default_nb)]
        res_ids = connect.bulk_insert(id_collection, default_entities, ids)
        with pytest.raises(Exception) as e:
            res_ids_new = connect.bulk_insert(id_collection, default_entities)

    # TODO: assert exception && enable
    @pytest.mark.level(2)
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_twice_not_ids_ids(self, connect, id_collection):
        '''
        target: check the result of insert, with params ids and no ids
        method: test insert vectors twice, use not ids first, and then use customize ids
        expected:  error raised
        '''
        with pytest.raises(Exception) as e:
            res_ids = connect.bulk_insert(id_collection, default_entities)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_ids_length_not_match_batch(self, connect, id_collection):
        '''
        target: test insert vectors in collection, use customize ids, len(ids) != len(vectors)
        method: create collection and insert vectors in it
        expected: raise an exception
        '''
        ids = [i for i in range(1, default_nb)]
        logging.getLogger().info(len(ids))
        with pytest.raises(Exception) as e:
            res_ids = connect.bulk_insert(id_collection, default_entities, ids)

    @pytest.mark.skip(" not suppport custom id")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_ids_length_not_match_single(self, connect, collection):
        '''
        target: test insert vectors in collection, use customize ids, len(ids) != len(vectors)
        method: create collection and insert vectors in it
        expected: raise an exception
        '''
        ids = [i for i in range(1, default_nb)]
        logging.getLogger().info(len(ids))
        with pytest.raises(Exception) as e:
            res_ids = connect.bulk_insert(collection, default_entity, ids)

    @pytest.mark.skip(" not support count entities")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_ids_fields(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, insert entities into id without ids
        method: create collection with diff fields: metric/field_type/..., insert, and count
        expected: row count correct
        '''
        nb = 5
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str("test_collection")
        fields = {
            "fields": [filter_field, vector_field],
            "segment_row_limit": default_segment_row_limit
        }
        connect.create_collection(collection_name, fields)
        entities = gen_entities_by_fields(fields["fields"], nb, default_dim)
        res_ids = connect.bulk_insert(collection_name, entities)
        connect.flush([collection_name])
        res_count = connect.count_entities(collection_name)
        assert res_count == nb

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag(self, connect, collection):
        '''
        target: test insert entities in collection created before
        method: create collection and insert entities in it, with the partition_tag param
        expected: the collection row count equals to nq
        '''
        connect.create_partition(collection, default_tag)
        ids = connect.bulk_insert(collection, default_entities, partition_tag=default_tag)
        assert len(ids) == default_nb
        assert connect.has_partition(collection, default_tag)

    @pytest.mark.skip("not support custom id")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag_with_ids(self, connect, id_collection):
        '''
        target: test insert entities in collection created before, insert with ids
        method: create collection and insert entities in it, with the partition_tag param
        expected: the collection row count equals to nq
        '''
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        res_ids = connect.bulk_insert(id_collection, default_entities, ids, partition_tag=default_tag)
        assert res_ids == ids


    @pytest.mark.skip(" not support custom id")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_default_tag(self, connect, collection):
        '''
        target: test insert entities into default partition
        method: create partition and insert info collection without tag params
        expected: the collection row count equals to nb
        '''
        connect.create_partition(collection, default_tag)
        ids = connect.bulk_insert(collection, default_entities)
        connect.flush([collection])
        assert len(ids) == default_nb
        res_count = connect.count_entities(collection)
        assert res_count == default_nb

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag_not_existed(self, connect, collection):
        '''
        target: test insert entities in collection created before
        method: create collection and insert entities in it, with the not existed partition_tag param
        expected: error raised
        '''
        tag = gen_unique_str()
        with pytest.raises(Exception) as e:
            ids = connect.bulk_insert(collection, default_entities, partition_tag=tag)

    @pytest.mark.skip(" not support count entities")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag_existed(self, connect, collection):
        '''
        target: test insert entities in collection created before
        method: create collection and insert entities in it repeatly, with the partition_tag param
        expected: the collection row count equals to nq
        '''
        connect.create_partition(collection, default_tag)
        ids = connect.bulk_insert(collection, default_entities, partition_tag=default_tag)
        ids = connect.bulk_insert(collection, default_entities, partition_tag=default_tag)
        connect.flush([collection])
        res_count = connect.count_entities(collection)
        assert res_count == 2 * default_nb

    @pytest.mark.level(2)
    def test_insert_without_connect(self, dis_connect, collection):
        '''
        target: test insert entities without connection
        method: create collection and insert entities in it, check if inserted successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            ids = dis_connect.bulk_insert(collection, default_entities)

    def test_insert_collection_not_existed(self, connect):
        '''
        target: test insert entities in collection, which not existed before
        method: insert entities collection not existed, check the status
        expected: error raised
        '''
        with pytest.raises(Exception) as e:
            ids = connect.bulk_insert(gen_unique_str("not_exist_collection"), default_entities)

    @pytest.mark.skip("to do add dim check ")
    def test_insert_dim_not_matched(self, connect, collection):
        '''
        target: test insert entities, the vector dimension is not equal to the collection dimension
        method: the entities dimension is half of the collection dimension, check the status
        expected: error raised
        '''
        vectors = gen_vectors(default_nb, int(default_dim) // 2)
        insert_entities = copy.deepcopy(default_entities)
        insert_entities[-1]["values"] = vectors
        with pytest.raises(Exception) as e:
            ids = connect.bulk_insert(collection, insert_entities)


    def test_insert_with_field_name_not_match(self, connect, collection):
        '''
        target: test insert entities, with the entity field name updated
        method: update entity field name
        expected: error raised
        '''
        tmp_entity = update_field_name(copy.deepcopy(default_entity), "int64", "int64new")
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    @pytest.mark.skip(" todo support  type check")
    def test_insert_with_field_type_not_match(self, connect, collection):
        '''
        target: test insert entities, with the entity field type updated
        method: update entity field type
        expected: error raised
        '''
        tmp_entity = update_field_type(copy.deepcopy(default_entity), "int64", DataType.FLOAT)
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    @pytest.mark.skip("to do add field_type check ")
    @pytest.mark.level(2)
    def test_insert_with_field_type_not_match_B(self, connect, collection):
        '''
        target: test insert entities, with the entity field type updated
        method: update entity field type
        expected: error raised
        '''
        tmp_entity = update_field_type(copy.deepcopy(default_entity), "int64", DataType.DOUBLE)
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    @pytest.mark.level(2)
    def test_insert_with_field_value_not_match(self, connect, collection):
        '''
        target: test insert entities, with the entity field value updated
        method: update entity field value
        expected: error raised
        '''
        tmp_entity = update_field_value(copy.deepcopy(default_entity), DataType.FLOAT, 's')
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_field_more(self, connect, collection):
        '''
        target: test insert entities, with more fields than collection schema
        method: add entity field
        expected: error raised
        '''
        tmp_entity = add_field(copy.deepcopy(default_entity))
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_field_vector_more(self, connect, collection):
        '''
        target: test insert entities, with more fields than collection schema
        method: add entity vector field
        expected: error raised
        '''
        tmp_entity = add_vector_field(default_nb, default_dim)
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_field_less(self, connect, collection):
        '''
        target: test insert entities, with less fields than collection schema
        method: remove entity field
        expected: error raised
        '''
        tmp_entity = remove_field(copy.deepcopy(default_entity))
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_field_vector_less(self, connect, collection):
        '''
        target: test insert entities, with less fields than collection schema
        method: remove entity vector field
        expected: error raised
        '''
        tmp_entity = remove_vector_field(copy.deepcopy(default_entity))
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_no_field_vector_value(self, connect, collection):
        '''
        target: test insert entities, with no vector field value
        method: remove entity vector field
        expected: error raised
        '''
        tmp_entity = copy.deepcopy(default_entity)
        del tmp_entity[-1]["values"]
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_no_field_vector_type(self, connect, collection):
        '''
        target: test insert entities, with no vector field type
        method: remove entity vector field
        expected: error raised
        '''
        tmp_entity = copy.deepcopy(default_entity)
        del tmp_entity[-1]["type"]
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_no_field_vector_name(self, connect, collection):
        '''
        target: test insert entities, with no vector field name
        method: remove entity vector field
        expected: error raised
        '''
        tmp_entity = copy.deepcopy(default_entity)
        del tmp_entity[-1]["name"]
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    @pytest.mark.skip("to do add dim check ")
    @pytest.mark.level(2)
    @pytest.mark.timeout(30)
    def test_collection_insert_rows_count_multi_threading(self, args, collection):
        '''
        target: test collection rows_count is correct or not with multi threading
        method: create collection and insert entities in it(idmap),
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        if args["handler"] == "HTTP":
            pytest.skip("Skip test in http mode")
        thread_num = 8
        threads = []
        milvus = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"], try_connect=False)

        def insert(thread_i):
            logging.getLogger().info("In thread-%d" % thread_i)
            milvus.bulk_insert(collection, default_entities)
            milvus.flush([collection])

        for i in range(thread_num):
            t = MilvusTestThread(target=insert, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        res_count = milvus.count_entities(collection)
        assert res_count == thread_num * default_nb

    # TODO: unable to set config
    @pytest.mark.skip
    @pytest.mark.level(2)
    def _test_insert_disable_auto_flush(self, connect, collection):
        '''
        target: test insert entities, with disable autoflush
        method: disable autoflush and insert, get entity
        expected: the count is equal to 0
        '''
        delete_nums = 500
        disable_flush(connect)
        ids = connect.bulk_insert(collection, default_entities)
        res = connect.get_entity_by_id(collection, ids[:delete_nums])
        assert len(res) == delete_nums
        assert res[0] is None



class TestInsertBinary:
    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request):
        request.param["metric_type"] = "JACCARD"
        return request.param

    @pytest.mark.skip
    def test_insert_binary_entities(self, connect, binary_collection):
        '''
        target: test insert entities in binary collection
        method: create collection and insert binary entities in it
        expected: the collection row count equals to nb
        '''
        ids = connect.bulk_insert(binary_collection, default_binary_entities)
        assert len(ids) == default_nb
        connect.flush()
        assert connect.count_entities(binary_collection) == default_nb

    @pytest.mark.skip
    def test_insert_binary_entities_new(self, connect, binary_collection):
        '''
        target: test insert entities in binary collection
        method: create collection and insert binary entities in it
        expected: the collection row count equals to nb
        '''
        ids = connect.bulk_insert(binary_collection, default_binary_entities_new)
        assert len(ids) == default_nb
        connect.flush()
        assert connect.count_entities(binary_collection) == default_nb

    @pytest.mark.skip
    def test_insert_binary_tag(self, connect, binary_collection):
        '''
        target: test insert entities and create partition tag
        method: create collection and insert binary entities in it, with the partition_tag param
        expected: the collection row count equals to nb
        '''
        connect.create_partition(binary_collection, default_tag)
        ids = connect.bulk_insert(binary_collection, default_binary_entities, partition_tag=default_tag)
        assert len(ids) == default_nb
        assert connect.has_partition(binary_collection, default_tag)

    # TODO
    @pytest.mark.level(2)
    @pytest.mark.skip
    def test_insert_binary_multi_times(self, connect, binary_collection):
        '''
        target: test insert entities multi times and final flush
        method: create collection and insert binary entity multi and final flush
        expected: the collection row count equals to nb
        '''
        for i in range(default_nb):
            ids = connect.bulk_insert(binary_collection, default_binary_entity)
            assert len(ids) == 1
        connect.flush([binary_collection])
        assert connect.count_entities(binary_collection) == default_nb

    @pytest.mark.skip
    def test_insert_binary_after_create_index(self, connect, binary_collection, get_binary_index):
        '''
        target: test insert binary entities after build index
        method: build index and insert entities
        expected: no error raised
        '''
        connect.create_index(binary_collection, binary_field_name, get_binary_index)
        ids = connect.bulk_insert(binary_collection, default_binary_entities)
        assert len(ids) == default_nb
        connect.flush([binary_collection])
        info = connect.get_collection_info(binary_collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == binary_field_name:
                assert field["indexes"][0] == get_binary_index

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_binary_create_index(self, connect, binary_collection, get_binary_index):
        '''
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        '''
        ids = connect.bulk_insert(binary_collection, default_binary_entities)
        assert len(ids) == default_nb
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_binary_index)
        info = connect.get_collection_info(binary_collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == binary_field_name:
                assert field["indexes"][0] == get_binary_index

    @pytest.mark.skip
    def test_insert_binary_search(self, connect, binary_collection):
        '''
        target: test search vector after insert vector after a while
        method: insert vector, sleep, and search collection
        expected: no error raised
        '''
        ids = connect.bulk_insert(binary_collection, default_binary_entities)
        connect.flush([binary_collection])
        query, vecs = gen_query_vectors(binary_field_name, default_binary_entities, default_top_k, 1, metric_type="JACCARD")
        res = connect.search(binary_collection, query)
        logging.getLogger().debug(res)
        assert res


class TestInsertAsync:
    @pytest.fixture(scope="function", autouse=True)
    def skip_http_check(self, args):
        if args["handler"] == "HTTP":
            pytest.skip("skip in http mode")

    @pytest.fixture(
        scope="function",
        params=[
            1,
            1000
        ],
    )
    def insert_count(self, request):
        yield request.param

    def check_status(self, result):
        logging.getLogger().info("In callback check status")
        assert not result

    def check_result(self, result):
        logging.getLogger().info("In callback check status")
        assert result

    def test_insert_async(self, connect, collection, insert_count):
        '''
        target: test insert vectors with different length of vectors
        method: set different vectors as insert method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        future = connect.bulk_insert(collection, gen_entities(nb), _async=True)
        ids = future.result()
        connect.flush([collection])
        assert len(ids) == nb

    @pytest.mark.level(2)
    def test_insert_async_false(self, connect, collection, insert_count):
        '''
        target: test insert vectors with different length of vectors
        method: set different vectors as insert method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        ids = connect.bulk_insert(collection, gen_entities(nb), _async=False)
        # ids = future.result()
        connect.flush([collection])
        assert len(ids) == nb

    def test_insert_async_callback(self, connect, collection, insert_count):
        '''
        target: test insert vectors with different length of vectors
        method: set different vectors as insert method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        future = connect.bulk_insert(collection, gen_entities(nb), _async=True, _callback=self.check_result)
        future.done()
        ids = future.result()
        assert len(ids) == nb


    @pytest.mark.skip(" not support count entites ")
    @pytest.mark.level(2)
    def test_insert_async_long(self, connect, collection):
        '''
        target: test insert vectors with different length of vectors
        method: set different vectors as insert method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = 50000
        future = connect.bulk_insert(collection, gen_entities(nb), _async=True, _callback=self.check_result)
        result = future.result()
        assert len(result) == nb
        connect.flush([collection])
        count = connect.count_entities(collection)
        logging.getLogger().info(count)
        assert count == nb

    @pytest.mark.skip(" not support count entites ")
    @pytest.mark.level(2)
    def test_insert_async_callback_timeout(self, connect, collection):
        '''
        target: test insert vectors with different length of vectors
        method: set different vectors as insert method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = 100000
        future = connect.bulk_insert(collection, gen_entities(nb), _async=True, _callback=self.check_status, timeout=1)
        with pytest.raises(Exception) as e:
            result = future.result()
        count = connect.count_entities(collection)
        assert count == 0

    @pytest.mark.skip(" later check")
    def test_insert_async_invalid_params(self, connect):
        '''
        target: test insert vectors with different length of vectors
        method: set different vectors as insert method params
        expected: raise exception
        '''
        collection_new = gen_unique_str()
        with pytest.raises(Exception) as e:
            future = connect.bulk_insert(collection_new, default_entities, _async=True)
            result = future.result()

    def test_insert_async_invalid_params_raise_exception(self, connect, collection):
        '''
        target: test insert vectors with different length of vectors
        method: set different vectors as insert method params
        expected: raise exception
        '''
        entities = []
        with pytest.raises(Exception) as e:
            future = connect.bulk_insert(collection, entities, _async=True)
            future.result()


class TestInsertMultiCollections:
    """
    ******************************************************************
      The following cases are used to test `insert` function
    ******************************************************************
    """

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

    @pytest.mark.skip
    def test_insert_vector_multi_collections(self, connect):
        '''
        target: test insert entities
        method: create 10 collections and insert entities into them in turn
        expected: row count
        '''
        collection_num = 10
        collection_list = []
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            ids = connect.bulk_insert(collection_name, default_entities)
            connect.flush([collection_name])
            assert len(ids) == default_nb
            count = connect.count_entities(collection_name)
            assert count == default_nb

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_drop_collection_insert_vector_another(self, connect, collection):
        '''
        target: test insert vector to collection_1 after collection_2 deleted
        method: delete collection_2 and insert vector to collection_1
        expected: row count equals the length of entities inserted
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        connect.drop_collection(collection)
        ids = connect.bulk_insert(collection_name, default_entity)
        connect.flush([collection_name])
        assert len(ids) == 1

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_create_index_insert_vector_another(self, connect, collection, get_simple_index):
        '''
        target: test insert vector to collection_2 after build index for collection_1
        method: build index and insert vector
        expected: status ok
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        connect.create_index(collection, field_name, get_simple_index)
        ids = connect.bulk_insert(collection, default_entity)
        connect.drop_collection(collection_name)

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_vector_create_index_another(self, connect, collection, get_simple_index):
        '''
        target: test insert vector to collection_2 after build index for collection_1
        method: build index and insert vector
        expected: status ok
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        ids = connect.bulk_insert(collection, default_entity)
        connect.create_index(collection, field_name, get_simple_index)
        count = connect.count_entities(collection_name)
        assert count == 0

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_vector_sleep_create_index_another(self, connect, collection, get_simple_index):
        '''
        target: test insert vector to collection_2 after build index for collection_1 for a while
        method: build index and insert vector
        expected: status ok
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        ids = connect.bulk_insert(collection, default_entity)
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        count = connect.count_entities(collection)
        assert count == 1

    @pytest.mark.skip
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_search_vector_insert_vector_another(self, connect, collection):
        '''
        target: test insert vector to collection_1 after search collection_2
        method: search collection and insert vector
        expected: status ok
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        ids = connect.bulk_insert(collection_name, default_entity)
        connect.flush()
        count = connect.count_entities(collection_name)
        assert count == 1

    @pytest.mark.skip(" todo fix search ")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_vector_search_vector_another(self, connect, collection):
        '''
        target: test insert vector to collection_1 after search collection_2
        method: search collection and insert vector
        expected: status ok
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        ids = connect.bulk_insert(collection, default_entity)
        result = connect.search(collection_name, default_single_query)

    @pytest.mark.skip(" todo fix search ")
    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_vector_sleep_search_vector_another(self, connect, collection):
        '''
        target: test insert vector to collection_1 after search collection_2 a while
        method: search collection , sleep, and insert vector
        expected: status ok
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        ids = connect.bulk_insert(collection, default_entity)
        connect.flush([collection])
        result = connect.search(collection_name, default_single_query)


class TestInsertInvalid(object):
    """
    Test inserting vectors with invalid collection names
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_type(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_int_value(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_entity_id(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_vectors()
    )
    def get_field_vectors_value(self, request):
        yield request.param

    def test_insert_ids_invalid(self, connect, id_collection, get_entity_id):
        '''
        target: test insert, with using customize ids, which are not int64
        method: create collection and insert entities in it
        expected: raise an exception
        '''
        entity_id = get_entity_id
        ids = [entity_id for _ in range(default_nb)]
        with pytest.raises(Exception):
            connect.bulk_insert(id_collection, default_entities, ids)

    def test_insert_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception):
            connect.bulk_insert(collection_name, default_entity)

    def test_insert_with_invalid_tag_name(self, connect, collection, get_tag_name):
        tag_name = get_tag_name
        connect.create_partition(collection, default_tag)
        if tag_name is not None:
            with pytest.raises(Exception):
                connect.bulk_insert(collection, default_entity, partition_tag=tag_name)
        else:
            connect.bulk_insert(collection, default_entity, partition_tag=tag_name)

    def test_insert_with_invalid_field_name(self, connect, collection, get_field_name):
        field_name = get_field_name
        tmp_entity = update_field_name(copy.deepcopy(default_entity), "int64", get_field_name)
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    @pytest.mark.skip("laster add check of field type")
    def test_insert_with_invalid_field_type(self, connect, collection, get_field_type):
        field_type = get_field_type
        tmp_entity = update_field_type(copy.deepcopy(default_entity), 'float', field_type)
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    @pytest.mark.skip("laster add check of field value")
    def test_insert_with_invalid_field_value(self, connect, collection, get_field_int_value):
        field_value = get_field_int_value
        tmp_entity = update_field_type(copy.deepcopy(default_entity), 'int64', field_value)
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)

    def test_insert_with_invalid_field_vector_value(self, connect, collection, get_field_vectors_value):
        tmp_entity = copy.deepcopy(default_entity)
        src_vector = tmp_entity[-1]["values"]
        src_vector[0][1] = get_field_vectors_value
        with pytest.raises(Exception):
            connect.bulk_insert(collection, tmp_entity)


class TestInsertInvalidBinary(object):
    """
    Test inserting vectors with invalid collection names
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_type(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_int_value(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_entity_id(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_vectors()
    )
    def get_field_vectors_value(self, request):
        yield request.param

    # @pytest.mark.skip
    @pytest.mark.level(2)
    def test_insert_with_invalid_field_name(self, connect, binary_collection, get_field_name):
        tmp_entity = update_field_name(copy.deepcopy(default_binary_entity), "int64", get_field_name)
        with pytest.raises(Exception):
            connect.bulk_insert(binary_collection, tmp_entity)

    @pytest.mark.skip
    @pytest.mark.level(2)
    def test_insert_with_invalid_field_value(self, connect, binary_collection, get_field_int_value):
        tmp_entity = update_field_type(copy.deepcopy(default_binary_entity), 'int64', get_field_int_value)
        with pytest.raises(Exception):
            connect.bulk_insert(binary_collection, tmp_entity)

    @pytest.mark.skip
    @pytest.mark.level(2)
    def test_insert_with_invalid_field_vector_value(self, connect, binary_collection, get_field_vectors_value):
        tmp_entity = copy.deepcopy(default_binary_entity)
        src_vector = tmp_entity[-1]["values"]
        src_vector[0][1] = get_field_vectors_value
        with pytest.raises(Exception):
            connect.bulk_insert(binary_collection, tmp_entity)

    @pytest.mark.skip
    @pytest.mark.level(2)
    def test_insert_ids_invalid(self, connect, binary_id_collection, get_entity_id):
        '''
        target: test insert, with using customize ids, which are not int64
        method: create collection and insert entities in it
        expected: raise an exception
        '''
        entity_id = get_entity_id
        ids = [entity_id for _ in range(default_nb)]
        with pytest.raises(Exception):
            connect.bulk_insert(binary_id_collection, default_binary_entities, ids)

    @pytest.mark.skip
    @pytest.mark.level(2)
    def test_insert_with_invalid_field_type(self, connect, binary_collection, get_field_type):
        field_type = get_field_type
        tmp_entity = update_field_type(copy.deepcopy(default_binary_entity), 'int64', field_type)
        with pytest.raises(Exception):
            connect.bulk_insert(binary_collection, tmp_entity)

    @pytest.mark.skip
    @pytest.mark.level(2)
    def test_insert_with_invalid_field_vector_value(self, connect, binary_collection, get_field_vectors_value):
        tmp_entity = copy.deepcopy(default_binary_entities)
        src_vector = tmp_entity[-1]["values"]
        src_vector[1] = get_field_vectors_value
        with pytest.raises(Exception):
            connect.bulk_insert(binary_collection, tmp_entity)
