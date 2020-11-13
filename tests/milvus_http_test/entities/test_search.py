import logging
import pytest
import requests
from utils import *
from constants import *

uid = "test_search"
epsilon = 0.001
field_name = default_float_vec_field_name
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
default_query, default_query_vectors = gen_query_vectors(field_name, default_entities, default_top_k, default_nq)


def init_data(client, collection, nb=default_nb, partition_tags=None, auto_id=True):
    insert_entities = default_entities if nb == default_nb else gen_entities(nb)
    if partition_tags is None:
        if auto_id:
            ids = client.insert(collection, insert_entities)
        else:
            ids = client.insert(collection, insert_entities, ids=[i for i in range(nb)])
    else:
        if auto_id:
            ids = client.insert(collection, insert_entities, partition_tag=partition_tags)
        else:
            ids = client.insert(collection, insert_entities, ids=[i for i in range(nb)], partition_tag=partition_tags)
    client.flush([collection])
    assert client.count_collection(collection) == nb
    return insert_entities, ids


def init_binary_data(client, collection, nb=default_nb, partition_tags=None, auto_id=True):
    """
    Generate binary entities and insert to collection
    """
    if nb == default_nb:
        insert_entities = default_binary_entities
        insert_raw_vectors = default_raw_binary_vectors
    else:
        insert_raw_vectors, insert_entities = gen_binary_entities(nb)
    if partition_tags is None:
        if auto_id:
            ids = client.insert(collection, insert_entities)
        else:
            ids = client.insert(collection, insert_entities, ids=[i for i in range(nb)])
    else:
        if auto_id:
            ids = client.insert(collection, insert_entities, partition_tag=partition_tags)
        else:
            ids = client.insert(collection, insert_entities, ids=[i for i in range(nb)], partition_tag=partition_tags)
    client.flush([collection])
    assert client.count_collection(collection) == nb
    return insert_raw_vectors, insert_entities, ids


def check_id_result(results, ids):
    ids_res = []
    for result in results:
        ids_res.extend([int(item['id']) for item in result])
    for id in ids:
        if int(id) not in ids_res:
            return False
    return True


class TestSearchBase:
    """
    ******************************************************************
      The following cases are used to test `search` function
    ******************************************************************
    """

    """
        generate top-k params
        """

    @pytest.fixture(
        scope="function",
        params=[1, 10]
    )
    def get_top_k(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=[1, 10]
    )
    def get_nq(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("sq8h not support in CPU mode")
        return request.param

    def test_search_flat(self, client, collection, get_nq, get_top_k):
        """
        target: test basic search function, all the search params is correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of result is top_k
        """
        top_k = get_top_k
        nq = get_nq
        entities, ids = init_data(client, collection)
        query, query_vectors = gen_query_vectors(field_name, entities, top_k, nq)
        data = client.search(collection, query)
        res = data['result']
        assert data['nq'] == nq
        assert len(res) == nq
        assert len(res[0]) == top_k
        assert float(res[0][0]['distance']) <= epsilon
        assert check_id_result(res, ids[:nq])

    # TODO
    def test_search_invalid_top_k(self, client, collection):
        """
        target: test search with invalid top_k that large than max_top_k
        method: call search with invalid top_k
        expected: exception
        """
        top_k = max_top_k + 1
        nq = 1
        entities, ids = init_data(client, collection)
        query, query_vectors = gen_query_vectors(field_name, entities, top_k, nq)
        assert not client.search(collection, query)

    def test_search_fields(self, client, collection, ):
        """
        target: test search with field
        method: call search with field and check return whether contain field value
        expected: return field value
        """
        entities, ids = init_data(client, collection)
        query, query_vectors = gen_query_vectors(field_name, entities, default_top_k, default_nq)
        data = client.search(collection, query, fields=[default_int_field_name])
        res = data['result']
        assert data['nq'] == default_nq
        assert len(res) == default_nq
        assert len(res[0]) == default_top_k
        assert default_int_field_name in res[0][0]['entity'].keys()

    # TODO
    def test_search_invalid_n_probe(self, client, collection, ):
        """
        target: test basic search function with invalid n_probe
        method: call search function
        expected: not ok
        """
        entities, ids = init_data(client, collection)
        assert client.create_index(collection, default_float_vec_field_name, default_index)
        query, query_vectors = gen_query_vectors(field_name, entities, default_top_k, default_nq,
                                                 search_params={"nprobe": 0})
        assert not client.search(collection, query)

    def test_search_not_existed_collection(self, client, collection):
        """
        target: test basic search with not existed collection
        method: call search function
        expected: not ok
        """
        collection_name = gen_unique_str(uid)
        assert not client.search(collection_name, default_query)

    def test_search_empty_collection(self, client, collection):
        """
        target: test basic search function with empty collection
        method: call search function
        expected: return 0 entities
        """
        assert 0 == client.count_collection(collection)
        data = client.search(collection, default_query)
        res = data['result']
        assert data['nq'] == default_nq
        assert res[0] == None

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

    def test_get_collection_stats_name_invalid(self, client, collection, get_invalid_collection_name):
        """
        target: test search when collection name is invalid
        method: call search with invalid collection_name
        expected: status not ok
        """
        collection_name = get_invalid_collection_name
        assert not client.search(collection_name, default_query)

    # TODO
    def test_search_invalid_format_query(self, client, collection):
        """
        target: test search with invalid format query
        method: call search with invalid query string
        expected: status not ok and url `/collections/xxx/entities` return correct
        """
        entities, ids = init_data(client, collection)
        must_param = {"vector": {field_name: {"topk": default_top_k, "query": [[[]]], "params": {"nprobe": 10}}}}
        must_param["vector"][field_name]["metric_type"] = 'L2'
        query = {
            "bool": {
                "must": [must_param]
            }
        }
        assert not client.search(collection, query)

    # TODO
    def test_search_with_invalid_metric_type(self, client, collection):
        """
        target: test search function with invalid metric type
        method:
        expected:
        """
        entities, ids = init_data(client, collection)
        query, query_vectors = gen_query_vectors(field_name, entities, default_top_k, default_nq, metric_type="l1")
        assert not client.search(collection, query)

    def test_search_with_empty_partition(self, client, collection):
        """
        target: test search function with empty partition
        method: create collection and insert entities, then create partition and search with partition
        expected: empty result
        """
        entities, ids = init_data(client, collection)
        client.create_partition(collection, default_tag)
        query, query_vectors = gen_query_vectors(field_name, entities, default_top_k, default_nq)
        data = client.search(collection, query, partition_tags=default_tag)
        res = data['result']
        assert data['nq'] == default_nq
        assert len(res) == default_nq
        assert len(res[0]) == 0

    def test_search_binary_flat(self, client, binary_collection):
        """
        target: test basic search function on binary collection
        method: call search function with binary query vectors
        expected:
        """
        raw_vectors, binary_entities, ids = init_binary_data(client, binary_collection)
        query, query_vectors = gen_query_vectors(default_binary_vec_field_name, binary_entities, default_top_k,default_nq, metric_type='JACCARD')
        data = client.search(binary_collection, query)
        res = data['result']
        assert data['nq'] == default_nq
        assert len(res) == default_nq
        assert len(res[0]) == default_top_k
        assert float(res[0][0]['distance']) <= epsilon
        assert check_id_result(res, ids[:default_nq])
