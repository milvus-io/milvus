import pdb
import logging

import pytest
from pymilvus import DataType

import utils.utils as ut
from common.common_type import CaseLabel

default_entities = ut.gen_entities(ut.default_nb, is_normal=True)
raw_vectors, default_binary_entities = ut.gen_binary_entities(ut.default_nb)
default_int_field_name = "int64"
default_float_field_name = "float"
default_pos = 5 
default_term_expr = f'{default_int_field_name} in {[i for i in range(default_pos)]}'


def init_data(connect, collection, nb=ut.default_nb, partition_names=None, auto_id=True):
    """
    Generate entities and add it in collection
    """
    if nb == 3000:
        insert_entities = default_entities
    else:
        insert_entities = ut.gen_entities(nb, is_normal=True)
    if partition_names is None:
        if auto_id:
            res = connect.insert(collection, insert_entities)
        else:
            res = connect.insert(collection, insert_entities, ids=[i for i in range(nb)])
    else:
        if auto_id:
            res = connect.insert(collection, insert_entities, partition_name=partition_names)
        else:
            res = connect.insert(collection, insert_entities, ids=[i for i in range(nb)],
                                 partition_name=partition_names)
    connect.flush([collection])
    ids = res.primary_keys
    return insert_entities, ids


def init_binary_data(connect, collection, nb=3000, insert=True, partition_names=None):
    """
    Generate entities and add it in collection
    """
    ids = []
    # global binary_entities
    global raw_vectors
    if nb == 3000:
        insert_entities = default_binary_entities
        insert_raw_vectors = raw_vectors
    else:
        insert_raw_vectors, insert_entities = ut.gen_binary_entities(nb)
    if insert is True:
        if partition_names is None:
            res = connect.insert(collection, insert_entities)
        else:
            res = connect.insert(collection, insert_entities, partition_name=partition_names)
        connect.flush([collection])
        ids = res.primary_keys
    return insert_raw_vectors, insert_entities, ids


class TestQueryBase:
    """
    test Query interface
    query(collection_name, expr, output_fields=None, partition_names=None, timeout=None)
    """

    @pytest.fixture(
        scope="function",
        params=ut.gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=ut.gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        return request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_invalid(self, connect, collection):
        """
        target: test query
        method: query with term expr
        expected: verify query result
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        term_expr = f'{default_int_field_name} in {entities[:default_pos]}'
        with pytest.raises(Exception):
            res = connect.query(collection, term_expr)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_valid(self, connect, collection):
        """
        target: test query
        method: query with term expr
        expected: verify query result
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        term_expr = f'{default_int_field_name} in {ids[:default_pos]}'
        res = connect.query(collection, term_expr, output_fields=["*", "%"])
        assert len(res) == default_pos
        for _id, index in enumerate(ids[:default_pos]):
            if res[index][default_int_field_name] == entities[0]["values"][index]:
                assert res[index][default_float_field_name] == entities[1]["values"][index]
        res = connect.query(collection, term_expr, output_fields=[ut.default_float_vec_field_name])
        assert len(res) == default_pos
        for _id, index in enumerate(ids[:default_pos]):
            if res[index][default_int_field_name] == entities[0]["values"][index]:
                ut.assert_equal_vector(res[index][ut.default_float_vec_field_name], entities[2]["values"][index])

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_collection_not_existed(self, connect):
        """
        target: test query not existed collection
        method: query not existed collection
        expected: raise exception
        """
        collection = "not_exist"
        with pytest.raises(Exception):
            connect.query(collection, default_term_expr)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_invalid_collection_name(self, connect, get_collection_name):
        """
        target: test query with invalid collection name
        method: query with invalid collection name
        expected: raise exception
        """
        collection_name = get_collection_name
        with pytest.raises(Exception):
            connect.query(collection_name, default_term_expr)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("expr", [1, "1", "12-s", "中文", [], {}, ()])
    def test_query_expr_invalid_string(self, connect, collection, expr):
        """
        target: test query with non-string expr
        method: query with non-string expr, eg 1, [] ..
        expected: raise exception
        """
        # entities, ids = init_data(connect, collection)
        # assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        with pytest.raises(Exception):
            connect.query(collection, expr)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_expr_single_term_array(self, connect, collection):
        """
        target: test query with single array term expr
        method: query with single array value
        expected: query result is one entity
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        term_expr = f'{default_int_field_name} in [0]'
        res = connect.query(collection, term_expr, output_fields=["*", "%"])
        assert len(res) == 1
        assert res[0][default_int_field_name] == entities[0]["values"][0]
        assert res[0][default_float_field_name] == entities[1]["values"][0]
        ut.assert_equal_vector(res[0][ut.default_float_vec_field_name], entities[2]["values"][0])

    @pytest.mark.xfail(reason="#6072")
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_binary_expr_single_term_array(self, connect, binary_collection):
        """
        target: test query with single array term expr
        method: query with single array value
        expected: query result is one entity
        """
        _, binary_entities, ids = init_binary_data(connect, binary_collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(binary_collection)
        term_expr = f'{default_int_field_name} in [0]'
        res = connect.query(binary_collection, term_expr, output_fields=["*", "%"])
        assert len(res) == 1
        assert res[0][default_int_field_name] == binary_entities[0]["values"][0]
        assert res[1][default_float_field_name] == binary_entities[1]["values"][0]
        assert res[2][ut.default_float_vec_field_name] == binary_entities[2]["values"][0]

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_expr_all_term_array(self, connect, collection):
        """
        target: test query with all array term expr
        method: query with all array value
        expected: verify query result
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        term_expr = f'{default_int_field_name} in {ids}'
        res = connect.query(collection, term_expr, output_fields=["*", "%"])
        assert len(res) == ut.default_nb
        for _id, index in enumerate(ids):
            if res[index][default_int_field_name] == entities[0]["values"][index]:
                assert res[index][default_float_field_name] == entities[1]["values"][index]
                ut.assert_equal_vector(res[index][ut.default_float_vec_field_name], entities[2]["values"][index])

    # @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="Need to discuss")
    def test_query_expr_repeated_term_array(self, connect, collection):
        """
        target: test query with repeated term array on primary field with unique value
        method: query with repeated array value
        expected: verify query result
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        int_values = [0, 0]
        term_expr = f'{default_int_field_name} in {int_values}'
        res = connect.query(collection, term_expr)
        assert len(res) == 2 

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_expr_inconstant_term_array(self, connect, collection):
        """
        target: test query with term expr that field and array are inconsistent
        method: query with int field and float values
        expected: raise exception
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        expr = f'{default_int_field_name} in [1.0, 2.0]'
        with pytest.raises(Exception):
            connect.query(collection, expr)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_expr_mix_term_array(self, connect, collection):
        """
        target: test query with mix type value expr
        method: query with term expr that has int and float type value
        expected: todo
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        expr = f'{default_int_field_name} in [1, 2.]'
        with pytest.raises(Exception):
            connect.query(collection, expr)

    @pytest.mark.parametrize("constant", [[1], (), {}])
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_expr_non_constant_array_term(self, connect, collection, constant):
        """
        target: test query with non-constant array term expr
        method: query with non-constant array expr
        expected: raise exception
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        expr = f'{default_int_field_name} in [{constant}]'
        with pytest.raises(Exception):
            connect.query(collection, expr)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_field_empty(self, connect, collection):
        """
        target: test query with none output field
        method: query with output field=None
        expected: return all fields
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        res = connect.query(collection, default_term_expr, output_fields=[])
        assert default_int_field_name in res[0].keys()
        assert default_float_field_name not in res[0].keys()
        assert ut.default_float_vec_field_name not in res[0].keys()

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_one_field(self, connect, collection):
        """
        target: test query with output one field
        method: query with output one field
        expected: return one field
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        res = connect.query(collection, default_term_expr, output_fields=[default_int_field_name])
        assert default_int_field_name in res[0].keys()
        assert len(res[0].keys()) == 1

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_all_fields(self, connect, collection):
        """
        target: test query with none output field
        method: query with output field=None
        expected: return all fields
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        # fields = [default_int_field_name, default_float_field_name, ut.default_float_vec_field_name]
        fields = [default_int_field_name, default_float_field_name]
        res = connect.query(collection, default_term_expr, output_fields=fields)
        for field in fields:
            assert field in res[0].keys()

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_not_existed_field(self, connect, collection):
        """
        target: test query output not existed field
        method: query with not existed output field
        expected: raise exception
        """
        entities, ids = init_data(connect, collection)
        connect.load_collection(collection)
        with pytest.raises(Exception):
            connect.query(collection, default_term_expr, output_fields=["int"])

    # @pytest.mark.xfail(reason="#6074")
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_part_not_existed_field(self, connect, collection):
        """
        target: test query output part not existed field
        method: query with part not existed field
        expected: raise exception
        """
        entities, ids = init_data(connect, collection)
        connect.load_collection(collection)
        with pytest.raises(Exception):
            connect.query(collection, default_term_expr, output_fields=[default_int_field_name, "int"])

    @pytest.mark.parametrize("fields", ut.gen_invalid_strs())
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_invalid_output_fields(self, connect, collection, fields):
        """
        target: test query with invalid output fields
        method: query with invalid field fields
        expected: raise exception
        """
        entities, ids = init_data(connect, collection)
        connect.load_collection(collection)
        with pytest.raises(Exception):
            connect.query(collection, default_term_expr, output_fields=[fields])


class TestQueryPartition:
    """
    test Query interface
    query(collection_name, expr, output_fields=None, partition_names=None, timeout=None)
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_partition(self, connect, collection):
        """
        target: test query on partition
        method: create a partition and query
        expected: verify query result
        """
        connect.create_partition(collection, ut.default_tag)
        entities, ids = init_data(connect, collection, partition_names=ut.default_tag)
        assert len(ids) == ut.default_nb
        connect.load_partitions(collection, [ut.default_tag])
        res = connect.query(collection, default_term_expr, partition_names=[ut.default_tag], output_fields=["*", "%"])
        for _id, index in enumerate(ids[:default_pos]):
            if res[index][default_int_field_name] == entities[0]["values"][index]:
                assert res[index][default_float_field_name] == entities[1]["values"][index]
                ut.assert_equal_vector(res[index][ut.default_float_vec_field_name], entities[2]["values"][index])
  
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_partition_without_loading(self, connect, collection):
        """
        target: test query on partition without loading
        method: query on partition and no loading
        expected: raise exception
        """
        connect.create_partition(collection, ut.default_tag)
        entities, ids = init_data(connect, collection, partition_names=ut.default_tag)
        assert len(ids) == ut.default_nb
        with pytest.raises(Exception):
            connect.query(collection, default_term_expr, partition_names=[ut.default_tag])

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_default_partition(self, connect, collection):
        """
        target: test query on default partition
        method: query on default partition
        expected: verify query result
        """
        entities, ids = init_data(connect, collection)
        assert len(ids) == ut.default_nb
        connect.load_collection(collection)
        res = connect.query(collection, default_term_expr, partition_names=[ut.default_partition_name], output_fields=["*", "%"])
        for _id, index in enumerate(ids[:default_pos]):
            if res[index][default_int_field_name] == entities[0]["values"][index]:
                assert res[index][default_float_field_name] == entities[1]["values"][index]
                ut.assert_equal_vector(res[index][ut.default_float_vec_field_name], entities[2]["values"][index])

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_empty_partition(self, connect, collection):
        """
        target: test query on empty partition
        method: query on a empty collection
        expected: empty query result
        """
        connect.create_partition(collection, ut.default_tag)
        connect.load_partitions(collection, [ut.default_partition_name])
        res = connect.query(collection, default_term_expr, partition_names=[ut.default_partition_name])
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_not_existed_partition(self, connect, collection):
        """
        target: test query on a not existed partition
        method: query on not existed partition
        expected: raise exception
        """
        connect.load_partitions(collection, [ut.default_partition_name])
        tag = ut.gen_unique_str()
        with pytest.raises(Exception):
            connect.query(collection, default_term_expr, partition_names=[tag])

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_partition_repeatedly(self, connect, collection):
        """
        target: test query repeatedly on partition
        method: query on partition twice
        expected: verify query result
        """
        connect.create_partition(collection, ut.default_tag)
        entities, ids = init_data(connect, collection, partition_names=ut.default_tag)
        assert len(ids) == ut.default_nb
        connect.load_partitions(collection, [ut.default_tag])
        res_one = connect.query(collection, default_term_expr, partition_names=[ut.default_tag])
        res_two = connect.query(collection, default_term_expr, partition_names=[ut.default_tag])
        assert res_one == res_two

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_another_partition(self, connect, collection):
        """
        target: test query another partition
        method: 1. insert entities into two partitions
                2. query on one partition and query result empty
        expected: query result is empty
        """
        insert_entities_into_two_partitions_in_half(connect, collection)
        half = ut.default_nb // 2
        term_expr = f'{default_int_field_name} in [{half}]'
        res = connect.query(collection, term_expr, partition_names=[ut.default_tag])
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_multi_partitions_multi_results(self, connect, collection):
        """
        target: test query on multi partitions and get multi results
        method: 1.insert entities into two partitions
                2.query on two partitions and query multi result
        expected: query results from two partitions
        """
        entities, entities_2 = insert_entities_into_two_partitions_in_half(connect, collection)
        half = ut.default_nb // 2
        term_expr = f'{default_int_field_name} in [{half - 1}]'
        res = connect.query(collection, term_expr, partition_names=[ut.default_tag, ut.default_partition_name])
        assert len(res) == 1
        assert res[0][default_int_field_name] == entities[0]["values"][-1]
        term_expr = f'{default_int_field_name} in [{half}]'
        res = connect.query(collection, term_expr, partition_names=[ut.default_tag, ut.default_partition_name])
        assert len(res) == 1
        assert res[0][default_int_field_name] == entities_2[0]["values"][0]

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_multi_partitions_single_result(self, connect, collection):
        """
        target: test query on multi partitions and get single result
        method: 1.insert into two partitions
                2.query on two partitions and query single result
        expected: query from two partitions and get single result
        """
        entities, entities_2 = insert_entities_into_two_partitions_in_half(connect, collection)
        half = ut.default_nb // 2
        term_expr = f'{default_int_field_name} in [{half}]'
        res = connect.query(collection, term_expr, partition_names=[ut.default_tag, ut.default_partition_name])
        assert len(res) == 1
        assert res[0][default_int_field_name] == entities_2[0]["values"][0]


def insert_entities_into_two_partitions_in_half(connect, collection):
    """
    insert default entities into two partitions(default_tag and _default) in half(int64 and float fields values)
    :param connect: milvus connect
    :param collection: milvus created collection
    :return: entities of default_tag and entities_2 of _default
    """
    connect.create_partition(collection, ut.default_tag)
    half = ut.default_nb // 2
    entities, _ = init_data(connect, collection, nb=half, partition_names=ut.default_tag)
    vectors = ut.gen_vectors(half, ut.default_dim)
    entities_2 = [
        {"name": "int64", "type": DataType.INT64, "values": [i for i in range(half, ut.default_nb)]},
        {"name": "float", "type": DataType.FLOAT, "values": [float(i) for i in range(half, ut.default_nb)]},
        {"name": ut.default_float_vec_field_name, "type": DataType.FLOAT_VECTOR, "values": vectors}
    ]
    connect.insert(collection, entities_2)
    connect.flush([collection])
    connect.load_collection(collection)
    return entities, entities_2
