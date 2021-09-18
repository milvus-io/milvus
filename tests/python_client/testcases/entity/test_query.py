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
