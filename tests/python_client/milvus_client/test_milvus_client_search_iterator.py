import pytest
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import DataType
from base.client_v2_base import TestMilvusClientV2Base

prefix = "milvus_client_api_search_iterator"
epsilon = ct.epsilon
user_pre = "user"
role_pre = "role"
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_batch_size = ct.default_batch_size
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name


def external_filter_half(hits):
    return hits[0: len(hits) // 2]


def external_filter_all(hits):
    return []


def external_filter_nothing(hits):
    return hits


def external_filter_invalid_arguments(hits, iaminvalid):
    pass


def external_filter_with_outputs(hits):
    results = []
    for hit in hits:
        # equals filter nothing if there are output_fields
        if hit.distance < 1.0 and len(hit.fields) > 0:
            results.append(hit)
    return results


class TestMilvusClientSearchIteratorInValid(TestMilvusClientV2Base):
    """ Test case of search iterator interface """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_iterator_using_mul_db(self):
        """
        target: test search iterator(high level api) case about mul db
        method: create connection, collection, insert and search iterator
        expected: search iterator error after switch to another db
        """
        batch_size = 20
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        my_db = cf.gen_unique_str(prefix)
        self.create_database(client, my_db)
        self.using_database(client, my_db)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.using_database(client, "default")
        # 3. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        # 4. insert
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 5. search_iterator
        vectors_to_search = cf.gen_vectors(1, default_dim)
        search_params = {"params": {}}
        error_msg = "alias or database may have been changed"
        self.search_iterator(client, collection_name, vectors_to_search, batch_size, search_params=search_params,
                             use_mul_db=True, another_db=my_db,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={ct.err_code: 1, ct.err_msg: error_msg})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_iterator_alias_different_col(self):
        """
        target: test search iterator(high level api) case about alias
        method: create connection, collection, insert and search iterator
        expected: search iterator error after alter alias
        """
        batch_size = 20
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        collection_name_new = cf.gen_unique_str(prefix)
        alias = cf.gen_unique_str("collection_alias")
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.create_alias(client, collection_name, alias)
        self.create_collection(client, collection_name_new, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name_new in collections
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.insert(client, collection_name_new, rows)
        self.flush(client, collection_name_new)
        # 3. search_iterator
        vectors_to_search = cf.gen_vectors(1, default_dim)
        search_params = {"params": {}}
        error_msg = "alias or database may have been changed"
        self.search_iterator(client, alias, vectors_to_search, batch_size, search_params=search_params,
                             use_alias=True, another_collection=collection_name_new,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={ct.err_code: 1, ct.err_msg: error_msg})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)
        self.release_collection(client, collection_name_new)
        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name_new)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip("ambiguous error info")
    def test_milvus_client_search_iterator_collection_not_existed(self):
        """
        target: test search iterator
        method: search iterator with nonexistent collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str("nonexistent")
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default]"
                             f"[collection={collection_name}]"}
        vectors_to_search = cf.gen_vectors(1, default_dim)
        insert_ids = [i for i in range(default_nb)]
        self.search_iterator(client, collection_name, vectors_to_search,
                             batch_size=5,
                             check_task=CheckTasks.err_res,
                             check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", ["str", [[1, 2], [3, 4]]])
    def test_milvus_client_search_iterator_with_multiple_vectors(self, data):
        """
        target: test search iterator with multiple vectors
        method: run search iterator with multiple vectors
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        error = {ct.err_code: 1,
                 ct.err_msg: f"search_iterator_v2 does not support processing multiple vectors simultaneously"}
        self.search_iterator(client, collection_name, data,
                             batch_size=5,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", [[]])
    def test_milvus_client_search_iterator_with_empty_data(self, data):
        """
        target: test search iterator with empty vector
        method: run search iterator with empty vector
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        error = {ct.err_code: 1,
                 ct.err_msg: f"The vector data for search cannot be empty"}
        self.search_iterator(client, collection_name, data,
                             batch_size=5,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("batch_size", [-1])
    def test_milvus_client_search_iterator_with_invalid_batch_size(self, batch_size):
        """
        target: test search iterator with invalid batch size
        method: run search iterator with invalid batch size
        expected: Raise exception
        """
        # These are two inappropriate error messages:
        # 1.5: `limit` value 1.5 is illegal
        # "1": '<' not supported between instances of 'str' and 'int'
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"batch size cannot be less than zero"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             batch_size=batch_size,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr", ["invalidexpr"])
    def test_milvus_client_search_iterator_with_invalid_expr(self, expr):
        """
        target: test search iterator with invalid expr
        method: run search iterator with invalid expr
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: predicate is not a boolean expression: invalidexpr, "
                             f"data type: JSON: invalid parameter"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             filter=expr,
                             batch_size=20,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [-10])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/39066")
    def test_milvus_client_search_iterator_with_invalid_limit(self, limit):
        """
        target: test search iterator with invalid limit
        method: run search iterator with invalid limit
        expected: Raise exception
        note: limit param of search_iterator will be deprecated in the future
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"`limit` value {limit} is illegal"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             batch_size=5,
                             limit=limit,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", ["id"])
    @pytest.mark.skip("A field that does not currently exist will simply have no effect, "
                      "but it would be better if an error were reported.")
    def test_milvus_client_search_iterator_with_invalid_output(self, output_fields):
        """
        target: test search iterator with nonexistent output field
        method: run search iterator with nonexistent output field
        expected: Raise exception
        actual: have no error, just have no effect
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"`output_fields` value {output_fields} is illegal"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             batch_size=5,
                             limit=10,
                             output_fields=output_fields,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_params", ["tt"])
    @pytest.mark.skip("A param that does not currently exist will simply have no effect, "
                      "but it would be better if an error were reported.")
    def test_milvus_client_search_iterator_with_invalid_search_params(self, search_params):
        """
        target: test search iterator with nonexistent search_params key
        method: run search iterator with nonexistent search_params key
        expected: Raise exception
        actual: have no error, just have no effect
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"'str' object has no attribute 'get'"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             batch_size=5,
                             limit=10,
                             output_fields=["id", "float", "varchar"],
                             search_params=search_params,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ["client_partition_85Jv3Pf3"])
    def test_milvus_client_search_iterator_with_invalid_partition_name(self, partition_name):
        """
        target: test search iterator with invalid partition name
        method: run search iterator with invalid partition name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        self.create_partition(client, collection_name, partition_name)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2,
                                              "num_partitions": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"`partition_name_array` value {partition_name} is illegal"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             partition_names=partition_name,
                             batch_size=5,
                             limit=10,
                             output_fields=["id", "float", "varchar"],
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ["nonexistent"])
    def test_milvus_client_search_iterator_with_nonexistent_partition_name(self, partition_name):
        """
        target: test search iterator with invalid partition name
        method: run search iterator with invalid partition name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 65535,
                 ct.err_msg: f"partition name {partition_name} not found"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             partition_names=[partition_name],
                             batch_size=5,
                             limit=10,
                             output_fields=["id", "float", "varchar"],
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("anns_field", ["nonexistent", ])
    def test_milvus_client_search_iterator_with_nonexistent_anns_field(self, anns_field):
        """
        target: test search iterator with nonexistent anns field
        method: run search iterator with nonexistent anns field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: failed to get field schema by name: "
                             f"fieldName({anns_field}) not found: invalid parameter"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             batch_size=5,
                             limit=10,
                             anns_field=anns_field,
                             output_fields=["id", "float", "varchar"],
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("round_decimal", ["tt"])
    def test_milvus_client_search_iterator_with_invalid_round_decimal(self, round_decimal):
        """
        target: test search iterator with invalid round_decimal
        method: run search iterator with invalid round_decimal
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 2})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"`round_decimal` value {round_decimal} is illegal"}
        self.search_iterator(client, collection_name, vectors_to_search,
                             batch_size=5,
                             limit=10,
                             round_decimal=round_decimal,
                             output_fields=["id", "float", "varchar"],
                             check_task=CheckTasks.err_res,
                             check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_iterator_with_invalid_external_func(self):
        """
        target: test search iterator (high level api) normal case
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully
        """
        batch_size = 20
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim})
        # 2. insert
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search iterator
        vectors_to_search = cf.gen_vectors(1, default_dim)
        search_params = {}
        with pytest.raises(TypeError, match="got an unexpected keyword argument 'metric_type'"):
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=100,
                                 external_filter_func=external_filter_invalid_arguments(metric_type="L2"),
                                 check_task=CheckTasks.check_nothing)
        it = self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                  search_params=search_params, limit=100,
                                  external_filter_func=external_filter_invalid_arguments,
                                  check_task=CheckTasks.check_nothing)[0]
        with pytest.raises(TypeError, match="missing 1 required positional argument: 'iaminvalid'"):
            it.next()


class TestMilvusClientSearchIteratorValid(TestMilvusClientV2Base):
    """ Test case of search iterator interface """

    @pytest.fixture(scope="function", params=[True, False])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["IP", "COSINE"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["INVERTED"])
    def supported_varchar_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["DOUBLE", "VARCHAR", "BOOL", "double", "varchar", "bool"])
    def supported_json_cast_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("metric_type", ct.float_metrics)
    def test_milvus_client_search_iterator_default(self, metric_type):
        """
        target: test search iterator (high level api) normal case
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully
        """
        batch_size = 20
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, metric_type=metric_type,
                               consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        # 2. insert
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0,
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search iterator
        vectors_to_search = cf.gen_vectors(1, default_dim)
        search_params = {"params": {}}
        self.search_iterator(client, collection_name=collection_name, data=vectors_to_search,
                             anns_field=default_vector_field_name,
                             search_params=search_params, batch_size=batch_size,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"metric_type": metric_type, "batch_size": batch_size})
        limit = 200
        res = self.search(client, collection_name, vectors_to_search,
                          search_params=search_params, limit=200,
                          check_task=CheckTasks.check_search_results,
                          check_items={"nq": 1, "limit": limit, "enable_milvus_client_api": True})[0]
        for limit in [batch_size - 3, batch_size, batch_size * 2, -1]:
            if metric_type != "L2":
                radius = res[0][limit // 2].get('distance', 0) - 0.1  # pick a radius to make sure there exists results
                range_filter = res[0][0].get('distance', 0) + 0.1
            else:
                radius = res[0][limit // 2].get('distance', 0) + 0.1
                range_filter = res[0][0].get('distance', 0) - 0.1
            search_params = {"params": {"radius": radius, "range_filter": range_filter}}
            log.debug(f"search iterator with limit={limit} radius={radius}, range_filter={range_filter}")
            expected_batch_size = batch_size if limit == -1 else min(batch_size, limit)
            # external filter not set
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter half
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 external_filter_func=external_filter_half,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter nothing
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 external_filter_func=external_filter_nothing,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter with outputs
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit, output_fields=["*"],
                                 external_filter_func=external_filter_with_outputs,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter all
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 external_filter_func=external_filter_all,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": 0, "iterate_times": 1})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    # @pytest.mark.tags(CaseLabel.L1)
    # @pytest.mark.parametrize("nullable", [True, False])
    # @pytest.mark.skip("TODO: need update the case steps and assertion")
    # def test_milvus_client_search_iterator_about_nullable_default(self, nullable, search_params):
    #     """
    #     target: test search iterator (high level api) normal case about nullable and default value
    #     method: create connection, collection, insert and search iterator
    #     expected: search iterator successfully
    #     """
    #     batch_size = 20
    #     client = self._client()
    #     collection_name = cf.gen_unique_str(prefix)
    #     dim = 128
    #     # 1. create collection
    #     schema = self.create_schema(client, enable_dynamic_field=False)[0]
    #     schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
    #                      auto_id=False)
    #     schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
    #     schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
    #     schema.add_field("nullable_field", DataType.INT64, nullable=True, default_value=10)
    #     schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
    #                      max_length=64, nullable=True)
    #     index_params = self.prepare_index_params(client)[0]
    #     index_params.add_index(default_vector_field_name, metric_type="COSINE")
    #     self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
    #     # 2. insert
    #     rows = [
    #         {default_primary_key_field_name: str(i), default_vector_field_name: list(cf.gen_vectors(1, dim)[0]),
    #          default_string_field_name: str(i), "nullable_field": None, "array_field": None} for i in range(default_nb)]
    #     self.insert(client, collection_name, rows)
    #     self.flush(client, collection_name)
    #     # 3. search iterator
    #     vectors_to_search = cf.gen_vectors(1, dim)
    #     insert_ids = [i for i in range(default_nb)]
    #     search_params = {"params": search_params}
    #     self.search_iterator(client, collection_name, vectors_to_search, batch_size, filter="nullable_field>=10",
    #                          search_params=search_params,
    #                          check_task=CheckTasks.check_search_iterator,
    #                          check_items={"enable_milvus_client_api": True,
    #                                       "nq": len(vectors_to_search),
    #                                       "ids": insert_ids,
    #                                       "limit": default_limit})
    #     if self.has_collection(client, collection_name)[0]:
    #         self.drop_collection(client, collection_name)
    #
    # @pytest.mark.tags(CaseLabel.L1)
    # @pytest.mark.skip("TODO: need update the case steps and assertion")
    # def test_milvus_client_rename_search_iterator_default(self, search_params):
    #     """
    #     target: test search iterator(high level api) normal case
    #     method: create connection, collection, insert and search iterator
    #     expected: search iterator successfully
    #     """
    #     batch_size = 20
    #     client = self._client()
    #     collection_name = cf.gen_unique_str(prefix)
    #     # 1. create collection
    #     self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
    #     collections = self.list_collections(client)[0]
    #     assert collection_name in collections
    #     self.describe_collection(client, collection_name,
    #                              check_task=CheckTasks.check_describe_collection_property,
    #                              check_items={"collection_name": collection_name,
    #                                           "dim": default_dim,
    #                                           "consistency_level": 0})
    #     old_name = collection_name
    #     new_name = collection_name + "new"
    #     self.rename_collection(client, old_name, new_name)
    #     # 2. insert
    #     rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
    #              default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
    #     self.insert(client, new_name, rows)
    #     self.flush(client, new_name)
    #     # assert self.num_entities(client, collection_name)[0] == default_nb
    #     # 3. search_iterator
    #     vectors_to_search = cf.gen_vectors(1, default_dim)
    #     insert_ids = [i for i in range(default_nb)]
    #     search_params = {"params": search_params}
    #     self.search_iterator(client, new_name, vectors_to_search, batch_size, search_params=search_params,
    #                          check_task=CheckTasks.check_search_iterator,
    #                          check_items={"enable_milvus_client_api": True,
    #                                       "nq": len(vectors_to_search),
    #                                       "ids": insert_ids,
    #                                       "limit": default_limit})
    #     self.release_collection(client, new_name)
    #     self.drop_collection(client, new_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize('id_type', ["int", "string"])
    def test_milvus_client_search_iterator_delete_with_ids(self, id_type):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search iterator
        expected: search iterator/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type=id_type, max_length=128,
                               consistency_level="Strong")
        # 2. insert
        default_nb = 2000
        if id_type == 'int':
            rows = [{default_primary_key_field_name: i,
                     default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                     default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        else:
            rows = [
                {default_primary_key_field_name: cf.gen_unique_str()+str(i),
                 default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)[0]
        # 3. search_iterator and delete
        vectors_to_search = cf.gen_vectors(1, default_dim)
        batch_size = 200
        search_params = {"params": {}}
        it = self.search_iterator(client, collection_name, vectors_to_search, batch_size=batch_size,
                                  search_params=search_params, limit=500,
                                  check_task=CheckTasks.check_nothing)[0]
        res = it.next()
        it.close()
        delete_ids = res.ids()
        self.delete(client, collection_name, ids=delete_ids)
        # search iterator again
        it2 = self.search_iterator(client, collection_name, vectors_to_search, batch_size=batch_size,
                                   search_params=search_params, limit=500,
                                   check_task=CheckTasks.check_nothing)[0]
        res2 = it2.next()
        it2.close()
        for del_id in delete_ids:
            assert del_id not in res2.ids()
        # search iterator again
        self.search_iterator(client, collection_name, vectors_to_search, batch_size=batch_size,
                             search_params=search_params, limit=500,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"batch_size": batch_size})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_search_iterator_external_filter_func_default(self):
        pass

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("metric_type", ct.float_metrics)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_search_iterator_after_json_path_index(self, metric_type, enable_dynamic_field,
                                                                 supported_json_cast_type,
                                                                 supported_varchar_scalar_index):
        """
        target: test search iterator after creating json path index
        method: Search iterator after creating json path index
        Step: 1. create schema
              2. prepare index_params with vector and all the json path index params
              3. create collection with the above schema and index params
              4. insert
              5. flush
              6. release collection
              7. load collection
              8. search iterator
        expected: Search successfully
        """
        batch_size = 20
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type=metric_type)
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, metric_type=metric_type)
        # 2. insert
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0,
                 default_string_field_name: str(i),
                 json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. release and load collection to make sure the new index is loaded
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        # 4. search iterator
        vectors_to_search = cf.gen_vectors(1, default_dim)
        search_params = {"params": {}}
        self.search_iterator(client, collection_name=collection_name, data=vectors_to_search,
                             anns_field=default_vector_field_name,
                             search_params=search_params, batch_size=batch_size,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"metric_type": metric_type, "batch_size": batch_size})
        limit = 200
        res = self.search(client, collection_name, vectors_to_search,
                          search_params=search_params, limit=limit,
                          check_task=CheckTasks.check_search_results,
                          check_items={"nq": 1, "limit": limit, "enable_milvus_client_api": True})[0]
        for limit in [batch_size - 3, batch_size, batch_size * 2, -1]:
            if metric_type != "L2":
                radius = res[0][limit // 2].get('distance', 0) - 0.1  # pick a radius to make sure there exists results
                range_filter = res[0][0].get('distance', 0) + 0.1
            else:
                radius = res[0][limit // 2].get('distance', 0) + 0.1
                range_filter = res[0][0].get('distance', 0) - 0.1
            search_params = {"params": {"radius": radius, "range_filter": range_filter}}
            log.debug(f"search iterator with limit={limit} radius={radius}, range_filter={range_filter}")
            expected_batch_size = batch_size if limit == -1 else min(batch_size, limit)
            # external filter not set
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter half
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 external_filter_func=external_filter_half,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter nothing
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 external_filter_func=external_filter_nothing,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter with outputs
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit, output_fields=["*"],
                                 external_filter_func=external_filter_with_outputs,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": expected_batch_size})
            # external filter all
            self.search_iterator(client, collection_name, vectors_to_search, batch_size,
                                 search_params=search_params, limit=limit,
                                 external_filter_func=external_filter_all,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"batch_size": 0, "iterate_times": 1})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)