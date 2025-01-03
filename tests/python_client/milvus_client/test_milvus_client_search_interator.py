import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import DataType
from base.high_level_api_wrapper import HighLevelApiWrapper

client_w = HighLevelApiWrapper()

prefix = "milvus_client_api_search_iterator"
epsilon = ct.epsilon
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


class TestMilvusClientSearchInteratorValid(TestcaseBase):
    """ Test case of search iterator interface """

    @pytest.fixture(scope="function", params=[True, False])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["IP", "COSINE"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[{}, {"radius": 0.1, "range_filter": 0.9}])
    def search_params(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_search_iterator_default(self, search_params):
        """
        target: test search iterator (high level api) normal case
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        client_w.using_database(client, "default")
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": default_dim,
                                                  "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)
        client_w.flush(client, collection_name)
        # 3. search iterator
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        search_params = {"params": search_params}
        client_w.search_interator(client, collection_name, vectors_to_search, search_params=search_params,
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "ids": insert_ids,
                                               "limit": default_limit})
        client_w.release_collection(client, collection_name)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_iterator_about_nullable_default(self, nullable, search_params):
        """
        target: test search iterator (high level api) normal case about nullable and default value
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = client_w.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field("nullable_field", DataType.INT64, nullable=True, default_value=10)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=True)
        index_params = client_w.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        client_w.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i), "nullable_field": None, "array_field": None} for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)
        client_w.flush(client, collection_name)
        # 3. search iterator
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        search_params = {"params": search_params}
        client_w.search_interator(client, collection_name, vectors_to_search, filter="nullable_field>=10",
                                  search_params=search_params,
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "ids": insert_ids,
                                               "limit": default_limit})
        if client_w.has_collection(client, collection_name)[0]:
            client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_rename_search_iterator_default(self, search_params):
        """
        target: test search iterator(high level api) normal case
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": default_dim,
                                                  "consistency_level": 0})
        old_name = collection_name
        new_name = collection_name + "new"
        client_w.rename_collection(client, old_name, new_name)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        client_w.insert(client, new_name, rows)
        client_w.flush(client, new_name)
        # assert client_w.num_entities(client, collection_name)[0] == default_nb
        # 3. search_iterator
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        search_params = {"params": search_params}
        client_w.search_interator(client, new_name, vectors_to_search, search_params=search_params,
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "ids": insert_ids,
                                               "limit": default_limit})
        client_w.release_collection(client, new_name)
        client_w.drop_collection(client, new_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_array_insert_search_iterator(self, search_params):
        """
        target: test search iterator (high level api) normal case
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = client_w.list_collections(client)[0]
        assert collection_name in collections
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
            default_float_field_name: i * 1.0,
            default_int32_array_field_name: [i, i + 1, i + 2],
            default_string_array_field_name: [str(i), str(i + 1), str(i + 2)]
        } for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)
        # 3. search iterator
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        search_params = {"params": search_params}
        client_w.search_interator(client, collection_name, vectors_to_search, search_params=search_params,
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "ids": insert_ids,
                                               "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_iterator_string(self, search_params):
        """
        target: test search iterator (high level api) for string primary key
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        client_w.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": default_dim})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)
        client_w.flush(client, collection_name)
        # 3. search_iterator
        vectors_to_search = rng.random((1, default_dim))
        search_params = {"params": search_params}
        client_w.search_interator(client, collection_name, vectors_to_search, search_params=search_params,
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "limit": default_limit})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_iterator_different_metric_type_no_specify_in_search_params(self, metric_type, auto_id,
                                                                                             search_params):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                                   consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        client_w.insert(client, collection_name, rows)
        # 3. search_iterator
        vectors_to_search = rng.random((1, default_dim))
        search_params = {"params": search_params}
        client_w.search_interator(client, collection_name, vectors_to_search, batch_size=default_batch_size,
                                  limit=default_limit, search_params=search_params,
                                  output_fields=[default_primary_key_field_name],
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "limit": default_limit})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_iterator_different_metric_type_specify_in_search_params(self, metric_type, auto_id,
                                                                                          search_params):
        """
        target: test search iterator (high level api) normal case
        method: create connection, collection, insert and search iterator
        expected: search iterator successfully with limit(topK)
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                                   consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        client_w.insert(client, collection_name, rows)
        # 3. search_iterator
        vectors_to_search = rng.random((1, default_dim))
        search_params = {"params": search_params}
        search_params.update({"metric_type": metric_type})
        client_w.search_interator(client, collection_name, vectors_to_search, batch_size=default_batch_size,
                                  limit=default_limit, search_params=search_params,
                                  output_fields=[default_primary_key_field_name],
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "limit": default_limit})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_iterator_delete_with_ids(self, search_params):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search iterator
        expected: search iterator successfully without deleted data
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        pks = client_w.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        client_w.delete(client, collection_name, ids=[i for i in range(delete_num)])
        # 4. search_iterator
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        search_params = {"params": search_params}
        client_w.search_interator(client, collection_name, vectors_to_search, batch_size=default_batch_size,
                                  search_params=search_params, limit=default_nb,
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "ids": insert_ids,
                                               "limit": limit})
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_iterator_delete_with_filters(self, search_params):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search iterator
        expected: search iterator/query successfully without deleted data
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        pks = client_w.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        client_w.delete(client, collection_name, filter=f"id < {delete_num}")
        # 4. search_iterator
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        search_params = {"params": search_params}
        client_w.search_interator(client, collection_name, vectors_to_search, batch_size=default_batch_size,
                                  search_params=search_params, limit=default_nb,
                                  check_task=CheckTasks.check_search_iterator,
                                  check_items={"enable_milvus_client_api": True,
                                               "nq": len(vectors_to_search),
                                               "ids": insert_ids,
                                               "limit": limit})
        # 5. query
        client_w.query(client, collection_name, filter=default_search_exp,
                       check_task=CheckTasks.check_query_results,
                       check_items={exp_res: rows[delete_num:],
                                    "with_vec": True,
                                    "primary_field": default_primary_key_field_name})
        client_w.drop_collection(client, collection_name)
