import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus import DataType

prefix = "client_search"
partition_prefix = "client_partition"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
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


class TestMilvusClientSearchInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="pymilvus issue 1554")
    def test_milvus_client_collection_invalid_primary_field(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 1, ct.err_msg: f"Param id_type must be int or string"}
        self.create_collection(client, collection_name, default_dim, id_type="invalid",
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_string_auto_id(self):
        """
        target: test high level api: client.create_collection
        method: create collection with auto id on string primary key
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 65535, ct.err_msg: f"type param(max_length) should be specified for varChar "
                                                 f"field of collection {collection_name}"}
        self.create_collection(client, collection_name, default_dim, id_type="string", auto_id=True,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_same_collection_different_params(self):
        """
        target: test high level api: client.create_collection
        method: create
        expected: 1. Successfully to create collection with same params
                  2. Report errors for creating collection with same name and different params
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. create collection with same params
        self.create_collection(client, collection_name, default_dim)
        # 3. create collection with same name and different params
        error = {ct.err_code: 1, ct.err_msg: f"create duplicate collection with different parameters, "
                                             f"collection: {collection_name}"}
        self.create_collection(client, collection_name, default_dim + 1,
                               check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_invalid_metric_type(self):
        """
        target: test high level api: client.create_collection
        method: create collection with auto id on string primary key
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 1100,
                 ct.err_msg: "float vector index does not support metric type: invalid: "
                             "invalid parameter[expected=valid index params][actual=invalid index params]"}
        self.create_collection(client, collection_name, default_dim, metric_type="invalid",
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/29880")
    def test_milvus_client_search_not_consistent_metric_type(self, metric_type):
        """
        target: test search with inconsistent metric type (default is IP) with that of index
        method: create connection, collection, insert and search with not consistent metric type
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        search_params = {"metric_type": metric_type}
        error = {ct.err_code: 1100,
                 ct.err_msg: f"metric type not match: invalid parameter[expected=IP][actual={metric_type}]"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    search_params=search_params,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


class TestMilvusClientSearchValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_search_query_default(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
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
                                              "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #36484")
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_search_query_self_creation_default(self, nullable):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field("nullable_field", DataType.INT64, nullable=True, default_value=10)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i), "nullable_field": None, "array_field": None} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_rename_search_query_default(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        old_name = collection_name
        new_name = collection_name + "new"
        self.rename_collection(client, old_name, new_name)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, new_name, rows)
        self.flush(client, new_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, new_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        # 4. query
        self.query(client, new_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        self.release_collection(client, new_name)
        self.drop_collection(client, new_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_array_insert_search(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
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
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 25110")
    def test_milvus_client_search_query_string(self):
        """
        target: test search (high level api) for string primary key
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_different_metric_types_not_specifying_in_search_params(self, metric_type, auto_id):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                               consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        # search_params = {"metric_type": metric_type}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    output_fields=[default_primary_key_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("pymilvus issue #1866")
    def test_milvus_client_search_different_metric_types_specifying_in_search_params(self, metric_type, auto_id):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, metric_type=metric_type, auto_id=auto_id,
                               consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        if auto_id:
            for row in rows:
                row.pop(default_primary_key_field_name)
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        search_params = {"metric_type": metric_type}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    search_params=search_params,
                    output_fields=[default_primary_key_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_delete_with_ids(self):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        pks = self.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        self.delete(client, collection_name, ids=[i for i in range(delete_num)])
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        self.search(client, collection_name, vectors_to_search, limit=default_nb,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[delete_num:],
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_delete_with_filters(self):
        """
        target: test delete (high level api)
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        pks = self.insert(client, collection_name, rows)[0]
        # 3. delete
        delete_num = 3
        self.delete(client, collection_name, filter=f"id < {delete_num}")
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        for insert_id in range(delete_num):
            if insert_id in insert_ids:
                insert_ids.remove(insert_id)
        limit = default_nb - delete_num
        self.search(client, collection_name, vectors_to_search, limit=default_nb,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[delete_num:],
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_client_search_with_iterative_filter(self):
        """
        target: test search with iterative filter
        method: create connection, collection, insert, search with iterative filter
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        dim = 32
        pk_field_name = 'id'
        vector_field_name = 'embeddings'
        str_field_name = 'title'
        json_field_name = 'json_field'
        max_length = 16
        schema.add_field(pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(str_field_name, DataType.VARCHAR, max_length=max_length)
        schema.add_field(json_field_name, DataType.JSON)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name, metric_type="COSINE",
                               index_type="IVF_FLAT", params={"nlist": 128})
        index_params.add_index(field_name=str_field_name)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            pk_field_name: i,
            vector_field_name: list(rng.random((1, dim))[0]),
            str_field_name: cf.gen_str_by_length(max_length),
            json_field_name: {"number": i}
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 3. search
        search_vector = list(rng.random((1, dim))[0])
        search_params = {'hints': "iterative_filter",
                         'params': cf.get_search_params_params('IVF_FLAT')}
        self.search(client, collection_name, data=[search_vector], filter='id >= 10',
                    search_params=search_params, limit=default_limit)
        not_supported_hints = "not_supported_hints"
        error = {ct.err_code: 0,
                 ct.err_msg: f"Create Plan by expr failed:  => hints: {not_supported_hints} not supported"}
        search_params = {'hints': not_supported_hints,
                         'params': cf.get_search_params_params('IVF_FLAT')}
        self.search(client, collection_name, data=[search_vector], filter='id >= 10',
                    search_params=search_params, check_task=CheckTasks.err_res, check_items=error)

class TestMilvusClientSearchIteratorInvalid(TestMilvusClientV2Base):
    """ Test case of search iterator """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip("ambiguous error info")
    def test_search_iterator_collection_not_existed(self):
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
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search_iterator(client, collection_name, vectors_to_search,
                            batch_size=5,
                            check_task=CheckTasks.err_res,
                            check_items=error)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", ["str", [[1,2],[3,4]]])
    def test_search_iterator_with_multiple_vectors(self, data):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
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
    def test_search_iterator_with_empty_data(self, data):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
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
    def test_search_iterator_with_invalid_batch_size(self, batch_size):
        """
        target: test search iterator with invalid batch size
        method: run search iterator with invalid batch size
        expected: Raise exception
        """
        #These are two inappropriate error messages:
        #1.5: `limit` value 1.5 is illegal
        #"1": '<' not supported between instances of 'str' and 'int'
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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
    def test_search_iterator_with_invalid_expr(self, expr):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("limit", [-10])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/39066")
    def test_search_iterator_with_invalid_limit(self, limit):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        error = {ct.err_code: 1,
                 ct.err_msg: f"`limit` value {limit} is illegal"}
        self.search_iterator(client, collection_name, vectors_to_search,
                        batch_size=5,
                        limit=limit,
                        check_task=CheckTasks.err_res,
                        check_items=error)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("output_fields", ["id"])
    @pytest.mark.skip("A field that does not currently exist will simply have no effect, "
                     "but it would be better if an error were reported.")
    def test_search_iterator_with_invalid_output(self, output_fields):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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
    def test_search_iterator_with_invalid_search_params(self, search_params):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["client_partition_85Jv3Pf3"])
    def test_search_iterator_with_invalid_partition_name(self, partition_name):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["nonexistent"])
    def test_search_iterator_with_nonexistent_partition_name(self, partition_name):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("anns_field", ["nonexistent", ])
    def test_search_iterator_with_nonexistent_anns_field(self, anns_field):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", ["tt"])
    def test_search_iterator_with_invalid_round_decimal(self, round_decimal):
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
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

class TestMilvusClientSearchIteratorValid(TestMilvusClientV2Base):
    """ Test case of search iterator """

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_iterator_normal(self):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
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
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search_iterator(client, collection_name, vectors_to_search,
                        batch_size=40,
                        limit=-1,
                        check_task=CheckTasks.check_search_iterator,
                        check_items={"batch_size": 40,
                                     "limit": -1})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("metric_type", ["COSINE", "IP"])
    @pytest.mark.parametrize("params", [{"radius": 0.8, "range_filter": 1}])
    def test_search_iterator_with_different_metric_type_with_params(self, metric_type, params):
        """
        target: test search iterator with COSINE and IP metric types and search params
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, 
                               metric_type=metric_type, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": default_dim,
                                                  "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        search_params = {"metric_type": metric_type, "params": params}
        self.search_iterator(client, collection_name, vectors_to_search,
                        batch_size=100,
                        search_params=search_params,
                        check_task=CheckTasks.check_search_iterator,
                        check_items={"metric_type": metric_type,
                                     "radius": 0.8,
                                     "range_filter": 1})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("metric_type", ["L2"])
    @pytest.mark.parametrize("params", [{"radius": 0.8, "range_filter": 1}])
    def test_search_iterator_with_L2_metric_type_with_params(self, metric_type, params):
        """
        target: test search iterator with L2 metric type and search params
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim,
                               metric_type=metric_type, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": default_dim,
                                                  "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        search_params = {"metric_type": metric_type, "params": params}
        self.search_iterator(client, collection_name, vectors_to_search,
                        batch_size=100,
                        search_params=search_params,
                        check_task=CheckTasks.check_search_iterator,
                        check_items={"metric_type": metric_type,
                                     "radius": 0.8,
                                     "range_filter": 1})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)