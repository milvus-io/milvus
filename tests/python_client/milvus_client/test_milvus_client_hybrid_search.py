import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus import DataType
from pymilvus import AnnSearchRequest
from pymilvus import WeightedRanker

prefix = "client_hybrid_search"
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


class TestMilvusClientHybridSearchInvalid(TestMilvusClientV2Base):
    """ Test case of hybrid search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_hybrid_search_invalid_collection_name_string(self, name):
        """
        target: test hybrid search with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection={name}]"}
        self.hybrid_search(client, name, [sub_search1, sub_search1], ranker, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2587")
    @pytest.mark.parametrize("name", [1])
    def test_milvus_client_hybrid_search_invalid_collection_name_non_string(self, name):
        """
        target: test hybrid search with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection={name}]"}
        self.hybrid_search(client, name, [sub_search1, sub_search1], ranker, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2588")
    @pytest.mark.parametrize("reqs", ["12-s", 1])
    def test_milvus_client_hybrid_search_invalid_reqs(self, reqs):
        """
        target: test hybrid search with invalid reqs
        method: create connection, collection, insert and hybrid search with invalid reqs
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection=1]"}
        self.hybrid_search(client, collection_name, reqs, ranker, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2588")
    @pytest.mark.parametrize("invalid_ranker", [1])
    def test_milvus_client_hybrid_search_invalid_ranker(self, invalid_ranker):
        """
        target: test hybrid search with invalid ranker
        method: create connection, collection, insert and hybrid search with invalid ranker
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection=1]"}
        self.hybrid_search(client, collection_name, [sub_search1, sub_search1], invalid_ranker, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_limit", [-1, ct.min_limit-1, "1", "12-s", "中文", "%$#"])
    def test_milvus_client_hybrid_search_invalid_limit(self, invalid_limit):
        """
        target: test hybrid search with invalid limit
        method: create connection, collection, insert and hybrid search with invalid limit
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 1,
                 ct.err_msg: f"`limit` value {invalid_limit} is illegal"}
        self.hybrid_search(client, collection_name, [sub_search1, sub_search1], ranker, limit=invalid_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_limit", [ct.max_limit+1])
    def test_milvus_client_hybrid_search_limit_out_of_range(self, invalid_limit):
        """
        target: test hybrid search with invalid limit (out of range)
        method: create connection, collection, insert and hybrid search with invalid limit
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 65535,
                 ct.err_msg: "invalid max query result window, (offset+limit) should be in range [1, 16384], but got 16385"}
        self.hybrid_search(client, collection_name, [sub_search1, sub_search1], ranker, limit=invalid_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_output_fields", [1, "1"])
    def test_milvus_client_hybrid_search_invalid_output_fields(self, invalid_output_fields):
        """
        target: test hybrid search with invalid output_fields
        method: create connection, collection, insert and hybrid search with invalid output_fields
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 1,
                 ct.err_msg: f"`output_fields` value {invalid_output_fields} is illegal"}
        self.hybrid_search(client, collection_name, [sub_search1, sub_search1], ranker, limit=default_limit,
                           output_fields=invalid_output_fields, check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2589")
    @pytest.mark.parametrize("invalid_partition_names", [1, "1"])
    def test_milvus_client_hybrid_search_invalid_partition_names(self, invalid_partition_names):
        """
        target: test hybrid search with invalid partition names
        method: create connection, collection, insert and hybrid search with invalid partition names
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 1,
                 ct.err_msg: f"`partition_name_array` value {invalid_partition_names} is illegal"}
        self.hybrid_search(client, collection_name, [sub_search1, sub_search1], ranker, limit=default_limit,
                           partition_names=invalid_partition_names, check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_partition_names", ["not_exist"])
    def test_milvus_client_hybrid_search_not_exist_partition_names(self, invalid_partition_names):
        """
        target: test hybrid search with not exist partition names
        method: create connection, collection, insert and hybrid search with not exist partition names
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        sub_search1 = AnnSearchRequest(vectors_to_search, "embeddings", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 65535,
                ct.err_msg: f"partition name {invalid_partition_names} not found"}
        self.hybrid_search(client, collection_name, [sub_search1, sub_search1], ranker, limit=default_limit,
                           partition_names=[invalid_partition_names], check_task=CheckTasks.err_res,
                           check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_hybrid_search_not_exist_vector_name(self):
        """
        target: test hybrid search normal default case
        method: create connection, collection, insert and hybrid search
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        not_exist_vector_field = "not_exist_vector_field"
        sub_search1 = AnnSearchRequest(vectors_to_search, not_exist_vector_field, {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: failed to get field schema by name: "
                             f"fieldName({not_exist_vector_field}) not found: invalid parameter"}
        self.hybrid_search(client, collection_name, [sub_search1, sub_search1], ranker, limit=default_limit,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_hybrid_search_requests_mismatch(self):
        """
        target: test hybrid search when the length of weights param mismatch with ann search requests
        method: create connection, collection, insert and hybrid search
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 2. hybrid search
        vectors_to_search = rng.random((1, default_dim))
        sub_search1 = AnnSearchRequest(vectors_to_search, "vector", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 1100,
                 ct.err_msg: "the length of weights param mismatch with ann search requests: "
                             "invalid parameter[expected=1][actual=2]"}
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=default_limit,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


_json_path_index_params = [
    ("INVERTED", "BOOL"),
    ("INVERTED", "DOUBLE"),
    ("INVERTED", "VARCHAR"),
    ("INVERTED", "JSON"),
    ("STL_SORT", "DOUBLE"),
    ("STL_SORT", "VARCHAR"),
    ("BITMAP", "BOOL"),
    ("BITMAP", "VARCHAR"),
]


class TestMilvusClientHybridSearchValid(TestMilvusClientV2Base):
    """ Test case of hybrid search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=_json_path_index_params, ids=[f"{t[0]}_{t[1]}" for t in _json_path_index_params])
    def json_index_params(self, request):
        yield request.param

    @pytest.fixture(scope="function")
    def supported_varchar_scalar_index(self, json_index_params):
        yield json_index_params[0]

    @pytest.fixture(scope="function")
    def supported_json_cast_type(self, json_index_params):
        yield json_index_params[1]

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_hybrid_search_default(self):
        """
        target: test hybrid search with default normal case (2 vector fields)
        method: create connection, collection, insert and hybrid search
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        schema.add_field(default_vector_field_name+"new", DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(default_vector_field_name+"new", metric_type="L2")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rows = cf.gen_row_data_by_schema(ct.default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. hybrid search
        vectors_to_search = cf.gen_vectors(1, dim=dim)
        insert_ids = [i for i in range(default_nb)]
        sub_search1 = AnnSearchRequest(vectors_to_search, default_vector_field_name,
                                       {"level": 1}, 20, expr="id>=0")
        sub_search2 = AnnSearchRequest(vectors_to_search, default_vector_field_name+"new",
                                       {"level": 1}, 20, expr="id>=0")
        ranker = WeightedRanker(0.2, 0.8)
        self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker, limit=default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": len(vectors_to_search),
                                        "ids": insert_ids,
                                        "limit": default_limit,
                                        "pk_name": default_primary_key_field_name})
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.INT64,
                                  nullable=True, max_length=100)
        self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker, limit=default_limit,
                           filter="field_new is null",
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": len(vectors_to_search),
                                        "ids": insert_ids,
                                        "limit": default_limit,
                                        "pk_name": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_hybrid_search_single_vector(self):
        """
        target: test hybrid search with just one vector field
        method: create connection, collection, insert and hybrid search
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0])}
            for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. hybrid search
        rng = np.random.default_rng(seed=19530)
        insert_ids = [i for i in range(default_nb)]
        vectors_to_search = rng.random((1, default_dim))
        sub_search1 = AnnSearchRequest(vectors_to_search, "vector", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(1)
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": len(vectors_to_search),
                                        "ids": insert_ids,
                                        "pk_name": default_primary_key_field_name,
                                        "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    def test_milvus_client_hybrid_search_after_json_path_index(self, supported_varchar_scalar_index,
                                                               supported_json_cast_type, is_flush, is_release):
        """
        target: test hybrid search after json path index created
        method: create connection, collection, insert and hybrid search
        Step: 1. create schema
              2. prepare index_params with the required vector index params and json path index
              3. create collection with the above schema and index params
              4. insert
              5. hybrid search
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_vector_field_name+"new", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(default_vector_field_name+"new", metric_type="L2")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
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
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i),
             json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        if is_flush:
            self.flush(client, collection_name)
        if is_release:
            self.release_collection(client, collection_name)
            self.load_collection(client, collection_name)
        # 3. hybrid search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        sub_search1 = AnnSearchRequest(vectors_to_search, default_vector_field_name, {"level": 1}, 20, expr="id>=0")
        sub_search2 = AnnSearchRequest(vectors_to_search, default_vector_field_name+"new", {"level": 1}, 20, expr="id>=0")
        ranker = WeightedRanker(0.2, 0.8)
        self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker, limit=default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": len(vectors_to_search),
                                        "ids": insert_ids,
                                        "pk_name": default_primary_key_field_name,
                                        "limit": default_limit})
        sub_search1 = AnnSearchRequest(vectors_to_search, default_vector_field_name, {"level": 1}, 20,
                                       expr=f"{json_field_name}['a']['b']>=10")
        sub_search2 = AnnSearchRequest(vectors_to_search, default_vector_field_name + "new", {"level": 1}, 20,
                                       expr=f"{json_field_name}['a']['b']>=10")
        ranker = WeightedRanker(0.2, 0.8)
        insert_ids = [i for i in range(10, default_nb)]
        self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker, limit=default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": len(vectors_to_search),
                                        "ids": insert_ids,
                                        "pk_name": default_primary_key_field_name,
                                        "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_hybrid_search_weighted_groupby_l2_no_norm_score(self):
        """
        target: verify weighted hybrid search with group_by + L2 metric +
                norm_score=false preserves "smaller distance = better match"
                semantics throughout the GroupByOp.
        method: 1. create collection with two FLOAT_VECTOR fields (both L2,
                   FLAT index) and a category VARCHAR field
                2. insert rows in 3 categories where vectors progressively
                   move away from the query
                3. hybrid search with WeightedRanker(0.5, 0.5) (no norm_score)
                   + group_by="category", group_size=2, limit=2 groups
        expected: best L2 group ranks first, within-group best L2 first.
        Without the fix the entire ordering is reversed: GroupByOp hard-coded
        DESC sort, so it kept the WORST groupSize rows per group and put the
        category with the worst best-row first. The category with the best
        match (cat A) would be dropped entirely from a limit=2 result.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection with two FLOAT_VECTOR fields (both L2 + FLAT)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64,
                         is_primary=True, auto_id=False)
        schema.add_field("vec1", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("vec2", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("category", DataType.VARCHAR, max_length=16)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("vec1", index_type="FLAT", metric_type="L2")
        index_params.add_index("vec2", index_type="FLAT", metric_type="L2")
        self.create_collection(client, collection_name, dimension=dim,
                               schema=schema, index_params=index_params)
        # 2. insert deterministic data:
        #    cat A: ids 1,2,3 — best L2 cluster, vector magnitudes 0.1, 0.2, 0.5
        #    cat B: ids 4,5   — middling cluster, magnitudes 1.0, 2.0
        #    cat C: id  6     — worst, magnitude 5.0
        rows = [
            {default_primary_key_field_name: 1, "vec1": [0.1] * dim, "vec2": [0.1] * dim, "category": "A"},
            {default_primary_key_field_name: 2, "vec1": [0.2] * dim, "vec2": [0.2] * dim, "category": "A"},
            {default_primary_key_field_name: 3, "vec1": [0.5] * dim, "vec2": [0.5] * dim, "category": "A"},
            {default_primary_key_field_name: 4, "vec1": [1.0] * dim, "vec2": [1.0] * dim, "category": "B"},
            {default_primary_key_field_name: 5, "vec1": [2.0] * dim, "vec2": [2.0] * dim, "category": "B"},
            {default_primary_key_field_name: 6, "vec1": [5.0] * dim, "vec2": [5.0] * dim, "category": "C"},
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. hybrid search: query vector = [0]*dim → L2 increases monotonically with row id
        query_vector = [[0.0] * dim]
        sub1 = AnnSearchRequest(query_vector, "vec1", {"level": 1}, 10)
        sub2 = AnnSearchRequest(query_vector, "vec2", {"level": 1}, 10)
        # WeightedRanker with no norm_score → raw weighted L2 distances are
        # combined; the merged $score column ends up "smaller = better".
        ranker = WeightedRanker(0.5, 0.5)
        # group_by category, top 2 rows per group, top 2 groups
        results = self.hybrid_search(
            client, collection_name, [sub1, sub2], ranker,
            limit=2, group_by_field="category", group_size=2,
            output_fields=[default_primary_key_field_name, "category"],
        )[0]
        hits = results[0]
        ids = [h[default_primary_key_field_name] for h in hits]
        cats = [h["category"] for h in hits]
        log.info(f"weighted hybrid + group_by + L2 + no_norm: ids={ids} cats={cats}")
        # Expected (correct ASC semantics): cat A best 2 [1, 2] then cat B best 2 [4, 5]
        # Buggy (DESC hardcoded): cat C [6] then cat B worst [5, 4] — id 6 first, total 3 rows
        assert ids[0] == 1, \
            f"row 1 (best L2 + cat A best representative) must rank first; got {ids} cats={cats}"
        # cat C must be excluded entirely (it's the worst category, dropped by limit=2)
        assert "C" not in cats, \
            f"cat C (worst representative L2) must be excluded by limit=2 groups; got cats={cats}"
        # All surviving rows must come from cat A and cat B in that order
        assert cats == ["A", "A", "B", "B"], \
            f"expected ordering [A, A, B, B] (best 2 per group, ASC group ordering); got {cats} ids={ids}"
        self.drop_collection(client, collection_name)
