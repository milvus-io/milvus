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
        self.hybrid_search(client, name, [sub_search1], ranker, limit=default_limit,
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
        self.hybrid_search(client, name, [sub_search1], ranker, limit=default_limit,
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
        self.hybrid_search(client, collection_name, [sub_search1], invalid_ranker, limit=default_limit,
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
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=invalid_limit,
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
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=invalid_limit,
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
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=default_limit,
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
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=default_limit,
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
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=default_limit,
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
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=default_limit,
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
        # 2. hybrid search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, default_dim))
        sub_search1 = AnnSearchRequest(vectors_to_search, "vector", {"level": 1}, 20, expr="id<100")
        ranker = WeightedRanker(0.2, 0.8)
        error = {ct.err_code: 1100,
                 ct.err_msg: "the length of weights param mismatch with ann search requests: "
                             "invalid parameter[expected=1][actual=2]"}
        self.hybrid_search(client, collection_name, [sub_search1], ranker, limit=default_limit,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


class TestMilvusClientHybridSearchValid(TestMilvusClientV2Base):
    """ Test case of hybrid search interface """

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

    @pytest.mark.tags(CaseLabel.L1)
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
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_vector_field_name+"new", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(default_vector_field_name+"new", metric_type="L2")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
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
                                        "limit": default_limit})
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
                                        "limit": default_limit})
        self.drop_collection(client, collection_name)
