import pytest
import time

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


prefix = "client_compact"
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


class TestMilvusClientCompactInvalid(TestMilvusClientV2Base):
    """ Test case of compact interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2588")
    @pytest.mark.parametrize("name", [1, "12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_compact_invalid_collection_name_string(self, name):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {name}. the first character of a collection name "
                             f"must be an underscore or letter: invalid parameter"}
        self.compact(client, name,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2587")
    @pytest.mark.parametrize("name", [1])
    def test_milvus_client_compact_invalid_collection_name_non_string(self, name):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {name}. the first character of a collection name "
                             f"must be an underscore or letter: invalid parameter"}
        self.compact(client, name,
                     check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_clustering", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_compact_invalid_is_clustering(self, invalid_clustering):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"is_clustering value {invalid_clustering} is illegal"}
        self.compact(client, collection_name, is_clustering=invalid_clustering,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_job_id", ["12-s"])
    def test_milvus_client_get_compact_state_invalid_job_id(self, invalid_job_id):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"compaction_id value {invalid_job_id} is illegal"}
        self.get_compaction_state(client, invalid_job_id,
                    check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientCompactValid(TestMilvusClientV2Base):
    """ Test case of hybrid search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def is_clustering(self, request):
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_compact_normal(self, is_clustering):
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
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64,
                         is_partition_key=True, is_clustering_key=is_clustering)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(default_vector_field_name+"new", metric_type="L2")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i)} for i in range(10*default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact
        compact_id = self.compact(client, collection_name, is_clustering=is_clustering)[0]
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id, is_clustering=is_clustering)[0]
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(1, f"Compact after index cost more than {cost}s")

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_compact_empty_collection(self, is_clustering):
        """
        target: test compact to empty collection
        method: create connection, collection, compact
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64,
                         is_partition_key=True, is_clustering_key=is_clustering)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. compact
        self.compact(client, collection_name, is_clustering=is_clustering)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_compact_json_path_index(self, is_clustering, supported_varchar_scalar_index,
                                                   supported_json_cast_type):
        """
        target: test hybrid search with default normal case (2 vector fields)
        method: create connection, collection, insert and hybrid search
        expected: successfully
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
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64,
                         is_partition_key=True, is_clustering_key=is_clustering)
        schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(default_vector_field_name+"new", metric_type="L2")
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

        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i),
             json_field_name: {'a': {"b": i}}} for i in range(10*default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact
        compact_id = self.compact(client, collection_name, is_clustering=is_clustering)[0]
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id, is_clustering=is_clustering)[0]
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(1, f"Compact after index cost more than {cost}s")

        self.drop_collection(client, collection_name)