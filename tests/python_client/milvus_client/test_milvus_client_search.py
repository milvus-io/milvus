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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_search_invalid_collection_name_string(self, invalid_collection_name):
        """
        target: test search with invalid collection name
        method: create connection, collection, insert and search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection={invalid_collection_name}]"}
        self.search(client, invalid_collection_name, vectors_to_search, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2587")
    @pytest.mark.parametrize("invalid_collection_name", [1])
    def test_milvus_client_search_invalid_collection_name_non_string(self, invalid_collection_name):
        """
        target: test search with invalid collection name
        method: create connection, collection, insert and search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection={invalid_collection_name}]"}
        self.search(client, invalid_collection_name, vectors_to_search, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_data", [1, "12-s","中文", "% $#"])
    def test_milvus_client_search_invalid_data(self, invalid_data):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 100,
                 ct.err_msg: f"`search_data` value {invalid_data} is illegal"}
        self.search(client, collection_name, invalid_data, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_limit", [-1, ct.min_limit-1, "1", "12-s", "中文", "%$#"])
    def test_milvus_client_search_invalid_limit(self, invalid_limit):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1,
                 ct.err_msg: f"`limit` value {invalid_limit} is illegal"}
        self.search(client, collection_name, vectors_to_search, limit=invalid_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_limit", [ct.max_limit+1])
    def test_milvus_client_search_limit_out_of_range(self, invalid_limit):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 65535,
                 ct.err_msg: "topk [16385] is invalid, it should be in range [1, 16384], but got 16385"}
        self.search(client, collection_name, vectors_to_search, limit=invalid_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_filter", ["12-s"])
    def test_milvus_client_search_invalid_filter(self, invalid_filter):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: predicate is not a boolean expression: {invalid_filter}, "
                             f"data type: Int64: invalid parameter"}
        self.search(client, collection_name, vectors_to_search, filter=invalid_filter, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_output_fields", [1, "1"])
    def test_milvus_client_search_invalid_output_fields(self, invalid_output_fields):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1,
                 ct.err_msg: f"`output_fields` value {invalid_output_fields} is illegal"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit, output_fields=invalid_output_fields,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2588")
    @pytest.mark.parametrize("invalid_search_params", [1, "1"])
    def test_milvus_client_search_invalid_search_params(self, invalid_search_params):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1,
                 ct.err_msg: f"`search_params` value {invalid_search_params} is illegal"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit, search_params=invalid_search_params,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_partition_names", [1, "1"])
    def test_milvus_client_search_invalid_partition_names(self, invalid_partition_names):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1,
                 ct.err_msg: f"`partition_name_array` value {invalid_partition_names} is illegal"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    partition_names=invalid_partition_names,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_anns_field", [1])
    def test_milvus_client_search_invalid_anns_field(self, invalid_anns_field):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1,
                 ct.err_msg: f"`anns_field` value {invalid_anns_field} is illegal"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    anns_field=invalid_anns_field,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_anns_field", ["not_exist_field"])
    def test_milvus_client_search_not_exist_anns_field(self, invalid_anns_field):
        """
        target: test search with invalid data
        method: create connection, collection, insert and search with invalid data
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, 8))
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: failed to get field schema by name: "
                             f"fieldName({invalid_anns_field}) not found: invalid parameter"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    anns_field=invalid_anns_field,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 1554")
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
        method: create collection with auto id on string primary key without mx length
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        error = {ct.err_code: 65535, ct.err_msg: f"type param(max_length) should be specified for the "
                                                 f"field({default_primary_key_field_name}) of collection {collection_name}"}
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_vector_field(self, null_expr_op):
        """
        target: test search with null expression on vector field
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        null_expr = default_vector_field_name + " " + null_expr_op
        log.info(null_expr)
        error = {ct.err_code: 65535,
                 ct.err_msg: f"unsupported data type: VECTOR_FLOAT"}
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_not_exist_field(self, null_expr_op):
        """
        target: test search with null expression on vector field
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        not_exist_field_name = "not_exist_field"
        null_expr = not_exist_field_name + " " + null_expr_op
        log.info(null_expr)
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: cannot parse expression: "
                             f"{null_expr}, error: field {not_exist_field_name} not exist: invalid parameter"}
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_json_key(self, nullable,  null_expr_op):
        """
        target: test search with null expression on each key of json
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(nullable_field_name, DataType.JSON, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, dim)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                    nullable_field_name: {'a': None}} for i in range(default_nb)]
            null_expr = nullable_field_name + "['a']" + " " + null_expr_op
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                    nullable_field_name: {'a': 1, 'b': None}} for i in range(default_nb)]
            null_expr = nullable_field_name + "['b']" + " " + null_expr_op
        self.insert(client, collection_name, rows)
        # 3. search
        log.info(null_expr)
        self.search(client, collection_name, [vectors[0]],
                    filter=null_expr)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_array_element(self, nullable, null_expr_op):
        """
        target: test search with null expression on each key of json
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(nullable_field_name, DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, dim)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                    nullable_field_name: None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                    nullable_field_name: [1, 2, 3]} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        null_expr = nullable_field_name + "[0]" + " " + null_expr_op
        log.info(null_expr)
        error = {ct.err_code: 65535,
                 ct.err_msg: f"unsupported data type: ARRAY"}
        self.search(client, collection_name, [vectors[0]],
                    filter=null_expr,
                    check_task=CheckTasks.err_res, check_items=error)


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

    @pytest.mark.tags(CaseLabel.L1)
    def test_client_search_with_expr_float_vector(self):
        """
        target: test search using float vector field as filter
        method: create connection, collection, insert, search with float vector field as filter
        expected: raise error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        dim = 5
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
        raw_vector = [random.random() for _ in range(dim)]
        vectors = np.array(raw_vector, dtype=np.float32)
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: cannot parse expression"}
        self.search(client, collection_name, data=[search_vector], filter=f"{vector_field_name} == {raw_vector}",
                    search_params=default_search_params, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)
        self.search(client, collection_name, data=[search_vector], filter=f"{vector_field_name} == {vectors}",
                    search_params=default_search_params, limit=default_limit,
                    check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientSearchNullExpr(TestMilvusClientV2Base):
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
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr(self, nullable, null_expr_op):
        """
        target: test search with null expression on int64 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": i} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_int8(self, nullable, null_expr_op):
        """
        target: test search with null expression on int8 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT8, nullable=nullable)
        # schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
        #                  max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.int8(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_int16(self, nullable, null_expr_op):
        """
        target: test search with null expression on int16 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT16, nullable=nullable)
        # schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
        #                  max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.int16(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_int32(self, nullable, null_expr_op):
        """
        target: test search with null expression on int32 fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.INT32, nullable=nullable)
        # schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
        #                  max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.int32(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_float(self, nullable, null_expr_op):
        """
        target: test search with null expression on float fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.FLOAT, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": i*1.0} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_double(self, nullable, null_expr_op):
        """
        target: test search with null expression on double fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.DOUBLE, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.double(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_bool(self, nullable, null_expr_op):
        """
        target: test search with null expression on bool fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.BOOL, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": np.bool_(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_varchar(self, nullable, null_expr_op):
        """
        target: test search with null expression on varchar fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.VARCHAR, nullable=nullable, max_length=128)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_json(self, nullable, null_expr_op):
        """
        target: test search with null expression on json fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.JSON, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(field_name=nullable_field_name, index_name="json_index", index_type="INVERTED",
                               params={"json_cast_type": "double",
                                       "json_path": f"{nullable_field_name}['a']['b']"})
        index_params.add_index(field_name=nullable_field_name, index_name="json_index_1", index_type="INVERTED",
                               params={"json_cast_type": "varchar",
                                       "json_path": f"{nullable_field_name}['a']['c']"})
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: {'a': {'b': i, 'c': None}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    output_fields = [nullable_field_name],
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_json_after_flush(self, nullable, null_expr_op):
        """
        target: test search with null expression on json fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.JSON, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), nullable_field_name: {'a': {'b': i, 'c': None}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. flush
        self.flush(client, collection_name)
        # 4. create vector and json index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(field_name=nullable_field_name, index_name="json_index", index_type="INVERTED",
                               params={"json_cast_type": "DOUBLE",
                                       "json_path": f"{nullable_field_name}['a']['b']"})
        index_params.add_index(field_name=nullable_field_name, index_name="json_index_1", index_type="INVERTED",
                               params={"json_cast_type": "double",
                                       "json_path": f"{nullable_field_name}['a']['c']"})
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        # 5. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    output_fields = [nullable_field_name],
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("null_expr_op", ["is null", "IS NULL", "is not null", "IS NOT NULL"])
    def test_milvus_client_search_null_expr_array(self, nullable, null_expr_op):
        """
        target: test search with null expression on array fields
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 5
        # 1. create collection
        nullable_field_name = "nullable_field"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        schema.add_field(nullable_field_name, DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        if nullable:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": None} for i in range(default_nb)]
        else:
            rows = [{default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_string_field_name: str(i), "nullable_field": [1, 2]} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, dim))
        insert_ids = [str(i) for i in range(default_nb)]
        null_expr = nullable_field_name + " " + null_expr_op
        log.info(null_expr)
        if nullable:
            if "not" in null_expr or "NOT" in null_expr:
                insert_ids = []
                limit = 0

            else:
                limit = default_limit
        else:
            if "not" in null_expr or "NOT" in null_expr:
                limit = default_limit
            else:
                insert_ids = []
                limit = 0
        self.search(client, collection_name, vectors_to_search,
                    filter=null_expr,
                    output_fields=[nullable_field_name],
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": limit})


class TestMilvusClientSearchJsonPathIndex(TestMilvusClientV2Base):
    """ Test case of search interface """

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
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_default(self, enable_dynamic_field, supported_json_cast_type,
                                                          supported_varchar_scalar_index, is_flush):
        """
        target: test search after the json path index created
        method: Search after creating json path index
        Step: 1. create schema
              2. prepare index_params with the required vector index params
              3. create collection with the above schema and index params
              4. insert
              5. flush if specified
              6. prepare json path index params
              7. create json path index using the above index params created in step 6
              8. create the same json path index again
              9. search with expressions related with the json paths
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb+60, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i, "b": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb+10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb+10, default_nb+20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': 1}} for i in
                range(default_nb + 50, default_nb + 60)]
        self.insert(client, collection_name, rows)
        if is_flush:
            self.flush(client, collection_name)
        # 2. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 3. create index
        self.create_index(client, collection_name, index_params)
        # 4. create same json index twice
        self.create_index(client, collection_name, index_params)
        # 5. search without filter
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb+60)]
        self.search(client, collection_name, vectors_to_search,
                    output_fields = [json_field_name],
                    consistency_level = "Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        # 6. search with filter on json without output_fields
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb/2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": 1})[0]
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb+5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'] == 1"
        insert_ids = [i for i in range(default_nb + 50, default_nb + 60)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_search_json_path_index_default_index_name(self, enable_dynamic_field, supported_json_cast_type,
                                                                     supported_varchar_scalar_index):
        """
        target: test json path index without specifying the index_name parameter
        method: create json path index without specifying the index_name parameter
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, is_primary=True, auto_id=False, max_length=128)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{json_field_name}['a']['b']"})
        # 4. create index
        index_name = json_field_name + '/a/b'
        self.create_index(client, collection_name, index_params)
        # 5. search with filter on json with output_fields
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        vectors_to_search = [vectors[0]]
        insert_ids = [str(int(default_nb / 2))]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields = [json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": 1})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #40636")
    def test_milvus_client_search_json_path_index_on_non_json_field(self, supported_json_cast_type,
                                                                    supported_varchar_scalar_index):
        """
        target: test json path index on non-json field
        method: create json path index on int64 field
        expected: successfully with original inverted index
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 2. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=default_primary_key_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{default_string_field_name}['a']['b']"})
        # 3. create index
        index_name = default_string_field_name
        self.create_index(client, collection_name, index_params)
        self.describe_index(client, collection_name, index_name,
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                #"json_cast_type": supported_json_cast_type, # issue 40426
                                "json_path": f"{default_string_field_name}['a']['b']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": default_string_field_name,
                                "index_name": index_name})
        self.flush(client, collection_name)
        # 5. search with filter on json with output_fields
        expr = f"{default_primary_key_field_name} >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[default_string_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_search_diff_index_same_field_diff_index_name_diff_index_params(self, enable_dynamic_field,
                                                                                          supported_json_cast_type,
                                                                                          supported_varchar_scalar_index):
        """
        target: test search after different json path index with different default index name at the same time
        method: Search after different json path index with different default index name at the same index_params object
        expected: Search successfully
        """
        if enable_dynamic_field:
            pytest.skip('need to fix the field name when enabling dynamic field')
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        self.load_collection(client, collection_name)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{json_field_name}['a']['b']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        self.create_index(client, collection_name, index_params)
        # 4. release and load collection to make sure new index is loaded
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        # 5. search with filter on json with output_fields
        expr = f"{json_field_name}['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[default_string_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    def test_milvus_client_json_search_index_same_json_path_diff_field(self, enable_dynamic_field, supported_json_cast_type,
                                                                       supported_varchar_scalar_index, is_flush, is_release):
        """
        target: test search after creating same json path for different field
        method: Search after creating same json path for different field
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
            schema.add_field(json_field_name + "1", DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {'b': i}},
                 json_field_name + "1": {'a': {'b': i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 3. release and drop index if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 4. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']['b']"})
        self.create_index(client, collection_name, index_params)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=json_field_name + "1",
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}1['a']['b']"})
        # 5. create index with json path index
        self.create_index(client, collection_name, index_params)
        if is_release:
            self.load_collection(client, collection_name)
        # 6. search with filter on json with output_fields on each json field
        expr = f"{json_field_name}['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        expr = f"{json_field_name}1['a']['b'] >= 0"
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    output_fields=[json_field_name+"1"],
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_before_load(self, enable_dynamic_field, supported_json_cast_type,
                                                              supported_varchar_scalar_index, is_flush):
        """
        target: test search after creating json path index before load
        method: Search after creating json path index before load
        Step: 1. create schema
              2. prepare index_params with vector index params
              3. create collection with the above schema and index params
              4. release collection
              5. insert
              6. flush if specified
              7. prepare json path index params
              8. create index
              9. load collection
              10. search
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. release collection
        self.release_collection(client, collection_name)
        # 3. insert with different data distribution
        vectors = cf.gen_vectors(default_nb+50, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb+10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb+10, default_nb+20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        # 4. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 5. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 5. create index
        self.create_index(client, collection_name, index_params)
        # 6. load collection
        self.load_collection(client, collection_name)
        # 7. search with filter on json without output_fields
        vectors_to_search = [vectors[0]]
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_milvus_client_search_json_path_index_after_release_load(self, enable_dynamic_field, supported_json_cast_type,
                                                                     supported_varchar_scalar_index, is_flush):
        """
        target: test search after creating json path index after release and load
        method: Search after creating json path index after release and load
        Step: 1. create schema
              2. prepare index_params with vector index params
              3. create collection with the above schema and index params
              4. insert
              5. flush if specified
              6. prepare json path index params
              7. create index
              8. release collection
              9. create index again
              10. load collection
              11. search with expressions related with the json paths
        expected: Search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb+50, default_dim)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': {"b": i}}} for i in
                range(default_nb)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: i} for i in
                range(default_nb, default_nb+10)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {}} for i in
                range(default_nb+10, default_nb+20)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [1, 2, 3]}} for i in
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': 1}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 40, default_nb + 50)]
        self.insert(client, collection_name, rows)
        #3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. prepare index params
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=json_field_name, index_name=index_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '1',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '2',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '3',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name, index_name=index_name + '4',
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})
        # 5. create json index
        self.create_index(client, collection_name, index_params)
        # 6. release collection
        self.release_collection(client, collection_name)
        # 7. create json index again
        self.create_index(client, collection_name, index_params)
        # 8. load collection
        self.load_collection(client, collection_name)
        # 9. search with filter on json without output_fields
        vectors_to_search = [vectors[0]]
        expr = f"{json_field_name}['a']['b'] == {default_nb / 2}"
        insert_ids = [default_nb / 2]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": 1})
        expr = f"{json_field_name} == {default_nb + 5}"
        insert_ids = [default_nb + 5]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": 1})
        expr = f"{json_field_name}['a'][0] == 1"
        insert_ids = [i for i in range(default_nb + 20, default_nb + 30)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})
        expr = f"{json_field_name}['a'][0]['b'] == 1"
        insert_ids = [i for i in range(default_nb + 30, default_nb + 40)]
        self.search(client, collection_name, vectors_to_search,
                    filter=expr,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit})