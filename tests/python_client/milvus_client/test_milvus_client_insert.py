import pytest
import numpy as np

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

prefix = "client_insert"
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
default_dynamic_field_name = "field_new"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name
default_int32_field_name = ct.default_int32_field_name
default_int32_value = ct.default_int32_value

class TestMilvusClientInsertInvalid(TestMilvusClientV2Base):
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
    def test_milvus_client_insert_after_client_closed(self):
        """
        target: test insert after client is closed
        method: insert after client is closed
        expected: raise exception
        """
        client = self._client(alias='my_client')
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        self.close(client)

        data = cf.gen_default_list_data(10)
        error = {ct.err_code: 999, ct.err_msg: 'should create connection first'}
        self.insert(client, collection_name, data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_insert_column_data(self):
        """
        target: test insert column data
        method: create connection, collection, insert and search
        expected: raise error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nb)]
        data = [[i for i in range(default_nb)], vectors]
        error = {ct.err_code: 999,
                 ct.err_msg: "The Input data type is inconsistent with defined schema, please check it."}
        self.insert(client, collection_name, data,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_empty_collection_name(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = ""
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 1, ct.err_msg: f"`collection_name` value {collection_name} is illegal"}
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_insert_invalid_collection_name(self, collection_name):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_insert_collection_name_over_max_length(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters"}
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_insert_not_exist_collection_name(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection[database=default][collection={collection_name}]"}
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("data", ["12-s", "中文", "%$#", " ", ""])
    def test_milvus_client_insert_data_invalid_type(self, data):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        error = {ct.err_code: 999,
                 ct.err_msg: "wrong type of argument 'data',expected 'Dict' or list of 'Dict', got 'str'"}
        self.insert(client, collection_name, data,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_data_vector_field_missing(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i,
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 1,
                 ct.err_msg: f"Insert missed an field `vector` to collection "
                             f"without set nullable==true or set default_value"}
        self.insert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_data_id_field_missing(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 1,
                 ct.err_msg: f"Insert missed an field `id` to collection without set nullable==true or set default_value"}
        self.insert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_data_extra_field(self):
        """
        target: test milvus client: insert extra field than schema
        method: insert extra field than schema when enable_dynamic_field is False
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, enable_dynamic_field=False)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 1,
                 ct.err_msg: f"Attempt to insert an unexpected field `float` to collection without enabling dynamic field"}
        self.insert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_data_dim_not_match(self):
        """
        target: test milvus client: insert extra field than schema
        method: insert extra field than schema when enable_dynamic_field is False
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim + 1))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 65536, ct.err_msg: f"of float data should divide the dim({default_dim})"}
        self.insert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_not_matched_data(self):
        """
        target: test milvus client: insert not matched data then defined
        method: insert string to int primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 1,
                 ct.err_msg: f"The Input data type is inconsistent with defined schema, "
                             f"{{id}} field should be a int64"}
        self.insert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#", " "])
    def test_milvus_client_insert_invalid_partition_name(self, partition_name):
        """
        target: test milvus client: insert extra field than schema
        method: insert extra field than schema when enable_dynamic_field is False
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}."}
        if partition_name == " ":
            error = {ct.err_code: 1, ct.err_msg: f"Invalid partition name: . Partition name should not be empty."}
        self.insert(client, collection_name, data=rows, partition_name=partition_name,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_not_exist_partition_name(self):
        """
        target: test milvus client: insert extra field than schema
        method: insert extra field than schema when enable_dynamic_field is False
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        partition_name = cf.gen_unique_str("partition_not_exist")
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.insert(client, collection_name, data=rows, partition_name=partition_name,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_insert_collection_partition_not_match(self):
        """
        target: test milvus client: insert extra field than schema
        method: insert extra field than schema when enable_dynamic_field is False
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        another_collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.create_collection(client, another_collection_name, default_dim)
        self.create_partition(client, another_collection_name, partition_name)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.insert(client, collection_name, data=rows, partition_name=partition_name,
                    check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientInsertValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def nullable(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR,
                                              DataType.BFLOAT16_VECTOR, DataType.INT8_VECTOR])
    def vector_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_insert_default(self, vector_type, nullable):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, max_length=64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, vector_type, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field(default_float_field_name, DataType.FLOAT, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        vectors = cf.gen_vectors(default_nb, dim, vector_data_type=vector_type)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. search
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "pk_name": default_primary_key_field_name})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name,
                                "vector_type": vector_type})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_insert_different_fields(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
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
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. insert diff fields
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, "new_diff_str_field": str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "pk_name": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_insert_empty_data(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rows = []
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == 0
        # 3. search
        rng = np.random.default_rng(seed=19530)
        vectors_to_search = rng.random((1, default_dim))
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": [],
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str('partition')
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows, partition_name=partition_name)[0]
        assert results['insert_count'] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "pk_name": default_primary_key_field_name})
        # partition_number = self.get_partition_stats(client, collection_name, "_default")[0]
        # assert partition_number == default_nb
        # partition_number = self.get_partition_stats(client, collection_name, partition_name)[0]
        # assert partition_number[0]['value'] == 0
        if self.has_partition(client, collection_name, partition_name)[0]:
            self.release_partitions(client, collection_name, partition_name)
            self.drop_partition(client, collection_name, partition_name)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", ["a" * 64, "aa"])
    def test_milvus_client_insert_with_added_field(self, default_value):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert, add field, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, max_length=64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field(default_float_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert before add field
        vectors = cf.gen_vectors(default_nb * 2, dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. add new field
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.VARCHAR,
                                  nullable=True, default_value=default_value, max_length=64)
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        # 4. check old dynamic data search is not impacted after add new field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 5. insert data(old + new field)
        rows_t = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                  default_float_field_name: i * 1.0, default_string_field_name: str(i),
                  "field_new": "field_new"} for i in range(default_nb, default_nb * 2)]
        results = self.insert(client, collection_name, rows_t)[0]
        assert results['insert_count'] == default_nb
        insert_ids_after_add_field = [i for i in range(default_nb, default_nb * 2)]
        # 6. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter=f'field_new=="{default_value}"',
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter=f"field_new=='field_new'",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids_after_add_field,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)


class TestInsertOperation(TestMilvusClientV2Base):
    """
    ******************************************************************
      The following cases are used to test insert interface operations
    ******************************************************************
    """

    @pytest.fixture(scope="function", params=[8, 4096])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[ct.default_int64_field_name, ct.default_string_field_name])
    def pk_field(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_without_connection(self):
        """
        target: test insert without connection
        method: insert after remove connection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        self.close(client)
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(10)]
        error = {ct.err_code: 999, ct.err_msg: 'should create connection first'}
        self.insert(client, collection_name, rows, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_default_partition(self):
        """
        target: test insert entities into default partition
        method: create partition and insert info collection
        expected: the collection insert count equals to nb
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]
        results = self.insert(client, collection_name, rows, partition_name=partition_name)[0]
        assert results['insert_count'] == ct.default_nb
        self.drop_collection(client, collection_name)

    def test_insert_partition_not_existed(self):
        """
        target: test insert entities in collection created before
        method: create collection and insert entities in it, with the not existed partition_name param
        expected: error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(10)]
        error = {ct.err_code: 200, ct.err_msg: "partition not found[partition=p]"}
        self.insert(client, collection_name, rows, partition_name="p", 
                   check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_partition_repeatedly(self):
        """
        target: test insert entities in collection created before
        method: create collection and insert entities in it repeatedly, with the partition_name param
        expected: the collection row count equals to nq
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]
        result_1 = self.insert(client, collection_name, rows, partition_name=partition_name_1)[0]
        result_2 = self.insert(client, collection_name, rows, partition_name=partition_name_2)[0]
        assert result_1['insert_count'] == ct.default_nb
        assert result_2['insert_count'] == ct.default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_partition_with_ids(self):
        """
        target: test insert entities in collection created before, insert with ids
        method: create collection and insert entities in it, with the partition_name param
        expected: the collection insert count equals to nq
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]
        results = self.insert(client, collection_name, rows, partition_name=partition_name)[0]
        assert results['insert_count'] == ct.default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_exceed_varchar_limit(self):
        """
        target: test insert exceed varchar limit
        method: create a collection with varchar limit=2 and insert invalid data
        expected: error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create schema with varchar limit
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=ct.default_dim)
        schema.add_field("small_limit", DataType.VARCHAR, max_length=2)
        schema.add_field("big_limit", DataType.VARCHAR, max_length=65530)
        
        self.create_collection(client, collection_name, dimension=ct.default_dim, schema=schema)
        
        # Insert data exceeding varchar limit
        rows = [
            {"vector": list(cf.gen_vectors(1, ct.default_dim)[0]), "small_limit": "limit_1___________", "big_limit": "1"},
            {"vector": list(cf.gen_vectors(1, ct.default_dim)[0]), "small_limit": "limit_2___________", "big_limit": "2"}
        ]
        error = {ct.err_code: 999, ct.err_msg: "length of varchar field small_limit exceeds max length"}
        self.insert(client, collection_name, rows, check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_no_vector_field_dtype(self):
        """
        target: test insert entities, with no vector field
        method: vector field is missing in data
        expected: error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        
        # Generate data without vector field
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i,
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(10)]
        error = {ct.err_code: 1, ct.err_msg: f"Insert missed an field `vector` to collection"}
        self.insert(client, collection_name, rows, check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_vector_field_dismatch_dtype(self):
        """
        target: test insert entities, with no vector field
        method: vector field is missing in data
        expected: error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        
        # Generate data with wrong vector type (scalar instead of list)
        rows = [{default_primary_key_field_name: 0, default_vector_field_name: 0.0001,
                 default_float_field_name: 0.0, default_string_field_name: "0"}]
        error = {ct.err_code: 1, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        self.insert(client, collection_name, rows, check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_drop_collection(self):
        """
        target: test insert and drop
        method: insert data and drop collection
        expected: verify collection if exist
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]
        self.insert(client, collection_name, rows)
        
        self.drop_collection(client, collection_name)
        collections = self.list_collections(client)[0]
        assert collection_name not in collections

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_create_index(self):
        """
        target: test insert and create index
        method: 1. insert 2. create index
        expected: verify num entities and index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb
        
        # Create index (note: quick setup collection already has index)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        self.create_index(client, collection_name, index_params)
        
        indexes = self.list_indexes(client, collection_name)[0]
        assert default_vector_field_name in indexes
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_after_create_index(self):
        """
        target: test insert after create index
        method: 1. create index 2. insert data
        expected: verify index and num entities
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        
        # Create index first
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        self.create_index(client, collection_name, index_params)
        
        indexes = self.list_indexes(client, collection_name)[0]
        assert default_vector_field_name in indexes
        
        # Then insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]
        self.insert(client, collection_name, rows)
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_binary_after_index(self):
        """
        target: test insert binary after index
        method: 1.create index 2.insert binary data
        expected: 1.index ok 2.num entities correct
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create binary vector collection
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(ct.default_binary_vec_field_name, index_type="BIN_IVF_FLAT", metric_type="HAMMING")
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, index_params=index_params)
        
        indexes = self.list_indexes(client, collection_name)[0]
        assert ct.default_binary_vec_field_name in indexes
        
        # Insert binary data
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_create_index(self):
        """
        target: test create index in auto_id=True collection
        method: 1.create auto_id=True collection and insert
                2.create index
        expected: index correct
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create schema with auto_id=True
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, 
                              index_params=index_params, auto_id=True)
        
        # Insert without primary key (auto_id)
        rng = np.random.default_rng(seed=19530)
        rows = [{default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == ct.default_nb
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb
        
        indexes = self.list_indexes(client, collection_name)[0]
        assert default_vector_field_name in indexes
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true(self, pk_field):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert without ids
        expected: verify primary_keys and num_entities
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create schema with auto_id=True and specific primary field
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=True)[0]
        if pk_field == ct.default_int64_field_name:
            schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=True)
        else:
            schema.add_field(pk_field, DataType.VARCHAR, max_length=ct.default_length, is_primary=True, auto_id=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        if pk_field != ct.default_string_field_name:
            schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, auto_id=True)
        
        # Insert without primary key (auto_id)
        rng = np.random.default_rng(seed=19530)
        rows = [{default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0} for i in range(ct.default_nb)]
        if pk_field != ct.default_string_field_name:
            for i, row in enumerate(rows):
                row[default_string_field_name] = str(i)
        
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == ct.default_nb
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_twice_auto_id_true(self, pk_field):
        """
        target: test insert ids fields twice when auto_id=True
        method: 1.create collection with auto_id=True 2.insert twice
        expected: verify primary_keys unique
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 10
        
        # Create schema with auto_id=True and specific primary field
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=True)[0]
        if pk_field == ct.default_int64_field_name:
            schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=True)
        else:
            schema.add_field(pk_field, DataType.VARCHAR, max_length=ct.default_length, is_primary=True, auto_id=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        if pk_field != ct.default_string_field_name:
            schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, auto_id=True)
        
        # Insert twice
        rng = np.random.default_rng(seed=19530)
        rows = [{default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0} for i in range(nb)]
        if pk_field != ct.default_string_field_name:
            for i, row in enumerate(rows):
                row[default_string_field_name] = str(i)
        
        results_1 = self.insert(client, collection_name, rows)[0]
        assert results_1['insert_count'] == nb
        
        results_2 = self.insert(client, collection_name, rows)[0]
        assert results_2['insert_count'] == nb
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == nb * 2
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true_list_data(self, pk_field):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert list data with ids field values
        expected: assert num entities
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        
        # Create schema with auto_id=True and specific primary field
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=True)[0]
        if pk_field == ct.default_int64_field_name:
            schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=True)
        else:
            schema.add_field(pk_field, DataType.VARCHAR, max_length=ct.default_length, is_primary=True, auto_id=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        if pk_field != ct.default_string_field_name:
            schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, auto_id=True)
        
        # Insert without primary key (auto_id)
        rng = np.random.default_rng(seed=19530)
        rows = [{default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0} for i in range(ct.default_nb)]
        if pk_field != ct.default_string_field_name:
            for i, row in enumerate(rows):
                row[default_string_field_name] = str(i)
        
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == ct.default_nb
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_true_with_dataframe_values(self, pk_field):
        """
        target: test insert with dataframe data
        method: create collection with auto_id=True
        expected: milvus client does not support insert with dataframe
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create schema with auto_id=True
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=True)[0]
        if pk_field == ct.default_int64_field_name:
            schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=True)
        else:
            schema.add_field(pk_field, DataType.VARCHAR, max_length=ct.default_length, is_primary=True, auto_id=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        if pk_field != ct.default_string_field_name:
            schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, auto_id=True)
        
        # Try to insert with primary key included (should fail)
        df = cf.gen_default_dataframe_data(nb=100, auto_id=True)
        error = {ct.err_code: 999,
                 ct.err_msg: f"wrong type of argument 'data',expected 'Dict' or list of 'Dict', got 'DataFrame'"}
        self.insert(client, collection_name, df, check_task=CheckTasks.err_res, check_items=error)

        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == 0
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true_with_list_values(self, pk_field):
        """
        target: test insert with auto_id=True
        method: create collection with auto_id=True
        expected: 1.verify num entities 2.verify ids
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        
        # Create schema with auto_id=True
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=True)[0]
        if pk_field == ct.default_int64_field_name:
            schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=True)
        else:
            schema.add_field(pk_field, DataType.VARCHAR, max_length=ct.default_length, is_primary=True, auto_id=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        if pk_field != ct.default_string_field_name:
            schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, auto_id=True)
        
        # Insert without primary key (auto_id)
        rng = np.random.default_rng(seed=19530)
        rows = [{default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0} for i in range(nb)]
        if pk_field != ct.default_string_field_name:
            for i, row in enumerate(rows):
                row[default_string_field_name] = str(i)
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_false_same_values(self):
        """
        target: test insert same ids with auto_id false
        method: 1.create collection with auto_id=False 2.insert same int64 field values
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        
        self.create_collection(client, collection_name, default_dim, auto_id=False)
        
        # Insert with same primary key values
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: 1, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_false_negative_values(self):
        """
        target: test insert negative ids with auto_id false
        method: auto_id=False, primary field values is negative
        expected: verify num entities
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        
        self.create_collection(client, collection_name, default_dim, auto_id=False)
        
        # Insert with negative primary key values
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: -i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == nb

        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    # @pytest.mark.xfail(reason="issue 15416")
    def test_insert_multi_threading(self):
        """
        target: test concurrent insert
        method: multi threads insert
        expected: verify num entities
        """
        import threading
        
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        
        thread_num = 4
        threads = []
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(ct.default_nb)]

        def insert(thread_i):
            log.debug(f'In thread-{thread_i}')
            # Adjust primary keys to be unique per thread
            thread_rows = [{default_primary_key_field_name: i + thread_i * ct.default_nb,
                           default_vector_field_name: row[default_vector_field_name],
                           default_float_field_name: row[default_float_field_name],
                           default_string_field_name: row[default_string_field_name]} for i, row in enumerate(rows)]
            results = self.insert(client, collection_name, thread_rows)[0]
            assert results['insert_count'] == ct.default_nb

        for i in range(thread_num):
            x = threading.Thread(target=insert, args=(i,))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb * thread_num
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_multi_times(self, dim):
        """
        target: test insert multi times
        method: insert data multi times
        expected: verify num entities
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        step = 120
        nb = 12000
        
        self.create_collection(client, collection_name, dim, auto_id=False)
        
        rng = np.random.default_rng(seed=19530)
        start_id = 0
        for _ in range(nb // step):
            rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                     default_float_field_name: i * 1.0, default_string_field_name: str(i)} 
                    for i in range(start_id, start_id + step)]
            results = self.insert(client, collection_name, rows)[0]
            assert results['insert_count'] == step
            start_id += step

        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_all_datatype_collection(self):
        """
        target: test insert into collection that contains all datatype fields
        method: 1.create all datatype collection 2.insert data
        expected: verify num entities
        """
        # MilvusClient doesn't support construct_from_dataframe, skip this test
        # or reimplement using schema with all data types
        pytest.skip("MilvusClient doesn't support construct_from_dataframe")

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_equal_to_resource_limit(self):
        """
        target: test insert data equal to RPC limitation 64MB (67108864)
        method: calculated critical value and insert equivalent data
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # nb = 127583 without json field
        nb = 108993
        
        self.create_collection(client, collection_name, default_dim, auto_id=False)
        
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(nb)]
        self.insert(client, collection_name, rows)
        
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("default_value_type", ["empty", "none"])
    def test_insert_one_field_using_default_value(self, default_value_type, nullable, auto_id):
        """
        target: test insert with one field using default value
        method: 1. create a collection with one field using default value
                2. insert using default value to replace the field value []/[None]
        expected: insert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create schema with default value field
        schema = self.create_schema(client, auto_id=auto_id, enable_dynamic_field=False)[0]
        if not auto_id:
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        else:
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length, 
                        default_value="abc", nullable=nullable)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema, auto_id=auto_id)
        
        # Insert data with None or omitting the default value field
        rng = np.random.default_rng(seed=19530)
        rows = []
        for i in range(ct.default_nb):
            row = {default_float_field_name: float(i),
                   default_vector_field_name: list(rng.random((1, default_dim))[0])}
            if not auto_id:
                row[default_primary_key_field_name] = i
            if default_value_type == "none":
                row[default_string_field_name] = None
            # If default_value_type == "empty", we don't include the field at all
            rows.append(row)
        
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_multi_fields_none_with_default_value(self):
        """
        target: test insert with multi fields include array using none value
        method: 1. create a collection with multi fields using default value
                2. insert using none value to replace the field value
        expected: insert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        dim = 16
        nb = 100
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True)
        schema.add_field(default_int32_field_name, DataType.INT32, default_value=np.int32(1), nullable=True)
        schema.add_field(default_float_field_name, DataType.FLOAT, default_value=np.float32(1.0), nullable=True)
        schema.add_field(default_string_field_name, DataType.VARCHAR, default_value="abc", max_length=100, nullable=True)
        schema.add_field('int32_array', datatype=DataType.ARRAY, element_type=DataType.INT32,  max_capacity=20, nullable=True)
        schema.add_field('float_array', datatype=DataType.ARRAY, element_type=DataType.FLOAT,   max_capacity=20, nullable=True)
        schema.add_field('string_array', datatype=DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=20, max_length=100, nullable=True)
        schema.add_field('json', DataType.JSON,  nullable=True)
        schema.add_field(default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        rows = [{
            default_primary_key_field_name: i,
            default_int32_field_name: None,
            default_float_field_name: None,
            default_string_field_name: None,
            'int32_array': None,
            'float_array': None,
            'string_array': None,
            'json': None,
            default_float_vec_field_name: cf.gen_vectors(1, dim=dim)[0]
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == nb

        # build index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # try to query None value entities, should be empty
        res, _ = self.query(client, collection_name, filter=f"{default_string_field_name} is null")
        assert len(res) == 0

        # try to query default value entities, should be not empty
        res, _ = self.query(client, collection_name, filter=f"{default_string_field_name}=='abc'")
        assert len(res) == nb

        # try to query None value entities on json field, should not be empty
        res, _ = self.query(client, collection_name, filter=f"json is null")
        assert len(res) == nb

        res, _ = self.query(client, collection_name, filter=f"int32_array is null")
        assert len(res) == nb

        self.drop_collection(client, collection_name)
