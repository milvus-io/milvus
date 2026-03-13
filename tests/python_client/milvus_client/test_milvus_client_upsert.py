import pytest
import numpy as np

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import Function, FunctionType

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


class TestMilvusClientUpsertInvalid(TestMilvusClientV2Base):
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
    def test_milvus_client_upsert_column_data(self):
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
        self.upsert(client, collection_name, data,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_empty_collection_name(self):
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
        self.upsert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_upsert_invalid_collection_name(self, collection_name):
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
        self.upsert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_collection_name_over_max_length(self):
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
        self.upsert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_not_exist_collection_name(self):
        """
        target: test high level api: client.create_collection
        method: create collection with invalid primary field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str("insert_not_exist")
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection[database=default][collection={collection_name}]"}
        self.upsert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", ["12-s", "12 s", "(mn)", "中文", "%$#", " "])
    def test_milvus_client_upsert_data_invalid_type(self, data):
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
        error = {ct.err_code: 1, ct.err_msg: f"wrong type of argument 'data',expected 'Dict' or list of 'Dict'"}
        self.upsert(client, collection_name, data,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_milvus_client_upsert_data_type_dismatch(self, primary_field):
        """
        target: test upsert with invalid data type
        method: upsert data type string, set, number, float...
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100

        # 1. Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        if primary_field == ct.default_int64_field_name:
            schema.add_field(primary_field, DataType.INT64, is_primary=True, auto_id=False)
        else:
            schema.add_field(primary_field, DataType.VARCHAR, max_length=ct.default_length, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        schema.add_field(default_bool_field_name, DataType.BOOL)
        if primary_field != ct.default_string_field_name:
            schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)

        # 2. Create collection
        self.create_collection(client, collection_name, schema=schema)

        # 3. Generate row data
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)

        # 4. Test invalid data types at different positions (first, middle, last)
        for dirty_i in [0, nb // 2, nb - 1]:  # check the dirty data at first, middle and last
            log.debug(f"dirty_i: {dirty_i}")
            # Iterate through all fields in the row
            for field_name, field_value in rows[dirty_i].items():
                # Get the actual value type
                value_type = type(field_value)
                error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}

                # Inject type errors based on value type (only for simple scalar types)
                if value_type in (int, bool, float):
                    tmp = rows[dirty_i][field_name]
                    rows[dirty_i][field_name] = "iamstring"
                    self.upsert(client, collection_name, data=rows,
                                check_task=CheckTasks.err_res, check_items=error)
                    rows[dirty_i][field_name] = tmp
                elif value_type is str:
                    tmp = rows[dirty_i][field_name]
                    rows[dirty_i][field_name] = random.randint(0, 1000)
                    self.upsert(client, collection_name, data=rows,
                                check_task=CheckTasks.err_res, check_items=error)
                    rows[dirty_i][field_name] = tmp
                else:
                    continue

        # 5. Verify correct data can be upserted
        results = self.upsert(client, collection_name, rows)[0]
        assert results['upsert_count'] == nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_vector_type_unmatch(self):
        """
        target: test upsert with unmatched vector type
        method: 1. create a collection with float_vector
                2. upsert with binary_vector data
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection with float_vector
        self.create_collection(client, collection_name, default_dim)

        # 2. Generate binary vector data
        _, binary_vectors = cf.gen_binary_vectors(default_nb, default_dim)
        rows = [{default_primary_key_field_name: i, ct.default_binary_vec_field_name: binary_vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        # 3. Verify error on upsert
        error = {ct.err_code: 999, ct.err_msg: "Insert missed an field `vector` to collection without set nullable==true or set default_value"}
        self.upsert(client, collection_name, data=rows, check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_data_empty(self):
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
        error = {ct.err_code: 1, ct.err_msg: f"wrong type of argument 'data',expected 'Dict' or list of 'Dict'"}
        self.upsert(client, collection_name, data="",
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_data_vector_field_missing(self):
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
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(10)]
        error = {ct.err_code: 1,
                 ct.err_msg: "Insert missed an field `vector` to collection without set nullable==true or set default_value"}
        self.upsert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_data_id_field_missing(self):
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
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(20)]
        error = {ct.err_code: 1,
                 ct.err_msg: f"Insert missed an field `id` to collection without set nullable==true or set default_value"}
        self.upsert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_data_extra_field(self):
        """
        target: test milvus client: insert extra field than schema
        method: insert extra field than schema when enable_dynamic_field is False
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim = 32
        self.create_collection(client, collection_name, dim, enable_dynamic_field=False)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(10)]
        error = {ct.err_code: 1,
                 ct.err_msg: f"Attempt to insert an unexpected field `float` to collection without enabling dynamic field"}
        self.upsert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [default_dim + 1, 2 * default_dim])
    def test_milvus_client_upsert_data_dim_not_match(self, dim):
        """
        target: test upsert with unmatched vector dim
        method: 1. create a collection with default dim 128
                2. upsert with mismatched dim (129, 256)
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        error = {ct.err_code: 65535, ct.err_msg: f"dim"}
        self.upsert(client, collection_name, data=rows, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [default_dim - 8, default_dim + 8])
    def test_milvus_client_upsert_binary_dim_unmatch(self, dim):
        """
        target: test upsert with unmatched binary vector dim
        method: 1. create a collection with default dim 128
                2. upsert with mismatched dim (120, 136)
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create binary vector collection with default dim
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)

        self.create_collection(client, collection_name, schema=schema)

        # 2. Generate binary vector data with mismatched dim
        binary_vectors = cf.gen_binary_vectors(default_nb, dim)[1]
        rows = [{default_primary_key_field_name: i, ct.default_binary_vec_field_name: binary_vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]

        # 3. Verify error on upsert
        error = {ct.err_code: 1100, ct.err_msg: f"the dim ({dim}) of field data(binary_vector) is not equal to schema dim ({default_dim})"}
        self.upsert(client, collection_name, data=rows, check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_not_matched_data(self):
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
                 ct.err_msg: "The Input data type is inconsistent with defined schema, {id} field should be a int64"}
        self.upsert(client, collection_name, data=rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#", " "])
    def test_milvus_client_upsert_invalid_partition_name(self, partition_name):
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
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}"}
        if partition_name == " ":
            error = {ct.err_code: 1, ct.err_msg: f"Invalid partition name: . Partition name should not be empty."}
        self.upsert(client, collection_name, data=rows, partition_name=partition_name,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_not_exist_partition_name(self):
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
        self.upsert(client, collection_name, data=rows, partition_name=partition_name,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_collection_partition_not_match(self):
        """
        target: test milvus client: insert extra field than schema
        method: insert extra field than schema when enable_dynamic_field is False
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        another_collection_name = cf.gen_unique_str(prefix + "another")
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
        self.upsert(client, collection_name, data=rows, partition_name=partition_name,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_insert_array_element_null(self, nullable):
        """
        target: test search with null expression on each key of json
        method: create connection, collection, insert and search
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        rows = [{default_primary_key_field_name: str(i), default_vector_field_name: vectors[i],
                 nullable_field_name: [None, 2, 3]} for i in range(default_nb)]
        error = {ct.err_code: 1,
                 ct.err_msg: "The Input data type is inconsistent with defined schema, {nullable_field} field "
                             "should be a array, but got a {<class 'list'>} instead."}
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res,
                    check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_with_auto_id_pk_type_dismatch(self):
        """
        target: test upsert with primary key type mismatch
        method: 1. create a collection with INT64 primary key and auto_id=False
                2. upsert with string type primary key (type mismatch)
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 16
        nb = 10

        # 1. Create collection with INT64 primary key, auto_id=False
        self.create_collection(client, collection_name, dim, auto_id=False)

        # 2. Insert initial data
        rows = cf.gen_row_data_by_schema(nb=nb, schema=self.describe_collection(client, collection_name)[0])
        self.insert(client, collection_name, rows)

        # 3. Generate upsert data with string type primary key (type mismatch)
        upsert_rows = cf.gen_row_data_by_schema(nb=nb, schema=self.describe_collection(client, collection_name)[0])
        # Set primary key field to string type (should be INT64)
        for i, row in enumerate(upsert_rows):
            row[default_primary_key_field_name] = str(i)

        # 4. Verify error on upsert (type mismatch)
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        self.upsert(client, collection_name, data=upsert_rows, check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_bm25_sparse_vector_field(self):
        """
        target: test upsert with functional sparse vector field
        method: create collection with functional sparse vector field,
        insert data with functional sparse vector field,
        upsert data with functional sparse vector field
        expected: upsert failed with errors
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("text", DataType.VARCHAR, max_length=256, enable_analyzer=True, nullable=True)
        schema.add_field("text_sparse_emb", DataType.SPARSE_FLOAT_VECTOR, nullable=False )

        bm25_function = Function(
            name=f"text",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        self.create_collection(client, collection_name, schema=schema)
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. upsert data
        new_rows = [{
            default_primary_key_field_name: i,
            "text": "hello world",
            "text_sparse_emb": cf.gen_sparse_vectors(1, dim=128),
        } for i in range(10)]
        error = {ct.err_code: 999,
                 ct.err_msg: "Attempt to insert an unexpected function output field `text_sparse_emb` to collection"}
        self.upsert(client, collection_name, new_rows,
                    check_task=CheckTasks.err_res, check_items=error)
        
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_duplicate_pk_int64(self):
        """
        target: test upsert with duplicate primary keys (Int64)
        method:
            1. create collection with Int64 primary key
            2. upsert data with duplicate primary keys in the same batch
        expected: raise error - duplicate primary keys are not allowed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. upsert with duplicate PKs: 1, 2, 1 (duplicate)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: 1, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: 1.0, default_string_field_name: "first"},
            {default_primary_key_field_name: 2, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: 2.0, default_string_field_name: "second"},
            {default_primary_key_field_name: 1, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: 1.1, default_string_field_name: "duplicate"},
        ]
        error = {ct.err_code: 1100,
                 ct.err_msg: "duplicate primary keys are not allowed in the same batch"}
        self.upsert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_duplicate_pk_varchar(self):
        """
        target: test upsert with duplicate primary keys (VarChar)
        method:
            1. create collection with VarChar primary key
            2. upsert data with duplicate primary keys in the same batch
        expected: raise error - duplicate primary keys are not allowed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = default_dim
        # 1. create collection with VarChar primary key
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=64, is_primary=True,
                         auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. upsert with duplicate PKs: "a", "b", "a" (duplicate)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: "a", default_vector_field_name: list(rng.random((1, dim))[0]),
             default_float_field_name: 1.0},
            {default_primary_key_field_name: "b", default_vector_field_name: list(rng.random((1, dim))[0]),
             default_float_field_name: 2.0},
            {default_primary_key_field_name: "a", default_vector_field_name: list(rng.random((1, dim))[0]),
             default_float_field_name: 1.1},
        ]
        error = {ct.err_code: 1100,
                 ct.err_msg: "duplicate primary keys are not allowed in the same batch"}
        self.upsert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], 123])
    def test_milvus_client_upsert_rows_using_default_value(self, default_value):
        """
        target: test upsert with invalid type for field that has default value
        method: upsert with invalid type (list or int) for VARCHAR field that has default_value
        expected: raise exception (type check takes precedence over default value)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create schema with default value field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_float_field_name, DataType.FLOAT)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=ct.default_length, default_value="abc")
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # 2. Create collection
        self.create_collection(client, collection_name, dimension=default_dim, schema=schema)

        # 3. Generate vectors
        vectors = cf.gen_vectors(ct.default_nb, default_dim)

        # 4. Prepare upsert data with invalid type for varchar field (list or int instead of string)
        rows = [{default_primary_key_field_name: 1, default_vector_field_name: vectors[1],
                 default_string_field_name: default_value, default_float_field_name: np.float32(1.0)}]

        # 5. Verify error on upsert (type check takes precedence over default value)
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        self.upsert(client, collection_name, data=rows, check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)

class TestMilvusClientUpsertValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    def gen_default_schema_for_upsert(self, description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                      auto_id=False, dim=ct.default_dim, enable_dynamic_field=True, is_binary=False,
                                      with_json=False, **kwargs):
        """
        Generate collection schema for upsert operations using MilvusClient API.
        """
        schema = MilvusClient.create_schema(auto_id=auto_id, enable_dynamic_field=enable_dynamic_field, description=description, **kwargs)
        if primary_field == ct.default_int64_field_name:
            schema.add_field(field_name=primary_field, datatype=DataType.INT64, is_primary=True, auto_id=auto_id)
        else:
            schema.add_field(field_name=primary_field, datatype=DataType.VARCHAR, max_length=ct.default_length, is_primary=True, auto_id=auto_id)
        if is_binary:
            schema.add_field(field_name=ct.default_binary_vec_field_name, datatype=DataType.BINARY_VECTOR, dim=dim)
        else:
            schema.add_field(field_name=ct.default_float_vec_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name=ct.default_float_field_name, datatype=DataType.FLOAT)
        if with_json:
            schema.add_field(field_name=ct.default_json_field_name, datatype=DataType.JSON)
        if primary_field != ct.default_string_field_name:
            schema.add_field(field_name=ct.default_string_field_name, datatype=DataType.VARCHAR,
                             max_length=ct.default_length)
        return schema

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_upsert_default(self):
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
        results = self.upsert(client, collection_name, rows)[0]
        assert results['upsert_count'] == default_nb
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
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_empty_data(self):
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
        results = self.upsert(client, collection_name, rows)[0]
        assert results['upsert_count'] == 0
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
    def test_milvus_client_upsert_data_pk_not_exist(self):
        """
        target: test upsert with collection has no data
        method: 1. create a collection with no initialized data
                2. upsert data
        expected: upsert run normally as insert
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection with no data
        self.create_collection(client, collection_name, default_dim)

        # 2. Upsert data (collection is empty, so upsert should work as insert)
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=self.describe_collection(client, collection_name)[0])
        results = self.upsert(client, collection_name, rows)[0]
        assert results['upsert_count'] == ct.default_nb

        # 3. Verify num entities
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("start", [0, 1500, 3500])
    def test_milvus_client_upsert_data_pk_exist(self, start):
        """
        target: test upsert data and collection pk exists
        method: 1. create a collection and insert data
                2. upsert data whose pk exists
        expected: upsert succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        upsert_nb = 1000
        initial_nb = 5000

        # 1. Create collection and insert initial data
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=False, with_json=True)
        self.create_collection(client, collection_name, schema=schema)
        initial_rows = cf.gen_row_data_by_schema(nb=initial_nb, schema=schema)
        self.insert(client, collection_name, initial_rows)
        self.flush(client, collection_name)

        # 2. Upsert data whose pk exists
        upsert_rows = cf.gen_row_data_by_schema(nb=upsert_nb, schema=schema, start=start)
        float_values = [row[default_float_field_name] for row in upsert_rows]
        results = self.upsert(client, collection_name, upsert_rows)[0]
        assert results['upsert_count'] == upsert_nb

        # 3. Query and verify
        self.flush(client, collection_name)

        # build index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        exp = f"int64 >= {start} && int64 < {upsert_nb + start}"
        res = self.query(client, collection_name, filter=exp, output_fields=[default_float_field_name])[0]
        assert len(res) == upsert_nb
        assert [res[i][default_float_field_name] for i in range(upsert_nb)] == float_values

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_upsert_with_auto_id(self):
        """
        target: test upsert with auto id
        method: 1. create a collection with autoID=true
                2. upsert 10 entities with non-existing pks
                verify: success, and the pks are auto-generated
                3. query 10 entities to get the existing pks
                4. upsert 10 entities with existing pks
                verify: success, and the pks are re-generated, and the new pks are visibly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        nb = 10

        # 1. Create collection with auto_id=True
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=False, auto_id=True, with_json=True)
        self.create_collection(client, collection_name, dimension=dim, schema=schema)

        # Insert initial data
        initial_rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        insert_results = self.insert(client, collection_name, initial_rows)[0]
        insert_ids = insert_results.get('ids', [])
        self.flush(client, collection_name)

        # 2. Upsert 10 entities with non-existing pks (auto_id will generate new pks)
        upsert_schema = self.gen_default_schema_for_upsert(enable_dynamic_field=False, auto_id=False, with_json=True)
        start = ct.default_nb * 10
        upsert_rows1 = cf.gen_row_data_by_schema(nb=nb, schema=upsert_schema, start=start)
        res_upsert1 = self.upsert(client, collection_name, upsert_rows1)[0]
        upsert1_ids = res_upsert1.get('ids', [])
        self.flush(client, collection_name)

        # Assert the pks are auto-generated, and num_entities increased for upsert with non_existing pks
        assert len(upsert1_ids) == nb
        assert upsert1_ids[0] > insert_ids[-1]
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb + nb

        # build index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 3. Query 10 entities to get the existing pks
        res_q = self.query(client, collection_name, filter="", limit=nb)[0]
        existing_pks = [res_q[i][ct.default_int64_field_name] for i in range(nb)]
        existing_count = self.query(client, collection_name, filter=f"{ct.default_int64_field_name} in {existing_pks}", output_fields=[ct.default_count_output])[0]
        assert nb == existing_count[0].get(ct.default_count_output)

        # 4. Upsert 10 entities with the existing pks
        start = ct.default_nb * 20
        upsert_rows2 = cf.gen_row_data_by_schema(nb=nb, schema=upsert_schema, start=start)
        # Set primary key to existing pks (but with auto_id, they will be regenerated)
        for i, row in enumerate(upsert_rows2):
            row[ct.default_int64_field_name] = existing_pks[i] if i < len(existing_pks) else existing_pks[0]

        res_upsert2 = self.upsert(client, collection_name, upsert_rows2)[0]
        self.flush(client, collection_name)

        # Assert the new pks are auto-generated again
        upsert2_ids = res_upsert2.get('ids', [])
        assert len(upsert2_ids) == nb
        assert upsert2_ids[0] > upsert1_ids[-1]

        # Verify existing pks are no longer in collection (replaced by new auto-generated ones)
        existing_count = self.query(client, collection_name, filter=f"{ct.default_int64_field_name} in {existing_pks}", output_fields=[ct.default_count_output])[0]
        assert 0 == existing_count[0].get(ct.default_count_output)

        # Verify new upserted entities exist
        res_q = self.query(client, collection_name, filter=f"{ct.default_int64_field_name} in {upsert2_ids}", output_fields=["*"])[0]
        assert nb == len(res_q)

        # Verify total count
        current_count = self.query(client, collection_name, filter="", output_fields=[ct.default_count_output])[0]
        assert current_count[0].get(ct.default_count_output) == ct.default_nb + nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_with_primary_key_string(self, auto_id):
        """
        target: test upsert with string primary key
        method: 1. create a collection with pk string
                2. insert data
                3. upsert data with ' ' before or after string
        expected: raise no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection with string primary key
        schema = client.create_schema(auto_id=auto_id, enable_dynamic_field=False)
        schema.add_field(field_name=ct.default_string_field_name, datatype=DataType.VARCHAR,
                         max_length=ct.default_length, is_primary=True, auto_id=auto_id)
        schema.add_field(field_name=ct.default_float_vec_field_name, datatype=DataType.FLOAT_VECTOR, dim=ct.default_dim)

        self.create_collection(client, collection_name, dimension=ct.default_dim, schema=schema)

        # 2. Insert data
        rng = np.random.default_rng(seed=19530)
        vectors = [list(rng.random(ct.default_dim)) for _ in range(2)]

        if not auto_id:
            # Insert with explicit primary keys
            rows = [{ct.default_string_field_name: "a", ct.default_float_vec_field_name: vectors[0]},
                    {ct.default_string_field_name: "b", ct.default_float_vec_field_name: vectors[1]}]
            self.insert(client, collection_name, rows)

            # 3. Upsert with spaces before or after string
            upsert_rows = [{ct.default_string_field_name: " a", ct.default_float_vec_field_name: vectors[0]},
                           {ct.default_string_field_name: "b  ", ct.default_float_vec_field_name: vectors[1]}]
            res_upsert = self.upsert(client, collection_name, upsert_rows)[0]
            upsert_ids = res_upsert.get('ids', [])
            assert upsert_ids[0] == " a" and upsert_ids[1] == "b  "
        else:
            # Insert without primary keys (auto_id)
            rows = [{ct.default_float_vec_field_name: vectors[0]}, {ct.default_float_vec_field_name: vectors[1]}]
            self.insert(client, collection_name, rows)

            # 3. Upsert with spaces before or after string (but auto_id will regenerate)
            upsert_rows = [{ct.default_string_field_name: " a", ct.default_float_vec_field_name: vectors[0]},
                           {ct.default_string_field_name: "b  ", ct.default_float_vec_field_name: vectors[1]}]
            res_upsert = self.upsert(client, collection_name, upsert_rows)[0]
            upsert_ids = res_upsert.get('ids', [])
            assert upsert_ids[0] != " a" and upsert_ids[1] != "b  "

        # Verify total entities
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == 4
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_binary_data(self):
        """
        target: test upsert binary data
        method: 1. create a collection and insert data
                2. upsert data
                3. check the results
        expected: raise no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 500

        # 1. Create binary vector collection
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=True, is_binary=True)
        self.create_collection(client, collection_name, dimension=ct.default_dim, schema=schema)

        # Insert initial data
        initial_rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, initial_rows)
        self.flush(client, collection_name)

        # 2. Generate binary vectors and upsert data
        upsert_rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        binary_vectors = [row[default_binary_vec_field_name] for row in upsert_rows]
        results = self.upsert(client, collection_name, upsert_rows)[0]
        assert results['upsert_count'] == nb
        self.flush(client, collection_name)

        # build index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_binary_vec_field_name, metric_type="HAMMING")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 3. Query and check the results
        res = self.query(client, collection_name, filter=f"{ct.default_int64_field_name} >= 0",
                         output_fields=[default_binary_vec_field_name], limit=nb)[0]
        assert len(res) >= 1
        # Verify binary vector matches (compare first vector)
        assert binary_vectors[0] == res[0][default_binary_vec_field_name][0]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_data_is_none(self):
        """
        target: test upsert with data=None
        method: 1. create a collection
                2. insert data
                3. upsert data=None
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection and insert data
        self.create_collection(client, collection_name, default_dim, auto_id=False)
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=self.describe_collection(client, collection_name)[0])
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Verify num entities
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb

        # 3. Upsert data=None, should raise exception
        error = {ct.err_code: -1,
                 ct.err_msg: "wrong type of argument 'data',expected 'Dict' or list of 'Dict', got 'NoneType'"}
        self.upsert(client, collection_name, data=None, check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        # 3. upsert to default partition
        results = self.upsert(client, collection_name, rows, partition_name=partitions[0])[0]
        assert results['upsert_count'] == default_nb
        # 4. upsert to non-default partition
        results = self.upsert(client, collection_name, rows, partition_name=partition_name)[0]
        assert results['upsert_count'] == default_nb
        # 5. search
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_in_specific_partition(self):
        """
        target: test upsert in specific partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. upsert in the given partition
        expected: raise no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = "partition_new"
        upsert_nb = 10

        # 1. Create a collection and 2 partitions
        schema = self.gen_default_schema_for_upsert()
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions

        # 2. Insert data into both partitions (average distribution)
        half_nb = ct.default_nb // 2
        data_default = cf.gen_row_data_by_schema(nb=half_nb, schema=schema, start=0)
        data_partition_new = cf.gen_row_data_by_schema(nb=half_nb, schema=schema, start=half_nb)
        self.insert(client, collection_name, data_default, partition_name="_default")
        self.insert(client, collection_name, data_partition_new, partition_name=partition_name)

        # 3. Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        self.flush(client, collection_name)

        # 4. Check the ids which will be upserted is in partition _default
        expr = f"{ct.default_int64_field_name} >= 0 && {ct.default_int64_field_name} < {upsert_nb}"
        res0 = self.query(client, collection_name, filter=expr,
                          output_fields=[default_float_field_name],
                          partition_names=["_default"])[0]
        assert len(res0) == upsert_nb
        res1 = self.query(client, collection_name, filter=expr,
                          output_fields=[default_float_field_name],
                          partition_names=[partition_name])[0]

        # Verify partition_new has half_nb entities
        partition_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert partition_stats.get("row_count", None) == half_nb

        # 5. Upsert ids in partition _default
        upsert_rows = cf.gen_row_data_by_schema(nb=upsert_nb, schema=schema)
        float_values = [row[default_float_field_name] for row in upsert_rows]
        self.upsert(client, collection_name, upsert_rows, partition_name="_default")

        # 6. Check the result in partition _default(upsert successfully) and others(no missing, nothing new)
        self.flush(client, collection_name)
        res0 = self.query(client, collection_name, filter=expr,
                          output_fields=[default_float_field_name],
                          partition_names=["_default"])[0]
        res2 = self.query(client, collection_name, filter=expr,
                          output_fields=[default_float_field_name],
                          partition_names=[partition_name])[0]

        # Verify partition_new data unchanged
        assert res1 == res2

        # Verify _default partition data updated
        assert [res0[i][default_float_field_name] for i in range(upsert_nb)] == float_values

        # Verify partition_new still has half_nb entities
        partition_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert partition_stats.get("row_count", None) == half_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.skip(reason="issue #22592")
    def test_milvus_client_upsert_in_mismatched_partitions(self):
        """
        target: test upsert in unmatched partition
        method: 1. create a collection and 2 partitions
                2. insert data and load
                3. upsert in unmatched partitions
        expected: upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = "partition_1"
        partition_name2 = "partition_2"
        upsert_nb = 100

        # 1. Create a collection and 2 partitions
        # Use gen_default_schema_for_upsert to match gen_default_data_for_upsert
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=False)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name1 in partitions
        assert partition_name2 in partitions

        # 2. Insert data and load collection
        # For 3 partitions (_default, partition_1, partition_2), each gets default_nb // 3
        num_partitions = 3
        data_per_partition = ct.default_nb // num_partitions
        data_default = cf.gen_row_data_by_schema(nb=data_per_partition, schema=schema, start=0)
        data_partition1 = cf.gen_row_data_by_schema(nb=data_per_partition, schema=schema, start=data_per_partition)
        data_partition2 = cf.gen_row_data_by_schema(nb=data_per_partition, schema=schema, start=data_per_partition * 2)
        self.insert(client, collection_name, data_default, partition_name="_default")
        self.insert(client, collection_name, data_partition1, partition_name=partition_name1)
        self.insert(client, collection_name, data_partition2, partition_name=partition_name2)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 3. Check the ids which will be upserted is not in partition 'partition_1'
        expr = f"{ct.default_int64_field_name} >= 0 && {ct.default_int64_field_name} <= {upsert_nb}"
        res = self.query(client, collection_name, filter=expr,
                         output_fields=[default_float_field_name],
                         partition_names=[partition_name1])[0]
        assert len(res) == 0

        # 4. Upsert in partition 'partition_1'
        upsert_rows = cf.gen_row_data_by_schema(nb=upsert_nb, schema=schema)
        float_values = [row[default_float_field_name] for row in upsert_rows]
        self.upsert(client, collection_name, upsert_rows, partition_name=partition_name1)

        # 5. Check the upserted data in 'partition_1'
        self.flush(client, collection_name)
        res1 = self.query(client, collection_name, filter=expr,
                          output_fields=[default_float_field_name],
                          partition_names=[partition_name1])[0]
        assert [res1[i][default_float_field_name] for i in range(upsert_nb)] == float_values

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_same_with_inserted_data(self):
        """
        target: test upsert with data same with collection inserted data
        method: 1. create a collection and insert data
                2. upsert data same with inserted
                3. check the update data number
        expected: upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        upsert_nb = 1000

        # 1. Create collection and insert data
        self.create_collection(client, collection_name, default_dim, auto_id=False)
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=self.describe_collection(client, collection_name)[0])
        self.insert(client, collection_name, data)

        # 2. Upsert data same with inserted (first upsert_nb rows)
        upsert_data = data[:upsert_nb]
        res = self.upsert(client, collection_name, upsert_data)[0]

        # 3. Check the update data number
        assert res['upsert_count'] == upsert_nb
        self.flush(client, collection_name)

        # Verify total count
        current_count = self.query(client, collection_name, filter="", output_fields=[ct.default_count_output])[0]
        assert current_count[0].get(ct.default_count_output) == ct.default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_upsert(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        # 3. insert and upsert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows, partition_name=partition_name)[0]
        assert results['insert_count'] == default_nb
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, "new_diff_str_field": str(i)} for i in range(default_nb)]
        results = self.upsert(client, collection_name, rows, partition_name=partition_name)[0]
        assert results['upsert_count'] == default_nb
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
        if self.has_partition(client, collection_name, partition_name)[0]:
            self.release_partitions(client, collection_name, partition_name)
            self.drop_partition(client, collection_name, partition_name)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_same_pk_concurrently(self):
        """
        target: test upsert the same pk concurrently
        method: 1. create a collection and insert data
                2. load collection
                3. upsert the same pk
        expected: not raise exception
        """
        import threading

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        upsert_nb = 1000

        # 1. Initialize a collection and insert data
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=False)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 2. Prepare upsert data
        upsert_rows1 = cf.gen_row_data_by_schema(nb=upsert_nb, schema=schema)
        upsert_rows2 = cf.gen_row_data_by_schema(nb=upsert_nb, schema=schema)
        float_values1 = [row[default_float_field_name] for row in upsert_rows1]
        float_values2 = [row[default_float_field_name] for row in upsert_rows2]

        # 3. Upsert at the same time using threads
        def do_upsert1():
            self.upsert(client, collection_name, upsert_rows1)

        def do_upsert2():
            self.upsert(client, collection_name, upsert_rows2)

        t1 = threading.Thread(target=do_upsert1, args=())
        t2 = threading.Thread(target=do_upsert2, args=())

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # 4. Check the result
        self.flush(client, collection_name)
        exp = f"{ct.default_int64_field_name} >= 0 && {ct.default_int64_field_name} <= {upsert_nb}"
        res = self.query(client, collection_name, filter=exp, output_fields=[default_float_field_name])[0]
        res_values = [res[i][default_float_field_name] for i in range(upsert_nb)]
        assert res_values == float_values1 or res_values == float_values2

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_multiple_times(self):
        """
        target: test upsert multiple times
        method: 1. create a collection and insert data
                2. upsert repeatedly
        expected: not raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        upsert_nb = 1000
        step = 500

        # 1. Initialize a collection and insert data
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=False)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 2. Upsert repeatedly
        for i in range(10):
            upsert_rows = cf.gen_row_data_by_schema(nb=upsert_nb, schema=schema, start=i * step)
            self.upsert(client, collection_name, upsert_rows)

        # 3. Check the result
        self.flush(client, collection_name)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == upsert_nb * 10 - step * 9

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_upsert_pk_string_multiple_times(self):
        """
        target: test upsert multiple times
        method: 1. create a collection and insert data
                2. upsert repeatedly
        expected: not raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        upsert_nb = 1000
        step = 500

        # 1. Initialize a collection with string primary key and insert data
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=False, primary_field=ct.default_string_field_name)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 2. Upsert repeatedly
        for i in range(10):
            upsert_rows = cf.gen_row_data_by_schema(nb=upsert_nb, schema=schema, start=i * step)
            self.upsert(client, collection_name, upsert_rows)

        # 3. Check the result
        self.flush(client, collection_name)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == upsert_nb * 10 - step * 9

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_milvus_client_upsert_in_row_with_enable_dynamic_field(self, auto_id):
        """
        target: test upsert in rows when enable dynamic field is True
        method: 1. create a collection and insert data
                2. upsert in rows
        expected: upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        upsert_nb = ct.default_nb
        start = ct.default_nb // 2

        # 1. Initialize a collection with dynamic field enabled and insert data
        schema = self.gen_default_schema_for_upsert(enable_dynamic_field=True, auto_id=auto_id)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_default_rows_data(with_json=False, auto_id=auto_id)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 2. Prepare upsert data with dynamic field
        upsert_data = cf.gen_default_rows_data(with_json=False, start=start)
        for i in range(start, start + upsert_nb):
            upsert_data[i - start]["new"] = [i, i + 1]

        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)

        # 3. Check the result
        expr = f"{ct.default_float_field_name} >= {start} && {ct.default_float_field_name} <= {upsert_nb + start}"
        extra_num = start if auto_id is True else 0  # upsert equals insert in this case if auto_id is True
        res = self.query(client, collection_name, filter=expr, output_fields=['count(*)'])[0]
        assert res[0].get('count(*)') == upsert_nb + extra_num

        res = self.query(client, collection_name, filter=expr, output_fields=["new"])[0]
        assert len(res[upsert_nb + extra_num - 1]["new"]) == 2

        res = self.query(client, collection_name, filter="", output_fields=['count(*)'])[0]
        assert res[0].get('count(*)') == start + upsert_nb + extra_num

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("default_value", [[], [None for i in range(ct.default_nb)]])
    def test_milvus_client_upsert_one_field_using_default_value(self, default_value, nullable):
        """
        target: test insert/upsert with one field using default value
        method: 1. create a collection with one field using default value
                2. insert using default value to replace the field value []/[None]
        expected: insert/upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create schema with default value field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length,
                         default_value="abc", nullable=nullable)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)

        self.create_collection(client, collection_name, schema=schema)

        # Insert initial data
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema, skip_field_names=[ct.default_string_field_name])
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 2. Prepare upsert data with default value field ([] or [None])
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        upsert_data = []
        for i in range(ct.default_nb):
            row = {ct.default_int64_field_name: i, ct.default_float_field_name: np.float32(i),
                   ct.default_float_vec_field_name: vectors[i]}
            # Add string field value based on default_value parameter
            if len(default_value) > 0:
                # If default_value is [None, None, ...], set the field to None
                row[ct.default_string_field_name] = default_value[i]
            # If default_value is [], omit the field to use default value
            upsert_data.append(row)

        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)

        # 3. Check the result - all records should have string field == 'abc'
        exp = f"{ct.default_string_field_name} == 'abc'"
        res = self.query(client, collection_name, filter=exp, output_fields=[default_float_field_name])[0]
        assert len(res) == ct.default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("default_value", [[], [None for _ in range(ct.default_nb)]])
    def test_milvus_client_upsert_multi_fields_using_none_data(self, enable_partition_key, default_value):
        """
        target: test insert/upsert with multi fields include array using none value
        method: 1. create a collection with multi fields include array using default value
                2. insert using none value to replace the field value
        expected: insert/upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        json_embedded_object = "json_embedded_object"

        # 1. Create schema with multi fields including arrays and JSON
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_int32_field_name, DataType.INT32, default_value=np.int32(1), nullable=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, default_value=np.float32(1.0), nullable=True)
        # Partition key field cannot be nullable
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length,
                         default_value="abc", nullable=not enable_partition_key, is_partition_key=enable_partition_key)
        schema.add_field(ct.default_int32_array_field_name, DataType.ARRAY, element_type=DataType.INT32,
                         max_capacity=ct.default_max_capacity, nullable=True)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY, element_type=DataType.FLOAT,
                         max_capacity=ct.default_max_capacity, nullable=True)
        schema.add_field(ct.default_string_array_field_name, DataType.ARRAY, element_type=DataType.VARCHAR,
                         max_capacity=ct.default_max_capacity, max_length=100, nullable=True)
        schema.add_field(json_embedded_object, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)

        self.create_collection(client, collection_name, schema=schema)

        # 2. Insert initial data
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        json_data = cf.gen_json_data_for_diff_json_types(nb=ct.default_nb, start=0, json_type=json_embedded_object)
        insert_data = []
        for i in range(ct.default_nb):
            row = {
                ct.default_int64_field_name: i,
                ct.default_float_field_name: np.float32(2.0),
                ct.default_string_field_name: str(i),
                ct.default_int32_array_field_name: [np.int32(j) for j in range(10)],
                ct.default_float_array_field_name: [np.float32(j) for j in range(10)],
                ct.default_string_array_field_name: [str(j) for j in range(10)],
                json_embedded_object: json_data[i][json_embedded_object],
                ct.default_float_vec_field_name: vectors[i]
            }
            # Add int32 field based on default_value
            if len(default_value) > 0:
                row[ct.default_int32_field_name] = default_value[i]
            # If default_value is [], omit the field to use default value
            insert_data.append(row)

        self.insert(client, collection_name, insert_data)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 3. Prepare upsert data with default value fields ([] or [None])
        vectors_upsert = cf.gen_vectors(ct.default_nb, ct.default_dim)
        upsert_data = []
        for i in range(ct.default_nb):
            row = {
                ct.default_int64_field_name: i,
                ct.default_int32_array_field_name: [np.int32(j) for j in range(10)],
                ct.default_float_array_field_name: [np.float32(j) for j in range(10)],
                ct.default_float_vec_field_name: vectors_upsert[i]
            }
            if len(default_value) > 0:
                # If default_value is [None, None, ...], set fields to None
                row[ct.default_int32_field_name] = default_value[i]
                row[ct.default_float_field_name] = default_value[i]
                row[ct.default_string_field_name] = default_value[i]
                row[ct.default_string_array_field_name] = default_value[i]
                row[json_embedded_object] = default_value[i]
            # If default_value is [], omit these fields to use default values
            upsert_data.append(row)

        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)

        # 4. Check the result
        exp = f"{ct.default_float_field_name} == {np.float32(1.0)}"
        res = self.query(client, collection_name, filter=exp,
                         output_fields=[default_float_field_name, json_embedded_object,
                                        ct.default_string_array_field_name])[0]
        assert len(res) == ct.default_nb
        assert res[0][json_embedded_object] is None
        assert res[0][ct.default_string_array_field_name] is None

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_upsert_multi_fields_by_rows_using_default(self, enable_partition_key, nullable):
        """
        target: test upsert multi fields by rows with default value
        method: 1. create a collection with one field using default value
                2. upsert using default value to replace the field value
        expected: upsert successfully
        """
        # Skip if partition key and nullable both are True
        if enable_partition_key is True and nullable is True:
            pytest.skip("partition key field not support nullable")

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create schema with default value fields
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, default_value=np.float32(3.14), nullable=nullable)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length,
                         default_value="abc", nullable=nullable, is_partition_key=enable_partition_key)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)

        self.create_collection(client, collection_name, schema=schema)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 2. Prepare upsert data and set None for even-indexed records
        upsert_data = cf.gen_default_rows_data()
        for i in range(0, ct.default_nb, 2):
            upsert_data[i][ct.default_float_field_name] = None
            upsert_data[i][ct.default_string_field_name] = None

        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)

        # 3. Check the result
        exp = f"{ct.default_float_field_name} == {np.float32(3.14)} and {ct.default_string_field_name} == 'abc'"
        res = self.query(client, collection_name, filter=exp,
                         output_fields=[ct.default_float_field_name, ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb // 2

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    def test_milvus_client_upsert_multi_fields_by_rows_using_none(self, enable_partition_key):
        """
        target: test insert/upsert multi fields by rows with none value
        method: 1. create a collection with one field using none value
                2. insert/upsert using none to replace the field value
        expected: insert/upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        # Partition key field cannot be nullable
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length,
                         default_value="abc", nullable=not enable_partition_key, is_partition_key=enable_partition_key)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)

        self.create_collection(client, collection_name, schema=schema)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 2. Insert data with None for odd-indexed records
        insert_data = cf.gen_default_rows_data()
        for i in range(1, ct.default_nb, 2):
            insert_data[i][ct.default_float_field_name] = None
            insert_data[i][ct.default_string_field_name] = None

        self.insert(client, collection_name, insert_data)
        self.flush(client, collection_name)

        # 3. Upsert data with None for even-indexed records
        upsert_data = cf.gen_default_rows_data()
        for i in range(0, ct.default_nb, 2):
            upsert_data[i][ct.default_float_field_name] = None
            upsert_data[i][ct.default_string_field_name] = None

        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)

        # 4. Check the result
        exp = f"{ct.default_int64_field_name} >= 0"
        res = self.query(client, collection_name, filter=exp,
                         output_fields=[ct.default_float_field_name, ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb
        assert res[0][ct.default_float_field_name] is None

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    def test_milvus_client_upsert_sparse_data(self, index):
        """
        target: multiple upserts and counts(*)
        method: multiple upserts and counts(*)
        expected: number of data entries normal
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create schema with sparse vector
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        schema.add_field(ct.default_sparse_vec_field_name, DataType.SPARSE_FLOAT_VECTOR)

        self.create_collection(client, collection_name, schema=schema)

        # 2. Prepare upsert data
        sparse_vectors = cf.gen_sparse_vectors(nb=ct.default_nb, dim=128)
        rows = [{ct.default_int64_field_name: i, ct.default_float_field_name: np.float32(i),
                 ct.default_string_field_name: str(i),
                 ct.default_sparse_vec_field_name: sparse_vectors[i]} for i in range(ct.default_nb)]

        self.upsert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify num entities
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == ct.default_nb

        # 3. Create index and load
        params = cf.get_index_params_params(index)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_sparse_vec_field_name, index_type=index, metric_type="IP",
                               params=params)
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # 4. Multiple upserts and verify count
        for i in range(5):
            self.upsert(client, collection_name, rows)
            self.flush(client, collection_name)
            res = self.query(client, collection_name, filter=f'{ct.default_int64_field_name} >= 0',
                             output_fields=[ct.default_count_output])[0]
            assert res[0][ct.default_count_output] == ct.default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_nullable_vector_field_for_times(self):
        """
        target: test upsert with nullable vector field for times
        method: create collection with nullable vector field, insert data with nullable vector field, upsert data with nullable vector field
        expected: upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=32, nullable=True)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field(default_float_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        # 3. upsert data for 10 times
        for i in range(10):
            rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
            self.upsert(client, collection_name, rows)
            if i % 3 == 0:
                self.flush(client, collection_name)
            # 4. query output all fields and assert all the field values
            self.query(client, collection_name, filter=default_search_exp,
                       output_fields=["*"],
                       check_task=CheckTasks.check_query_results,
                       check_items={exp_res: rows,
                                    "with_vec": True,
                                    "pk_name": default_primary_key_field_name})

        self.drop_collection(client, collection_name)
