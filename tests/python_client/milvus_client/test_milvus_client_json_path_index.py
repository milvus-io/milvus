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


class TestMilvusClientInsertJsonPathIndexValid(TestMilvusClientV2Base):
    """ Test case of insert interface """

    @pytest.fixture(scope="function", params=["INVERTED"])
    def supported_varchar_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["BOOL", "Double", "Varchar", "json"])
    def supported_json_cast_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_insert_before_json_path_index(self, enable_dynamic_field, supported_json_cast_type,
                                                         supported_varchar_scalar_index):
        """
        target: test insert and then create json path index
        method: create json path index after insert
        steps: 1. create schema
               2. create collection
               3. insert
               4. prepare json path index params with parameter "json_cast_type" and "json_path"
               5. create index
        expected: insert and create json path index successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
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
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
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
        self.describe_index(client, collection_name, index_name,
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a']['b']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name})
        self.describe_index(client, collection_name, index_name + '1',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '1'})
        self.describe_index(client, collection_name, index_name +'2',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '2'})
        self.describe_index(client, collection_name, index_name + '3',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a'][0]['b']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '3'})
        self.describe_index(client, collection_name, index_name + '4',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a'][0]",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '4'})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_insert_after_json_path_index(self, enable_dynamic_field, supported_json_cast_type,
                                                         supported_varchar_scalar_index):
        """
        target: test insert after create json path index
        method: create json path index after insert
        steps: 1. create schema
               2. create all the index parameters including json path index
               3. create collection with schema and index params
               4. insert
               5. check the index
        expected: insert successfully after create json path index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection with schema and all the index parameters
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        if not enable_dynamic_field:
            schema.add_field(json_field_name, DataType.JSON)
        index_name = "json_index"
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
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
                range(default_nb + 20, default_nb + 30)]
        self.insert(client, collection_name, rows)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i), json_field_name: {'a': [{'b': None}, 2, 3]}} for i in
                range(default_nb + 30, default_nb + 40)]
        self.insert(client, collection_name, rows)
        # 3. check the json path index
        self.describe_index(client, collection_name, index_name,
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a']['b']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name})
        self.describe_index(client, collection_name, index_name + '1',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '1'})
        self.describe_index(client, collection_name, index_name +'2',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '2'})
        self.describe_index(client, collection_name, index_name + '3',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a'][0]['b']",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '3'})
        self.describe_index(client, collection_name, index_name + '4',
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": supported_json_cast_type,
                                "json_path": f"{json_field_name}['a'][0]",
                                "index_type": supported_varchar_scalar_index,
                                "field_name": json_field_name,
                                "index_name": index_name + '4'})


    """ Test case of partial update interface """
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
    def test_milvus_client_partial_update_new_pk_with_missing_field(self):
        """
        target:  Test PU will return error when provided new pk and partial field
        method:
            1. Create a collection
            2. partial upsert a new pk with only partial field
        expected: Step 2 should result fail
        """
        # step 1: create collection
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_int32_field_name, DataType.INT32, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_int32_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: partial upsert a new pk with only partial field
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                         desired_field_names=[default_primary_key_field_name, default_int32_field_name])
        error = {ct.err_code: 1100, ct.err_msg: 
                f"fieldSchema({default_vector_field_name}) has no corresponding fieldData pass in: invalid parameter"}
        self.upsert(client, collection_name, rows, partial_update=True, 
                    check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_new_field_without_dynamic_field(self):
        """
        target:  Test PU will return error when provided new field without dynamic field
        method:
            1. Create a collection with dynamic field
            2. partial upsert a new field 
        expected: Step 2 should result fail
        """
        # step 1: create collection with dynamic field
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: partial upsert a new field
        row = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, row, partial_update=True)

        new_row = [{default_primary_key_field_name: i, default_int32_field_name: 99} for i in range(default_nb)]
        error = {ct.err_code: 1, 
                ct.err_msg: f"Attempt to insert an unexpected field `{default_int32_field_name}` to collection without enabling dynamic field"}
        self.upsert(client, collection_name, new_row, partial_update=True, check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_after_release_collection(self):
        """
        target: test basic function of partial update
        method: 
                1. create collection
                2. insert a full row of data using partial update
                3. partial update data
                4. release collection
                5. partial update data
        expected: step 5 should fail
        """
        # Step 1: create collection
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_string_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # Step 2: insert a full row of data using partial update
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # Step 3: partial update data
        new_row = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                            desired_field_names=[default_primary_key_field_name, default_string_field_name])
        self.upsert(client, collection_name, new_row, partial_update=True)

        # Step 4: release collection
        self.release_collection(client, collection_name)

        # Step 5: partial update data
        new_row = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                            desired_field_names=[default_primary_key_field_name, default_string_field_name])
        error = {ct.err_code: 101, 
                 ct.err_msg: f"failed to query: collection not loaded"}
        self.upsert(client, collection_name, new_row, partial_update=True,
                    check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_same_pk_after_delete(self):
        """
        target: test PU will fail when provided same pk and partial field
        method:
            1. Create a collection with dynamic field
            2. Insert rows
            3. delete the rows
            4. upsert the rows with same pk and partial field
        expected: step 4 should fail
        """
        # Step 1: create collection
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_int32_field_name, DataType.INT32)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_int32_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # Step 2: insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # Step 3: delete the rows
        result = self.delete(client, collection_name, filter=default_search_exp)[0]
        assert result["delete_count"] == default_nb
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_nothing)[0]
        assert len(result) == 0
        
        # Step 4: upsert the rows with same pk and partial field
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                            desired_field_names=[default_primary_key_field_name, default_vector_field_name])
        error = {ct.err_code: 1100, 
                 ct.err_msg: f"fieldSchema({default_int32_field_name}) has no corresponding fieldData pass in: invalid parameter"}
        self.upsert(client, collection_name, new_rows, partial_update=True,
                    check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_pk_in_wrong_partition(self):
        """
        target: test PU will fail when provided pk in wrong partition
        method:
            1. Create a collection
            2. Create 2 partitions
            3. Insert rows
            4. upsert the rows with pk in wrong partition
        expected: step 4 should fail
        """
        # Step 1: create collection
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_int32_field_name, DataType.INT32)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_int32_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # Step 2: Create 2 partitions
        num_of_partitions = 2
        partition_names = []
        for _ in range(num_of_partitions):
            partition_name = cf.gen_unique_str("partition")
            self.create_partition(client, collection_name, partition_name)
            partition_names.append(partition_name)
        
        # Step 3: Insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        gap = default_nb // num_of_partitions
        for i, partition in enumerate(partition_names):
            self.upsert(client, collection_name, rows[i*gap:(i+1)*gap], partition_name=partition, partial_update=True)

        # Step 4: upsert the rows with pk in wrong partition
        new_rows = cf.gen_row_data_by_schema(nb=gap, schema=schema, 
                                            desired_field_names=[default_primary_key_field_name, default_vector_field_name])
        error = {ct.err_code: 1100, 
                 ct.err_msg: f"fieldSchema({default_int32_field_name}) has no corresponding fieldData pass in: invalid parameter"}
        self.upsert(client, collection_name, new_rows, partition_name=partition_names[-1], partial_update=True,
                    check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_same_pk_multiple_fields(self):
        """
        target:  Test PU will success and query will success
        method:
            1. Create a collection
            2. Insert rows
            3. Upsert the rows with same pk and different field
        expected: Step 3 should fail
        """
        # step 1: create collection
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_int32_field_name, DataType.INT32, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_int32_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: Insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)

        # step 3: Upsert the rows with same pk and different field
        new_rows = []
        for i in range(default_nb):
            data = {}
            if i % 2 == 0:
                data[default_int32_field_name] = i + 1000
                data[default_primary_key_field_name] = 0
            else:
                data[default_vector_field_name] = [random.random() for _ in range(default_dim)]
                data[default_primary_key_field_name] = 0
            new_rows.append(data)

        error = {ct.err_code: 1, 
                 ct.err_msg: f"The data fields length is inconsistent. previous length is 2000, current length is 1000"}
        self.upsert(client, collection_name, new_rows, partial_update=True,
                    check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)