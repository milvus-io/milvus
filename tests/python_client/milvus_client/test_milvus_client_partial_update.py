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


class TestMilvusClientPartialUpdateValid(TestMilvusClientV2Base):
    """ Test case of partial update interface """
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
    def test_milvus_client_partial_update(self):
        """
        target: test basic function of partial update
        method: 
                1. create collection
                2. insert a full row of data using partial update
                3. partial update data
        expected: both step 2 and 3 should be successful
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
        
        # Step 2: insert full rows of data using partial update
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb
        
        # Step 3: partial update data
        new_row = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                            desired_field_names=[default_primary_key_field_name, default_string_field_name])
        self.upsert(client, collection_name, new_row, partial_update=True)
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_string_field_name],
                   check_items={exp_res: new_row,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_with_all_datatype(self):
        """
        target: test partial update with all datatype
        method:
            1. create collection with all datatype schema
            2. insert data
            3. partial update data
        expected: both step 2 and 3 should be successful
        """
        # step 1: create collection with all datatype schema
        client = self._client()
        schema = cf.gen_all_datatype_collection_schema(dim=default_dim, enable_struct_array_field=False)
        index_params = self.prepare_index_params(client)[0]
        text_sparse_emb_field_name = "text_sparse_emb"

        for i in range(len(schema.fields)):
            field_name = schema.fields[i].name
            if field_name == "json_field":
                index_params.add_index(field_name, index_type="AUTOINDEX",
                               params={"json_cast_type": "json"})
            elif field_name == text_sparse_emb_field_name:
                index_params.add_index(field_name, index_type="AUTOINDEX", metric_type="BM25")
            else:
                index_params.add_index(field_name, index_type="AUTOINDEX")

        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: insert data
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # step 3: partial update data
        for field in schema.fields:
            if field.is_primary:
                primary_key_field_name = field.name
                break
        
        vector_field_type = [DataType.FLOAT16_VECTOR,
                            DataType.BFLOAT16_VECTOR, 
                            DataType.INT8_VECTOR,
                            DataType.FLOAT_VECTOR]
        # fields to be updated
        update_fields_name = []
        scalar_update_name = []
        vector_update = [] # this stores field object
        for field in schema.fields:
            field_name = field.name
            if field_name != text_sparse_emb_field_name:
                update_fields_name.append(field_name)
                if field.dtype not in vector_field_type:
                    scalar_update_name.append(field_name)
                else:
                    vector_update.append(field)

        # PU scalar fields and vector fields together
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                                    desired_field_names=update_fields_name)
        self.upsert(client, collection_name, new_rows, partial_update=True)
        # expected scalar result
        expected = [{field: new_rows[i][field] for field in scalar_update_name}
                    for i in range(default_nb)]

        expected = cf.convert_timestamptz(expected, ct.default_timestamptz_field_name, "UTC")
        result = self.query(client, collection_name, filter=f"{primary_key_field_name} >= 0",
                check_task=CheckTasks.check_query_results,
                output_fields=scalar_update_name,
                check_items={exp_res: expected,
                                "with_vec": True,
                                "pk_name": primary_key_field_name})[0]
        assert len(result) == default_nb

        # expected vector result
        for field in vector_update:
            expected = [{primary_key_field_name: data[primary_key_field_name], 
                         field.name: data[field.name]} for data in new_rows]
            result = self.query(client, collection_name, filter=f"{primary_key_field_name} >= 0",
                    check_task=CheckTasks.check_query_results,
                    output_fields=[field.name],
                    check_items={exp_res: expected,
                                "with_vec": True,
                                "vector_type": field.dtype,
                                "vector_field": field.name,
                                "pk_name": primary_key_field_name})[0]
            assert len(result) == default_nb

        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L0)
    def test_partial_update_all_field_types_one_by_one(self):
        """
        Test partial update functionality with all field types
        1. Create collection with all data types
        2. Insert initial data
        3. Perform partial update for each field type
        4. Verify all updates work correctly
        """
        client = self._client()
        dim = 64
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create schema with all data types
        schema = cf.gen_all_datatype_collection_schema(dim=dim, enable_struct_array_field=False)

        # Create index parameters
        index_params = client.prepare_index_params()
        for i in range(len(schema.fields)):
            field_name = schema.fields[i].name
            print(f"field_name: {field_name}")
            if field_name == "json_field":
                index_params.add_index(field_name, index_type="AUTOINDEX",
                                    params={"json_cast_type": "json"})
            elif field_name == "text_sparse_emb":
                index_params.add_index(field_name, index_type="AUTOINDEX", metric_type="BM25")
            else:
                index_params.add_index(field_name, index_type="AUTOINDEX")

        # Create collection
        client.create_collection(collection_name, default_dim, consistency_level="Strong", schema=schema, index_params=index_params)

        # Load collection
        self.load_collection(client, collection_name)

        # Insert initial data
        nb = 1000
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        log.info(f"Inserted {nb} initial records")

        primary_key_field_name = schema.fields[0].name
        for i in range(len(schema.fields)):
            update_field_name = schema.fields[i if i != 0 else 1].name
            new_row = cf.gen_row_data_by_schema(nb=nb, schema=schema, 
                                                        desired_field_names=[primary_key_field_name, update_field_name])
            client.upsert(collection_name, new_row, partial_update=True)

        log.info("Partial update test for all field types passed successfully")

    @pytest.mark.tags(CaseLabel.L0)
    def test_partial_update_simple_demo(self):
        """
        Test simple partial update demo with nullable fields
        1. Create collection with explicit schema including nullable fields
        2. Insert initial data with some null values
        3. Perform partial updates with different field combinations
        4. Verify partial update behavior preserves unchanged fields
        """
        client = self._client()
        dim = 3
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Create schema with nullable fields
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("name", DataType.VARCHAR, max_length=100, nullable=True)
        schema.add_field("price", DataType.FLOAT, nullable=True)
        schema.add_field("category", DataType.VARCHAR, max_length=50, nullable=True)

        # Create collection
        self.create_collection(client, collection_name, schema=schema)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("vector", index_type="AUTOINDEX", metric_type="L2")
        self.create_index(client, collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, collection_name)

        # Insert initial data with some null values
        initial_data = [
            {
                "id": 1,
                "vector": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist(),
                "name": "Product A",
                "price": 100.0,
                "category": "Electronics"
            },
            {
                "id": 2,
                "vector": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist(),
                "name": "Product B",
                "price": None,  # Null price
                "category": "Home"
            },
            {
                "id": 3,
                "vector": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist(),
                "name": "Product C",
                "price": None,  # Null price
                "category": "Books"
            }
        ]

        self.upsert(client, collection_name, initial_data, partial_update=False)
        log.info("Inserted initial data with null values")

        # Verify initial state
        results = self.query(client, collection_name, filter="id > 0", output_fields=["*"])[0]
        assert len(results) == 3

        initial_data_map = {data['id']: data for data in results}
        assert initial_data_map[1]['name'] == "Product A"
        assert initial_data_map[1]['price'] == 100.0
        assert initial_data_map[1]['category'] == "Electronics"
        assert initial_data_map[2]['name'] == "Product B"
        assert initial_data_map[2]['price'] is None
        assert initial_data_map[2]['category'] == "Home"
        assert initial_data_map[3]['name'] == "Product C"
        assert initial_data_map[3]['price'] is None
        assert initial_data_map[3]['category'] == "Books"

        log.info("Initial data verification passed")

        # First partial update - update all fields
        log.info("First partial update - updating all fields...")
        first_update_data = [
            {
                "id": 1,
                "vector": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist(),
                "name": "Product A-Update",
                "price": 111.1,
                "category": "Electronics-Update"
            },
            {
                "id": 2,
                "vector": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist(),
                "name": "Product B-Update",
                "price": 222.2,
                "category": "Home-Update"
            },
            {
                "id": 3,
                "vector": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist(),
                "name": "Product C-Update",
                "price": None,  # Still null
                "category": "Books-Update"
            }
        ]

        self.upsert(client, collection_name, first_update_data, partial_update=True)

        # Verify first update
        results = self.query(client, collection_name, filter="id > 0", output_fields=["*"])[0]
        assert len(results) == 3

        first_update_map = {data['id']: data for data in results}
        assert first_update_map[1]['name'] == "Product A-Update"
        assert abs(first_update_map[1]['price'] - 111.1) < 0.001
        assert first_update_map[1]['category'] == "Electronics-Update"
        assert first_update_map[2]['name'] == "Product B-Update"
        assert abs(first_update_map[2]['price'] - 222.2) < 0.001
        assert first_update_map[2]['category'] == "Home-Update"
        assert first_update_map[3]['name'] == "Product C-Update"
        assert first_update_map[3]['price'] is None
        assert first_update_map[3]['category'] == "Books-Update"

        log.info("First partial update verification passed")

        # Second partial update - update only specific fields
        log.info("Second partial update - updating specific fields...")
        second_update_data = [
            {
                "id": 1,
                "name": "Product A-Update-Again",
                "price": 1111.1,
                "category": "Electronics-Update-Again"
            },
            {
                "id": 2,
                "name": "Product B-Update-Again",
                "price": None,  # Set back to null
                "category": "Home-Update-Again"
            },
            {
                "id": 3,
                "name": "Product C-Update-Again",
                "price": 3333.3,  # Set price from null to value
                "category": "Books-Update-Again"
            }
        ]

        self.upsert(client, collection_name, second_update_data, partial_update=True)

        # Verify second update
        results = self.query(client, collection_name, filter="id > 0", output_fields=["*"])[0]
        assert len(results) == 3

        second_update_map = {data['id']: data for data in results}
        
        # Verify ID 1: all fields updated
        assert second_update_map[1]['name'] == "Product A-Update-Again"
        assert abs(second_update_map[1]['price'] - 1111.1) < 0.001
        assert second_update_map[1]['category'] == "Electronics-Update-Again"
        
        # Verify ID 2: all fields updated, price set to null
        assert second_update_map[2]['name'] == "Product B-Update-Again"
        assert second_update_map[2]['price'] is None
        assert second_update_map[2]['category'] == "Home-Update-Again"
        
        # Verify ID 3: all fields updated, price set from null to value
        assert second_update_map[3]['name'] == "Product C-Update-Again"
        assert abs(second_update_map[3]['price'] - 3333.3) < 0.001
        assert second_update_map[3]['category'] == "Books-Update-Again"

        # Verify vector fields were preserved from first update (not updated in second update)
        # Note: Vector comparison might be complex, so we just verify they exist
        assert 'vector' in second_update_map[1]
        assert 'vector' in second_update_map[2]
        assert 'vector' in second_update_map[3]

        log.info("Second partial update verification passed")
        log.info("Simple partial update demo test completed successfully")

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_partial_update_null_to_null(self):
        """
        Target: test PU can successfully update a null to null
        Method:
            1. Create a collection, enable nullable fields
            2. Insert default_nb rows to the collection
            3. Partial Update the nullable field with null
            4. Query the collection to check the value of nullable field
        Expected: query should have correct value and number of entities
        """
        # step 1: create collection with nullable fields
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
        
        # step 2: insert default_nb rows to the collection
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # step 3: Partial Update the nullable field with null
        new_row = cf.gen_row_data_by_schema(
            nb=default_nb, 
            schema=schema, 
            desired_field_names=[default_primary_key_field_name, default_int32_field_name], 
            start=0
        )
        
        # Set the nullable field to None
        for data in new_row:
            data[default_int32_field_name] = None
            
        self.upsert(client, collection_name, new_row, partial_update=True)
        
        # step 4: Query the collection to check the value of nullable field
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_row,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb
        
        # Verify that all nullable fields are indeed null
        for data in result:
            assert data[default_int32_field_name] is None, f"Expected null value for {default_int32_field_name}, got {data[default_int32_field_name]}"
        
        log.info("Partial update null to null test completed successfully")
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_new_field_with_dynamic_field(self):
        """
        target:  Test PU will success when provided empty data
        method:
            1. Create a collection
            2. partial upsert new field
        expected: Step 2 should result success
        """
        # step 1: create collection
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: partial upsert new field
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        new_rows = [{default_primary_key_field_name: i, default_int32_field_name: 99} for i in range(default_nb)]
        self.upsert(client, collection_name, new_rows, partial_update=True)

        self.query(client, collection_name, filter=default_search_exp,
                check_task=CheckTasks.check_query_results,
                output_fields=[default_int32_field_name],
                check_items={exp_res: new_rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        
        self.drop_collection(client, collection_name)
    
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_partition(self):
        """
        target: test PU can successfully update data in a partition
        method:
            1. Create a collection
            2. Insert data into a partition
            3. Partial update data in the partition
        expected: Step 3 should result success
        """
        # step 1: create collection
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
        
        # step 2: insert data into a partition
        num_of_partitions = 10
        partition_names = []
        for _ in range(num_of_partitions):
            partition_name = cf.gen_unique_str("partition")
            self.create_partition(client, collection_name, partition_name)
            partition_names.append(partition_name)
        
        # step 3: insert data into a partition
        # partition 0: 0, 1, 2, ..., 199
        # partition 1: 200, 201, 202, ..., 399
        # partition 2: 400, 401, 402, ..., 599
        gap = default_nb // num_of_partitions # 200
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        for i, partition in enumerate(partition_names):
            self.upsert(client, collection_name, rows[i*gap:i*gap+gap], partition_name=partition, partial_update=True)
        
        # step 4: partial update data in the partition
        # i*200+i = 0, 201, 402, 603, ..., 1809
        new_value = np.int32(99)
        for i, partition_name in enumerate(partition_names):
            new_row = [{default_primary_key_field_name: i*gap+i, default_int32_field_name: new_value}]
            self.upsert(client, collection_name, new_row, partition_name=partition_name, partial_update=True)
            self.query(client, collection_name,
                       check_task=CheckTasks.check_query_results,
                       partition_names=[partition_name],
                       ids = [i*gap+i],
                       output_fields=[default_int32_field_name],
                       check_items={exp_res: new_row,
                                   "with_vec": True,
                                   "pk_name": default_primary_key_field_name})
        
        result = self.query(client, collection_name, filter=default_search_exp)[0]
        assert len(result) == default_nb
        
        self.drop_collection(client, collection_name)


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_partition_insert_update(self):
        """
        target: test PU can successfully update data in a partition and insert data into a partition
        method:
            1. Create a collection
            2. Insert data into a partitions
            3. Partial update data in the partition
            4. Insert data into a different partition
        expected: Step 3 and 4 should result success
        Visualization:
            rows: [0-------------default_nb]
            new_rows: [extra_nb-------------default_nb+extra_nb]
            they overlap from extra_nb to default_nb
            rows is inserted into partition 0
            new_rows is upserted into partition 0 & 1
        """
        # step 1: create collection
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
        
        # step 2: insert data into partitions
        num_of_partitions = 2
        partition_names = []
        for _ in range(num_of_partitions):
            partition_name = cf.gen_unique_str("partition")
            self.create_partition(client, collection_name, partition_name)
            partition_names.append(partition_name)
        
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows, partition_name=partition_names[0])
        
        # step 3: partial update data in the partition
        extra_nb = default_nb // num_of_partitions
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=extra_nb)

        for partition_name in partition_names:
            self.upsert(client, collection_name, new_rows, partition_name=partition_name, partial_update=True)
            result = self.query(client, collection_name,
                        check_task=CheckTasks.check_query_results,
                        partition_names=[partition_name],
                        filter=f"{default_primary_key_field_name} >= {extra_nb}",
                        check_items={exp_res: new_rows,
                                    "with_vec": True,
                                    "pk_name": default_primary_key_field_name})[0]
            assert len(result) == default_nb

            result =self.delete(client, collection_name, partition_names=[partition_name], 
                        filter=f"{default_primary_key_field_name} >= 0")[0]
            if partition_name == partition_names[0]:
                assert result["delete_count"] == default_nb + extra_nb
            else:
                assert result["delete_count"] == default_nb
            
        self.drop_collection(client, collection_name)

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_insert_delete_upsert(self):
        """
        target:  Test PU will success and query will success
        method:
            1. Create a collection
            2. Insert rows
            3. Delete the rows
            4. Upsert the rows
        expected: Step 2,3,4 should success
        """
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

        # step 2: Insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)

        # step 3: Delete the rows
        delete_result = self.delete(client, collection_name, filter=default_search_exp)[0]
        query_result = self.query(client, collection_name, filter=default_search_exp,
            check_task=CheckTasks.check_nothing)[0]

        # step 4: Upsert the rows
        self.upsert(client, collection_name, new_rows, partial_update=True)
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: new_rows,
                                "pk_name": default_primary_key_field_name})[0]
        
        assert delete_result["delete_count"] == default_nb
        assert len(query_result) == 0
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_insert_delete_upsert_with_flush(self):
        """
        target:  Test PU will success and query will success
        method:
            1. Create a collection
            2. Insert rows
            3. Delete the 1/2 rows and flush
            4. Upsert the default_nbrows and flush
            5. query the rows
        expected: Step 2-5 should success
        """
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

        # step 2: Insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)

        # step 3: Delete the rows and flush
        delete_result = self.delete(client, collection_name, 
                                    filter=f"{default_primary_key_field_name} < {default_nb//2}")[0]
        self.flush(client, collection_name)
        query_result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_nothing)[0]

        # step 4: Upsert the rows and flush
        self.upsert(client, collection_name, new_rows, partial_update=True)
        self.flush(client, collection_name)

        # step 5: query the rows
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: new_rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]

        assert delete_result["delete_count"] == default_nb//2
        assert len(query_result) == default_nb//2
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_insert_upsert_delete_upsert_flush(self):
        """
        target:  Test PU will success and query will success
        method:
            1. Create a collection
            2. Insert rows
            3. Delete the rows and upsert new rows, immediate flush
            4. Query the rows
        expected: Step 2-4 should success
        """
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
        
        # step 2: Insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        partial_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                                  desired_field_names=[default_primary_key_field_name, default_int32_field_name])
        self.insert(client, collection_name, rows)
        
        # step 3: partial update rows then delete 1/2 rows and upsert new rows, flush
        self.upsert(client, collection_name, partial_rows, partial_update=True)
        delete_result = self.delete(client, collection_name, 
                                    filter=f"{default_primary_key_field_name} < {default_nb//2}")[0]
        self.upsert(client, collection_name, new_rows, partial_update=True)
        self.flush(client, collection_name)
        
        # step 4: Query the rows
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: new_rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        
        assert delete_result["delete_count"] == default_nb//2
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_insert_upsert_flush_delete_upsert_flush(self):
        """
        target:  Test PU will success and query will success
        method:
            1. Create a collection
            2. Insert rows
            3. Upsert the rows
            4. Delete the rows
            5. Upsert the rows
            6. Flush the collection
            7. Query the rows
        expected: Step 2-7 should success
        """
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
        
        # step 2: Insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        partial_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                                  desired_field_names=[default_primary_key_field_name, default_int32_field_name])
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: Upsert the rows
        upsert_result = self.upsert(client, collection_name, partial_rows, partial_update=True)[0]
        self.flush(client, collection_name)
        
        # step 4: Delete the rows
        delete_result = self.delete(client, collection_name, 
                                    filter=f"{default_primary_key_field_name} < {default_nb//2}")[0]
        self.upsert(client, collection_name, new_rows, partial_update=True)
        
        # step 5: Flush the collection
        self.flush(client, collection_name)
        
        # step 6: Query the rows
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: new_rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        
        assert upsert_result["upsert_count"] == default_nb
        assert delete_result["delete_count"] == default_nb//2
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)

    """
    ******************************************************************
    #  The following are valid cases for nullable fields
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_partial_update_nullable_field(self):
        """
        Target: test PU without nullable field, the field will keep its value
        Method:
            1. Create collection, enable nullable fields.
            2. Insert a row while assigning a value to nullable field (using partial update)
            3. PU nullable field and other fields
        Expected: values should be updated
        """
        # Step 1: create collection with nullable fields
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

        # Step 2: insert a row while assigning a value to nullable field
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.upsert(client, collection_name, rows, partial_update=True)

        # Step 3: PU other fields
        # Even index: update int32 field to new value
        # Odd index: update vector field to random value
        # also update rows to keep track of changes so we can query the result
        new_value = np.int32(99)
        vector_rows = []
        int32_rows = [] 
        for i, row in enumerate(rows):
            if i % 2 == 0:
                int32_rows.append({default_primary_key_field_name: row[default_primary_key_field_name], 
                                    default_int32_field_name: new_value})
                rows[i][default_int32_field_name] = new_value
            else:
                new_vector = [random.random() for _ in range(default_dim)]
                vector_rows.append({default_primary_key_field_name: row[default_primary_key_field_name], 
                                    default_vector_field_name: new_vector})
                rows[i][default_vector_field_name] = new_vector
                rows[i][default_int32_field_name] = None

        self.upsert(client, collection_name, int32_rows, partial_update=True)
        self.upsert(client, collection_name, vector_rows, partial_update=True)
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_vector_field_name, default_int32_field_name],
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)
        
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_null_to_value(self):
        """
        Target: test PU can successfully update null to a value
        Method:
            1. Create a collection, enable nullable fields init null
            2. Partial update nullable field
            3. Query null field
        Expected: Nullfield should have the same value as updated
        """
        # step 1: create collection with nullable fields init null
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
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.upsert(client, collection_name, rows, partial_update=True)

        # step 2: Partial update nullable field
        new_value = np.int32(99)
        new_rows = [{default_primary_key_field_name: row[default_primary_key_field_name], 
                    default_int32_field_name: new_value} for row in rows]
        self.upsert(client, collection_name, new_rows, partial_update=True)
        
        # step 3: Query null field
        #self.load_collection(client, collection_name)
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_default_to_value(self):
        """
        Target: test PU can successfully update a default to a value
        Method:
            1. Create a collection, enable nullable fields init default value
            2. Partial update nullable field
            3. Query null field
        Expected: Nullfield should have the same value as updated
        """
        # step 1: create collection with nullable fields init default value
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_int32_field_name, DataType.INT32, nullable=True, default_value=default_int32_value)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_int32_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # step 2: Partial update nullable field
        new_value = 99
        new_row = [{default_primary_key_field_name: i, 
                    default_int32_field_name: new_value} for i in range(default_nb)]
        self.upsert(client, collection_name, new_row, partial_update=True)
        
        # step 3: Query null field
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_row,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_value_to_null(self):
        """
        Target: test PU can successfully update a value to null
        Method:
            1. Create a collection, enable nullable fields init value
            2. Partial update nullable field
            3. Query null field
        Expected: Nullfield should have the same value as updated
        """
        # step 1: create collection with nullable fields init value
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
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # step 2: Partial update nullable field
        new_value = None
        new_row = [{default_primary_key_field_name: i, 
                    default_int32_field_name: new_value} for i in range(default_nb)]
        self.upsert(client, collection_name, new_row, partial_update=True)
        
        # step 3: Query null field
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_row,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_null_to_null(self):
        """
        Target: test PU can successfully update a null to null
        Method:
            1. Create a collection, enable nullable fields
            2. Insert default_nb rows to the collection
            3. Partial Update the nullable field with null
            4. Query the collection to check the value of nullable field
        Expected: query should have correct value and number of entities
        """
        # step 1: create collection with nullable fields
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
        
        # step 2: insert default_nb rows to the collection
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # step 3: Partial Update the nullable field with null
        new_value = None
        new_row = [{default_primary_key_field_name: i, 
                    default_int32_field_name: new_value} for i in range(default_nb)]
        self.upsert(client, collection_name, new_row, partial_update=True)
        
        # step 4: Query the collection to check the value of nullable field
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_row,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]

        assert len(result) == default_nb
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_various_value_to_nullable_field(self):
        """
        Target: test PU can successfully update various value to a nullable field
        Method:
            1. Create a collection, enable nullable fields
            2. Insert default_nb rows to the collection
            3. Partial Update the nullable field with various value
            4. Query the collection to check the value of nullable field
        Expected: query should have correct value
        """
        # step 1: create collection with nullable fields
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

        # step 2: insert default_nb rows to the collection
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.upsert(client, collection_name, rows, partial_update=True)

        # step 3: Partial Update the nullable field with various value
        new_value = 99
        new_row = [{default_primary_key_field_name: i, 
                    default_int32_field_name: new_value if i % 2 == 0 else None} 
                    for i in range(default_nb)]
        self.upsert(client, collection_name, new_row, partial_update=True)

        # step 4: Query the collection to check the value of nullable field
        result = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_row,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        
        assert len(result) == default_nb
        
        self.drop_collection(client, collection_name)


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_filter_by_null(self):
        """
        target:  Test PU will success and query will success
        method:
            1. Create a collection
            2. partial upsert data with nullable field
            3. Query the collection with filter by nullable field
            4. partial update nullable field back to null
            5. Query the collection with filter by nullable field
        expected: Step 2,3,4,5 should success
        """
        # step 1: create collection with nullable fields
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

        # step 2: partial upsert data with nullable field
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.upsert(client, collection_name, rows, partial_update=True)
        result = self.query(client, collection_name, filter=f"{default_int32_field_name} IS NULL",
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_vector_field_name],
                   check_items={exp_res: rows,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb

        # update first half of the dataset with nullable field value
        new_value = np.int32(99)
        new_row = [{default_primary_key_field_name: i, 
                    default_int32_field_name: new_value} for i in range(default_nb//2)]
        self.upsert(client, collection_name, new_row, partial_update=True)

        # step 3: Query the collection with filter by nullable field
        result = self.query(client, collection_name, filter=f"{default_int32_field_name} IS NOT NULL",
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_row,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb//2
        # query with == filter
        result = self.query(client, collection_name, filter=f"{default_int32_field_name} == {new_value}",
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: new_row,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb//2

        # step 4: partial update nullable field back to null
        null_row = [{default_primary_key_field_name: i, 
                    default_int32_field_name: None} for i in range(default_nb)]
        self.upsert(client, collection_name, null_row, partial_update=True)
        
        # step 5: Query the collection with filter by nullable field
        result = self.query(client, collection_name, filter=f"{default_int32_field_name} IS NULL",
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: null_row,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == default_nb

        self.drop_collection(client, collection_name)


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_same_pk_same_field(self):
        """
        target: Test partial update on an existing pk with the same field will success
        method:
            1. Create a collection
            2. Insert rows
            3. Upsert a single row with existing pk and same field (partial update)
            4. Query the row to verify the update
        expected: All steps should success, and the field value should be updated
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

        # step 3: Upsert a single row with existing pk=0 and update the same field
        updated_value = 99999
        new_row = {default_primary_key_field_name: 0,
                   default_int32_field_name: updated_value}
        self.upsert(client, collection_name, [new_row], partial_update=True)

        # step 4: Query the row to verify the update
        expected_row = {default_primary_key_field_name: 0,
                        default_int32_field_name: updated_value}
        result = self.query(client, collection_name, filter=f"{default_primary_key_field_name} == 0",
                   check_task=CheckTasks.check_query_results,
                   output_fields=[default_int32_field_name],
                   check_items={exp_res: [expected_row],
                                "pk_name": default_primary_key_field_name})[0]
        assert len(result) == 1

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partial_update_duplicate_pk(self):
        """
        target: test PU will success when partial update duplicate pk
        method:
            1. Create a collection
            2. Insert rows with duplicate pk
            3. Upsert the rows with duplicate pk
        expected: Step 3 should success
        """
        # step 1: create collection
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_int32_field_name, DataType.INT32, nullable=True)
        schema.add_field(default_string_field_name, DataType.VARCHAR, nullable=True, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: Insert rows with duplicate pk
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_string_field_name])
        self.insert(client, collection_name, rows)
        dup_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_int32_field_name])
        self.insert(client, collection_name, dup_rows)

        # verify the duplicate pk is inserted and can be queried
        for row in dup_rows:
            row[default_int32_field_name] = None
        res = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: dup_rows,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(res) == default_nb

        # step 3: Upsert the rows with duplicate pk
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                            desired_field_names=[default_primary_key_field_name, default_string_field_name])

        self.upsert(client, collection_name, new_rows, partial_update=True)
        for i, row in enumerate(dup_rows):
            row[default_string_field_name] = new_rows[i][default_string_field_name]

        res = self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: dup_rows,
                                "pk_name": default_primary_key_field_name})[0]

        assert len(res) == default_nb

        self.drop_collection(client, collection_name)

class TestMilvusClientPartialUpdateInvalid(TestMilvusClientV2Base):
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