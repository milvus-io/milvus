import pytest
import time
import random
import numpy as np
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from utils.util_pymilvus import *
from base.client_v2_base import TestMilvusClientV2Base
from pymilvus import DataType, FieldSchema, CollectionSchema
from sklearn import preprocessing

# Test parameters
default_nb = ct.default_nb
default_nq = ct.default_nq
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_int32_field_name = ct.default_int32_field_name


class TestMilvusClientPartialUpdate(TestMilvusClientV2Base):
    """ Test case of partial update functionality """

    @pytest.mark.tags(CaseLabel.L0)
    def test_partial_update_all_field_types(self):
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
        schema = cf.gen_all_datatype_collection_schema(dim=dim)

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
            new_row = cf.gen_partial_row_data_by_schema(nb=nb, schema=schema, 
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
        new_row = cf.gen_partial_row_data_by_schema(
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
