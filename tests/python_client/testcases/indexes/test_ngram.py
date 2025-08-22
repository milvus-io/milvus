import logging
import time
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base
import pytest
from idx_ngram import NGRAM

index_type = "NGRAM"
success = "success"
pk_field_name = 'id'
vector_field_name = 'vector'
content_field_name = 'content_ngram'
json_field_name = 'json_field'
dim = ct.default_dim
default_nb = 2000
default_build_params = {"min_gram": 2, "max_gram": 3}


class TestNgramBuildParams(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", NGRAM.build_params)
    def test_ngram_build_params(self, params):
        """
        Test the build params of NGRAM index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=1000)
        
        # Check if this test case requires JSON field
        build_params = params.get("params", None)
        has_json_params = (build_params is not None and 
                          ("json_path" in build_params or "json_cast_type" in build_params))
        
        target_field_name = content_field_name  # Default to VARCHAR field
        
        if has_json_params:
            # Add JSON field for JSON-related parameter tests
            schema.add_field(json_field_name, datatype=DataType.JSON)
            target_field_name = json_field_name
            
        self.create_collection(client, collection_name, schema=schema)

        # Insert test data
        nb = 3
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        
        if has_json_params:
            # JSON test data
            json_test_data = [
                {"body": "The stadium is large", "title": "Sports"},
                {"body": "The park is beautiful", "title": "Nature"},
                {"body": "The school is nearby", "title": "Education"}
            ]
            for i, row in enumerate(rows):
                row[content_field_name] = f"text content {i}"  # Still provide VARCHAR data
                row[json_field_name] = json_test_data[i]
        else:
            # VARCHAR test data
            varchar_test_data = ["The stadium is large", "The park is beautiful", "The school is nearby"]
            for i, row in enumerate(rows):
                row[content_field_name] = varchar_test_data[i]
                
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_name = cf.gen_str_by_length(10, letters_only=True)
        index_params.add_index(field_name=target_field_name,
                               index_name=index_name,
                               index_type=index_type,
                               params=build_params)

        # Build index
        if params.get("expected", None) != success:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items=params.get("expected"))
        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=index_name)

            # Create vector index before loading collection
            vector_index_params = self.prepare_index_params(client)[0]
            vector_index_params.add_index(field_name=vector_field_name,
                                          metric_type=cf.get_default_metric_for_vector_type(
                                              vector_type=DataType.FLOAT_VECTOR),
                                          index_type="IVF_FLAT",
                                          params={"nlist": 128})
            self.create_index(client, collection_name, vector_index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

            # Load collection
            self.load_collection(client, collection_name)

            # Test query based on field type
            if has_json_params:
                filter_expr = f"{json_field_name}['body'] LIKE \"%stadium%\""
            else:
                filter_expr = f'{content_field_name} LIKE "%stadium%"'
                
            self.query(client, collection_name, filter=filter_expr,
                       output_fields=["count(*)"],
                       check_task=CheckTasks.check_query_results,
                       check_items={"enable_milvus_client_api": True,
                                    "count(*)": 1})

            # Verify the index params are persisted
            idx_info = client.describe_index(collection_name, index_name)
            if build_params is not None:
                for key, value in build_params.items():
                    if value is not None and key not in ["json_path", "json_cast_type"]:
                        assert key in idx_info.keys()
                        assert str(value) in idx_info.values()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("scalar_field_type", ct.all_scalar_data_types)
    def test_ngram_on_all_scalar_fields(self, scalar_field_type):
        """
        Test NGRAM index on all scalar field types and verify proper error handling
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        
        # Add the scalar field with appropriate parameters
        if scalar_field_type == DataType.VARCHAR:
            schema.add_field("scalar_field", datatype=scalar_field_type, max_length=1000)
        elif scalar_field_type == DataType.ARRAY:
            schema.add_field("scalar_field", datatype=scalar_field_type, 
                           element_type=DataType.VARCHAR, max_capacity=10, max_length=100)
        else:
            schema.add_field("scalar_field", datatype=scalar_field_type)

        self.create_collection(client, collection_name, schema=schema)

        # Generate appropriate test data for each field type
        nb = 3
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        
        # Update scalar field with appropriate test data
        if scalar_field_type == DataType.VARCHAR:
            test_data = ["The stadium is large", "The park is beautiful", "The school is nearby"]
            for i, row in enumerate(rows):
                row["scalar_field"] = test_data[i]
        elif scalar_field_type == DataType.JSON:
            test_data = [
                {"body": "This is a school", "title": "Education"},
                {"body": "This is a park", "title": "Recreation"},
                {"body": "This is a mall", "title": "Shopping"}
            ]
            for i, row in enumerate(rows):
                row["scalar_field"] = test_data[i]
        elif scalar_field_type == DataType.ARRAY:
            test_data = [
                ["word1", "word2", "stadium"],
                ["text1", "text2", "park"],
                ["data1", "data2", "school"]
            ]
            for i, row in enumerate(rows):
                row["scalar_field"] = test_data[i]
        # For other scalar types, keep the auto-generated data

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create index
        index_name = cf.gen_str_by_length(10, letters_only=True)
        index_params = self.prepare_index_params(client)[0]
        if scalar_field_type == DataType.JSON:
            # JSON field requires json_path and json_cast_type
            index_params.add_index(field_name="scalar_field",
                                   index_name=index_name,
                                   index_type=index_type,
                                   params={
                                       "min_gram": 2,
                                       "max_gram": 3,
                                       "json_path": "scalar_field['body']",
                                       "json_cast_type": "varchar"
                                   })
        else:
            index_params.add_index(field_name="scalar_field",
                                   index_name=index_name,
                                   index_type=index_type,
                                   params=default_build_params)

        # Check if the field type is supported for NGRAM index
        if scalar_field_type not in NGRAM.supported_field_types:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items={"err_code": 999,
                                           "err_msg": "ngram index can only be created on VARCHAR or JSON field"})
        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=index_name)

            # Create vector index before loading collection
            vector_index_params = self.prepare_index_params(client)[0]
            vector_index_params.add_index(field_name=vector_field_name,
                                         metric_type=cf.get_default_metric_for_vector_type(vector_type=DataType.FLOAT_VECTOR),
                                         index_type="IVF_FLAT",
                                         params={"nlist": 128})
            self.create_index(client, collection_name, vector_index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

            self.load_collection(client, collection_name)

            # Test query for supported types
            if scalar_field_type == DataType.VARCHAR:
                filter_expr = 'scalar_field LIKE "%stadium%"'
                self.query(client, collection_name, filter=filter_expr,
                          output_fields=["count(*)"],
                          check_task=CheckTasks.check_query_results,
                          check_items={"enable_milvus_client_api": True,
                                       "count(*)": 1})
            elif scalar_field_type == DataType.JSON:
                filter_expr = "scalar_field['body'] LIKE \"%school%\""
                self.query(client, collection_name, filter=filter_expr,
                          output_fields=["count(*)"],
                          check_task=CheckTasks.check_query_results,
                          check_items={"enable_milvus_client_api": True,
                                       "count(*)": 1})

