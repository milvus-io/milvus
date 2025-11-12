import pytest
import numpy

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus.client.types import LoadState

prefix = "client_collection"
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


class TestMilvusClientCollectionInvalid(TestMilvusClientV2Base):
    """ Test case of create collection interface """

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
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "español", "عربي", "हिंदी", "Русский"])
    def test_milvus_client_collection_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection with invalid collection name
        method: create collection with invalid collection
        expected: raise exception
        """
        client = self._client()
        # 1. create collection
        if collection_name == "español":
            expected_msg = "collection name can only contain numbers, letters and underscores"
        else:
            expected_msg = "the first character of a collection name must be an underscore or letter"
        
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. {expected_msg}: invalid parameter"}
        self.create_collection(client, collection_name, default_dim,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_name_over_max_length(self):
        """
        target: test fast create collection with over max collection name length
        method: create collection with over max collection name length
        expected: raise exception
        """
        client = self._client()
        # 1. create collection
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters"}
        self.create_collection(client, collection_name, default_dim,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_name_empty(self):
        """
        target: test fast create collection name with empty
        method: create collection name with empty
        expected: raise exception
        """
        client = self._client()
        # 1. create collection
        collection_name = "  "
        error = {ct.err_code: 1100, ct.err_msg: "Invalid collection name"}
        self.create_collection(client, collection_name, default_dim,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_dim", ct.invalid_dims)
    def test_milvus_client_collection_vector_invalid_dim_default_schema(self, invalid_dim):
        """
        target: Test collection with invalid vector dimension
        method: Create collection with vector field having invalid dimension
        expected: Raise exception with appropriate error message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Determine expected error based on invalid dimension type
        if isinstance(invalid_dim, int) and (invalid_dim > 32768):
            expected_msg = f"invalid dimension: {invalid_dim} of field {default_vector_field_name}. float vector dimension should be in range 2 ~ 32768"
        elif isinstance(invalid_dim, int) and (invalid_dim < 2):  # range errors: 1, -32
            expected_msg = f"invalid dimension: {invalid_dim}. should be in range 2 ~ 32768"
        elif isinstance(invalid_dim, str):  # type conversion errors: "vii", "十六"
            expected_msg = f"wrong type of argument [dimension], expected type: [int], got type: [str]"
        elif isinstance(invalid_dim, float):  # type conversion errors: 32.1
            expected_msg = f"wrong type of argument [dimension], expected type: [int], got type: [float]"
        # Try to create collection and expect error
        error = {ct.err_code: 65535, ct.err_msg: expected_msg}
        self.create_collection(client, collection_name, invalid_dim,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 1554")
    def test_milvus_client_collection_invalid_primary_field(self):
        """
        target: test fast create collection name with invalid primary field
        method: create collection name with invalid primary field
        expected: raise exception
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
        target: test creating a collection with string primary key and auto_id but without specifying max_length
        method: attempt to create collection with string primary key and auto_id=True, omitting max_length
        expected: raise exception due to missing max_length for string primary key
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        error = {ct.err_code: 65535, ct.err_msg: f"type param(max_length) should be specified for the field(id) "
                                                 f"of collection {collection_name}"}
        self.create_collection(client, collection_name, default_dim, id_type="string", auto_id=True,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("auto_id", [None, 1, "string"])
    def test_collection_auto_id_invalid_types(self, auto_id):
        """
        target: test collection creation with invalid auto_id types
        method: attempt to create a collection with auto_id set to non-bool values
        expected: raise exception indicating auto_id must be bool
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Attempt to create a collection with invalid auto_id
        error = {ct.err_code: 0, ct.err_msg: "Param auto_id must be bool type"}
        self.create_collection(client, collection_name, default_dim, auto_id=auto_id,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_auto_id_none_in_field(self):
        """
        target: test collection with auto_id set to None in field definition
        method: try to create a collection with a primary key field where auto_id=None
        expected: raise exception indicating auto_id must be bool
        """
        client = self._client()
        # Create schema and try to add field with auto_id=None - this should raise exception
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        error = {ct.err_code: 0, ct.err_msg: "Param auto_id must be bool type"}
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=None,
                       check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_multi_fields_auto_id(self):
        """
        target: test collection auto_id with multi fields (non-primary field with auto_id)
        method: specify auto_id=True for a non-primary int64 field
        expected: raise exception indicating auto_id can only be specified on primary key field
        """
        client = self._client()        
        # Create schema and try to add non-primary field with auto_id=True - this should raise exception
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Add primary key field
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        # Test that adding a non-primary field with auto_id=True raises exception
        error = {ct.err_code: 0, ct.err_msg: "auto_id can only be specified on the primary key field"}
        self.add_field(schema, "int_field", DataType.INT64, auto_id=True,
                       check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_auto_id_non_primary_field(self):
        """
        target: test collection set auto_id in non-primary field
        method: set auto_id=True in non-primary field directly
        expected: raise exception indicating auto_id can only be specified on primary key field
        """
        client = self._client()
        # Create schema and try to add non-primary field with auto_id=True - this should raise exception
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Test that creating a non-primary field with auto_id=True raises exception
        error = {ct.err_code: 999, ct.err_msg: "auto_id can only be specified on the primary key field"}
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, auto_id=True,
                       check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_collection_dup_name_different_params(self):
        """
        target: test create same collection with different parameters
        method: create same collection with different dims, schemas, and primary fields
        expected: raise exception for all different parameter cases
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        # Test 1: Different dimensions
        error = {ct.err_code: 1, ct.err_msg: f"create duplicate collection with different parameters, "
                                             f"collection: {collection_name}"}
        self.create_collection(client, collection_name, default_dim + 1,
                               check_task=CheckTasks.err_res, check_items=error)
        # Test 2: Different schemas  
        schema_diff = self.create_schema(client, enable_dynamic_field=False)[0]
        schema_diff.add_field("new_id", DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False)
        schema_diff.add_field("new_vector", DataType.FLOAT_VECTOR, dim=128)
        self.create_collection(client, collection_name, schema=schema_diff,
                               check_task=CheckTasks.err_res, check_items=error)
        # Test 3: Different primary fields
        schema2 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema2.add_field("id_2", DataType.INT64, is_primary=True, auto_id=False)
        schema2.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema2,
                               check_task=CheckTasks.err_res, check_items=error)
        # Verify original collection's primary field is unchanged
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                             "dim": default_dim,
                                             "id_name": "id"})
        self.drop_collection(client, collection_name)


    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", [1, " ", "invalid"])
    def test_milvus_client_collection_invalid_metric_type(self, metric_type):
        """
        target: test create same collection with invalid metric type
        method: create same collection with invalid metric type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        error = {ct.err_code: 1100, ct.err_msg: f"float vector index does not support metric type: {metric_type}: "
                                                f"invalid parameter[expected=valid index params][actual=invalid index params"}
        self.create_collection(client, collection_name, default_dim, metric_type=metric_type,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 1864")
    def test_milvus_client_collection_invalid_schema_field_name(self):
        """
        target: test create collection with invalid schema field name
        method: create collection with invalid schema field name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("%$#", DataType.VARCHAR, max_length=64,
                         is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=128)
        # 1. create collection
        error = {ct.err_code: 65535,
                 ct.err_msg: "metric type not found or not supported, supported: [L2 IP COSINE HAMMING JACCARD]"}
        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", [6, [[]], "int64", 5.1, (), "", "a", DataType.UNKNOWN])
    def test_milvus_client_collection_invalid_field_type(self, dtype):
        """
        target: test collection with invalid field type
        method: try to add a field with an invalid DataType to schema
        expected: raise exception
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Try to add a field with invalid dtype
        error = {ct.err_code: 999, ct.err_msg: "Field dtype must be of DataType"}
        # The add_field method should raise an error for invalid dtype
        self.add_field(schema, field_name="test", datatype=dtype,
                       check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("unsupported_field_type", [
        DataType.NONE, DataType.BOOL, DataType.INT8, DataType.INT16, DataType.INT32, 
        DataType.FLOAT, DataType.DOUBLE, DataType.STRING, DataType.JSON,
        DataType.ARRAY, DataType.GEOMETRY,
        DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR, DataType.SPARSE_FLOAT_VECTOR,
        DataType.INT8_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR
    ])
    def test_milvus_client_collection_unsupported_primary_field(self, unsupported_field_type):
        """
        target: test collection with unsupported primary field type
        method: create collection with unsupported primary field type
        expected: raise exception when creating collection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with unsupported primary field type
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        if unsupported_field_type in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR, 
                                     DataType.INT8_VECTOR, DataType.FLOAT16_VECTOR, 
                                     DataType.BFLOAT16_VECTOR]:
            schema.add_field("unsupported_primary", unsupported_field_type, is_primary=True, dim=default_dim)
        elif unsupported_field_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field("unsupported_primary", unsupported_field_type, is_primary=True)
        elif unsupported_field_type == DataType.ARRAY:
            schema.add_field("unsupported_primary", unsupported_field_type, is_primary=True, 
                           element_type=DataType.INT64, max_capacity=100)
        else:
            schema.add_field("unsupported_primary", unsupported_field_type, is_primary=True)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        # Try to create collection - should fail here
        error = {ct.err_code: 1100, ct.err_msg: "Primary key type must be DataType.INT64 or DataType.VARCHAR"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_name", ["中文", "español", "عربي", "हिंदी", "Русский", "!@#$%^&*()", "123abc"])
    def test_milvus_client_collection_schema_with_invalid_field_name(self, invalid_name):
        """
        target: test create collection schema with invalid field names
        method: try to create a schema with a field name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Add a field with an invalid name
        schema.add_field(invalid_name, DataType.VARCHAR, max_length=128)
        # Determine expected error message based on invalid field name type
        if invalid_name == "español":
            expected_msg = "Field name can only contain numbers, letters, and underscores."
        else:
            expected_msg = "The first character of a field name must be an underscore or letter."
        error = {ct.err_code: 1701, ct.err_msg: f"Invalid field name: {invalid_name}. {expected_msg}: field name invalid[field={invalid_name}]"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("keyword", [
        "$meta", "like", "exists", "EXISTS", "and", "or", "not", "in",
        "json_contains", "JSON_CONTAINS", "json_contains_all", "JSON_CONTAINS_ALL",
        "json_contains_any", "JSON_CONTAINS_ANY", "array_contains", "ARRAY_CONTAINS",
        "array_contains_all", "ARRAY_CONTAINS_ALL", "array_contains_any", "ARRAY_CONTAINS_ANY",
        "array_length", "ARRAY_LENGTH", "true", "True", "TRUE", "false", "False", "FALSE",
        "text_match", "TEXT_MATCH", "phrase_match", "PHRASE_MATCH", "random_sample", "RANDOM_SAMPLE"
    ])
    def test_milvus_client_collection_field_name_with_keywords(self, keyword):
        """
        target: test collection creation with field name using Milvus keywords
        method: create collection with field name using reserved keywords
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with field name using reserved keyword
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(keyword, DataType.FLOAT_VECTOR, dim=default_dim)
        # Attempt to create collection with invalid field name - should fail
        error = {ct.err_code: 1701, ct.err_msg: f"Invalid field name: {keyword}"}
        self.create_collection(client, collection_name, schema=schema,
                             check_task=CheckTasks.err_res, check_items=error)
    
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_empty_fields(self):
        """
        target: test create collection with empty fields
        method: create collection with schema that has no fields
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create an empty schema (no fields added)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        error = {ct.err_code: 1100, ct.err_msg: "Schema must have a primary key field"}
        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_over_maximum_limits(self):
        """
        target: combine validations for all over-maximum scenarios
        method:
          - Scenario 1: over maximum total fields
          - Scenario 2: over maximum vector fields
          - Scenario 3: multiple vector fields and over maximum total fields
          - Scenario 4: over maximum vector fields and over maximum total fields
        expected: each scenario raises the same errors as in the original individual tests
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # ========== Scenario 1: over maximum total fields ==========
        schema_1 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema_1.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema_1.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        limit_num = ct.max_field_num - 2
        for _ in range(limit_num):
            schema_1.add_field(cf.gen_unique_str("field_name"), DataType.INT64)
        schema_1.add_field(cf.gen_unique_str("extra_field"), DataType.INT64)
        error_fields_over = {ct.err_code: 1, ct.err_msg: "maximum field's number should be limited to 64"}
        self.create_collection(client, collection_name, default_dim, schema=schema_1,
                               check_task=CheckTasks.err_res, check_items=error_fields_over)
        # ========== Scenario 2: over maximum vector fields ==========
        schema_2 = self.create_schema(client, enable_dynamic_field=False)[0]
        for _ in range(ct.max_vector_field_num + 1):
            schema_2.add_field(cf.gen_unique_str("vector_field_name"), DataType.FLOAT_VECTOR, dim=default_dim)
        schema_2.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        error_vector_over = {ct.err_code: 65535, ct.err_msg: "maximum vector field's number should be limited to 4"}
        self.create_collection(client, collection_name, default_dim, schema=schema_2,
                               check_task=CheckTasks.err_res, check_items=error_vector_over)
        # ========== Scenario 3: multiple vector fields and over maximum total fields ==========
        schema_3 = self.create_schema(client, enable_dynamic_field=False)[0]
        vector_limit_num = ct.max_vector_field_num - 2
        for _ in range(vector_limit_num):
            schema_3.add_field(cf.gen_unique_str("field_name"), DataType.FLOAT_VECTOR, dim=default_dim)
        for _ in range(ct.max_field_num):
            schema_3.add_field(cf.gen_unique_str("field_name"), DataType.INT64)
        schema_3.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        error_fields_over_64 = {ct.err_code: 65535, ct.err_msg: "maximum field's number should be limited to 64"}
        self.create_collection(client, collection_name, default_dim, schema=schema_3,
                               check_task=CheckTasks.err_res, check_items=error_fields_over_64)
        # ========== Scenario 4: over maximum vector fields and over maximum total fields ==========
        schema_4 = self.create_schema(client, enable_dynamic_field=False)[0]
        for _ in range(ct.max_vector_field_num + 1):
            schema_4.add_field(cf.gen_unique_str("field_name"), DataType.FLOAT_VECTOR, dim=default_dim)
        for _ in range(limit_num - 4):
            schema_4.add_field(cf.gen_unique_str("field_name"), DataType.INT64)
        schema_4.add_field(cf.gen_unique_str("field_name"), DataType.FLOAT_VECTOR, dim=default_dim)
        schema_4.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        self.create_collection(client, collection_name, default_dim, schema=schema_4,
                               check_task=CheckTasks.err_res, check_items=error_fields_over_64)


    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_without_vectors(self):
        """
        target: test create collection without vectors
        method: create collection only with int field
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with only non-vector fields
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int_field", DataType.INT64, is_primary=True, auto_id=False)
        error = {ct.err_code: 1100, ct.err_msg: "schema does not contain vector field: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("vector_type", [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR])
    def test_milvus_client_collection_vector_without_dim(self, vector_type):
        """
        target: test creating a collection with a vector field missing the dimension
        method: define a vector field without specifying dim and attempt to create the collection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with a vector field missing the dim parameter
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        # Add vector field without dim
        schema.add_field("vector_field", vector_type)
        error = {ct.err_code: 1, ct.err_msg: "dimension is not defined in field type params"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", [DataType.FLOAT_VECTOR, DataType.INT8_VECTOR, DataType.BINARY_VECTOR])
    def test_milvus_client_collection_without_primary_field(self, vector_type):
        """
        target: test create collection without primary field
        method: no primary field specified in collection schema and fields
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with fields but no primary key
        schema1 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema1.add_field("int_field", DataType.INT64)  # Not primary
        schema1.add_field("vector_field", vector_type, dim=default_dim)
        error = {ct.err_code: 1100, ct.err_msg: "Schema must have a primary key field"}
        self.create_collection(client, collection_name, schema=schema1,
                               check_task=CheckTasks.err_res, check_items=error)
        # Create schema with only vector field
        schema2 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema2.add_field("vector_field", vector_type, dim=default_dim)
        error = {ct.err_code: 1100, ct.err_msg: "Schema must have a primary key field"}
        self.create_collection(client, collection_name, schema=schema2,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [[], 1, [1, "2", 3], (1,), {1: 1}])
    def test_milvus_client_collection_non_string_primary_field(self, primary_field):
        """
        target: test collection with non-string primary_field
        method: pass a non-string/non-int value as primary_field to schema creation
        expected: raise exception
        """
        client = self._client()
        # Test at schema creation level - create schema with invalid primary_field parameter
        error = {ct.err_code: 999, ct.err_msg: "Param primary_field must be int or str type"}
        # This should fail when creating schema with invalid primary_field type
        self.create_schema(client, enable_dynamic_field=False, primary_field=primary_field,
                          check_task=CheckTasks.err_res, check_items=error)


    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("is_primary", [None, 2, "string"])
    def test_milvus_client_collection_invalid_is_primary(self, is_primary):
        """
        target: test collection with invalid is_primary value
        method: define a field with is_primary set to a non-bool value and attempt to create a collection
        expected: raise exception indicating is_primary must be bool type
        """
        client = self._client()
        # Create schema and attempt to add a field with invalid is_primary value
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        error = {ct.err_code: 999, ct.err_msg: "Param is_primary must be bool type"}
        # Attempt to add a field with invalid is_primary value, expect error
        self.add_field(schema, "id", DataType.INT64, is_primary=is_primary,
                       check_task=CheckTasks.err_res, check_items=error)


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_dup_field(self):
        """
        target: test create collection with duplicate field names
        method: create schema with two fields having the same name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with duplicate field names
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_field", DataType.INT64, is_primary=True, auto_id=False, max_length=1000)
        schema.add_field("float_field", DataType.FLOAT, max_length=1000)
        schema.add_field("float_field", DataType.INT64, max_length=1000)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)

        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name"}
        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res, check_items=error)
        has_collection = self.has_collection(client, collection_name)[0]
        assert not has_collection

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_as_primary(self):
        """
        target: test fast create collection with add new field as primary
        method: create collection name with add new field as primary
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, "field_new"
        error = {ct.err_code: 1100, ct.err_msg: f"not support to add pk field, "
                                                f"field name = {field_name}: invalid parameter"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.INT64,
                                  nullable=True, is_primary=True, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_none_desc(self):
        """
        target: test create collection with none description
        method: create collection with none description in schema
        expected: raise exception due to invalid description type
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # Try to create schema with None description
        schema = self.create_schema(client, enable_dynamic_field=False, description=None)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=default_dim)
        
        error = {ct.err_code: 1100, ct.err_msg: "description [None] has type NoneType, but expected one of: bytes, str"}
        self.create_collection(client, collection_name, schema=schema,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_invalid_schema_multi_pk(self):
        """
        target: test create collection with schema containing multiple primary key fields
        method: create schema with two primary key fields and use it to create collection
        expected: raise exception due to multiple primary keys
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with multiple primary key fields
        schema_1 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema_1.add_field("field1", DataType.INT64, is_primary=True, auto_id=False)
        schema_1.add_field("field2", DataType.INT64, is_primary=True, auto_id=False)  # Second primary key
        schema_1.add_field("vector_field", DataType.FLOAT_VECTOR, dim=32)
        # Try to create collection with multiple primary keys
        error = {ct.err_code: 999, ct.err_msg: "Expected only one primary key field"}
        self.create_collection(client, collection_name, schema=schema_1,
                               check_task=CheckTasks.err_res, check_items=error)

        schema_2 = self.create_schema(client, enable_dynamic_field=False, primary_field="field2")[0]
        schema_2.add_field("field1", DataType.INT64, is_primary=True, auto_id=False)
        schema_2.add_field("field2", DataType.INT64)  # Second primary key
        schema_2.add_field("vector_field", DataType.FLOAT_VECTOR, dim=32)
        # Try to create collection with multiple primary keys
        error = {ct.err_code: 999, ct.err_msg: "Expected only one primary key field"}
        self.create_collection(client, collection_name, schema=schema_2,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("shards_num,error_type", [(ct.max_shards_num + 1, "range"), (257, "range"), (1.0, "type"), ("2", "type")])
    def test_milvus_client_collection_invalid_shards(self, shards_num, error_type):
        """
        target: test collection with invalid shards_num values
        method: create collection with shards_num that are out of valid range or wrong type
        expected: raise exception with appropriate error message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        if error_type == "range":
            error = {ct.err_code: 1, ct.err_msg: f"maximum shards's number should be limited to {ct.max_shards_num}"}
        else:  # error_type == "type"
            error = {ct.err_code: 999, ct.err_msg: "invalid num_shards type"}
        # Try to create collection with invalid shards_num (should fail)
        self.create_collection(client, collection_name, default_dim, shards_num=shards_num,
                               check_task=CheckTasks.err_res, check_items=error)

class TestMilvusClientCollectionValid(TestMilvusClientV2Base):
    """ Test case of create collection interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["int", "string"])
    def id_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("dim", [ct.min_dim, default_dim, ct.max_dim])
    def test_milvus_client_collection_fast_creation_default(self, dim):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": dim,
                                              "consistency_level": 0})
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        self.load_partitions(client, collection_name, "_default")
        self.release_partitions(client, collection_name, "_default")
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [ct.min_dim, default_dim, ct.max_dim])
    def test_milvus_client_collection_fast_creation_all_params(self, dim, metric_type, id_type, auto_id):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        max_length = 100
        # 1. create collection
        self.create_collection(client, collection_name, dim, id_type=id_type, metric_type=metric_type,
                               auto_id=auto_id, max_length=max_length)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": dim,
                                              "auto_id": auto_id,
                                              "consistency_level": 0})
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("vector_type", [DataType.FLOAT_VECTOR, DataType.INT8_VECTOR])
    @pytest.mark.parametrize("add_field", [True, False])
    def test_milvus_client_collection_self_creation_default(self, nullable, vector_type, add_field):
        """
        target: test self create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id_string", DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", vector_type, dim=dim)
        schema.add_field("title", DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field("nullable_field", DataType.INT64, nullable=nullable, default_value=10)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=nullable)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("embeddings", metric_type="COSINE")
        # index_params.add_index("title")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        check_items = {"collection_name": collection_name,
                       "dim": dim,
                       "consistency_level": 0,
                       "enable_dynamic_field": False,
                       "num_partitions": 16,
                       "id_name": "id_string",
                       "vector_name": "embeddings"}
        if nullable:
            check_items["nullable_fields"] = ["nullable_field", "array_field"]
        if add_field:
            self.add_collection_field(client, collection_name, field_name="field_new_int64", data_type=DataType.INT64,
                                      nullable=True, is_cluster_key=True)
            self.add_collection_field(client, collection_name, field_name="field_new_var", data_type=DataType.VARCHAR,
                                      nullable=True, default_vaule="field_new_var", max_length=64)
            check_items["add_fields"] = ["field_new_int64", "field_new_var"]
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items=check_items)
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['embeddings']
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_self_creation_multiple_vectors(self):
        """
        target: test self create collection with multiple vectors
        method: create collection with multiple vectors
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id_int64", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("embeddings_1", DataType.INT8_VECTOR, dim=dim * 2)
        schema.add_field("embeddings_2", DataType.FLOAT16_VECTOR, dim=int(dim / 2))
        schema.add_field("embeddings_3", DataType.BFLOAT16_VECTOR, dim=int(dim / 2))
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("embeddings", metric_type="COSINE")
        index_params.add_index("embeddings_1", metric_type="IP")
        index_params.add_index("embeddings_2", metric_type="L2")
        index_params.add_index("embeddings_3", metric_type="COSINE")
        # index_params.add_index("title")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        check_items = {"collection_name": collection_name,
                       "dim": [dim, dim * 2, int(dim / 2), int(dim / 2)],
                       "consistency_level": 0,
                       "enable_dynamic_field": False,
                       "id_name": "id_int64",
                       "vector_name": ["embeddings", "embeddings_1", "embeddings_2", "embeddings_3"]}
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items=check_items)
        index = self.list_indexes(client, collection_name)[0]
        assert sorted(index) == sorted(['embeddings', 'embeddings_1', 'embeddings_2', 'embeddings_3'])
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_key_type", ["int64", "varchar"])
    def test_milvus_client_collection_max_fields_and_max_vector_fields(self, primary_key_type):
        """
        target: merge validations for maximum total fields and maximum vector fields in one case
        method: 
          - Scenario A: create collection with maximum total fields (1 vector + scalars)
          - Scenario B: create collection with maximum vector fields and maximum total fields
        expected: collections created successfully and properties verified for both scenarios
        """
        client = self._client()
        # ===================== Scenario A: maximum total fields (single vector field) =====================
        collection_name_a = cf.gen_collection_name_by_testcase_name()
        schema_a = self.create_schema(client, enable_dynamic_field=False)[0]
        schema_a.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        # Add one vector field
        schema_a.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        # Fill remaining fields with scalars to reach the maximum field number
        remaining_scalar_a = ct.max_field_num - 2
        for _ in range(remaining_scalar_a):
            schema_a.add_field(cf.gen_unique_str("field_name"), DataType.INT64)
        # Create collection and verify
        self.create_collection(client, collection_name_a, default_dim, schema=schema_a)
        assert collection_name_a in self.list_collections(client)[0]
        self.describe_collection(client, collection_name_a,
            check_task=CheckTasks.check_describe_collection_property,
            check_items={
                "collection_name": collection_name_a,
                "fields_num": ct.max_field_num,
                "enable_dynamic_field": False,})
        self.drop_collection(client, collection_name_a)
        # ===================== Scenario B: maximum vector fields + maximum total fields =====================
        collection_name_b = cf.gen_collection_name_by_testcase_name()
        schema_b = self.create_schema(client, enable_dynamic_field=False)[0]
        if primary_key_type == "int64":
            schema_b.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        else:
            schema_b.add_field("pk_string", DataType.VARCHAR, max_length=100, is_primary=True)
        # Add maximum number of vector fields
        vector_field_names = []
        for _ in range(ct.max_vector_field_num):
            vector_field_name = cf.gen_unique_str("vector_field")
            vector_field_names.append(vector_field_name)
            schema_b.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        # Fill remaining with scalars to reach maximum fields
        remaining_scalar_b = ct.max_field_num - ct.max_vector_field_num - 1
        for _ in range(remaining_scalar_b):
            schema_b.add_field(cf.gen_unique_str("scalar_field"), DataType.INT64)
        # Create collection and verify
        self.create_collection(client, collection_name_b, default_dim, schema=schema_b)
        assert collection_name_b in self.list_collections(client)[0]
        self.describe_collection(client, collection_name_b,
            check_task=CheckTasks.check_describe_collection_property,
            check_items={
                "collection_name": collection_name_b,
                "dim": [default_dim] * ct.max_vector_field_num,
                "enable_dynamic_field": False,
                "id_name": ct.default_int64_field_name if primary_key_type == "int64" else "pk_string",
                "vector_name": vector_field_names,
                "fields_num": ct.max_field_num,
            }
        )
        self.drop_collection(client, collection_name_b)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_primary_in_schema(self):
        """
        target: test collection with primary field
        method: specify primary field in CollectionSchema
        expected: collection.primary_field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with primary field specified in CollectionSchema
        schema = self.create_schema(client, enable_dynamic_field=False, primary_field=ct.default_int64_field_name)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        self.describe_collection(client, collection_name,
            check_task=CheckTasks.check_describe_collection_property,
            check_items={"collection_name": collection_name,
                         "id_name": ct.default_int64_field_name,
                         "enable_dynamic_field": False}
                         )
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_primary_in_field(self):
        """
        target: test collection with primary field
        method: specify primary field in FieldSchema
        expected: collection.primary_field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema and specify primary field in FieldSchema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        self.describe_collection(client, collection_name,
            check_task=CheckTasks.check_describe_collection_property,
            check_items={"collection_name": collection_name,
                         "id_name": ct.default_int64_field_name,
                         "enable_dynamic_field": False})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_primary_field_consistency(self):
        """
        target: Test collection with both CollectionSchema and FieldSchema primary field specification
        method: Specify primary field in CollectionSchema and also set is_primary=True in FieldSchema
        expected: The collection's primary field is set correctly and matches the expected field name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with primary field specified in CollectionSchema
        schema = self.create_schema(client, enable_dynamic_field=False, primary_field="primary_field")[0]
        schema.add_field("primary_field", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        self.describe_collection(client, collection_name,
            check_task=CheckTasks.check_describe_collection_property,
            check_items={"collection_name": collection_name,
                         "id_name": "primary_field",
                         "enable_dynamic_field": False}
                         )
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("set_in", ["field", "schema", "both"])
    def test_milvus_client_collection_auto_id(self, auto_id, set_in):
        """
        target: Test auto_id setting in field schema, collection schema, and both
        method: Set auto_id in different ways and verify the behavior
        expected: auto_id is correctly applied and collection behavior matches expectation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        if set_in == "field":
            # Test setting auto_id in field schema only
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=auto_id)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        elif set_in == "schema":
            # Test setting auto_id in collection schema only
            schema = self.create_schema(client, enable_dynamic_field=False, auto_id=auto_id)[0]
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        else:  # both
            # Test setting auto_id in both field schema and collection schema (should be consistent)
            schema = self.create_schema(client, enable_dynamic_field=False, auto_id=auto_id)[0]
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=auto_id)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection properties
        res = self.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "auto_id": auto_id,
                                                  "enable_dynamic_field": False})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_auto_id_true_on_primary_and_false_on_non_primary(self):
        """
        target: Test collection with auto_id=True on primary field and auto_id=False on non-primary field
        method: Set auto_id=True on primary key field and auto_id=False on a non-primary field, then verify schema auto_id is True
        expected: Collection schema auto_id should be True when primary key field has auto_id=True, regardless of non-primary field auto_id setting
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with primary key field (auto_id=True) and a non-primary field (auto_id=False)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("field2", DataType.INT64, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection properties: auto_id should be True
        res = self.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "auto_id": True,
                                                  "enable_dynamic_field": False})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("field_auto_id", [True, False])
    @pytest.mark.parametrize("schema_auto_id", [True, False])
    def test_milvus_client_collection_auto_id_inconsistent(self, field_auto_id, schema_auto_id):
        """
        target: Test collection auto_id with different settings between field schema and collection schema
        method: Set different auto_id values in field schema and collection schema
        expected: If either field or schema has auto_id=True, final result is True
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with auto_id setting
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=schema_auto_id)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=field_auto_id)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Determine expected auto_id: True if either field or schema has auto_id=True
        expected_auto_id = field_auto_id or schema_auto_id
        # Verify that the final auto_id follows OR logic
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                             "auto_id": expected_auto_id,
                                             "enable_dynamic_field": False})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_collection_dup_name(self):
        """
        target: test create collection with same name
        method: create collection with same name with same default params
        expected: collection properties consistent
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. create collection with same params
        self.create_collection(client, collection_name, default_dim)
        
        collections = self.list_collections(client)[0]
        collection_count = collections.count(collection_name)
        assert collection_name in collections
        assert collection_count == 1, f"Expected only 1 collection named '{collection_name}', but found {collection_count}"
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_collection_dup_name_same_schema(self):
        """
        target: test create collection with dup name and same schema
        method: create collection with dup name and same schema
        expected: two collection object is available and properties consistent
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        description = "test collection description"
        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False, description=description)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=100)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=dim)
        # 1. Create collection with specific schema
        self.create_collection(client, collection_name, schema=schema)
        # Get first collection info
        collection_info_1 = self.describe_collection(client, collection_name)[0]
        description_1 = collection_info_1.get("description", "")
        # 2. Create collection again with same name and same schema
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection still exists and properties are consistent
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        # Get second collection info and compare
        collection_info_2 = self.describe_collection(client, collection_name)[0]
        description_2 = collection_info_2.get("description", "")
        # Verify collection properties are consistent
        assert collection_info_1["collection_name"] == collection_info_2["collection_name"]
        assert description_1 == description_2 == description
        assert len(collection_info_1["fields"]) == len(collection_info_2["fields"])
        # Verify field names and types are the same
        fields_1 = {field["name"]: field["type"] for field in collection_info_1["fields"]}
        fields_2 = {field["name"]: field["type"] for field in collection_info_2["fields"]}
        assert fields_1 == fields_2
        collection_count = collections.count(collection_name)
        assert collection_count == 1, f"Expected only 1 collection named '{collection_name}', but found {collection_count}"
        self.drop_collection(client, collection_name)

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
                                 "limit": default_limit,
                                 "pk_name": default_primary_key_field_name})

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
                                              "dim": default_dim,
                                              "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        self.query(client, collection_name, filter="id in [0, 1]",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
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
                                 "pk_name": default_primary_key_field_name,
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
                                 "pk_name": default_primary_key_field_name,
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
                                 "limit": limit,
                                 "pk_name": default_primary_key_field_name})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[delete_num:],
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
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
                                 "limit": limit,
                                 "pk_name": default_primary_key_field_name})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[delete_num:],
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_rename_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        old_name = collection_name
        new_name = collection_name + "new"
        self.rename_collection(client, old_name, new_name)
        collections = self.list_collections(client)[0]
        assert new_name in collections
        assert old_name not in collections
        self.describe_collection(client, new_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": new_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        index = self.list_indexes(client, new_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        error = {ct.err_code: 100, ct.err_msg: f"collection not found"}
        self.load_partitions(client, old_name, "_default",
                             check_task=CheckTasks.err_res, check_items=error)
        self.load_partitions(client, new_name, "_default")
        self.release_partitions(client, new_name, "_default")
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, new_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="db not ready")
    def test_milvus_client_collection_rename_collection_target_db(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        db_name = "new_db"
        self.using_database(client, db_name)
        old_name = collection_name
        new_name = collection_name + "new"
        self.rename_collection(client, old_name, new_name, target_db=db_name)
        collections = self.list_collections(client)[0]
        assert new_name in collections
        assert old_name not in collections
        self.describe_collection(client, new_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": new_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        index = self.list_indexes(client, new_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        error = {ct.err_code: 100, ct.err_msg: f"collection not found"}
        self.load_partitions(client, old_name, "_default",
                             check_task=CheckTasks.err_res, check_items=error)
        self.load_partitions(client, new_name, "_default")
        self.release_partitions(client, new_name, "_default")
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, new_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_dup_name_drop(self):
        """
        target: test collection with dup name, and drop
        method: 1. create collection with client1
                2. create collection with client2 with same name
                3. use client2 to drop collection
                4. verify collection is dropped and client1 operations fail
        expected: collection is successfully dropped and subsequent operations from the first client should fail with collection not found error
        """
        client1 = self._client(alias="client1")
        client2 = self._client(alias="client2") 
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. Create collection with client1
        self.create_collection(client1, collection_name, default_dim)
        # 2. Create collection with client2 using same name
        self.create_collection(client2, collection_name, default_dim)
        # 3. Use client2 to drop collection
        self.drop_collection(client2, collection_name)
        # 4. Verify collection is deleted
        has_collection = self.has_collection(client1, collection_name)[0]
        assert not has_collection
        error = {ct.err_code: 100, ct.err_msg:  f"can't find collection[database=default]"
                                                f"[collection={collection_name}]"}
        self.describe_collection(client1, collection_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_long_desc(self):
        """
        target: test create collection with long description
        method: create collection with description longer than 255 characters
        expected: collection created successfully with long description
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create long description
        long_desc = "a".join("a" for _ in range(256))
        # Create schema with long description
        schema = self.create_schema(client, enable_dynamic_field=False, description=long_desc)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection with long description
        self.create_collection(client, collection_name, schema=schema)
        collection_info = self.describe_collection(client, collection_name)[0]
        actual_desc = collection_info.get("description", "")
        assert actual_desc == long_desc
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ct.valid_resource_names)
    def test_milvus_client_collection_valid_naming_rules(self, collection_name):
        """
        target: test create collection with valid names following naming rules
        method: 1. create collection using names that follow all supported naming rule elements
                2. create fields with names that also use naming rule elements
                3. verify collection is created successfully with correct properties
        expected: collection created successfully for all valid names
        """
        client = self._client()
        
        # Create schema with fields that also use naming rule elements
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("_1nt", DataType.INT64)  # field name using naming rule elements
        schema.add_field("f10at_", DataType.FLOAT_VECTOR, dim=default_dim)  # vector field with naming elements
        
        # Create collection with valid name
        self.create_collection(client, collection_name, schema=schema)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info["collection_name"] == collection_name
        
        field_names = [field["name"] for field in collection_info["fields"]]
        assert ct.default_int64_field_name in field_names
        assert "_1nt" in field_names
        assert "f10at_" in field_names
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_binary(self):
        """
        target: test collection with binary-vec
        method: create collection with binary vector field
        expected: collection created successfully with binary vector field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with binary vector field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        collections = self.list_collections(client)[0]
        assert collection_name in collections
        collection_info = self.describe_collection(client, collection_name)[0]
        field_names = [field["name"] for field in collection_info["fields"]]
        assert ct.default_int64_field_name in field_names
        assert ct.default_binary_vec_field_name in field_names

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_multi_create_drop(self):
        """
        target: test cycle creation and deletion of multiple collections
        method: in a loop, collections are created and deleted sequentially
        expected: no exception, each collection is created and dropped successfully
        """
        client = self._client()
        c_num = 20

        for i in range(c_num):
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_{i}"
            self.create_collection(client, collection_name, default_dim)
            collections = self.list_collections(client)[0]
            assert collection_name in collections
            # Drop the collection
            self.drop_collection(client, collection_name)
            collections_after_drop = self.list_collections(client)[0]
            assert collection_name not in collections_after_drop

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_after_drop(self):
        """
        target: test create collection after create and drop
        method: 1. create a collection 2. drop the collection 3. re-create with same name
        expected: no exception, collection can be recreated with the same name after dropping
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)        
        self.drop_collection(client, collection_name)
        assert not self.has_collection(client, collection_name)[0]
        
        self.create_collection(client, collection_name, default_dim)
        assert self.has_collection(client, collection_name)[0]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_collection_multithread(self):
        """
        target: Test create collection with multi-thread
        method: Create collection using multi-thread
        expected: Collections are created successfully
        """
        client = self._client()
        threads_num = 8
        threads = []
        collection_names = []

        def create():
            """Create collection in separate thread"""
            collection_name = cf.gen_collection_name_by_testcase_name() + "_" + cf.gen_unique_str()
            collection_names.append(collection_name)
            self.create_collection(client, collection_name, default_dim)
        # Start multiple threads to create collections
        for i in range(threads_num):
            t = MyThread(target=create, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        # Wait for all threads to complete
        for t in threads:
            t.join()
        # Verify all collections were created successfully
        collections_list = self.list_collections(client)[0]
        for collection_name in collection_names:
            assert collection_name in collections_list
            # Clean up: drop the created collection
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_drop_collection_multithread(self):
        """
        target: test create and drop collection with multi-thread
        method: create and drop collection using multi-thread
        expected: collections are created and dropped successfully
        """
        client = self._client()
        threads_num = 8
        threads = []
        collection_names = []

        def create():
            collection_name = cf.gen_collection_name_by_testcase_name()
            collection_names.append(collection_name)
            self.create_collection(client, collection_name, default_dim)
            self.drop_collection(client, collection_name)

        for i in range(threads_num):
            t = MyThread(target=create, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        
        for t in threads:
            t.join()

        # Verify all collections have been dropped
        for collection_name in collection_names:
            assert not self.has_collection(client, collection_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_count_no_vectors(self):
        """
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
                assert the value returned by get_collection_stats is equal to 0
        expected: the count is equal to 0
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        # Get collection stats for empty collection
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == 0
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_non_vector_field_dim(self):
        """
        target: test collection with dim for non-vector field
        method: define int64 field with dim parameter
        expected: no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with non-vector field having dim parameter
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Add INT64 field with dim parameter
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, dim=ct.default_dim)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection
        self.create_collection(client, collection_name, default_dim, schema=schema)
        # Verify collection was created successfully
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        # Verify schema properties
        collection_desc = self.describe_collection(client, collection_name)[0]
        assert collection_desc['collection_name'] == collection_name
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_multi_sparse_vectors(self):
        """
        target: Test multiple sparse vectors in a collection
        method: create 2 sparse vectors in a collection 
        expected: successful creation of a collection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with multiple vector fields including sparse vector
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("vec_sparse", DataType.SPARSE_FLOAT_VECTOR)
        # Create collection
        self.create_collection(client, collection_name, default_dim, schema=schema)
        # Verify collection was created successfully
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.drop_collection(client, collection_name)




class TestMilvusClientDropCollectionInvalid(TestMilvusClientV2Base):
    """ Test case of drop collection interface """
    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/pull/43064 change drop alias restraint")
    def test_milvus_client_drop_collection_invalid_collection_name(self, name):
        """
        target: Test drop collection with invalid params
        method: drop collection with invalid collection name
        expected: raise exception
        """
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. "
                                                f"the first character of a collection name must be an underscore or letter"}
        self.drop_collection(client, name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("nonexisted")
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ['', None])
    def test_milvus_client_drop_collection_with_empty_or_None_collection_name(self, collection_name):
        """
        target: test drop invalid collection
        method: drop collection with empty or None collection name
        expected: raise exception
        """
        client = self._client()
        # Set different error messages based on collection_name value
        error = {ct.err_code: 1, ct.err_msg: f"`collection_name` value {collection_name} is illegal"}
        self.drop_collection(client, collection_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_collection_after_disconnect(self):
        """
        target: test drop collection operation after connection is closed
        method: 1. create collection with client
                2. close the client connection
                3. try to drop_collection with disconnected client
        expected: operation should raise appropriate connection error
        """
        client_temp = self._client(alias="client_drop_collection")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client_temp, collection_name, default_dim)
        self.close(client_temp)
        error = {ct.err_code: 1, ct.err_msg: 'should create connection first'}
        self.drop_collection(client_temp, collection_name,
                           check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientReleaseCollectionInvalid(TestMilvusClientV2Base):
    """ Test case of release collection interface """
    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_release_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {name}. "
                             f"the first character of a collection name must be an underscore or letter"}
        self.release_collection(client, name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("nonexisted")
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default]"
                                                f"[collection={collection_name}]"}
        self.release_collection(client, collection_name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_release_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        # 1. create collection
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters"}
        self.release_collection(client, collection_name, default_dim,
                                check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientReleaseCollectionValid(TestMilvusClientV2Base):
    """ Test case of release collection interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["int", "string"])
    def id_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_unloaded_collection(self):
        """
        target: Test releasing a collection that has not been loaded
        method: Create a collection and call release_collection multiple times without loading
        expected: No raising errors, and the collection can still be dropped
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.release_collection(client, collection_name)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_after_load_collection(self):
        """
        target: test releasing specific partitions after loading entire collection
        method: 1. create collection and partition
                2. load entire collection 
                3. attempt to release specific partition while collection is loaded
        expected: partition release operations work correctly with loaded collection
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        # 1. create collection and partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        self.release_partitions(client, collection_name, ["_default", partition_name])
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, [partition_name])
        self.release_collection(client, collection_name)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)


class TestMilvusClientLoadCollectionInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """
    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_load_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {name}. "
                             f"the first character of a collection name must be an underscore or letter"}
        self.load_collection(client, name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("nonexisted")
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default]"
                                                f"[collection={collection_name}]"}
        self.load_collection(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_after_drop(self):
        """
        target: test load collection after it has been dropped
        method: 1. create collection
                2. drop the collection
                3. try to load the dropped collection
        expected: raise exception indicating collection not found
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        self.drop_collection(client, collection_name)
        error = {ct.err_code: 999, ct.err_msg: "collection not found"}
        self.load_collection(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)
    
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_release_collection(self):
        """
        target: test load, release non-exist collection
        method: 1. load, release and drop collection
                2. load and release dropped collection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # Prepare and create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # Load, release and drop collection
        self.load_collection(client, collection_name)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)
        # Try to load and release dropped collection - should raise exception
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.load_collection(client, collection_name, check_task=CheckTasks.err_res, check_items=error)
        self.release_collection(client, collection_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        self.load_collection(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_load_collection_without_index(self):
        """
        target: test loading a collection without an index
        method: create a collection, drop its index, then attempt to load the collection
        expected: loading should fail with an 'index not found' error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        error = {ct.err_code: 700, ct.err_msg: f"index not found[collection={collection_name}]"}
        self.load_collection(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_names", [[], None])
    def test_milvus_client_load_partition_names_empty(self, partition_names):
        """
        target: test load partitions with empty partition names list
        method: 1. create collection and partition
                2. insert data into both default partition and custom partition
                3. create index
                4. attempt to load with empty partition_names list
        expected: should raise exception indicating no partition specified
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # 1. Create collection and partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # 2. Insert data into both partitions
        rng = np.random.default_rng(seed=19530)
        half = default_nb // 2
        # Insert into default partition  
        data_default = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
            default_float_field_name: i * 1.0
        } for i in range(half)]
        self.insert(client, collection_name, data_default, partition_name="_default")
        # Insert into custom partition
        data_partition = [{
            default_primary_key_field_name: i + half,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
            default_float_field_name: (i + half) * 1.0
        } for i in range(half)]
        self.insert(client, collection_name, data_partition, partition_name=partition_name)
        # 3. Create index
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # 4. Attempt to load with empty partition_names list
        error = {ct.err_code: 0, ct.err_msg: "due to no partition specified"}
        self.load_partitions(client, collection_name, partition_names=partition_names,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_num_replica", [0.2, "not-int"])
    def test_milvus_client_load_replica_non_number(self, invalid_num_replica):
        """
        target: test load collection with non-number replicas
        method: load with non-number replicas
        expected: raise exceptions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # 2. Insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # 3. Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # 4. Attempt to load with invalid replica_number
        error = {ct.err_code: 999, ct.err_msg: f"`replica_number` value {invalid_num_replica} is illegal"}
        self.load_collection(client, collection_name, replica_number=invalid_num_replica,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("replicas", [None, -1, 0])
    def test_milvus_client_load_replica_invalid_input(self, replicas):
        """
        target: test load partition with invalid replica number or None
        method: load with invalid replica number or None
        expected: load successfully as replica = 1
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. Create collection and prepare
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # 2. Insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # 3. Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # 4. Load with invalid replica_number (should succeed as replica=1)
        self.load_collection(client, collection_name, replica_number=replicas)
        # 5. Verify replicas
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after loading collection, but got {load_state['state']}"
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_replica_greater_than_querynodes(self):
        """
        target: test load with replicas that greater than querynodes
        method: load with 3 replicas (2 querynode)
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. Create collection
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # 2. Insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # 4. Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # 5. Load with replica_number=3 (should fail if only 2 querynodes available)
        error = {ct.err_code: 999,
                 ct.err_msg: "call query coordinator LoadCollection: when load 3 replica count: "
                             "service resource insufficient[currentStreamingNode=2][expectedStreamingNode=3]"}
        self.load_collection(client, collection_name, replica_number=3,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_create_collection_without_connection(self):
        """
        target: test create collection without connection
        method: 1. create collection after connection removed
        expected: raise exception
        """
        client_temp = self._client(alias="client_temp")
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Remove connection
        self.close(client_temp)
        error = {ct.err_code: 1, ct.err_msg: 'should create connection first'}
        self.create_collection(client_temp, collection_name, default_dim,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_after_disconnect(self):
        """
        target: test load/release collection operations after connection is closed
        method: 1. create collection with client
                2. close the client connection
                3. try to load collection with disconnected client
        expected: operations should raise appropriate connection errors
        """
        client_temp = self._client(alias="client_temp")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client_temp, collection_name, default_dim)
        self.close(client_temp)
        error = {ct.err_code: 1, ct.err_msg: 'should create connection first'}
        self.load_collection(client_temp, collection_name,
                            check_task=CheckTasks.err_res, check_items=error)
        
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_collection_after_disconnect(self):
        """
        target: test load/release collection operations after connection is closed
        method: 1. create collection with client
                2. close the client connection
                3. try to release collection with disconnected client
        expected: operations should raise appropriate connection errors
        """
        client_temp = self._client(alias="client_temp2")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client_temp, collection_name, default_dim)
        self.close(client_temp)
        error = {ct.err_code: 999, ct.err_msg: 'should create connection first'}
        self.release_collection(client_temp, collection_name,
                            check_task=CheckTasks.err_res, check_items=error)



class TestMilvusClientLoadCollectionValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["int", "string"])
    def id_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_loaded_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.load_collection(client, collection_name)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partition_after_release_collection(self):
        """
        target: test mixed loading scenarios with partial partitions and full collection
        method: 1. create collection and partition
                2. load specific partition first
                3. then load entire collection  
                4. release and load again
        expected: all loading operations work correctly without conflicts
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        # Step 1: Create collection and partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        # Step 2: Release collection and verify state NotLoad
        self.release_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad after release, but got {load_state['state']}"
        # Step 3: Load specific partition and verify state changes to Loaded
        self.load_partitions(client, collection_name, [partition_name])
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after loading partition, but got {load_state['state']}"
        # Step 4: Load entire collection and verify state remains Loaded
        self.load_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after loading collection, but got {load_state['state']}"
        # Step 5: Release collection and verify state changes to NotLoad
        self.release_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad after release, but got {load_state['state']}"
        # Step 6: Load multiple partitions and verify state changes to Loaded
        self.load_partitions(client, collection_name, ["_default", partition_name])
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after loading partitions, but got {load_state['state']}"
        # Step 7: Load collection again and verify state remains Loaded
        self.load_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after final load collection, but got {load_state['state']}"
        # Step 8: Cleanup - drop collection if it exists
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_load_partitions_after_load_collection(self):
        """
        target: test load partitions after load collection
        method: 1. load collection
                2. load partitions
                3. search on one partition
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        # Create collection and partitions
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        # Verify initial state is Loaded
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after loading collection, but got {load_state['state']}"
        # Load collection and verify state
        self.load_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after loading collection, but got {load_state['state']}"
        # Load partitions and verify state (should remain Loaded)
        self.load_partitions(client, collection_name, [partition_name_1, partition_name_2])
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after loading partitions, but got {load_state['state']}"
        # Search on one partition
        vectors_to_search = np.random.default_rng(seed=19530).random((1, default_dim))
        self.search(client, collection_name, vectors_to_search, 
                   limit=default_limit, partition_names=[partition_name_1])
        # Verify state remains Loaded after search
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after search, but got {load_state['state']}"
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_load_release_comprehensive(self):
        """
        target: comprehensive test for collection load/release operations with search/query validation
        method: 1. test collection load -> search/query (should work)
                2. test collection release -> search/query (should fail)
                3. test repeated load/release operations
                4. test load after release
        expected: proper search/query behavior based on collection load/release state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Step 1: Create collection with data for testing
        self.create_collection(client, collection_name, default_dim)
        # Step 2: Test point 1 - loaded collection can be searched/queried
        self.load_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded, but got {load_state['state']}"
        vectors_to_search = np.random.default_rng(seed=19530).random((1, default_dim))
        self.search(client, collection_name, vectors_to_search, limit=default_limit)
        self.query(client, collection_name, filter=default_search_exp)        
        # Step 3: Test point 2 - loaded collection can be loaded again
        self.load_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after repeated load, but got {load_state['state']}"
        # Step 4: Test point 3 - released collection cannot be searched/queried
        self.release_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad, but got {load_state['state']}"
        error_search = {ct.err_code: 101, ct.err_msg: "collection not loaded"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                   check_task=CheckTasks.err_res, check_items=error_search)
        error_query = {ct.err_code: 101, ct.err_msg: "collection not loaded"}
        self.query(client, collection_name, filter=default_search_exp,
                  check_task=CheckTasks.err_res, check_items=error_query)
        # Step 5: Test point 4 - released collection can be released again
        self.release_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad after repeated release, but got {load_state['state']}"
        # Step 6: Test point 5 - released collection can be loaded again
        self.load_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after reload, but got {load_state['state']}"
        self.search(client, collection_name, vectors_to_search, limit=default_limit)
        # Step 7: Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_partition_load_release_comprehensive(self):
        """
        target: comprehensive test for partition load/release operations with search/query validation
        method: 1. test partition load -> search/query
                2. test partition release -> search/query (should fail)
                3. test repeated load/release operations
                4. test load after release
        expected: proper search/query behavior based on partition load/release state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        # Step 1: Create collection with partitions
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        # Step 2: Test point 1 - loaded partitions can be searched/queried
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, [partition_name_1, partition_name_2])
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded, but got {load_state['state']}"
        vectors_to_search = np.random.default_rng(seed=19530).random((1, default_dim))
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_1, partition_name_2])
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name_1, partition_name_2])
        # Step 3: Test point 2 - loaded partitions can be loaded again
        self.load_partitions(client, collection_name, [partition_name_1, partition_name_2])
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_1, partition_name_2])
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name_1, partition_name_2])
        # Step 4: Test point 3 - released partitions cannot be searched/queried
        self.release_partitions(client, collection_name, [partition_name_1])
        error_search = {ct.err_code: 201, ct.err_msg: "partition not loaded"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_1],
                   check_task=CheckTasks.err_res, check_items=error_search)
        error_query = {ct.err_code: 201, ct.err_msg: "partition not loaded"}
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name_1],
                  check_task=CheckTasks.err_res, check_items=error_query)
        # Non-released partition should still work
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_2])
        # Step 5: Test point 4 - released partitions can be released again
        self.release_partitions(client, collection_name, [partition_name_1])  # Release again
        error_search = {ct.err_code: 201, ct.err_msg: "partition not loaded"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_1],
                   check_task=CheckTasks.err_res, check_items=error_search)
        # Step 6: Test point 5 - released partitions can be loaded again
        self.load_partitions(client, collection_name, [partition_name_1])
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_1])
        # Step 8: Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_mixed_collection_partition_operations_comprehensive(self):
        """
        target: comprehensive test for mixed collection/partition load/release operations
        method: 1. test collection load -> partition release -> mixed behavior
                2. test partition load -> collection load -> behavior
                3. test collection release -> partition load -> behavior
        expected: consistent behavior across mixed operations
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        # Step 1: Setup collection with partitions
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        vectors_to_search = np.random.default_rng(seed=19530).random((1, default_dim))
        # Step 2: Test Release partition after collection release
        self.release_collection(client, collection_name)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad after collection release, but got {load_state['state']}"
        self.release_partitions(client, collection_name, ["_default"])
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad after default partition release, but got {load_state['state']}"
        # Step 3: Load specific partitions
        self.load_partitions(client, collection_name, [partition_name_1])
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded, f"Expected Loaded after partition load, but got {load_state['state']}"
        # Search should work on loaded partitions
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_1])
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name_1])
        # Step 4: Test load collection after partition load
        self.load_collection(client, collection_name)
        self.search(client, collection_name, vectors_to_search, limit=default_limit, partition_names=[partition_name_1, partition_name_2])
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name_1, partition_name_2])
        # Step 5: Test edge case - release all partitions individually
        self.release_partitions(client, collection_name, ["_default", partition_name_1, partition_name_2])
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad after releasing all partitions, but got {load_state['state']}"
        error_search = {ct.err_code: 101, ct.err_msg: "collection not loaded"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                   check_task=CheckTasks.err_res, check_items=error_search)
        # Step 6: Test release collection after partition release
        self.release_collection(client, collection_name)
        assert load_state["state"] == LoadState.NotLoad, f"Expected NotLoad after releasing all partitions, but got {load_state['state']}"
        error = {ct.err_code: 101, ct.err_msg: "collection not loaded"}
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                   check_task=CheckTasks.err_res, check_items=error)
        self.query(client, collection_name, filter=default_search_exp,
                  check_task=CheckTasks.err_res, check_items=error)
        # Step 7: Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_after_drop_partition_and_release_another(self):
        """
        target: test load collection after drop a partition and release another
        method: 1. load collection
                2. drop a partition
                3. release left partition
                4. query on the left partition
                5. load collection
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, [partition_name_1])
        self.drop_partition(client, collection_name, partition_name_1)
        self.release_partitions(client, collection_name, [partition_name_2])
        error = {ct.err_code: 65538, ct.err_msg: 'partition not loaded'}
        self.query(client, collection_name, filter=default_search_exp,
                   partition_names=[partition_name_2],
                   check_task=CheckTasks.err_res, check_items=error)
    
        self.load_collection(client, collection_name)
        self.query(client, collection_name, filter=default_search_exp,
                   partition_names=[partition_name_2])
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partition_after_drop_partition_and_release_another(self):
        """
        target: test load partition after drop a partition and release another
        method: 1. load collection
                2. drop a partition
                3. release left partition
                4. load partition
                5. query on the partition
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, [partition_name_1])
        self.drop_partition(client, collection_name, partition_name_1)
        self.release_partitions(client, collection_name, [partition_name_2])
        error = {ct.err_code: 65538, ct.err_msg: 'partition not loaded'}
        self.query(client, collection_name, filter=default_search_exp,
                   partition_names=[partition_name_2],
                   check_task=CheckTasks.err_res, check_items=error)
        self.load_partitions(client, collection_name, [partition_name_2])
        self.query(client, collection_name, filter=default_search_exp,
                   partition_names=[partition_name_2])
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_another_partition_after_drop_one_partition(self):
        """
        target: test load another partition after drop a partition
        method: 1. load collection
                2. drop a partition
                3. load another partition
                4. query on the partition
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, [partition_name_1])
        self.drop_partition(client, collection_name, partition_name_1)
        self.load_partitions(client, collection_name, [partition_name_2])
        self.query(client, collection_name, filter=default_search_exp,
                   partition_names=[partition_name_2])
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_after_drop_one_partition(self):
        """
        target: test load collection after drop a partition
        method: 1. load collection
                2. drop a partition
                3. load collection
                4. query on the partition
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, [partition_name_1])
        self.drop_partition(client, collection_name, partition_name_1)
        self.load_collection(client, collection_name)
        
        # Query on the remaining partition
        self.query(client, collection_name, filter=default_search_exp,
                   partition_names=[partition_name_2])
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("vector_type", [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR])
    def test_milvus_client_load_collection_after_index(self, vector_type):
        """
        target: test load collection after index created
        method: insert data and create index, load collection with correct params
        expected: no error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        if vector_type == DataType.FLOAT_VECTOR:
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        elif vector_type == DataType.BINARY_VECTOR:
            schema.add_field("binary_vector", DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        
        index_params = self.prepare_index_params(client)[0]
        if vector_type == DataType.FLOAT_VECTOR:
            index_params.add_index(field_name="vector", index_type="IVF_SQ8", metric_type="L2")
        elif vector_type == DataType.BINARY_VECTOR:
            index_params.add_index(field_name="binary_vector", index_type="BIN_IVF_FLAT", metric_type="JACCARD")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_milvus_client_load_replica_change(self):
        """
        target: test load replica change
                2.load with a new replica number
                3.release collection
                4.load with a new replica
                5.verify replica changes and query functionality
        expected: The second time successfully loaded with a new replica number
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # Create index and load with replica_number=1
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name, replica_number=1)
        # Verify initial load state
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded
        # Query to verify functionality
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} in [0]",
                  check_task=CheckTasks.check_query_results,
                  check_items={"exp_res": [rows[0]], "with_vec": True})
        # Load with replica_number=2 (should work)
        self.load_collection(client, collection_name, replica_number=2)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded
        # Release and reload with replica_number=2
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name, replica_number=2)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded
        # Verify query still works after replica change
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} in [0]",
                  check_task=CheckTasks.check_query_results,
                  check_items={"exp_res": [rows[0]], "with_vec": True})
        # Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_milvus_client_load_replica_multi(self):
        """
        target: test load with multiple replicas
        method: 1.create collection with one shard
                2.insert multiple segments
                3.load with multiple replicas
                4.query and search
        expected: Query and search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection with one shard
        self.create_collection(client, collection_name, default_dim, shards_num=1)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        schema_info = self.describe_collection(client, collection_name)[0]
        # Insert multiple segments
        replica_number = 2
        total_entities = 0
        all_rows = []
        for i in range(replica_number):
            rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info, start=i * default_nb)
            self.insert(client, collection_name, rows)
            total_entities += default_nb
            all_rows.extend(rows)
        # Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == total_entities
        # Create index and load with multiple replicas
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name, replica_number=replica_number)
        # Verify load state
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded
        # Query test
        query_res, _ = self.query(client, collection_name, filter=f"{default_primary_key_field_name} in [0, {default_nb}]",
                  check_task=CheckTasks.check_query_results,
                  check_items={"exp_res": [all_rows[0], all_rows[default_nb]], "with_vec": True})
        assert len(query_res) == 2
        # Search test
        vectors_to_search = cf.gen_vectors(default_nq, default_dim)
        self.search(client, collection_name, vectors_to_search,
                   check_task=CheckTasks.check_search_results,
                   check_items={"enable_milvus_client_api": True,
                               "nq": len(vectors_to_search),
                               "limit": default_limit,
                               "pk_name": default_primary_key_field_name})
        # Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_milvus_client_load_replica_partitions(self):
        """
        target: test load replica with partitions
        method: 1.Create collection and one partition
                2.Insert data into collection and partition
                3.Load multi replicas with partition
                4.Query
        expected: Verify query result
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # Create collection
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # Insert data into collection and partition
        schema_info = self.describe_collection(client, collection_name)[0]
        rows_1 = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        rows_2 = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info, start=default_nb)
        self.insert(client, collection_name, rows_1)
        self.create_partition(client, collection_name, partition_name)
        self.insert(client, collection_name, rows_2, partition_name=partition_name)
        # Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb * 2
        # Create index and load partition with multiple replicas
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_partitions(client, collection_name, [partition_name], replica_number=2)
        # Verify load state
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded
        # Query on loaded partition (should succeed)
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} in [{default_nb}]",
                  partition_names=[partition_name],
                  check_task=CheckTasks.check_query_results,
                  check_items={"exp_res": [rows_2[0]], "with_vec": True})
        # Query on non-loaded partition (should fail)
        error = {ct.err_code: 65538, ct.err_msg: "partition not loaded"}
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} in [0]",
                  partition_names=[ct.default_partition_name, partition_name],
                  check_task=CheckTasks.err_res, check_items=error)
        # Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_count_multi_replicas(self):
        """
        target: test count multi replicas
        method: 1. load data with multi replicas
                2. count
        expected: verify count
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, rows)
        # Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # Create index and load with multiple replicas
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name, replica_number=2)
        # Verify load state
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded
        # Count with multi replicas
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                  output_fields=["count(*)"],
                  check_task=CheckTasks.check_query_results,
                  check_items={"exp_res": [{"count(*)": default_nb}]})
        
        # Cleanup
        self.drop_collection(client, collection_name)


    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_load_collection_after_load_release(self):
        """
        target: test load collection after load and release
        method: 1.load and release collection after entities flushed
                2.re-load collection
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # Insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # Prepare and create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # Load, release, and re-load collection
        self.load_collection(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_repeatedly(self):
        """
        target: test load collection repeatedly
        method: load collection twice
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # Insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # Verify entity count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == default_nb
        # Prepare and create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # Load collection twice (test repeated loading)
        self.load_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self.drop_collection(client, collection_name)


class TestMilvusClientLoadPartition(TestMilvusClientV2Base):
    """ Test case of load partition interface """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_after_load_loaded_partition(self):
        """
        target: test load partition after load partition
        method: 1. create collection and two partitions
                4. release collection and load one partition twice
                5. query on the non-loaded partition (should fail)
                6. load the whole collection (should succeed)
        expected: No exception on repeated partition load, error on querying non-loaded partition, success on collection load
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        # 2. create partitions
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        # 5. load partition1 twice
        self.load_partitions(client, collection_name, [partition_name_1])
        self.load_partitions(client, collection_name, [partition_name_1])
        # 6. query on the non-loaded partition2 (should fail)
        error = {ct.err_code: 65538, ct.err_msg: 'partition not loaded'}
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name_2],
                   check_task=CheckTasks.err_res, check_items=error)
        # 7. load partition2 twice
        self.load_partitions(client, collection_name, [partition_name_2])
        self.load_partitions(client, collection_name, [partition_name_2])
        self.query(client, collection_name, filter=default_search_exp, partition_names=[partition_name_2])
        # 8. load the whole collection (should succeed)
        self.load_collection(client, collection_name)
        self.query(client, collection_name, filter=default_search_exp)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_load_collection_after_load_partition_drop_another(self):
        """
        target: test release/load collection after loading one partition and dropping another
        method: 1) load partitions 2) drop one partition 3) release another partition 4) load collection 5) query
        expected: no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        # Create collection and partitions
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        # Load one partition, drop the other, then release the loaded partition
        self.load_partitions(client, collection_name, [partition_name_1])
        self.release_partitions(client, collection_name, [partition_name_2])
        self.drop_partition(client, collection_name, partition_name_2)
        self.release_partitions(client, collection_name, [partition_name_1])
        # Load the whole collection and run a query
        self.load_collection(client, collection_name)
        self.query(client, collection_name, filter=default_search_exp)
        # Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_collection_after_partition_operations(self):
        """
        target: comprehensive test for load collection after various partition operations
        method: combines three V1 test scenarios:
                1. load partition -> release partition -> load collection -> search
                2. load partition -> release partitions -> query (should fail) -> load collection -> query
                3. load partition -> drop partition -> query (should fail) -> drop another -> load collection -> query
        expected: all operations should work correctly with proper error handling
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name_1 = cf.gen_unique_str("partition1")
        partition_name_2 = cf.gen_unique_str("partition2")
        
        # Create collection and partitions
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name_1)
        self.create_partition(client, collection_name, partition_name_2)
        self.release_collection(client, collection_name)
        
        # Scenario 1: load partition -> release partition -> load collection -> search
        self.load_partitions(client, collection_name, [partition_name_1])
        self.release_partitions(client, collection_name, [partition_name_1])
        self.load_collection(client, collection_name)
        vectors_to_search = np.random.default_rng(seed=19530).random((1, default_dim))
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                   partition_names=[partition_name_1, partition_name_2])
        
        # Scenario 2: load partition -> release partitions -> query (should fail) -> load collection -> query
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, [partition_name_1])
        self.release_partitions(client, collection_name, [partition_name_1])
        self.release_partitions(client, collection_name, [partition_name_2])
        error = {ct.err_code: 65535, ct.err_msg: 'collection not loaded'}
        self.query(client, collection_name, filter=default_search_exp,
                  partition_names=[partition_name_1, partition_name_2],
                  check_task=CheckTasks.err_res, check_items=error)
        self.load_collection(client, collection_name)
        self.query(client, collection_name, filter=default_search_exp,
                  partition_names=[partition_name_1, partition_name_2])
        
        # Scenario 3: load partition -> drop partition -> query (should fail) -> drop another -> load collection -> query
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, [partition_name_1])
        self.release_partitions(client, collection_name, [partition_name_1])
        self.drop_partition(client, collection_name, partition_name_1)
        error = {ct.err_code: 65535, ct.err_msg: f'partition name {partition_name_1} not found'}
        self.query(client, collection_name, filter=default_search_exp,
                  partition_names=[partition_name_1, partition_name_2],
                  check_task=CheckTasks.err_res, check_items=error)
        self.drop_partition(client, collection_name, partition_name_2)
        self.load_collection(client, collection_name)
        self.query(client, collection_name, filter=default_search_exp)
        
        # Cleanup
        self.drop_collection(client, collection_name)



class TestMilvusClientDescribeCollectionInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """
    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_describe_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {name}. "
                             f"the first character of a collection name must be an underscore or letter"}
        self.describe_collection(client, name,
                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_describe_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = "nonexisted"
        error = {ct.err_code: 100, ct.err_msg: "can't find collection[database=default][collection=nonexisted]"}
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_describe_collection_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.drop_collection(client, collection_name)
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection[database=default][collection={collection_name}]"}
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientDescribeCollectionValid(TestMilvusClientV2Base):
    """
    ******************************************************************
      The following cases are used to test `describe_collection` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_describe(self):
        """
        target: test describe collection
        method: create a collection and check its information when describe
        expected: return correct information
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # Expected description structure 
        expected_description = {
            'collection_name': collection_name, 
            'auto_id': False, 
            'num_shards': ct.default_shards_num, 
            'description': '',
            'fields': [
                {'field_id': 100, 'name': 'id', 'description': '', 'type': DataType.INT64, 'params': {},
                 'is_primary': True},
                {'field_id': 101, 'name': 'vector', 'description': '', 'type': DataType.FLOAT_VECTOR,
                 'params': {'dim': default_dim}}
            ],
            'functions': [], 
            'aliases': [], 
            'consistency_level': 0, 
            'properties': {'timezone': 'UTC'},
            'num_partitions': 1, 
            'enable_dynamic_field': True
        }
        # Get actual description
        res = self.describe_collection(client, collection_name)[0]
        # Remove dynamic fields that vary between runs (like V1 test)
        assert isinstance(res['collection_id'], int) and isinstance(res['created_timestamp'], int)
        del res['collection_id']
        del res['created_timestamp']
        del res['update_timestamp']
        # Exact comparison
        assert expected_description == res, f"Description mismatch:\nExpected: {expected_description}\nActual: {res}"
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_describe_nullable_default_value(self):
        """
        target: test describe collection with nullable and default_value fields
        method: create a collection with nullable and default_value fields, then check its information when describe
        expected: return correct information
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection with nullable and default_value fields
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT, nullable=True)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=65535, default_value="default_string")
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        # Describe collection and verify nullable and default_value properties
        res = self.describe_collection(client, collection_name)[0]
        # Check fields for nullable and default_value properties
        for field in res["fields"]:
            if field["name"] == "float_field":
                assert field.get("nullable") is True, f"Expected nullable=True for float_field, got {field.get('nullable')}"
            if field["name"] == "varchar_field":
                assert field["default_value"].string_data == "default_string", f"Expected 'default_string', got {field['default_value'].string_data}"
        self.drop_collection(client, collection_name)


class TestMilvusClientHasCollectionValid(TestMilvusClientV2Base):
    """ Test case of has collection interface """
    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_collection_multithread(self):
        """
        target: test has collection with multi-thread
        method: create collection and use multi-thread to check if collection exists
        expected: all threads should correctly identify that collection exists
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        threads_num = 4
        threads = []

        def has():
            result = self.has_collection(client, collection_name)[0]
            assert result == True
        
        for i in range(threads_num):
            t = MyThread(target=has, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        
        for t in threads:
            t.join()

        # Cleanup
        self.drop_collection(client, collection_name)



class TestMilvusClientHasCollectionInvalid(TestMilvusClientV2Base):
    """ Test case of has collection interface """
    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_milvus_client_has_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        if name == "a".join("a" for i in range(256)):
            error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        else:
            error = {ct.err_code: 1100,
                     ct.err_msg: f"Invalid collection name: {name}. "
                                 f"the first character of a collection name must be an underscore or letter"}
        self.has_collection(client, name,
                            check_task=CheckTasks.err_res, check_items=error)
    
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ['', None])
    def test_milvus_client_has_collection_with_empty_or_none_collection_name(self, collection_name):
        """
        target: test has collection with empty or None collection name
        method: call has_collection with empty string or None as collection name
        expected: raise exception with appropriate error message
        """
        client = self._client()
        if collection_name is None:
            error = {ct.err_code: -1, ct.err_msg: '`collection_name` value None is illegal'}
        else:  # empty string
            error = {ct.err_code: -1, ct.err_msg: '`collection_name` value  is illegal'}
        self.has_collection(client, collection_name,
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_collection_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = "nonexisted"
        result = self.has_collection(client, collection_name)[0]
        assert result == False

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_collection_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.drop_collection(client, collection_name)
        result = self.has_collection(client, collection_name)[0]
        assert result == False

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_collection_after_disconnect(self):
        """
        target: test has collection operation after connection is closed
        method: 1. create collection with client
                2. close the client connection
                3. try to has_collection with disconnected client
        expected: operation should raise appropriate connection error
        """
        client_temp = self._client(alias="client_has_collection")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client_temp, collection_name, default_dim)
        self.close(client_temp)
        error = {ct.err_code: 1, ct.err_msg: 'should create connection first'}
        self.has_collection(client_temp, collection_name,
                          check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientListCollection(TestMilvusClientV2Base):
    """ Test case of list collection interface """
    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_list_collections_multi_collections(self):
        """
        target: test list collections with multiple collections
        method: create multiple collections, assert each collection appears in list_collections result
        expected: all created collections are listed correctly
        """
        client = self._client()
        collection_num = 50
        collection_names = []
        # Create multiple collections and verify each collection in list_collections
        for i in range(collection_num):
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_{i}"
            collection_names.append(collection_name)
            self.create_collection(client, collection_name, default_dim)
            assert collection_names[i] in self.list_collections(client)[0]
        # Cleanup - drop all created collections
        for collection_name in collection_names:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_list_collections_after_disconnect(self):
        """
        target: test list collections operation after connection is closed
        method: 1. create collection with client
                2. close the client connection
                3. try to list_collections with disconnected client
        expected: operation should raise appropriate connection error
        """
        client_temp = self._client(alias="client_list_collections")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client_temp, collection_name, default_dim)
        self.close(client_temp)
        error = {ct.err_code: 999, ct.err_msg: 'should create connection first'}
        self.list_collections(client_temp,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_list_collections_multithread(self):
        """
        target: test list collections with multi-threads
        method: create collection and use multi-threads to list collections
        expected: all threads should correctly identify that collection exists in list
        """        
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection first
        self.create_collection(client, collection_name, default_dim)
        threads_num = 10
        threads = []
        def _list():
            collections_list = self.list_collections(client)[0]
            assert collection_name in collections_list
        
        for i in range(threads_num):
            t = MyThread(target=_list)
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()
            
        # Cleanup
        self.drop_collection(client, collection_name)



class TestMilvusClientRenameCollectionInValid(TestMilvusClientV2Base):
    """ Test case of rename collection interface """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_rename_collection_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.rename_collection(client, name, "new_collection",
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_collection_not_existed_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = "nonexisted"
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.rename_collection(client, collection_name, "new_collection",
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_collection_duplicated_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1100, ct.err_msg: f"collection name or database name should be different"}
        self.rename_collection(client, collection_name, collection_name,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        self.drop_collection(client, collection_name)
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.rename_collection(client, collection_name, "new_collection",
                               check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientRenameCollectionValid(TestMilvusClientV2Base):
    """ Test case of rename collection interface """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_rename_collection_multiple_times(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 2. rename with invalid new_name
        new_name = "new_name_rename"
        self.create_collection(client, collection_name, default_dim)
        times = 3
        for _ in range(times):
            self.rename_collection(client, collection_name, new_name)
            self.rename_collection(client, new_name, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_rename_collection_deleted_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        another_collection_name = cf.gen_unique_str("another_collection")
        # 1. create 2 collections
        self.create_collection(client, collection_name, default_dim)
        self.create_collection(client, another_collection_name, default_dim)
        # 2. drop one collection
        self.drop_collection(client, another_collection_name)
        # 3. rename to dropped collection
        self.rename_collection(client, collection_name, another_collection_name)


class TestMilvusClientUsingDatabaseInvalid(TestMilvusClientV2Base):
    """ Test case of using database interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 1900")
    @pytest.mark.parametrize("db_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_using_database_not_exist_db_name(self, db_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        # db_name = cf.gen_unique_str("nonexisted")
        error = {ct.err_code: 999, ct.err_msg: f"database not found[database={db_name}]"}
        self.using_database(client, db_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="# this case is dup to using a non exist db name, try to add one for create database")
    def test_milvus_client_using_database_db_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        pass

class TestMilvusClientCollectionPropertiesInvalid(TestMilvusClientV2Base):
    """ Test case of alter/drop collection properties """
    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("alter_name", ["%$#", "test", " "])
    def test_milvus_client_alter_collection_properties_invalid_collection_name(self, alter_name):
        """
        target: test alter collection properties with invalid collection name
        method: alter collection properties with non-existent collection name
        expected: raise exception
        """
        client = self._client()
        # alter collection properties
        properties = {'mmap.enabled': True}
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default][collection={alter_name}]"}
        self.alter_collection_properties(client, alter_name, properties,
                                     check_task=CheckTasks.err_res,
                                     check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("properties", [""])
    def test_milvus_client_alter_collection_properties_invalid_properties(self, properties):
        """
        target: test alter collection properties with invalid properties
        method: alter collection properties with invalid properties
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        self.describe_collection(client, collection_name,
                                     check_task=CheckTasks.check_describe_collection_property,
                                     check_items={"collection_name": collection_name,
                                                  "dim": default_dim,
                                                  "consistency_level": 0})
        error = {ct.err_code: 1, ct.err_msg: f"`properties` value {properties} is illegal"}
        self.alter_collection_properties(client, collection_name, properties,
                                     check_task=CheckTasks.err_res,
                                     check_items=error)

        self.drop_collection(client, collection_name)

    #TODO properties with non-existent params

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("drop_name", ["%$#", "test", " "])
    def test_milvus_client_drop_collection_properties_invalid_collection_name(self, drop_name):
        """
        target: test drop collection properties with invalid collection name
        method: drop collection properties with non-existent collection name
        expected: raise exception
        """
        client = self._client()
        # drop collection properties
        properties = {'mmap.enabled': True}
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default][collection={drop_name}]"}
        self.drop_collection_properties(client, drop_name, properties,
                                        check_task=CheckTasks.err_res,
                                        check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("property_keys", ["", {}, []])
    def test_milvus_client_drop_collection_properties_invalid_properties(self, property_keys):
        """
        target: test drop collection properties with invalid properties
        method: drop collection properties with invalid properties
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        error = {ct.err_code: 1100, ct.err_msg: f"no properties or delete keys provided"}
        self.drop_collection_properties(client, collection_name, property_keys,
                                        check_task=CheckTasks.err_res,
                                        check_items=error)

        self.drop_collection(client, collection_name)

    # TODO properties with non-existent params


class TestMilvusClientCollectionPropertiesValid(TestMilvusClientV2Base):
    """ Test case of alter/drop collection properties """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_alter_collection_properties(self):
        """
        target: test alter collection
        method: alter collection
        expected: alter successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.release_collection(client, collection_name)
        properties = {"mmap.enabled": True}
        self.alter_collection_properties(client, collection_name, properties)
        describe = self.describe_collection(client, collection_name)[0].get("properties")
        assert describe["mmap.enabled"] == 'True'
        self.release_collection(client, collection_name)
        properties = {"mmap.enabled": False}
        self.alter_collection_properties(client, collection_name, properties)
        describe = self.describe_collection(client, collection_name)[0].get("properties")
        assert describe["mmap.enabled"] == 'False'
        #TODO add case that confirm the parameter is actually valid
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_drop_collection_properties(self):
        """
        target: test drop collection
        method: drop collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, "default")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.release_collection(client, collection_name)
        properties = {"mmap.enabled": True}
        self.alter_collection_properties(client, collection_name, properties)
        describe = self.describe_collection(client, collection_name)[0].get("properties")
        assert describe["mmap.enabled"] == 'True'
        property_keys = ["mmap.enabled"]
        self.drop_collection_properties(client, collection_name, property_keys)
        describe = self.describe_collection(client, collection_name)[0].get("properties")
        assert "mmap.enabled" not in describe
        #TODO add case that confirm the parameter is actually invalid
        self.drop_collection(client, collection_name)


class TestMilvusClientCollectionNullInvalid(TestMilvusClientV2Base):
    """ Test case of collection interface """

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", ct.all_float_vector_dtypes)
    def test_milvus_client_collection_set_nullable_on_pk_field(self, vector_type):
        """
        target: test create collection with nullable=True on primary key field
        method: create collection schema with primary key field set as nullable
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with nullable primary key field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False, nullable=True)
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field("vector", vector_type)
        else:
            schema.add_field("vector", vector_type, dim=default_dim)
        error = {ct.err_code: 1100, ct.err_msg: "primary field not support null"}
        self.create_collection(client, collection_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", ct.all_float_vector_dtypes)
    def test_milvus_client_collection_set_nullable_on_vector_field(self, vector_type):
        """
        target: test create collection with nullable=True on vector field
        method: create collection schema with vector field set as nullable
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with nullable vector field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field("vector", vector_type, nullable=True)
        else:
            schema.add_field("vector", vector_type, dim=default_dim, nullable=True)
        error = {ct.err_code: 1100, ct.err_msg: "vector type not support null"}
        self.create_collection(client, collection_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_set_nullable_on_partition_key_field(self):
        """
        target: test create collection with nullable=True on partition key field
        method: create collection schema with partition key field set as nullable
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with nullable partition key field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("partition_key", DataType.VARCHAR, max_length=64, is_partition_key=True, nullable=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        error = {ct.err_code: 1100, ct.err_msg: "partition key field not support nullable: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)



class TestMilvusClientCollectionDefaultValueInvalid(TestMilvusClientV2Base):
    """ Test case of collection interface """

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", ct.all_float_vector_dtypes)
    def test_milvus_client_create_collection_default_value_on_pk_field(self, vector_type):
        """
        target: test create collection with set default value on pk field
        method: create collection with default value on primary key field
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with primary key field that has default value
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False, default_value=10)
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field("vector", vector_type)
        else:
            schema.add_field("vector", vector_type, dim=default_dim)
        error = {ct.err_code: 1100, ct.err_msg: "primary field not support default_value"}
        self.create_collection(client, collection_name, schema=schema,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", ct.all_float_vector_dtypes)
    def test_milvus_client_create_collection_default_value_on_vector_field(self, vector_type):
        """
        target: test create collection with set default value on vector field
        method: create collection with default value on vector field
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with vector field that has default value
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field("vector", vector_type, default_value=10)
        else:
            schema.add_field("vector", vector_type, dim=default_dim, default_value=10)
        error = {ct.err_code: 1100, ct.err_msg: f"type not support default_value"}
        self.create_collection(client, collection_name, schema=schema,
                             check_task=CheckTasks.err_res, check_items=error)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("scalar_type", ["JSON", "Array"])
    def test_milvus_client_create_collection_default_value_on_not_support_scalar_field(self, scalar_type):
        """
        target: test create collection with set default value on not supported scalar field
        method: create collection with default value on json and array field
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with scalar field that has default value
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        # Add scalar field with default value based on type
        if scalar_type == "JSON":
            schema.add_field("json_field", DataType.JSON, default_value=10)
        elif scalar_type == "Array":
            schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, 
                           max_capacity=ct.default_max_capacity, default_value=10)
        # Add vector field
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        
        error = {ct.err_code: 1100, ct.err_msg: f"type not support default_value, type:{scalar_type}"}
        self.create_collection(client, collection_name, schema=schema,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("default_value", ["abc", 9.09, 1, False])
    @pytest.mark.parametrize("field_type", [DataType.INT8, DataType.FLOAT])
    def test_milvus_client_create_collection_non_match_default_value(self, default_value, field_type):
        """
        target: test create collection with set data type not matched default value
        method: create collection with data type not matched default value
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with field that has mismatched default value type
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Add field with mismatched default value type based on field_type
        if field_type == DataType.INT8:
            schema.add_field("int8_field", DataType.INT8, default_value=default_value)
            field_name = "int8_field"
            field_type_str = "Int8"
        elif field_type == DataType.FLOAT:
            schema.add_field("float_field", DataType.FLOAT, default_value=default_value)
            field_name = "float_field"
            field_type_str = "Float"
        # Determine expected error message based on default_value type
        if isinstance(default_value, str):
            expected_type = "DataType_VarChar"
        elif isinstance(default_value, bool):
            expected_type = "DataType_Bool"
        elif isinstance(default_value, float):
            expected_type = "DataType_Double"
        elif isinstance(default_value, int):
            expected_type = "DataType_Int64"
        error = {ct.err_code: 1100,
                 ct.err_msg: f"type ({field_type_str}) of field ({field_name}) is not equal to the type({expected_type}) of default_value"}
        self.create_collection(client, collection_name, schema=schema,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    def test_milvus_client_create_collection_default_value_none(self, nullable):
        """
        target: test create field with None as default value when nullable is False or True
        method: create collection with default_value=None on one field
        expected: 1. raise exception when nullable=False and default_value=None
                  2. create field successfully when nullable=True and default_value=None
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        
        if nullable:
            schema.add_field("int8_field", DataType.INT8, nullable=nullable, default_value=None)
            self.create_collection(client, collection_name, schema=schema)
        else:
            error = {ct.err_code: 1,
                     ct.err_msg: "Default value cannot be None for a field that is defined as nullable == false"}
            self.add_field(schema, "int8_field", DataType.INT8, nullable=nullable, default_value=None,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", ["abc"])
    def test_milvus_client_create_collection_with_invalid_default_value_string(self, default_value):
        """
        target: Test create collection with invalid default_value for string field
        method: Create collection with string field where default_value exceeds max_length
        expected: Raise exception with appropriate error message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        max_length = 2
        # Create schema with string field having default_value longer than max_length
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("string_field", DataType.VARCHAR, max_length=max_length, default_value=default_value)
        error = {ct.err_code: 1100, ct.err_msg: f"the length ({len(default_value)}) of string exceeds max length ({max_length}): "
                                                f"invalid parameter[expected=valid length string][actual=string length exceeds max length]"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientCollectionDefaultValueValid(TestMilvusClientV2Base):
    """ Test case of collection interface """

    """
    ******************************************************************
    #  The followings are valid cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_collection_default_value_twice(self):
        """
        target: test create collection with set default value twice
        method: create collection with default value twice
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with float field that has default value
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT, default_value=numpy.float32(10.0))
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection twice with same schema and name
        collection_1 = self.create_collection(client, collection_name, schema=schema)[0]
        collection_2 = self.create_collection(client, collection_name, schema=schema)[0]
        # Verify both collections are the same
        assert collection_1 == collection_2
        # Clean up: drop the collection
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_collection_none_twice(self):
        """
        target: test create collection with nullable field twice
        method: create collection with nullable field twice
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with nullable float field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT, nullable=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection twice with same schema and name
        collection_1 = self.create_collection(client, collection_name, schema=schema)[0]
        collection_2 = self.create_collection(client, collection_name, schema=schema)[0]
        # Verify both collections are the same
        assert collection_1 == collection_2
        # Clean up: drop the collection
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_milvus_client_create_collection_using_default_value(self, auto_id):
        """
        target: Test create collection with default_value fields
        method: Create a schema with various fields using default values
        expected: Collection is created successfully with default values
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=auto_id)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Add various scalar fields with default values
        schema.add_field(ct.default_int8_field_name, DataType.INT8, default_value=numpy.int8(8))
        schema.add_field(ct.default_int16_field_name, DataType.INT16, default_value=numpy.int16(16))
        schema.add_field(ct.default_int32_field_name, DataType.INT32, default_value=numpy.int32(32))
        schema.add_field(ct.default_int64_field_name, DataType.INT64, default_value=numpy.int64(64))
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, default_value=numpy.float32(3.14))
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE, default_value=numpy.double(3.1415))
        schema.add_field(ct.default_bool_field_name, DataType.BOOL, default_value=False)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100, default_value="abc")
        # Create collection with default value fields
        self.create_collection(client, collection_name, schema=schema)
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                             "auto_id": auto_id,
                                             "enable_dynamic_field": False,
                                             "schema": schema})
        self.drop_collection(client, collection_name)


class TestMilvusClientCollectionCountIP(TestMilvusClientV2Base):
    """
    Test collection count functionality with different entity counts
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("insert_count", [1, 1000, 2001])
    def test_milvus_client_collection_count_after_index_created(self, insert_count):
        """
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling get_collection_stats with correct params
        expected: count_entities returns correct count
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_vector_field_name)
        # Prepare and insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=insert_count, schema=schema_info)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # Verify entity count
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == insert_count
        self.drop_collection(client, collection_name)


class TestMilvusClientCollectionCountBinary(TestMilvusClientV2Base):
    """
    Test collection count functionality with binary vectors
    Params means different nb, the nb value may trigger merge, or not
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("insert_count", [8, 1000, 2001])
    def test_milvus_client_collection_count_after_index_created_binary(self, insert_count):
        """
        target: Test collection count after binary index is created
        method: Create binary collection, insert data, create index, then verify count
        expected: Collection count equals entities count just inserted
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create binary collection schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Generate and insert binary data
        data = cf.gen_row_data_by_schema(nb=insert_count, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_binary_vec_field_name, index_type="BIN_IVF_FLAT", metric_type="JACCARD")
        self.create_index(client, collection_name, index_params)
        # Verify entity count
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == insert_count
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_milvus_client_binary_collection_with_min_dim(self, auto_id):
        """
        target: Test binary collection when dim=1 (invalid for binary vectors)
        method: Create collection with binary vector field having dim=1
        expected: Raise exception with appropriate error message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with invalid binary vector dimension
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=auto_id)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        # Try to add binary vector field with invalid dimension
        error = {ct.err_code: 1, 
                 ct.err_msg: f"invalid dimension: {ct.min_dim} of field {ct.default_binary_vec_field_name}. "
                             f"binary vector dimension should be multiple of 8."}
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=ct.min_dim)
        # Try to create collection
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_count_no_entities(self):
        """
        target: Test collection count when collection is empty
        method: Create binary collection with binary vector field but insert no data
        expected: The count should be equal to 0
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create binary collection schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        # Create collection without inserting any data
        self.create_collection(client, collection_name, schema=schema)
        # Verify entity count is 0
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == 0
        self.drop_collection(client, collection_name)


class TestMilvusClientCollectionMultiCollections(TestMilvusClientV2Base):
    """
    Test collection count functionality with multiple collections
    Params means different nb, the nb value may trigger merge, or not
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("insert_count", [1, 1000, 2001])
    def test_milvus_client_collection_count_multi_collections_l2(self, insert_count):
        """
        target: Test collection rows_count with multiple float vector collections (L2 metric)
        method: Create multiple collections, insert entities, and verify count for each
        expected: The count equals the length of entities for each collection
        """
        client = self._client()
        collection_list = []
        collection_num = 10
        # Create multiple collections and insert data
        for i in range(collection_num):
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_{i}"
            self.create_collection(client, collection_name, default_dim)
            schema_info = self.describe_collection(client, collection_name)[0]
            data = cf.gen_row_data_by_schema(nb=insert_count, schema=schema_info)
            self.insert(client, collection_name, data)
            self.flush(client, collection_name)
            collection_list.append(collection_name)
        # Verify count for each collection
        for collection_name in collection_list:
            stats = self.get_collection_stats(client, collection_name)[0]
            assert stats['row_count'] == insert_count
        # Cleanup
        for collection_name in collection_list:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("insert_count", [1, 1000, 2001])
    def test_milvus_client_collection_count_multi_collections_binary(self, insert_count):
        """
        target: Test collection rows_count with multiple binary vector collections (JACCARD metric)
        method: Create multiple binary collections, insert entities, and verify count for each
        expected: The count equals the length of entities for each collection
        """
        client = self._client()
        collection_list = []
        collection_num = 20
        # Create multiple binary collections and insert data
        for i in range(collection_num):
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_{i}"
            # Create binary collection schema
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
            schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
            # Create collection
            self.create_collection(client, collection_name, schema=schema)
            # Generate and insert binary data
            data = cf.gen_row_data_by_schema(nb=insert_count, schema=schema)
            self.insert(client, collection_name, data)
            self.flush(client, collection_name)
            collection_list.append(collection_name)
        # Verify count for each collection
        for collection_name in collection_list:
            stats = self.get_collection_stats(client, collection_name)[0]
            assert stats['row_count'] == insert_count
        # Cleanup
        for collection_name in collection_list:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_count_multi_collections_mix(self):
        """
        target: Test collection rows_count with mixed float and binary vector collections
        method: Create both float and binary collections, insert entities, and verify count for each
        expected: The count equals the length of entities for each collection
        """
        client = self._client()
        collection_list = []
        collection_num = 20
        insert_count = ct.default_nb
        # Create half float vector collections and half binary vector collections
        for i in range(0, int(collection_num / 2)):
            # Create float vector collection
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_float_{i}"
            self.create_collection(client, collection_name, default_dim)
            schema_info = self.describe_collection(client, collection_name)[0]
            data = cf.gen_row_data_by_schema(nb=insert_count, schema=schema_info)
            self.insert(client, collection_name, data)
            self.flush(client, collection_name)
            collection_list.append(collection_name)
        for i in range(int(collection_num / 2), collection_num):
            # Create binary vector collection
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_binary_{i}"
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
            schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
            self.create_collection(client, collection_name, schema=schema)
            # Generate and insert binary data
            data = cf.gen_row_data_by_schema(nb=insert_count, schema=schema)
            self.insert(client, collection_name, data)
            self.flush(client, collection_name)
            collection_list.append(collection_name)
        # Verify count for each collection
        for collection_name in collection_list:
            stats = self.get_collection_stats(client, collection_name)[0]
            assert stats['row_count'] == insert_count
        # Cleanup
        for collection_name in collection_list:
            self.drop_collection(client, collection_name)


class TestMilvusClientCollectionString(TestMilvusClientV2Base):
    """
    ******************************************************************
    #  The following cases are used to test about string fields
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_string_field_is_primary(self):
        """
        target: test create collection with string field as primary key
        method: create collection with id_type="string" using fast creation method
        expected: Create collection successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Use fast creation method with string primary key
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=100)
        # Verify collection properties
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                           "id_name": "id"})  
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_string_field_primary_auto_id(self):
        """
        target: test create collection with string primary field and auto_id=True
        method: create collection with string field, the string field primary and auto id are true
        expected: Create collection successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with VARCHAR primary key and auto_id=True
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("string_pk", DataType.VARCHAR, max_length=100, is_primary=True, auto_id=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection properties
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                           "id_name": "string_pk",
                                           "auto_id": True,
                                           "enable_dynamic_field": False})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_only_string_field(self):
        """
        target: test create collection with only string field (no vector field)
        method: create collection with only string field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with only string field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("string_pk", DataType.VARCHAR, max_length=100, is_primary=True, auto_id=False)
        # Try to create collection
        error = {ct.err_code: 1100, ct.err_msg: "schema does not contain vector field: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_string_field_over_max_length(self):
        """
        target: test create collection with string field exceeding max length
        method: 1. create collection with string field
                2. String field max_length exceeds maximum (65535)
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with string field exceeding max length
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Try to add string field with max_length > 65535
        max_length = 65535 + 1
        schema.add_field("string_field", DataType.VARCHAR, max_length=max_length)
        error = {ct.err_code: 1100, ct.err_msg: f"the maximum length specified for the field(string_field) should be in (0, 65535], "
                                                f"but got {max_length} instead: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_invalid_string_field_dtype(self):
        """
        target: test create collection with invalid string field datatype
        method: create collection with string field using DataType.STRING (deprecated)
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        # Try to add field with deprecated DataType.STRING
        error = {ct.err_code: 1100, ct.err_msg: "string data type not supported yet, please use VarChar type instead"}
        schema.add_field("string_field", DataType.STRING)
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientCollectionJSON(TestMilvusClientV2Base):
    """
    ******************************************************************
    #  The following cases are used to test about JSON fields
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_milvus_client_collection_json_field_as_primary_key(self, auto_id):
        """
        target: test create collection with JSON field as primary key
        method: 1. create collection with one JSON field, and vector field
                2. set json field is_primary=true
                3. set auto_id as true
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Test 1: create json field as primary key through field
        schema1 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema1.add_field("json_field", DataType.JSON, is_primary=True, auto_id=auto_id)
        schema1.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        error = {ct.err_code: 1100, ct.err_msg: "Primary key type must be DataType.INT64 or DataType.VARCHAR"}
        self.create_collection(client, collection_name, schema=schema1,
                              check_task=CheckTasks.err_res, check_items=error)
        # Test 2: create json field as primary key through schema
        schema2 = self.create_schema(client, enable_dynamic_field=False, primary_field="json_field", auto_id=auto_id)[0]
        schema2.add_field("json_field", DataType.JSON)
        schema2.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema2, primary_field="json_field",
                              check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientCollectionARRAY(TestMilvusClientV2Base):
    """
    ******************************************************************
    #  The following cases are used to test about ARRAY fields
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_array_field_element_type_not_exist(self):
        """
        target: test create collection with ARRAY field without element type
        method: create collection with one array field without element type
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("array_field", DataType.ARRAY, element_type=None)
        # Try to add array field without element_type
        error = {ct.err_code: 1100, ct.err_msg: "element data type None is not valid"}
        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("element_type", [1001, 'a', [], (), {1}, DataType.BINARY_VECTOR,
                                              DataType.FLOAT_VECTOR, DataType.JSON, DataType.ARRAY])
    def test_milvus_client_collection_array_field_element_type_invalid(self, element_type):
        """
        target: Create a field with invalid element_type
        method: Create a field with invalid element_type
                1. Type not in DataType: 1, 'a', ...
                2. Type in DataType: binary_vector, float_vector, json_field, array_field
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        # Determine expected error based on element_type
        error = {ct.err_code: 1100, ct.err_msg: f"element type {element_type} is not supported"}
        if element_type in ['a', {1}]:
            error = {ct.err_code: 1100, ct.err_msg: "Unexpected error"}
        elif element_type == []:
            error = {ct.err_code: 1100, ct.err_msg: "'list' object cannot be interpreted as an integer"}
        elif element_type == ():
            error = {ct.err_code: 1100, ct.err_msg: "'tuple' object cannot be interpreted as an integer"}
        elif element_type in [DataType.BINARY_VECTOR, DataType.FLOAT_VECTOR, DataType.JSON, DataType.ARRAY]:
            data_type = element_type.name
            if element_type == DataType.BINARY_VECTOR:
                data_type = "BinaryVector"
            elif element_type == DataType.FLOAT_VECTOR:
                data_type = "FloatVector"
            elif element_type == DataType.ARRAY:
                data_type = "Array"
            error = {ct.err_code: 1100, ct.err_msg: f"element type {data_type} is not supported"}
        # Try to add array field with invalid element_type
        schema.add_field("array_field", DataType.ARRAY, element_type=element_type)
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("max_capacity", [None, [], 'a', (), -1, 4097])
    def test_milvus_client_collection_array_field_invalid_capacity(self, max_capacity):
        """
        target: Create a field with invalid max_capacity
        method: Create a field with invalid max_capacity
                1. Type invalid: [], 'a', (), None
                2. Value invalid: <0, >max_capacity(4096)
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        # Determine expected error based on max_capacity type and value
        if max_capacity in [[], 'a', (), None]:
            error = {ct.err_code: 1100, ct.err_msg: "the value for max_capacity of field array_field must be an integer"}
        else:
            error = {ct.err_code: 1100, ct.err_msg: "the maximum capacity specified for a Array should be in (0, 4096]"}
        # Try to add array field with invalid max_capacity
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=max_capacity)
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_string_array_without_max_length(self):
        """
        target: Create string array without giving max length
        method: Create string array without giving max length
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        # Try to add string array field without max_length - should fail at add_field stage
        error = {ct.err_code: 1100, ct.err_msg: "type param(max_length) should be specified for the field(array_field)"}
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.VARCHAR)
        self.create_collection(client, collection_name, schema=schema,
                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("max_length", [-1, 65536])
    def test_milvus_client_collection_string_array_max_length_invalid(self, max_length):
        """
        target: Create string array with invalid max length
        method: Create string array with invalid max length
                Value invalid: <0, >max_length(65535)
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        # Try to add string array field with invalid max_length
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.VARCHAR, max_length=max_length)
        error = {ct.err_code: 1100, ct.err_msg: f"the maximum length specified for the field(array_field) should be in (0, 65535], "
                                                f"but got {max_length} instead: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema,
                      check_task=CheckTasks.err_res, check_items=error)


    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_array_field_all_datatype(self):
        """
        target: test create collection with ARRAY field all data type
        method: 1. Create field respectively: int8, int16, int32, int64, varchar, bool, float, double
                2. Insert data respectively: int8, int16, int32, int64, varchar, bool, float, double
        expected: Create collection successfully and insert data successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with all supported array data types
        nb = default_nb
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("int8_array", DataType.ARRAY, element_type=DataType.INT8, max_capacity=nb)
        schema.add_field("int16_array", DataType.ARRAY, element_type=DataType.INT16, max_capacity=nb)
        schema.add_field("int32_array", DataType.ARRAY, element_type=DataType.INT32, max_capacity=nb)
        schema.add_field("int64_array", DataType.ARRAY, element_type=DataType.INT64, max_capacity=nb)
        schema.add_field("bool_array", DataType.ARRAY, element_type=DataType.BOOL, max_capacity=nb)
        schema.add_field("float_array", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=nb)
        schema.add_field("double_array", DataType.ARRAY, element_type=DataType.DOUBLE, max_capacity=nb)
        schema.add_field("string_array", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=nb, max_length=100)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection properties and all fields
        expected_fields = [
            {"field_id": 100, "name": "int64", "description": "", "type": DataType.INT64, "params": {}, "element_type": 0, "is_primary": True},
            {"field_id": 101, "name": "vector", "description": "", "type": DataType.FLOAT_VECTOR, "params": {"dim": default_dim}, "element_type": 0},
            {"field_id": 102, "name": "int8_array", "description": "", "type": DataType.ARRAY, "params": {"max_capacity": nb}, "element_type": DataType.INT8},
            {"field_id": 103, "name": "int16_array", "description": "", "type": DataType.ARRAY, "params": {"max_capacity": nb}, "element_type": DataType.INT16},
            {"field_id": 104, "name": "int32_array", "description": "", "type": DataType.ARRAY, "params": {"max_capacity": nb}, "element_type": DataType.INT32},
            {"field_id": 105, "name": "int64_array", "description": "", "type": DataType.ARRAY, "params": {"max_capacity": nb}, "element_type": DataType.INT64},
            {"field_id": 106, "name": "bool_array", "description": "", "type": DataType.ARRAY, "params": {"max_capacity": nb}, "element_type": DataType.BOOL},
            {"field_id": 107, "name": "float_array", "description": "", "type": DataType.ARRAY, "params": {"max_capacity": nb}, "element_type": DataType.FLOAT},
            {"field_id": 108, "name": "double_array", "description": "", "type": DataType.ARRAY, "params": {"max_capacity": nb}, "element_type": DataType.DOUBLE},
            {"field_id": 109, "name": "string_array", "description": "", "type": DataType.ARRAY, "params": {"max_length": 100, "max_capacity": nb}, "element_type": DataType.VARCHAR}
        ]
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                           "id_name": "int64",
                                           "enable_dynamic_field": False,
                                           "fields": expected_fields})
        # Generate and insert test data manually
        insert_nb = 10
        pk_values = [i for i in range(insert_nb)]
        float_vec = cf.gen_vectors(insert_nb, default_dim)
        int8_values = [[numpy.int8(j) for j in range(insert_nb)] for i in range(insert_nb)]
        int16_values = [[numpy.int16(j) for j in range(insert_nb)] for i in range(insert_nb)]
        int32_values = [[numpy.int32(j) for j in range(insert_nb)] for i in range(insert_nb)]
        int64_values = [[numpy.int64(j) for j in range(insert_nb)] for i in range(insert_nb)]
        bool_values = [[numpy.bool_(j) for j in range(insert_nb)] for i in range(insert_nb)]
        float_values = [[numpy.float32(j) for j in range(insert_nb)] for i in range(insert_nb)]
        double_values = [[numpy.double(j) for j in range(insert_nb)] for i in range(insert_nb)]
        string_values = [[str(j) for j in range(insert_nb)] for i in range(insert_nb)]
        # Prepare data as list format
        data = []
        for i in range(insert_nb):
            row = {
                "int64": pk_values[i],
                "vector": float_vec[i],
                "int8_array": int8_values[i],
                "int16_array": int16_values[i],
                "int32_array": int32_values[i],
                "int64_array": int64_values[i],
                "bool_array": bool_values[i],
                "float_array": float_values[i],
                "double_array": double_values[i],
                "string_array": string_values[i]
            }
            data.append(row)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats['row_count'] == insert_nb
        self.drop_collection(client, collection_name)


class TestMilvusClientCollectionMultipleVectorValid(TestMilvusClientV2Base):
    """
    ******************************************************************
    #  Test case for collection with multiple vector fields - Valid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_key_type", ["int64", "varchar"])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("shards_num", [1, 3])
    def test_milvus_client_collection_multiple_vectors_all_supported_field_type(self, primary_key_type, auto_id, shards_num):
        """
        target: test create collection with multiple vector fields and all supported field types
        method: create collection with multiple vector fields and all supported field types
        expected: collection created successfully with all field types
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with multiple vector fields and all supported scalar types
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=auto_id)[0]
        # Add primary key field
        if primary_key_type == "int64":
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=auto_id)
        else:
            schema.add_field("id", DataType.VARCHAR, max_length=100, is_primary=True, auto_id=auto_id)
        # Add multiple vector fields
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(ct.default_float16_vec_field_name, DataType.FLOAT16_VECTOR, dim=default_dim)
        schema.add_field(ct.default_bfloat16_vec_field_name, DataType.BFLOAT16_VECTOR, dim=default_dim)
        # Add all supported scalar data types from DataType.__members__
        supported_types = []
        for k, v in DataType.__members__.items():
            if (v and v != DataType.UNKNOWN and v != DataType.STRING 
                and v != DataType.VARCHAR and v != DataType.FLOAT_VECTOR 
                and v != DataType.BINARY_VECTOR and v != DataType.ARRAY 
                and v != DataType.FLOAT16_VECTOR and v != DataType.BFLOAT16_VECTOR 
                and v != DataType.INT8_VECTOR and v != DataType.SPARSE_FLOAT_VECTOR):
                supported_types.append((k.lower(), v))
        for field_name, data_type in supported_types:
            if field_name.lower().startswith("_"):
                # skip private fields           
                continue
            if data_type == DataType.STRUCT:
                # add struct field
                struct_schema = client.create_struct_field_schema()
                struct_schema.add_field("struct_scalar_field", DataType.INT64)
                schema.add_field(field_name, DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct_schema, max_capacity=10)
                continue
            # Skip INT64 and VARCHAR as they're already added as primary key  
            if data_type != DataType.INT64 and data_type != DataType.VARCHAR: 
                schema.add_field(field_name, data_type)
        # Add ARRAY field separately with required parameters
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=10)
        # Create collection
        self.create_collection(client, collection_name, schema=schema, shards_num=shards_num)
        # Verify collection properties
        expected_field_count = len([name for name in supported_types]) + 5
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "enable_dynamic_field": False,
                                              "auto_id": auto_id,
                                              "num_shards": shards_num,
                                              "fields_num": expected_field_count})
        # Create same collection again
        self.create_collection(client, collection_name, schema=schema, shards_num=shards_num)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_key_type", ["int64", "varchar"])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_milvus_client_collection_multiple_vectors_different_dim(self, primary_key_type, auto_id, enable_dynamic_field):
        """
        target: test create collection with multiple vector fields having different dimensions
        method: create collection with multiple vector fields with different dims
        expected: collection created successfully with different vector dimensions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with different vector dimensions
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field, auto_id=auto_id)[0]
        # Add primary key field
        if primary_key_type == "int64":
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=auto_id)
        else:
            schema.add_field("id", DataType.VARCHAR, max_length=100, is_primary=True, auto_id=auto_id)
        # Add vector fields with different dimensions
        schema.add_field("float_vec_max_dim", DataType.FLOAT_VECTOR, dim=ct.max_dim)
        schema.add_field("float_vec_min_dim", DataType.FLOAT_VECTOR, dim=ct.min_dim)
        schema.add_field("float_vec_default_dim", DataType.FLOAT_VECTOR, dim=default_dim)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection properties
        expected_dims = [ct.max_dim, ct.min_dim, default_dim]
        expected_vector_names = ["float_vec_max_dim", "float_vec_min_dim", "float_vec_default_dim"]
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                           "auto_id": auto_id,
                                           "enable_dynamic_field": enable_dynamic_field,
                                           "dim": expected_dims,
                                           "vector_name": expected_vector_names})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_key_type", ["int64", "varchar"])
    def test_milvus_client_collection_multiple_vectors_maximum_dim(self, primary_key_type):
        """
        target: test create collection with multiple vector fields at maximum dimension
        method: create collection with multiple vector fields all using max dimension
        expected: collection created successfully with maximum dimensions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with maximum dimension vectors
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Add primary key field
        if primary_key_type == "int64":
            schema.add_field("id", DataType.INT64, is_primary=True)
        else:
            schema.add_field("id", DataType.VARCHAR, max_length=100, is_primary=True)
        # Add multiple vector fields with maximum dimension (up to max_vector_field_num)
        vector_field_names = []
        for i in range(ct.max_vector_field_num):
            vector_field_name = f"float_vec_{i+1}"
            vector_field_names.append(vector_field_name)
            schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=ct.max_dim)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection properties
        expected_dims = [ct.max_dim] * ct.max_vector_field_num
        expected_vector_names = vector_field_names
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                           "enable_dynamic_field": False,
                                           "dim": expected_dims,
                                           "vector_name": expected_vector_names})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_key_type", ["int64", "varchar"])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("partition_key_type", ["int64", "varchar"])
    def test_milvus_client_collection_multiple_vectors_partition_key(self, primary_key_type, auto_id, partition_key_type):
        """
        target: test create collection with multiple vector fields and partition key
        method: create collection with multiple vector fields and partition key
        expected: collection created successfully with partition key and multiple partitions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with multiple vector fields and partition key
        schema = self.create_schema(client, enable_dynamic_field=False, auto_id=auto_id)[0]
        # Add primary key field
        if primary_key_type == "int64":
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=auto_id)
        else:
            schema.add_field("id", DataType.VARCHAR, max_length=100, is_primary=True, auto_id=auto_id)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("vector_2", DataType.FLOAT_VECTOR, dim=default_dim)
        # Add scalar fields
        schema.add_field("int8_field", DataType.INT8)
        schema.add_field("int16_field", DataType.INT16)
        schema.add_field("int32_field", DataType.INT32)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("double_field", DataType.DOUBLE)
        schema.add_field("json_field", DataType.JSON)
        schema.add_field("bool_field", DataType.BOOL)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=10)
        schema.add_field("binary_vec_field", DataType.BINARY_VECTOR, dim=default_dim)
        # Add partition key field
        if partition_key_type == "int64":
            schema.add_field("partition_key_int", DataType.INT64, is_partition_key=True)
        else:
            schema.add_field("partition_key_varchar", DataType.VARCHAR, max_length=100, is_partition_key=True)
        # Create collection
        self.create_collection(client, collection_name, schema=schema)
        # Verify collection properties
        self.describe_collection(client, collection_name,
                                check_task=CheckTasks.check_describe_collection_property,
                                check_items={"collection_name": collection_name,
                                           "auto_id": auto_id,
                                           "enable_dynamic_field": False,
                                           "num_partitions": ct.default_partition_num})
        self.drop_collection(client, collection_name)


class TestMilvusClientCollectionMultipleVectorInvalid(TestMilvusClientV2Base):
    """
    ******************************************************************
    #  Test case for collection with multiple vector fields - Invalid cases
    ******************************************************************
    """

    @pytest.fixture(scope="function", params=ct.invalid_dims)
    def get_invalid_dim(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_key_type", ["int64", "varchar"])
    def test_milvus_client_collection_multiple_vectors_same_vector_field_name(self, primary_key_type):
        """
        target: test create collection with multiple vector fields having duplicate names
        method: create collection with multiple vector fields using same field name
        expected: raise exception for duplicated field name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with duplicate vector field names
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        # Add primary key field
        if primary_key_type == "int64":
            schema.add_field("id", DataType.INT64, is_primary=True)
        else:
            schema.add_field("id", DataType.VARCHAR, max_length=100, is_primary=True)
        # Add multiple vector fields with same name
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("vector_field", DataType.FLOAT_VECTOR, dim=default_dim)
        # Add other fields
        schema.add_field("int8_field", DataType.INT8)
        schema.add_field("int16_field", DataType.INT16)
        schema.add_field("int32_field", DataType.INT32)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("double_field", DataType.DOUBLE)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=100)
        schema.add_field("json_field", DataType.JSON)
        schema.add_field("bool_field", DataType.BOOL)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=10)
        schema.add_field("binary_vec_field", DataType.BINARY_VECTOR, dim=default_dim)
        # Try to create collection with duplicate field names
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_vector_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_milvus_client_collection_multiple_vectors_invalid_all_vector_field_name(self, invalid_vector_name):
        """
        target: test create collection with multiple vector fields where all have invalid names
        method: create collection with multiple vector fields, all with invalid names
        expected: raise exception for invalid field name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with all invalid vector field names
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        # Add vector fields - all with invalid names
        schema.add_field(invalid_vector_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(invalid_vector_name + " ", DataType.FLOAT_VECTOR, dim=default_dim)
        # Try to create collection with invalid field names
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid field name: {invalid_vector_name}"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip("issue #37543")
    def test_milvus_client_collection_multiple_vectors_invalid_dim(self, get_invalid_dim):
        """
        target: test create collection with multiple vector fields where one has invalid dimension
        method: create collection with multiple vector fields, one with invalid dimension
        expected: raise exception for invalid dimension
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema with one invalid vector dimension
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        # Add vector fields - one with invalid dimension, one with valid dimension
        schema.add_field("vector_field_1", DataType.FLOAT_VECTOR, dim=get_invalid_dim)
        schema.add_field("vector_field_2", DataType.FLOAT_VECTOR, dim=default_dim)
        # Try to create collection with invalid dimension
        error = {ct.err_code: 65535, ct.err_msg: "invalid dimension"}
        self.create_collection(client, collection_name, schema=schema,
                              check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientCollectionMmap(TestMilvusClientV2Base):
    """
    ******************************************************************
    #  Test case for collection mmap functionality
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_describe_collection_mmap(self):
        """
        target: enable or disable mmap in the collection
        method: enable or disable mmap in the collection
        expected: description information contains mmap
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        # Test enable mmap
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert "mmap.enabled" in properties.keys()
        assert properties["mmap.enabled"] == 'True'
        # Test disable mmap
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": False})
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert properties["mmap.enabled"] == 'False'
        # Test enable mmap again
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert properties["mmap.enabled"] == 'True'
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_load_mmap_collection(self):
        """
        target: after loading, enable mmap for the collection
        method: 1. data preparation and create index
        2. load collection
        3. enable mmap on collection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection with data and index
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # Get collection schema to generate compatible data
        collection_info = self.describe_collection(client, collection_name)[0]
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=collection_info)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        
        self.release_collection(client, collection_name)
        # Set mmap enabled before loading
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert properties["mmap.enabled"] == 'True'
        # Load collection
        self.load_collection(client, collection_name)
        # Try to alter mmap after loading - should raise exception
        error = {ct.err_code: 999, ct.err_msg: "can not alter mmap properties if collection loaded"}
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True},
                                        check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_mmap_collection(self):
        """
        target: set mmap on collection and then drop it
        method: 1. set mmap on collection
        2. drop collection
        3. recreate collection with same name
        expected: new collection doesn't inherit mmap settings
        """
        client = self._client()
        collection_name = "coll_mmap_test"
        # Create collection and set mmap
        self.create_collection(client, collection_name, default_dim)
        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert properties["mmap.enabled"] == 'True'
        # Drop collection
        self.drop_collection(client, collection_name)
        # Recreate collection with same name
        self.create_collection(client, collection_name, default_dim)
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert "mmap.enabled" not in properties.keys()
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_multiple_collections_enable_mmap(self):
        """
        target: enabling mmap for multiple collections in a single instance
        method: enabling mmap for multiple collections in a single instance
        expected: the collection description message for mmap is normal
        """
        client = self._client()
        collection_name_1 = cf.gen_collection_name_by_testcase_name() + "_1"
        collection_name_2 = cf.gen_collection_name_by_testcase_name() + "_2"
        collection_name_3 = cf.gen_collection_name_by_testcase_name() + "_3"
        # Create multiple collections
        self.create_collection(client, collection_name_1, default_dim)
        self.create_collection(client, collection_name_2, default_dim)
        self.create_collection(client, collection_name_3, default_dim)
        # Release collections before setting mmap
        self.release_collection(client, collection_name_1)
        self.release_collection(client, collection_name_2)
        self.release_collection(client, collection_name_3)
        # Enable mmap for first two collections
        self.alter_collection_properties(client, collection_name_1, properties={"mmap.enabled": True})
        self.alter_collection_properties(client, collection_name_2, properties={"mmap.enabled": True})
        # Verify mmap settings
        describe_res_1 = self.describe_collection(client, collection_name_1)[0]
        describe_res_2 = self.describe_collection(client, collection_name_2)[0]
        properties_1 = describe_res_1.get("properties")
        properties_2 = describe_res_2.get("properties")
        assert properties_1["mmap.enabled"] == 'True'
        assert properties_2["mmap.enabled"] == 'True'
        # Enable mmap for third collection
        self.alter_collection_properties(client, collection_name_3, properties={"mmap.enabled": True})
        describe_res_3 = self.describe_collection(client, collection_name_3)[0]
        properties_3 = describe_res_3.get("properties")
        assert properties_3["mmap.enabled"] == 'True'
        # Clean up
        self.drop_collection(client, collection_name_1)
        self.drop_collection(client, collection_name_2)
        self.drop_collection(client, collection_name_3)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_flush_collection_mmap(self):
        """
        target: after flush, collection enables mmap
        method: after flush, collection enables mmap
        expected: the collection description message for mmap is normal
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        # Get collection schema to generate compatible data
        collection_info = self.describe_collection(client, collection_name)[0]
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=collection_info)
        self.insert(client, collection_name, data)
        # Create index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        # Set index mmap to False
        self.alter_index_properties(client, collection_name, "vector", properties={"mmap.enabled": False})
        # Flush data
        self.flush(client, collection_name)
        # Set collection mmap to True
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        describe_res = self.describe_collection(client, collection_name)[0]
        properties = describe_res.get("properties")
        assert properties["mmap.enabled"] == 'True'
        # Set index mmap to True
        self.alter_index_properties(client, collection_name, "vector", properties={"mmap.enabled": True})
        # Load collection and perform search to verify functionality
        self.load_collection(client, collection_name)
        # Generate search vectors 
        search_data = cf.gen_vectors(default_nq, default_dim)
        self.search(client, collection_name, search_data, 
                               check_task=CheckTasks.check_search_results,
                               check_items={"nq": default_nq, "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_enable_mmap_after_drop_collection(self):
        """
        target: enable mmap after deleting a collection
        method: enable mmap after deleting a collection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create and drop collection
        self.create_collection(client, collection_name, default_dim)
        self.drop_collection(client, collection_name)
        # Try to enable mmap on dropped collection - should raise exception
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True},
                                        check_task=CheckTasks.err_res, check_items=error)



