import pytest
import random
import numpy as np
from typing import List, Dict, Any, Tuple, Callable, Union
import json
import os
import re
import pandas as pd
from datetime import datetime
from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import DataType
from check.func_check import Error

# Test configuration constants
prefix = "scalar_expression_filtering_optimized"
default_dim = 8
default_primary_key_field_name = "id"
default_vector_field_name = "vector"

# Operator definitions
comparison_operators = ["==", "!=", ">", "<", ">=", "<="]
range_operators = ["IN", "LIKE"]
null_operators = ["IS NULL", "IS NOT NULL"]


class TestScalarExpressionFilteringOptimized(TestMilvusClientV2Base):
    """
    Optimized test class for Milvus scalar expression filtering functionality.
    
    This test class provides comprehensive testing of scalar expression filtering
    across all supported data types and index types in a single collection.
    
    Features:
    - Single collection with all data types and their supported index types
    - Comprehensive operator coverage (comparison, range, null operators)
    - Index type comparison testing to ensure consistency
    - Automatic data saving on test failure for debugging
    - LIKE pattern testing with escape character support
    """

    def create_comprehensive_schema_with_index_types(self, client, enable_dynamic_field: bool = False):
        """
        Create comprehensive schema with all data types and their supported index types.
        
        Args:
            client: Milvus client instance
            enable_dynamic_field: Whether to enable dynamic field support
            
        Returns:
            Tuple of (schema, field_mapping, index_configs)
        """
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        field_mapping = {}
        index_configs = {}

        if not enable_dynamic_field:
            # Define all data types to test with their supported index types
            data_types_config = {
                # Integer types - support INVERTED, BITMAP, STL_SORT, AUTOINDEX
                DataType.INT8: ["no_index", "inverted", "bitmap", "stl_sort", "autoindex"],
                DataType.INT16: ["no_index", "inverted", "bitmap", "stl_sort", "autoindex"],
                DataType.INT32: ["no_index", "inverted", "bitmap", "stl_sort", "autoindex"],
                DataType.INT64: ["no_index", "inverted", "bitmap", "stl_sort", "autoindex"],

                # BOOL - supports INVERTED, BITMAP, AUTOINDEX
                DataType.BOOL: ["no_index", "inverted", "bitmap", "autoindex"],

                # Float types - support INVERTED, STL_SORT, AUTOINDEX
                DataType.FLOAT: ["no_index", "inverted", "stl_sort", "autoindex"],
                DataType.DOUBLE: ["no_index", "inverted", "stl_sort", "autoindex"],

                # VARCHAR - supports index types
                DataType.VARCHAR: ["no_index", "inverted", "bitmap", "trie", "ngram", "autoindex"],

                # JSON - supports INVERTED, NGRAM, AUTOINDEX
                DataType.JSON: ["no_index", "inverted", "ngram", "autoindex"],

                # ARRAY types - support depends on element type
                (DataType.ARRAY, DataType.INT32): ["no_index", "inverted", "bitmap", "autoindex"],
                (DataType.ARRAY, DataType.INT64): ["no_index", "inverted", "bitmap", "autoindex"],
                (DataType.ARRAY, DataType.VARCHAR): ["no_index", "inverted", "bitmap", "autoindex"],
            }

            # Add fields for each data type with their supported index types
            for data_type, supported_indexes in data_types_config.items():
                if isinstance(data_type, tuple):
                    # Array type
                    array_type, element_type = data_type
                    base_name = f"array_{element_type.name.lower()}"

                    # Create array field with proper element type
                    for index_type in supported_indexes:
                        field_name = f"{base_name}_{index_type}"
                        if element_type == DataType.VARCHAR:
                            # VARCHAR array needs max_length and max_capacity parameters
                            schema.add_field(
                                field_name, DataType.ARRAY, 
                                element_type=element_type, 
                                max_length=100, max_capacity=10, 
                                nullable=True
                            )
                        else:
                            # Other array types need max_capacity parameter
                            schema.add_field(
                                field_name, DataType.ARRAY, 
                                element_type=element_type, 
                                max_capacity=10, 
                                nullable=True
                            )
                        field_mapping[field_name] = data_type
                        index_configs[field_name] = index_type
                else:
                    # Scalar type
                    base_name = data_type.name.lower()

                    for index_type in supported_indexes:
                        field_name = f"{base_name}_{index_type}"
                        if data_type == DataType.VARCHAR:
                            # VARCHAR field needs max_length parameter
                            schema.add_field(field_name, data_type, max_length=100, nullable=True)
                        else:
                            schema.add_field(field_name, data_type, nullable=True)
                        field_mapping[field_name] = data_type
                        index_configs[field_name] = index_type

        return schema, field_mapping, index_configs

    def generate_random_scalar_value(self, data_type: DataType, need_none: bool = True) -> Any:
        """
        Generate random scalar values for different data types with 10% chance of None.
        
        Args:
            data_type: The data type to generate value for
            need_none: Whether to include None values (10% probability)
            
        Returns:
            Random value of the specified data type
        """
        # 10% probability to generate None
        if need_none and random.random() < 0.1:
            return None

        if data_type == DataType.INT8:
            return np.int8(random.randint(-128, 127))
        elif data_type == DataType.INT16:
            return np.int16(random.randint(-32768, 32767))
        elif data_type == DataType.INT32:
            return np.int32(random.randint(-1000000, 1000000))
        elif data_type == DataType.INT64:
            return random.randint(-1000000000, 1000000000)
        elif data_type == DataType.BOOL:
            return random.choice([True, False])
        elif data_type == DataType.FLOAT:
            return random.uniform(-1000.0, 1000.0)
        elif data_type == DataType.DOUBLE:
            return random.uniform(-10000.0, 10000.0)
        elif data_type == DataType.VARCHAR:
            ran_number = random.randint(0, 2)
            # Pattern group 1: Structured patterns for LIKE testing
            patterns_1 = [
                f"str_{ran_number}",
                f"{ran_number}_str",
                f"{ran_number}_str_%{ran_number}",
                f"str_%{ran_number}_str_%{ran_number}",
                f"str%{ran_number}",
                f"{ran_number}%str",
                f"{ran_number}%str%{ran_number}",
                f"str+{ran_number}str",
                f"{ran_number}+str",
                f"{ran_number}+str+{ran_number}",
                f"str{ran_number}",
                f"{ran_number}str",
                f"{ran_number}str{ran_number}"
            ]
            # Pattern group 2: Edge cases and special characters
            patterns_2 = [
                " ",
                "",
                "_",
                "%",
                "s",
                "\\",
                "*./&.*/"
            ]
            if random.random() < 0.8:
                return random.choice(patterns_1)
            else:
                return random.choice(patterns_2)
        elif data_type == DataType.JSON:
            ran_number = random.randint(0, 10)
            json_patterns = [
                {"int": ran_number,
                 "float": ran_number * 1.0,
                 "bool": random.choice([True, False]),
                 "varchar": f"str_{ran_number}",
                 "varchar_float": f"{ran_number * 1.0}"},
                 {"array_int": [random.randint(0, 10) for _ in range(random.randint(1, 5))],
                 "array_float": [random.uniform(0.0, 10.0) for _ in range(random.randint(1, 5))],
                 "array_bool": [random.choice([True, False]) for _ in range(random.randint(1, 5))],
                 "array_varchar": [f"str_{random.randint(0, 10)}" for _ in range(random.randint(1, 5))]},
                 {"array_json": [{"int": random.randint(0, 10),
                                 "float": random.randint(0, 10) * 1.0,
                                 "bool": random.choice([True, False]),
                                 "varchar": f"str_{random.randint(0, 10)}",
                                 "varchar_float": f"{random.randint(0, 10) * 1.0}"} for _ in range(random.randint(1, 5))]},
                {"nested_json": {"int": ran_number,
                          "float": ran_number * 1.0,
                          "nested_1": {"int": ran_number,
                                       "float": ran_number * 1.0,
                                       "nested_2": {"int": ran_number,
                                                    "float": ran_number * 1.0}}}},

            ]
            return random.choice(json_patterns)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")

    def generate_random_array_value(self, element_type: DataType, max_capacity: int = 5) -> List[Any]:
        """
        Generate random array values with specified element type with 10% chance of None.
        
        Args:
            element_type: The data type of array elements
            max_capacity: Maximum capacity of the array
            
        Returns:
            Random array of the specified element type
        """
        # 10% probability to generate None
        if random.random() < 0.1:
            return None

        array_length = random.randint(1, max_capacity)
        array_data = []

        for _ in range(array_length):
            if element_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64]:
                array_data.append(random.randint(0, 100))
            elif element_type in [DataType.FLOAT, DataType.DOUBLE]:
                array_data.append(random.uniform(0.0, 100.0))
            elif element_type == DataType.BOOL:
                array_data.append(random.choice([True, False]))
            elif element_type == DataType.VARCHAR:
                array_data.append(f"arr_str_{random.randint(0, 999)}")

        return array_data

    def generate_test_data_for_index_comparison(self, field_mapping: Dict, index_configs: Dict, 
                                               num_records: int) -> List[Dict]:
        """
        Generate test data where all fields of the same data type have identical data.
        
        This ensures that index consistency can be verified across different index types
        for the same data type.
        
        Args:
            field_mapping: Mapping of field names to data types
            index_configs: Mapping of field names to index types
            num_records: Number of records to generate
            
        Returns:
            List of test data records
        """
        test_data = []
        vectors = cf.gen_vectors(num_records, default_dim)

        for i in range(num_records):
            record = {
                default_primary_key_field_name: i,
                default_vector_field_name: vectors[i]
            }

            # Group fields by data type
            data_type_groups = {}
            for field_name, data_type in field_mapping.items():
                if field_name in [default_primary_key_field_name, default_vector_field_name]:
                    continue

                if data_type not in data_type_groups:
                    data_type_groups[data_type] = []
                data_type_groups[data_type].append(field_name)

            # Generate same data for all fields of the same type
            for data_type, field_names in data_type_groups.items():
                if isinstance(data_type, tuple) and data_type[0] == DataType.ARRAY:
                    # Array type
                    element_type = data_type[1]
                    array_value = self.generate_random_array_value(element_type)
                    for field_name in field_names:
                        record[field_name] = array_value
                else:
                    # Scalar type
                    scalar_value = self.generate_random_scalar_value(data_type)
                    for field_name in field_names:
                        record[field_name] = scalar_value

            test_data.append(record)

        return test_data

    def generate_simple_expression(self, field_name: str, data_type_info: Any, operator: str) -> Tuple[str, Callable]:
        """
        Generate a simple filter expression and a corresponding validation function.
        
        Args:
            field_name: The name of the field to filter on
            data_type_info: The data type of the field (scalar or tuple for array)
            operator: The operator to use for the expression
            
        Returns:
            Tuple of (expression, validator) or list of tuples for LIKE operator
        """
        if operator in ["IS NULL", "IS NOT NULL"]:
            # Handle IS NULL and IS NOT NULL operators
            expression = f"{field_name} {operator}"

            def validate_null(data_value, op=operator):
                if op == "IS NULL":
                    return data_value is None
                else:  # IS NOT NULL
                    return data_value is not None

            return expression, validate_null
        elif operator == "IN":
            # Handle IN operator for both array and scalar types
            if isinstance(data_type_info, tuple) and data_type_info[0] == DataType.ARRAY:
                element_type = data_type_info[1]
                values = [self.generate_random_scalar_value(element_type, need_none=False) for _ in range(20)]
            else:
                values = [self.generate_random_scalar_value(data_type_info, need_none=False) for _ in range(20)]

            expression = f"{field_name} IN {values}"

            def validate_in(data_value, filter_values=values):
                if data_value is None:
                    return False
                return data_value in filter_values

            return expression, validate_in
        elif operator == "LIKE":
            # Handle LIKE operator for string types
            if data_type_info == DataType.VARCHAR:
                # Generate a comprehensive set of LIKE patterns for testing
                # Patterns are based on Milvus documentation: https://milvus.io/docs/basic-operators.md
                ran_str = random.randint(0, 2)
                patterns = [
                    # Prefix match patterns (string starts with)
                    f'str%',  # Matches strings starting with 'str'
                    f'str_{ran_str}%',  # Matches strings starting with 'str_0', 'str_1', etc.
                    f'str%{ran_str}%',  # Matches strings starting with 'str' and containing '0', '1', or '2'
                    f'str+{ran_str}%',  # Matches strings starting with 'str+0', etc.
                    f'str{ran_str}%',  # Matches strings starting with 'str0', etc.

                    # Suffix match patterns (string ends with)
                    f'%str',  # Matches strings ending with 'str'
                    f'%{ran_str}_str',  # Matches strings ending with '0_str', etc.
                    f'%{ran_str}%str',  # Matches strings containing '0', '1', or '2' and ending with 'str'
                    f'%{ran_str}+str',  # Matches strings ending with '0+str', etc.
                    f'%{ran_str}str',  # Matches strings ending with '0str', etc.

                    # Infix match patterns (string contains)
                    f'%str%',  # Matches strings containing 'str'
                    f'%{ran_str}_str_%',  # Matches strings containing '0_str_', etc.
                    f'%{ran_str}%str%{ran_str}%',  # Matches strings containing '0', 'str', and '0' (or 1,2)
                    f'%{ran_str}+str+%',  # Matches strings containing '0+str+', etc.
                    f'%{ran_str}str%{ran_str}%',  # Matches strings containing '0str0', etc.

                    # Single character wildcard patterns (underscore)
                    'str_',  # Matches 'str' followed by any single character
                    '_str_',  # Matches any single character, then 'str', then any single character
                    '_str',  # Matches any single character followed by 'str'
                    'str%_',  # Matches 'str' followed by any sequence and a single character
                    '_%str',  # Matches any single character, any sequence, then 'str'
                    'str_%_',  # Matches 'str_', any sequence, then a single character
                    '_%_str',  # Matches any single character, any sequence, then '_str'

                    # Combination patterns with both % and _
                    'str_%_%',  # Matches 'str_', any sequence, single char, any sequence
                    '%_str_%',  # Matches any sequence, single char, '_str_', any sequence
                    '%_%_str',  # Matches any sequence, single char, any sequence, '_str'
                    'str_%_str',  # Matches 'str_', any sequence, '_str'
                    'str_%_str_%',  # Matches 'str_', any sequence, '_str_', any sequence

                    # Edge case patterns
                    'str',  # Exact match for 'str'
                    '',  # Empty string
                    '%',  # Matches everything
                    '\\\\%',  # Matches literal '%'
                    '_',  # Matches any single character
                    '\\\\_',  # Matches literal '_'

                    # Escape patterns (test literal % and _ with backslash)
                    'str\\\\_%',  # Matches 'str_' (escaped underscore) followed by any sequence
                    '%\\\\_str',  # Matches any sequence followed by '_str' (escaped underscore)
                    'str\\\\%%',  # Matches 'str%' (escaped percent) followed by any sequence
                    '%\\\\%str',  # Matches any sequence followed by '%str' (escaped percent)
                    'str\\\\_\\\\%%',  # Matches 'str_%' (both escaped) followed by any sequence
                    '%\\\\_\\\\%str',  # Matches any sequence followed by '_%str' (both escaped)
                    '\\\\'     # Matches literal backslash
                ]

                # Return all generated LIKE expressions and their validators
                expressions = []
                for pattern in patterns:
                    expression = f'{field_name} LIKE "{pattern}"'

                    def validate_like(data_value, pat=pattern):
                        if data_value is None:
                            return False
                        data_str = str(data_value)
                        regex_pattern = self._convert_like_to_regex(pat)
                        try:
                            return bool(re.match(regex_pattern, data_str))
                        except:
                            return False

                    expressions.append((expression, validate_like))

                return expressions
            else:
                # LIKE operator on non-string types (expected to fail)
                value = self.generate_random_scalar_value(DataType.INT32)
                expression = f"{field_name} LIKE {repr(value)}"

                def validate_eq(data_value, val=value):
                    return data_value == val

                return expression, validate_eq
        else:
            # Handle standard comparison operators (==, !=, >, <, >=, <=)
            if isinstance(data_type_info, tuple) and data_type_info[0] == DataType.ARRAY:
                element_type = data_type_info[1]
                value = self.generate_random_scalar_value(element_type)
            else:
                value = self.generate_random_scalar_value(data_type_info)

            expression = f"{field_name} {operator} {repr(value)}"

            def validate_comparison(data_value, val=value, op=operator):
                if data_value is None:
                    return False
                if op == "==":
                    return data_value == val
                elif op == "!=":
                    return data_value != val
                elif op == ">":
                    return data_value > val
                elif op == "<":
                    return data_value < val
                elif op == ">=":
                    return data_value >= val
                elif op == "<=":
                    return data_value <= val
                return False

            return expression, validate_comparison

    def _convert_like_to_regex(self, pattern: str) -> str:
        """
        Convert a custom pattern to a regular expression (optimized version).
        Rules:
          - '_' matches any single character → regex '.'
          - '%' matches any sequence of characters → regex '.*'
          - '\' is used as an escape character, remove the backslash and keep the following character
        """
        regex_parts = []
        i = 0
        length = len(pattern)

        while i < length:
            current_char = pattern[i]
            if current_char == '\\':
                # Handle escape character: remove backslash, use the literal character
                if i + 1 < length:
                    next_char = pattern[i + 1]
                    if next_char == '\\':
                        i += 1
                    else:
                        regex_parts.append(next_char)
                        i += 2
                else:
                    # Trailing escape character: add literal backslash
                    regex_parts.append('\\')
                    i += 1
            else:
                # Handle wildcards
                if current_char == '_':
                    regex_parts.append('.')  # Match any single character
                elif current_char == '%':
                    regex_parts.append('.*')  # Match any sequence of characters
                else:
                    # Ordinary character: add regex-escaped version (handles . * and other special chars)
                    regex_parts.append(re.escape(current_char))
                i += 1

        return '^' + ''.join(regex_parts) + '$'

    def is_parsing_error(self, exception: Exception) -> bool:
        """
        Check if the exception is a parsing error.
        
        Args:
            exception: The exception to check
            
        Returns:
            True if the exception is a parsing error, False otherwise
        """
        if not isinstance(exception, Error):
            return False

        error_message = str(exception.message).lower()
        return "cannot parse expression" in error_message or "unsupported data type" in error_message

    def save_failure_debug_info(self, test_data: List[Dict], schema, field_mapping: Dict,
                                index_configs: Dict, failed_expressions: List[str],
                                collection_name: str):
        """
        Save comprehensive debug information for failure reproduction.
        
        This method saves all necessary information to reproduce test failures,
        including test data, schema, configuration, and a reproduction script.
        
        Args:
            test_data: The test data used in the test
            schema: The collection schema
            field_mapping: Mapping of field names to data types
            index_configs: Mapping of field names to index types
            failed_expressions: List of failed expressions
            collection_name: Name of the collection
        """
        try:
            log_dir = "/tmp/ci_logs"
            os.makedirs(log_dir, exist_ok=True)

            # Use collection name for file naming to ensure consistency
            safe_collection_name = collection_name.replace("-", "_").replace(" ", "_")
            test_name = "test_all_data_types_with_different_indexes"

            # Custom JSON encoder to handle numpy types and other non-serializable objects
            class NumpyEncoder(json.JSONEncoder):
                def default(self, obj):
                    import numpy as np
                    if isinstance(obj, np.integer):
                        return int(obj)
                    elif isinstance(obj, np.floating):
                        return float(obj)
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    elif isinstance(obj, np.bool_):
                        return bool(obj)
                    elif obj is None:
                        return None
                    else:
                        return str(obj)

            # Save test data
            if test_data:
                df_test_data = pd.DataFrame(test_data)
                test_data_file = os.path.join(log_dir, f"{safe_collection_name}_test_data.parquet")
                df_test_data.to_parquet(test_data_file, index=False)
                log.info(f"Test data saved to: {test_data_file}")

            # Save schema information
            schema_info = {
                "collection_name": collection_name,
                "primary_key_field": default_primary_key_field_name,
                "vector_field": default_vector_field_name,
                "vector_dim": default_dim,
                "fields": []
            }

            # Extract field information from schema
            for field in schema.fields:
                field_info = {
                    "name": field.name,
                    "dtype": str(field.dtype),
                    "is_primary": field.is_primary,
                    "auto_id": field.auto_id,
                    "nullable": getattr(field, 'nullable', None)
                }
                if hasattr(field, 'element_type'):
                    field_info["element_type"] = str(field.element_type)
                if hasattr(field, 'max_length'):
                    field_info["max_length"] = field.max_length
                if hasattr(field, 'max_capacity'):
                    field_info["max_capacity"] = field.max_capacity
                if hasattr(field, 'dim'):
                    field_info["dim"] = field.dim
                schema_info["fields"].append(field_info)

            schema_file = os.path.join(log_dir, f"{safe_collection_name}_schema.json")
            with open(schema_file, 'w') as f:
                json.dump(schema_info, f, indent=2, cls=NumpyEncoder)
            log.info(f"Schema saved to: {schema_file}")

            # Save field mapping and index configs
            config_info = {
                "field_mapping": field_mapping,
                "index_configs": index_configs,
                "failed_expressions": failed_expressions,
                "collection_name": collection_name,
                "test_name": test_name,
                "test_data_count": len(test_data) if test_data else 0,
                "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
            }

            config_file = os.path.join(log_dir, f"{safe_collection_name}_config.json")
            with open(config_file, 'w') as f:
                json.dump(config_info, f, indent=2, cls=NumpyEncoder)
            log.info(f"Configuration saved to: {config_file}")

        except Exception as e:
            log.error(f"Failed to save failure debug info: {e}")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("num_records", [1200])
    def test_all_data_types_with_different_indexes(self, enable_dynamic_field, num_records):
        """
        Test all data types with their supported index types in a single collection.
        
        This comprehensive test creates a single collection with all supported data types
        and their corresponding index types, then verifies that all index types return
        consistent results for the same data and expressions.
        
        target: Test all data types with their supported index types
        method: Create single collection with all data types and index types, insert same data, verify query results
        expected: All index types return identical results matching ground truth
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        log.info("Testing all data types with different index types in a single collection")

        try:
            # Step 1: Create comprehensive schema with all data types and their supported index types
            schema, field_mapping, index_configs = self.create_comprehensive_schema_with_index_types(
                client, enable_dynamic_field
            )

            # Step 2: Create collection (without index_params to avoid auto index creation)
            self.create_collection(client, collection_name, schema=schema)

            # Step 3: Generate and insert test data
            test_data = self.generate_test_data_for_index_comparison(field_mapping, index_configs, num_records)
            self.insert(client, collection_name=collection_name, data=test_data)
            self.flush(client, collection_name)

            # Step 4: Create indexes
            self._create_all_indexes(client, collection_name, index_configs)

            # Step 5: Load collection
            self.load_collection(client, collection_name)

            log.info(f"Inserted {num_records} test records with all data types and index types")
            log.info(f"Total fields: {len(field_mapping)}")

            # Step 6: Run comprehensive expression testing
            stats = self._run_expression_tests(client, collection_name, field_mapping,
                                             index_configs, test_data)
            
            # Step 6.5: Run complex expression testing
            stats_2 = self._run_complex_expression_tests(client, collection_name, field_mapping,
                                             index_configs, test_data)

            # Step 7: Log final statistics and handle failures
            merged_stats = self._log_test_statistics(stats, stats_2)
            
            # Fail the test if any expression failed
            if merged_stats['failed']:
                # Save comprehensive debug information for failure reproduction
                self.save_failure_debug_info(
                    test_data=test_data,
                    schema=schema,
                    field_mapping=field_mapping,
                    index_configs=index_configs,
                    failed_expressions=merged_stats['failed'],
                    collection_name=collection_name
                )
                raise AssertionError(f"Test failed due to {len(merged_stats['failed'])} failed expressions: {merged_stats['failed']}")

        finally:
            # Clean up: drop collection
            pass
            # self.drop_collection(client, collection_name)

    def _create_all_indexes(self, client, collection_name: str, index_configs: Dict):
        """
        Create all indexes for the collection.
        
        Args:
            client: Milvus client instance
            collection_name: Name of the collection
            index_configs: Mapping of field names to index types
        """
        # Create vector index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, index_type="IVF_FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        
        # Create scalar indexes
        for field_name, index_type in index_configs.items():
            if index_type != "no_index":
                index_params = self.prepare_index_params(client)[0]
                if DataType.JSON.name.upper() in field_name.upper() and (index_type == "inverted" or index_type == "autoindex"):
                    index_params.add_index(field_name, index_type=index_type.upper(), params={"json_cast_type": "varchar"})
                elif DataType.JSON.name.upper() in field_name.upper() and index_type == "ngram":
                    index_params.add_index(field_name, index_type=index_type.upper(), min_gram=2, max_gram=3,
                                           params={"json_cast_type": "varchar"})
                elif DataType.VARCHAR.name.upper() in field_name.upper() and index_type == "ngram":
                    index_params.add_index(field_name, index_type=index_type.upper(), min_gram=2, max_gram=3)
                else:
                    index_params.add_index(field_name, index_type=index_type.upper())
                self.create_index(client, collection_name, index_params)
                log.info(f"Created index {index_type} for field {field_name}")

    def _run_expression_tests(self, client, collection_name: str, field_mapping: Dict, 
                             index_configs: Dict, test_data: List[Dict]) -> Dict:
        """
        Run comprehensive expression testing across all fields and operators.
        
        Args:
            client: Milvus client instance
            collection_name: Name of the collection
            field_mapping: Mapping of field names to data types
            index_configs: Mapping of field names to index types
            test_data: Test data records
            
        Returns:
            Dictionary containing test statistics
        """
        # Initialize statistics
        stats = {
            "success": [],
            "not_supported": [],
            "failed": [],
            "total": []
        }

        # Test expressions on each field with index consistency verification
        all_operators = comparison_operators + range_operators + null_operators

        for operator in all_operators:
            # Group fields by data type to test index consistency
            data_type_groups = self._group_fields_by_data_type(field_mapping)

            # Test each data type group
            for data_type, field_names in data_type_groups.items():
                self._test_data_type_group(client, collection_name, field_names, data_type, 
                                         operator, index_configs, test_data, stats)

        return stats

    def _group_fields_by_data_type(self, field_mapping: Dict) -> Dict:
        """
        Group fields by their data type for index consistency testing.
        
        Args:
            field_mapping: Mapping of field names to data types
            
        Returns:
            Dictionary mapping data types to lists of field names
        """
        data_type_groups = {}
        for field_name, data_type in field_mapping.items():
            if field_name in [default_primary_key_field_name, default_vector_field_name]:
                continue

            if data_type not in data_type_groups:
                data_type_groups[data_type] = []
            data_type_groups[data_type].append(field_name)
        
        return data_type_groups

    def _test_data_type_group(self, client, collection_name: str, field_names: List[str], 
                             data_type: Any, operator: str, index_configs: Dict, 
                             test_data: List[Dict], stats: Dict):
        """
        Test a group of fields with the same data type using a specific operator.
        
        Args:
            client: Milvus client instance
            collection_name: Name of the collection
            field_names: List of field names to test
            data_type: Data type of the fields
            operator: Operator to test
            index_configs: Mapping of field names to index types
            test_data: Test data records
            stats: Statistics dictionary to update
        """
        # Generate expressions for this data type
        sample_field = field_names[0]  # Use first field to generate expressions
        result = self.generate_simple_expression(sample_field, data_type, operator)

        # Handle both single expression and list of expressions (for LIKE operator)
        if isinstance(result, list):
            expressions = result
        else:
            expressions = [result]

        for expression, validator in expressions:
            # Test this expression on all fields of the same data type
            field_results = {}

            for field_name in field_names:
                # Replace field name in expression
                if sample_field != field_name:
                    test_expression = expression.replace(f"{sample_field}", field_name)
                else:
                    test_expression = expression

                expression_info = f"{field_name} ({index_configs[field_name]}) with {operator}: {test_expression}"
                stats["total"].append(expression_info)

                log.info(f"Testing {expression_info}")

                try:
                    # Execute query
                    res = self.query(
                        client, collection_name=collection_name,
                        filter=test_expression, output_fields=["*"],
                        check_task=CheckTasks.check_nothing
                    )[0]

                    # Check if res is an Error
                    if isinstance(res, Error):
                        if self.is_parsing_error(res):
                            log.warning(f"⚠️ {expression_info} cannot be parsed, skipping: {str(res)}")
                            stats["not_supported"].append(f"{expression_info}: {str(res)}")
                            continue
                        else:
                            log.error(f"✗ {expression_info} failed: {str(res)}")
                            stats["failed"].append(f"{expression_info}: {str(res)}")
                            continue
                    else:
                        # Store result for consistency check
                        field_results[field_name] = res

                        # Calculate expected results
                        expected_results = []
                        for record in test_data:
                            if validator(record.get(field_name)):
                                expected_results.append(record)

                        # Verify results against ground truth
                        if len(res) != len(expected_results):
                            log.error(
                                f"✗ {expression_info} returned {len(res)} results, expected {len(expected_results)}")
                            stats["failed"].append(
                                f"{expression_info}: returned {len(res)} results, expected {len(expected_results)}")
                            continue
                        else:
                            log.info(f"✓ {expression_info} passed with {len(res)} results")
                            stats["success"].append(f"{expression_info}: {len(res)} results")

                except Exception as e:
                    log.error(f"✗ {expression_info} encountered exception: {str(e)}")
                    stats["failed"].append(f"{expression_info}: exception {str(e)}")
                    continue

            # Verify index consistency - all fields should return same results
            self._verify_index_consistency(field_results, operator, field_names, stats)

    def _verify_index_consistency(self, field_results: Dict, operator: str, 
                                 field_names: List[str], stats: Dict):
        """
        Verify that all fields return consistent results for the same expression.
        
        Args:
            field_results: Dictionary mapping field names to query results
            operator: The operator being tested
            field_names: List of field names being tested
            stats: Statistics dictionary to update
        """
        if len(field_results) > 1:
            # Get the first result as reference
            reference_field = list(field_results.keys())[0]
            reference_result = field_results[reference_field]
            reference_ids = set(row[default_primary_key_field_name] for row in reference_result)

            for field_name, result in field_results.items():
                if field_name == reference_field:
                    continue

                current_ids = set(row[default_primary_key_field_name] for row in result)

                if reference_ids != current_ids:
                    log.error(
                        f"✗ Index consistency failed for {operator}: {field_name} returned different results than {reference_field}")
                    stats["failed"].append(
                        f"Index consistency failed: {field_name} vs {reference_field} for {operator}")
                    continue
                else:
                    log.info(
                        f"✓ Index consistency verified for {operator}: all fields returned same results")

    def _log_test_statistics(self, stats: Dict, stats_2: Dict = None):
        """
        Log comprehensive test statistics for both simple and complex expressions.
        
        Args:
            stats: Statistics dictionary containing simple expression test results
            stats_2: Statistics dictionary containing complex expression test results (optional)
        """
        # Log simple expression statistics
        log.info(f"=== Simple Expression Test Statistics ===")
        log.info(f"Total expressions tested: {len(stats['total'])}")
        log.info(f"Successful: {len(stats['success'])}")
        log.info(f"Not supported: {len(stats['not_supported'])}")
        log.info(f"Failed: {len(stats['failed'])}")
        log.info(f"Success rate: {len(stats['success']) / len(stats['total']) * 100:.2f}%")

        # Log detailed simple expression information
        if stats['success']:
            log.info(f"Successful expressions: {stats['success']}")
        if stats['not_supported']:
            log.info(f"Not supported expressions: {stats['not_supported']}")
        if stats['failed']:
            log.info(f"Failed expressions: {stats['failed']}")
        
        # Log complex expression statistics if provided
        if stats_2 is not None:
            log.info(f"\n=== Complex Expression Test Statistics ===")
            log.info(f"Total complex expressions tested: {len(stats_2['total'])}")
            log.info(f"Successful: {len(stats_2['success'])}")
            log.info(f"Not supported: {len(stats_2['not_supported'])}")
            log.info(f"Failed: {len(stats_2['failed'])}")
            if len(stats_2['total']) > 0:
                log.info(f"Success rate: {len(stats_2['success']) / len(stats_2['total']) * 100:.2f}%")

            # Log detailed complex expression information
            if stats_2['success']:
                log.info(f"Successful complex expressions: {stats_2['success']}")
            if stats_2['not_supported']:
                log.info(f"Not supported complex expressions: {stats_2['not_supported']}")
            if stats_2['failed']:
                log.info(f"Failed complex expressions: {stats_2['failed']}")
            
            # Log combined statistics
            merged_stats = {
                "success": stats["success"] + stats_2["success"],
                "not_supported": stats["not_supported"] + stats_2["not_supported"],
                "failed": stats["failed"] + stats_2["failed"],
                "total": stats["total"] + stats_2["total"]
            }
            
            log.info(f"\n=== Combined Test Statistics ===")
            log.info(f"Total all expressions tested: {len(merged_stats['total'])}")
            log.info(f"Total successful: {len(merged_stats['success'])}")
            log.info(f"Total not supported: {len(merged_stats['not_supported'])}")
            log.info(f"Total failed: {len(merged_stats['failed'])}")
            log.info(f"Overall success rate: {len(merged_stats['success']) / len(merged_stats['total']) * 100:.2f}%")
            
            return merged_stats
        
        return stats

    def generate_complex_expression(self, field_mapping: Dict, index_configs: Dict, operator: str) -> List[Tuple[str, Callable]]:
        """
        Generate complex filter expressions including field calculations, array indexing, and JSON path access.
        
        Args:
            field_mapping: Mapping of field names to data types
            index_configs: Mapping of field names to index types
            operator: The operator to use for the expression
            
        Returns:
            List of tuples containing (expression, validator)
        """
        expressions = []
        
        # Get all field names excluding primary key and vector fields
        scalar_fields = [name for name in field_mapping.keys() 
                        if name not in [default_primary_key_field_name, default_vector_field_name]]
        
        if not scalar_fields:
            return expressions
            
        # 1. Field calculation expressions (arithmetic operations)
        expressions.extend(self._generate_field_calculation_expressions(scalar_fields, field_mapping, operator))
        
        # 2. Array indexing expressions
        expressions.extend(self._generate_array_indexing_expressions(scalar_fields, field_mapping, operator))
        
        # 3. JSON path expressions
        expressions.extend(self._generate_json_path_expressions(scalar_fields, field_mapping, operator))
        
        # 4. Mixed complex expressions
        expressions.extend(self._generate_mixed_complex_expressions(scalar_fields, field_mapping, operator))
        
        return expressions
    
    def _generate_field_calculation_expressions(self, field_names: List[str], field_mapping: Dict, operator: str) -> List[Tuple[str, Callable]]:
        """Generate expressions with field arithmetic calculations."""
        expressions = []
        
        # Find numeric fields for arithmetic operations
        numeric_fields = []
        for field_name in field_names:
            data_type = field_mapping[field_name]
            if isinstance(data_type, tuple) and data_type[0] == DataType.ARRAY:
                element_type = data_type[1]
                if element_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.FLOAT, DataType.DOUBLE]:
                    numeric_fields.append(field_name)
            elif data_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.FLOAT, DataType.DOUBLE]:
                numeric_fields.append(field_name)
        
        if len(numeric_fields) < 2:
            return expressions
            
        # Generate field-to-field arithmetic expressions
        for i in range(min(3, len(numeric_fields))):  # Limit to 3 expressions
            field1 = numeric_fields[i]
            field2 = numeric_fields[(i + 1) % len(numeric_fields)]
            
            # Field arithmetic operations
            arithmetic_ops = [
                (f"{field1} + {field2}", lambda x, y: x + y if x is not None and y is not None else None),
                (f"{field1} - {field2}", lambda x, y: x - y if x is not None and y is not None else None),
                (f"{field1} * {field2}", lambda x, y: x * y if x is not None and y is not None else None),
            ]
            
            for arith_expr, arith_func in arithmetic_ops:
                if operator in ["==", "!=", ">", "<", ">=", "<="]:
                    # Generate comparison with arithmetic result
                    value = random.randint(-100, 100)
                    expression = f"{arith_expr} {operator} {value}"
                    
                    def validate_arithmetic(data_value1, data_value2, val=value, op=operator, func=arith_func):
                        if data_value1 is None or data_value2 is None:
                            return False
                        result = func(data_value1, data_value2)
                        if result is None:
                            return False
                        if op == "==":
                            return result == val
                        elif op == "!=":
                            return result != val
                        elif op == ">":
                            return result > val
                        elif op == "<":
                            return result < val
                        elif op == ">=":
                            return result >= val
                        elif op == "<=":
                            return result <= val
                        return False
                    
                    expressions.append((expression, lambda record, f1=field1, f2=field2, v=value, o=operator, f=arith_func: 
                                     validate_arithmetic(record.get(f1), record.get(f2), v, o, f)))
        
        # Generate field with constant arithmetic
        for field_name in numeric_fields[:3]:  # Limit to 3 expressions
            constant = random.randint(-50, 50)
            arithmetic_ops = [
                (f"{field_name} + {constant}", lambda x, c: x + c if x is not None else None),
                (f"{field_name} - {constant}", lambda x, c: x - c if x is not None else None),
                (f"{field_name} * {constant}", lambda x, c: x * c if x is not None else None),
            ]
            
            for arith_expr, arith_func in arithmetic_ops:
                if operator in ["==", "!=", ">", "<", ">=", "<="]:
                    value = random.randint(-100, 100)
                    expression = f"{arith_expr} {operator} {value}"
                    
                    def validate_constant_arithmetic(data_value, const=constant, val=value, op=operator, func=arith_func):
                        if data_value is None:
                            return False
                        result = func(data_value, const)
                        if result is None:
                            return False
                        if op == "==":
                            return result == val
                        elif op == "!=":
                            return result != val
                        elif op == ">":
                            return result > val
                        elif op == "<":
                            return result < val
                        elif op == ">=":
                            return result >= val
                        elif op == "<=":
                            return result <= val
                        return False
                    
                    expressions.append((expression, lambda record, f=field_name, c=constant, v=value, o=operator, func=arith_func: 
                                     validate_constant_arithmetic(record.get(f), c, v, o, func)))
        
        return expressions
    
    def _generate_array_indexing_expressions(self, field_names: List[str], field_mapping: Dict, operator: str) -> List[Tuple[str, Callable]]:
        """Generate expressions with array indexing."""
        expressions = []
        
        # Find array fields
        array_fields = []
        for field_name in field_names:
            data_type = field_mapping[field_name]
            if isinstance(data_type, tuple) and data_type[0] == DataType.ARRAY:
                array_fields.append((field_name, data_type[1]))
        
        for field_name, element_type in array_fields[:5]:  # Limit to 5 expressions
            # Array indexing expressions
            if operator in ["==", "!=", ">", "<", ">=", "<="]:
                # Direct array element comparison
                if element_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64]:
                    value = random.randint(0, 100)
                    expression = f"{field_name}[0] {operator} {value}"
                    
                    def validate_array_int(data_value, val=value, op=operator):
                        if data_value is None or not isinstance(data_value, list) or len(data_value) == 0:
                            return False
                        element = data_value[0] if len(data_value) > 0 else None
                        if element is None:
                            return False
                        if op == "==":
                            return element == val
                        elif op == "!=":
                            return element != val
                        elif op == ">":
                            return element > val
                        elif op == "<":
                            return element < val
                        elif op == ">=":
                            return element >= val
                        elif op == "<=":
                            return element <= val
                        return False
                    
                    expressions.append((expression, lambda record, f=field_name, v=value, o=operator: 
                                     validate_array_int(record.get(f), v, o)))
                
                elif element_type == DataType.VARCHAR:
                    # String array element comparison
                    patterns = ["str_0", "str_1", "str_2", "a", "b", "c"]
                    pattern = random.choice(patterns)
                    
                    if operator == "LIKE":
                        expression = f'{field_name}[0] LIKE "{pattern}"'
                        
                        def validate_array_string_like(data_value, pat=pattern):
                            if data_value is None or not isinstance(data_value, list) or len(data_value) == 0:
                                return False
                            element = data_value[0] if len(data_value) > 0 else None
                            if element is None:
                                return False
                            data_str = str(element)
                            regex_pattern = self._convert_like_to_regex(pat)
                            try:
                                return bool(re.match(regex_pattern, data_str))
                            except:
                                return False
                        
                        expressions.append((expression, lambda record, f=field_name, p=pattern: 
                                         validate_array_string_like(record.get(f), p)))
                    else:
                        expression = f'{field_name}[0] {operator} "{pattern}"'
                        
                        def validate_array_string(data_value, val=pattern, op=operator):
                            if data_value is None or not isinstance(data_value, list) or len(data_value) == 0:
                                return False
                            element = data_value[0] if len(data_value) > 0 else None
                            if element is None:
                                return False
                            if op == "==":
                                return str(element) == val
                            elif op == "!=":
                                return str(element) != val
                            return False
                        
                        expressions.append((expression, lambda record, f=field_name, v=pattern, o=operator: 
                                         validate_array_string(record.get(f), v, o)))
            
            elif operator == "IN":
                # Array element IN expression
                if element_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64]:
                    values = [random.randint(0, 100) for _ in range(3)]
                    expression = f"{field_name}[0] IN {values}"
                    
                    def validate_array_in(data_value, filter_values=values):
                        if data_value is None or not isinstance(data_value, list) or len(data_value) == 0:
                            return False
                        element = data_value[0] if len(data_value) > 0 else None
                        return element in filter_values
                    
                    expressions.append((expression, lambda record, f=field_name, v=values: 
                                     validate_array_in(record.get(f), v)))
                
                elif element_type == DataType.VARCHAR:
                    values = ["str_0", "str_1", "str_2", "a", "b", "c"]
                    expression = f'{field_name}[0] IN {values}'
                    
                    def validate_array_string_in(data_value, filter_values=values):
                        if data_value is None or not isinstance(data_value, list) or len(data_value) == 0:
                            return False
                        element = data_value[0] if len(data_value) > 0 else None
                        return str(element) in filter_values
                    
                    expressions.append((expression, lambda record, f=field_name, v=values: 
                                     validate_array_string_in(record.get(f), v)))
        
        return expressions
    
    def _generate_json_path_expressions(self, field_names: List[str], field_mapping: Dict, operator: str) -> List[Tuple[str, Callable]]:
        """Generate expressions with JSON path access."""
        expressions = []
        
        # Find JSON fields
        json_fields = [name for name in field_names if field_mapping[name] == DataType.JSON]
        
        for field_name in json_fields[:3]:  # Limit to 3 expressions
            # JSON path expressions
            if operator in ["==", "!=", ">", "<", ">=", "<="]:
                # Simple JSON key access
                expression = f"{field_name}['int'] {operator} 5"
                
                def validate_json_number(data_value, val=5, op=operator):
                    if data_value is None or not isinstance(data_value, dict):
                        return False
                    json_value = data_value.get('int')
                    if json_value is None:
                        if op == "!=":
                            return True
                        else:
                            return False
                    if op == "==":
                        return json_value == val
                    elif op == "!=":
                        return json_value != val
                    elif op == ">":
                        return json_value > val
                    elif op == "<":
                        return json_value < val
                    elif op == ">=":
                        return json_value >= val
                    elif op == "<=":
                        return json_value <= val
                    return False
                
                expressions.append((expression, lambda record, f=field_name, v=5, o=operator: 
                                 validate_json_number(record.get(f), v, o)))
                
                # JSON text comparison
                expression = f'{field_name}["varchar"] {operator} "str_5"'
                
                def validate_json_text(data_value, val="str_5", op=operator):
                    if data_value is None or not isinstance(data_value, dict):
                        return False
                    json_value = data_value.get('varchar')
                    if json_value is None:
                        if op == "!=":
                            return True
                        else:
                            return False
                    if op == "==":
                        return str(json_value) == str(val)
                    elif op == "!=":
                        return str(json_value) != str(val)
                    elif op == ">":
                        return str(json_value) > str(val)
                    elif op == "<":
                        return str(json_value) < str(val)
                    elif op == ">=":
                        return str(json_value) >= str(val)
                    elif op == "<=":
                        return str(json_value) <= str(val)
                    return False
                
                expressions.append((expression, lambda record, f=field_name, v="str_5", o=operator: 
                                 validate_json_text(record.get(f), v, o)))
            
            elif operator == "IN":
                # JSON array element IN
                expression = f"{field_name}['array_int'][0] IN [1, 2, 3]"
                
                def validate_json_array_in(data_value, filter_values=[1, 2, 3]):
                    if data_value is None or not isinstance(data_value, dict):
                        return False
                    json_array = data_value.get('array_int')
                    if json_array is None or not isinstance(json_array, list) or len(json_array) == 0:
                        return False
                    element = json_array[0]
                    return element in filter_values
                
                expressions.append((expression, lambda record, f=field_name, v=[1, 2, 3]: 
                                 validate_json_array_in(record.get(f), v)))
            
            elif operator == "LIKE":
                # JSON text LIKE
                expression = f'{field_name}["varchar"] LIKE "str_%"'
                
                def validate_json_like(data_value, pattern="str_%"):
                    if data_value is None or not isinstance(data_value, dict):
                        return False
                    json_value = data_value.get('varchar')
                    if json_value is None:
                        return False
                    data_str = str(json_value)
                    regex_pattern = self._convert_like_to_regex(pattern)
                    try:
                        return bool(re.match(regex_pattern, data_str))
                    except:
                        return False
                
                expressions.append((expression, lambda record, f=field_name, p="str_%": 
                                 validate_json_like(record.get(f), p)))
        
        return expressions
    
    def _generate_mixed_complex_expressions(self, field_names: List[str], field_mapping: Dict, operator: str) -> List[Tuple[str, Callable]]:
        """Generate mixed complex expressions combining multiple features."""
        expressions = []
        
        # Find different types of fields
        numeric_fields = []
        array_fields = []
        json_fields = []
        
        for field_name in field_names:
            data_type = field_mapping[field_name]
            if isinstance(data_type, tuple) and data_type[0] == DataType.ARRAY:
                element_type = data_type[1]
                if element_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.FLOAT, DataType.DOUBLE]:
                    array_fields.append((field_name, element_type))
            elif data_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.FLOAT, DataType.DOUBLE]:
                numeric_fields.append(field_name)
            elif data_type == DataType.JSON:
                json_fields.append(field_name)
        
        # Mixed expressions: array element + constant arithmetic
        if array_fields and operator in ["==", "!=", ">", "<", ">=", "<="]:
            field_name, element_type = array_fields[0]
            if element_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64]:
                constant = random.randint(1, 10)
                value = random.randint(0, 50)
                expression = f"{field_name}[0] + {constant} {operator} {value}"
                
                def validate_mixed_array_arithmetic(data_value, const=constant, val=value, op=operator):
                    if data_value is None or not isinstance(data_value, list) or len(data_value) == 0:
                        return False
                    element = data_value[0] if len(data_value) > 0 else None
                    if element is None:
                        if op == "!=":
                            return True
                        else:
                            return False
                    result = element + const
                    if op == "==":
                        return result == val
                    elif op == "!=":
                        return result != val
                    elif op == ">":
                        return result > val
                    elif op == "<":
                        return result < val
                    elif op == ">=":
                        return result >= val
                    elif op == "<=":
                        return result <= val
                    return False
                
                expressions.append((expression, lambda record, f=field_name, c=constant, v=value, o=operator: 
                                 validate_mixed_array_arithmetic(record.get(f), c, v, o)))
        
        # Mixed expressions: JSON array element comparison
        if json_fields and operator in ["==", "!=", ">", "<", ">=", "<="]:
            field_name = json_fields[0]
            value = random.randint(0, 10)
            expression = f"{field_name}['array_int'][0] {operator} {value}"
            
            def validate_json_array_element(data_value, val=value, op=operator):
                if data_value is None or not isinstance(data_value, dict):
                    return False
                json_array = data_value.get('array_int')
                if json_array is None:
                    if op == "!=":
                        return True
                    else:
                        return False
                element = json_array[0]
                if element is None:
                    if op == "!=":
                        return True
                    else:
                        return False
                if op == "==":
                    return element == val
                elif op == "!=":
                    return element != val
                elif op == ">":
                    return element > val
                elif op == "<":
                    return element < val
                elif op == ">=":
                    return element >= val
                elif op == "<=":
                    return element <= val
                return False
            
            expressions.append((expression, lambda record, f=field_name, v=value, o=operator: 
                             validate_json_array_element(record.get(f), v, o)))
        
        return expressions

    def _run_complex_expression_tests(self, client, collection_name: str, field_mapping: Dict, 
                                     index_configs: Dict, test_data: List[Dict]) -> Dict:
        """
        Run complex expression testing including field calculations, array indexing, and JSON path access.
        
        Args:
            client: Milvus client instance
            collection_name: Name of the collection
            field_mapping: Mapping of field names to data types
            index_configs: Mapping of field names to index types
            test_data: Test data records
            
        Returns:
            Dictionary containing test statistics
        """
        # Initialize statistics
        stats = {
            "success": [],
            "not_supported": [],
            "failed": [],
            "total": []
        }

        # Test complex expressions with all operators
        all_operators = comparison_operators + range_operators + null_operators

        for operator in all_operators:
            # Generate complex expressions for this operator
            complex_expressions = self.generate_complex_expression(field_mapping, index_configs, operator)
            
            for expression, validator in complex_expressions:
                expression_info = f"Complex expression with {operator}: {expression}"
                stats["total"].append(expression_info)

                log.info(f"Testing {expression_info}")

                try:
                    # Execute query
                    res = self.query(
                        client, collection_name=collection_name,
                        filter=expression, output_fields=["*"],
                        check_task=CheckTasks.check_nothing
                    )[0]

                    # Check if res is an Error
                    if isinstance(res, Error):
                        if self.is_parsing_error(res):
                            log.warning(f"⚠️ {expression_info} cannot be parsed, skipping: {str(res)}")
                            stats["not_supported"].append(f"{expression_info}: {str(res)}")
                            continue
                        else:
                            log.error(f"✗ {expression_info} failed: {str(res)}")
                            stats["failed"].append(f"{expression_info}: {str(res)}")
                            continue
                    else:
                        # Calculate expected results
                        expected_results = []
                        for record in test_data:
                            if validator(record):
                                expected_results.append(record)

                        # Verify results against ground truth
                        if len(res) != len(expected_results):
                            log.error(
                                f"✗ {expression_info} returned {len(res)} results, expected {len(expected_results)}")
                            stats["failed"].append(
                                f"{expression_info}: returned {len(res)} results, expected {len(expected_results)}")
                            continue
                        else:
                            log.info(f"✓ {expression_info} passed with {len(res)} results")
                            stats["success"].append(f"{expression_info}: {len(res)} results")

                except Exception as e:
                    log.error(f"✗ {expression_info} encountered exception: {str(e)}")
                    stats["failed"].append(f"{expression_info}: exception {str(e)}")
                    continue

        return stats
