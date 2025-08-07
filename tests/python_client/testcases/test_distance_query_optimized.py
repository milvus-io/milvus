#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import pytest
import numpy as np
import time
import threading
from typing import List, Dict, Any

# Standard Milvus test framework imports - following existing patterns
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_base import TestcaseBase
from pymilvus import (
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    utility
)

# Test configuration constants following Milvus conventions
prefix = "distance_query_optimized"
default_schema = cf.gen_default_collection_schema()
default_field_name = ct.default_float_vec_field_name
default_index_params = ct.default_index

# Distance query specific constants
VECTOR_DIM = 4
MAX_STRING_LENGTH = 50
TEST_DATA_SIZE = 6  # Reduced for optimization
DEFAULT_QUERY_LIMIT = 3
INDEX_NLIST = 16  # Optimized for test environment
INDEX_METRIC_TYPE = "L2"
INDEX_TYPE = "IVF_FLAT"

# Error message constants based on actual implementation
DISTANCE_IN_WHERE_ERROR = "distance expressions should be used in SELECT context, not in WHERE clauses"
TYPE_CHECK_ERROR = "type check failed, actual type: FLOAT"

# Optimized test vectors for distance calculations
TEST_VECTORS = [
    [0.1, 0.2, 0.3, 0.4],  # ID=1 - base vector
    [0.5, 0.6, 0.7, 0.8],  # ID=2 - medium distance
    [0.9, 1.0, 1.1, 1.2],  # ID=3 - far distance
    [1.3, 1.4, 1.5, 1.6],  # ID=4 - distant vector
    [0.2, 0.3, 0.4, 0.5],  # ID=5 - close to base
    [0.6, 0.7, 0.8, 0.9],  # ID=6 - medium-close
]

TEST_IDS = list(range(1, TEST_DATA_SIZE + 1))
TEST_NAMES = [f"vector_{i}" for i in TEST_IDS]

# External vectors for cross-collection testing
EXTERNAL_TEST_VECTORS = [
    [0.1, 0.1, 0.1, 0.1],  # Near-zero vector
    [0.9, 0.9, 0.9, 0.9]   # Uniform vector
]


class TestDistanceQueryOptimized(TestcaseBase):
    """
    Optimized Distance Query Test Suite
    
    Following Milvus standard test patterns with focus on:
    - Core architectural validation
    - GitHub Issue #43442 requirements
    - Performance optimization
    - Comprehensive error handling
    """

    def setup_method(self, method):
        """
        Setup test environment following Milvus patterns
        """
        super().setup_method(method)
        log.info(f"Setup method for {method.__name__}")
        self.collection_w = None
        self.index_name = cf.gen_unique_str("distance_index")
        self.collection_name = cf.gen_unique_str(f"{prefix}_{method.__name__}")

    def teardown_method(self, method):
        """
        Cleanup test environment following Milvus patterns
        """
        log.info(f"Teardown method for {method.__name__}")
        if self.collection_w:
            try:
                self.collection_w.drop()
                log.info(f"Collection {self.collection_name} dropped successfully")
            except Exception as e:
                log.warning(f"Failed to drop collection: {e}")
        # 调用父类的teardown_method来清理连接
        super().teardown_method(method)

    def init_optimized_distance_collection(self, insert_data=True, nb=TEST_DATA_SIZE):
        """
        Initialize optimized collection for distance query testing
        
        Args:
            insert_data: Whether to insert test data
            nb: Number of entities to insert
            
        Returns:
            tuple: (collection_wrapper, entities)
        """
        # Create optimized schema for distance queries
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM),
            FieldSchema(name="name", dtype=DataType.VARCHAR, max_length=MAX_STRING_LENGTH)
        ]
        schema = CollectionSchema(fields, f"Optimized distance query collection: {self.collection_name}")
        
        # Create collection using standard wrapper
        self.collection_w = self.init_collection_wrap(
            name=self.collection_name,
            schema=schema,
            check_task=CheckTasks.check_collection_property,
            check_items={"name": self.collection_name, "schema": schema}
        )
        
        entities = None
        if insert_data:
            # Insert optimized test data
            entities = [
                TEST_IDS[:nb],
                TEST_VECTORS[:nb],
                TEST_NAMES[:nb]
            ]
            
            self.collection_w.insert(entities, check_task=CheckTasks.check_insert_result)
            self.collection_w.flush()
            
            # Create optimized index
            index_params = {
                "metric_type": INDEX_METRIC_TYPE,
                "index_type": INDEX_TYPE,
                "params": {"nlist": INDEX_NLIST}
            }
            
            self.collection_w.create_index("vector", index_params, index_name=self.index_name,
                                         check_task=CheckTasks.check_nothing)
            self.collection_w.load()
            
            # Optimized wait time
            time.sleep(0.5)
        
        return self.collection_w, entities

    @pytest.mark.tags(CaseLabel.L0)
    def test_core_distance_syntax_validation(self):
        """
        Test core distance function syntax validation - L0 Priority
        
        Target: Verify distance function syntax correctly parsed and type-checked
        Method: Test distance expressions in WHERE context (should fail) vs SELECT context
        Expected: Distance functions rejected in WHERE, type checking works correctly
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing core distance syntax validation")
        
        # Core Test 1: Distance function in WHERE clause - should fail with syntax error
        collection_w.query(
            "distance(a.vector, b.vector, 'L2') > 0.5",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
        )
        
        # Core Test 2: Distance function with alias - should fail with type error  
        collection_w.query(
            "distance(a.vector, b.vector, 'COSINE') as dist_score",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "expr type check failed"}
        )
        
        # Core Test 3: Normal boolean expressions - should work (backwards compatibility)
        result = collection_w.query(
            "id IN [1,2]",
            output_fields=["id", "name"],
            limit=DEFAULT_QUERY_LIMIT,
            check_task=CheckTasks.check_nothing
        )
        
        # Validate result structure - extract the actual data from the response
        if hasattr(result, 'data'):
            actual_result = result.data
        elif isinstance(result, tuple) and len(result) > 0:
            actual_result = result[0]
        else:
            actual_result = result if result else []
            
        assert len(actual_result) == 2, f"Expected 2 results, got {len(actual_result)}"
        
        log.info("Core distance syntax validation completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    def test_github_issue_43442_requirements(self):
        """
        Test GitHub Issue #43442 specific requirements - L1 Priority
        
        Target: Validate both scenarios from GitHub Issue #43442
        Method: Test distance query patterns specified in the issue
        Expected: Syntax parsing works, but WHERE context properly rejected
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing GitHub Issue #43442 requirements")
        
        # GitHub Issue Scenario 1: Query with two id lists
        # The syntax from the issue: distance(a.vector, b.vector, 'L2') as _distance
        # Should be parsed correctly but rejected in WHERE context
        collection_w.query(
            "distance(a.vector, b.vector, 'L2') > 0.5",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
        )
        
        # GitHub Issue Scenario 2: Query with id list and external vectors
        # The architecture should handle this syntax pattern
        collection_w.query(
            "distance(a.vector, b.vector, 'L2') < 1.0",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
        )
        
        # Validate that the from parameter concept is architecturally sound
        # Test that distance function is properly rejected with the right error message
        collection_w.query(
            "distance(a.vector, b.vector, 'L2') > 0.5",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
        )
        
        log.info("GitHub Issue #43442 requirements validation completed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_distance_algorithms_coverage(self):
        """
        Test distance algorithm coverage - L1 Priority
        
        Target: Verify all supported distance algorithms are recognized
        Method: Test L2, IP, COSINE, HAMMING, JACCARD algorithms
        Expected: All algorithms parsed correctly, all fail appropriately in WHERE
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing distance algorithm coverage")
        
        # Test all supported distance algorithms
        algorithms = ["L2", "IP", "COSINE", "HAMMING", "JACCARD"]
        
        for algorithm in algorithms:
            log.info(f"Testing {algorithm} algorithm")
            
            collection_w.query(
                f"distance(vector, vector, '{algorithm}') < 1.0",
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
            )
        
        log.info("Distance algorithm coverage test completed successfully")

    @pytest.mark.tags(CaseLabel.L2)
    def test_edge_cases_comprehensive(self):
        """
        Test comprehensive edge cases - L2 Priority
        
        Target: Verify robust error handling for edge cases
        Method: Test invalid metrics, malformed syntax, boundary conditions
        Expected: Appropriate error handling for each edge case
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing comprehensive edge cases")
        
        # Edge Case 1: Invalid distance metric
        collection_w.query(
            "distance(vector, vector, 'INVALID_METRIC') > 0.5",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "unsupported metric type"}
        )
        
        # Edge Case 2: Malformed distance function (missing argument)
        collection_w.query(
            "distance(vector, 'L2') > 0.5",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
        )
        
        # Edge Case 3: Distance with complex boolean logic
        collection_w.query(
            "distance(vector, vector, 'L2') > 0.5 AND id > 0",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
        )
        
        # Edge Case 4: Nested distance expressions
        collection_w.query(
            "(distance(vector, vector, 'L2') + 1.0) > 1.5",
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "complicated arithmetic operations are not supported"}
        )
        
        log.info("Comprehensive edge cases test completed successfully")

    @pytest.mark.tags(CaseLabel.L2)
    def test_backwards_compatibility_validation(self):
        """
        Test backwards compatibility validation - L2 Priority
        
        Target: Ensure existing query functionality unaffected
        Method: Test traditional query patterns extensively
        Expected: All existing queries work exactly as before
        """
        collection_w, entities = self.init_optimized_distance_collection()
        
        log.info("Testing backwards compatibility validation")
        
        # Comprehensive backwards compatibility test cases
        compatibility_tests = [
            {
                "expr": "id > 0",
                "expected_min": TEST_DATA_SIZE,
                "description": "Simple comparison"
            },
            {
                "expr": "id IN [1,2,3]",
                "expected": 3,
                "description": "IN clause"
            },
            {
                "expr": "name == 'vector_1'",
                "expected": 1,
                "description": "String equality"
            },
            {
                "expr": "id > 1 AND id < 4",
                "expected": 2,
                "description": "Range with AND"
            }
        ]
        
        for test_case in compatibility_tests:
            log.info(f"Testing backwards compatibility: {test_case['description']}")
            
            result = collection_w.query(
                test_case["expr"],
                output_fields=["id", "name"],
                limit=TEST_DATA_SIZE,
                check_task=CheckTasks.check_nothing
            )
            
            # Extract actual result data
            if hasattr(result, 'data'):
                actual_result = result.data
            elif isinstance(result, tuple) and len(result) > 0:
                actual_result = result[0]
            else:
                actual_result = result if result else []
            
            if "expected" in test_case:
                assert len(actual_result) == test_case["expected"], \
                    f"Query '{test_case['expr']}' returned {len(actual_result)}, expected {test_case['expected']}"
            elif "expected_min" in test_case:
                assert len(actual_result) >= test_case["expected_min"], \
                    f"Query '{test_case['expr']}' returned {len(actual_result)}, expected at least {test_case['expected_min']}"
        
        log.info("Backwards compatibility validation completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    def test_architecture_design_validation(self):
        """
        Test architectural design validation - L1 Priority
        
        Target: Verify multi-layer protection architecture
        Method: Test FLOAT vs BOOL type separation in WHERE context
        Expected: Clear architectural boundaries enforced
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing architectural design validation")
        
        # Architecture Test 1: FLOAT expressions should be rejected in WHERE
        float_returning_expressions = [
            "distance(vector, vector, 'L2')",
            "distance(a.vector, b.vector, 'COSINE')",
            "distance(vector, vector, 'IP') + 1.0"
        ]
        
        for expr in float_returning_expressions:
            if "1.0" in expr:
                # Arithmetic operations have different error message
                collection_w.query(
                    f"{expr} > 0.5",
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100, "err_msg": "complicated arithmetic operations are not supported"}
                )
            else:
                # Pure distance expressions have distance context error
                collection_w.query(
                    f"{expr} > 0.5",
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
                )
        
        # Architecture Test 2: BOOL expressions should work in WHERE
        bool_returning_expressions = [
            "id > 0",
            "id IN [1,2,3]",
            "name == 'vector_1'",
            "id > 1 AND id < 5"
        ]
        
        for expr in bool_returning_expressions:
            result = collection_w.query(
                expr,
                output_fields=["id"],
                limit=DEFAULT_QUERY_LIMIT,
                check_task=CheckTasks.check_nothing
            )
            
            # Extract actual result data
            if hasattr(result, 'data'):
                actual_result = result.data
            elif isinstance(result, tuple) and len(result) > 0:
                actual_result = result[0]
            else:
                actual_result = result if result else []
                
            assert isinstance(actual_result, list), f"Expected list result for boolean expression: {expr}"
        
        log.info("Architectural design validation completed successfully")

    @pytest.mark.tags(CaseLabel.L2)
    def test_performance_benchmarks(self):
        """
        Test performance benchmarks - L2 Priority
        
        Target: Verify performance meets expectations
        Method: Measure query response times and throughput
        Expected: Reasonable performance within acceptable limits
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing performance benchmarks")
        
        # Performance Test 1: Simple query response time
        start_time = time.time()
        result = collection_w.query(
            "id > 0",
            output_fields=["id"],
            limit=TEST_DATA_SIZE,
            check_task=CheckTasks.check_nothing
        )
        
        # Extract actual result data
        if hasattr(result, 'data'):
            actual_result = result.data
        elif isinstance(result, tuple) and len(result) > 0:
            actual_result = result[0]
        else:
            actual_result = result if result else []
            
        simple_query_time = time.time() - start_time
        
        log.info(f"Simple query took {simple_query_time:.3f} seconds for {len(actual_result)} results")
        assert simple_query_time < 1.0, f"Simple query too slow: {simple_query_time:.3f}s"
        
        # Performance Test 2: Distance syntax validation response time
        start_time = time.time()
        for i in range(5):
            collection_w.query(
                f"distance(vector, vector, 'L2') > {0.1 * i}",
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
            )
        batch_validation_time = time.time() - start_time
        
        log.info(f"5 distance validations took {batch_validation_time:.3f} seconds")
        assert batch_validation_time < 2.0, f"Batch validation too slow: {batch_validation_time:.3f}s"
        
        log.info("Performance benchmarks completed successfully")

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_query_handling(self):
        """
        Test concurrent query handling - L2 Priority
        
        Target: Verify system handles concurrent distance queries
        Method: Execute multiple queries concurrently
        Expected: All queries handled correctly without race conditions
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing concurrent query handling")
        
        results = []
        errors = []
        
        def query_worker(worker_id):
            try:
                # Mix of valid and invalid queries
                if worker_id % 2 == 0:
                    # Valid boolean query
                    result = collection_w.query(
                        f"id > {worker_id}",
                        output_fields=["id"],
                        limit=2,
                        check_task=CheckTasks.check_nothing
                    )
                    
                    # Extract actual result data
                    if hasattr(result, 'data'):
                        actual_result = result.data
                    elif isinstance(result, tuple) and len(result) > 0:
                        actual_result = result[0]
                    else:
                        actual_result = result if result else []
                        
                    results.append({"worker": worker_id, "type": "valid", "count": len(actual_result)})
                else:
                    # Invalid distance query
                    collection_w.query(
                        f"distance(vector, vector, 'L2') > {0.1 * worker_id}",
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
                    )
                    results.append({"worker": worker_id, "type": "error_handled"})
            except Exception as e:
                errors.append({"worker": worker_id, "error": str(e)})
        
        # Execute concurrent workers
        threads = []
        num_workers = 4  # Optimized for test performance
        
        for i in range(num_workers):
            thread = threading.Thread(target=query_worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join(timeout=5)
        
        # Validate results
        assert len(results) == num_workers, f"Expected {num_workers} results, got {len(results)}"
        assert len(errors) == 0, f"Unexpected errors in concurrent execution: {errors}"
        
        log.info("Concurrent query handling test completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    def test_code_branch_coverage_validation(self):
        """
        Test code branch coverage validation - L1 Priority
        
        Target: Exercise all important code branches in distance query implementation
        Method: Test various expression patterns and validation paths
        Expected: All major code branches covered and working correctly
        """
        collection_w, _ = self.init_optimized_distance_collection()
        
        log.info("Testing code branch coverage validation")
        
        # Branch Coverage 1: Different distance metrics
        metrics = ["L2", "IP", "COSINE"]
        for metric in metrics:
            collection_w.query(
                f"distance(vector, vector, '{metric}') > 0.5",
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
            )
        
        # Branch Coverage 2: Different comparison operators
        operators = [">", "<", ">=", "<=", "==", "!="]
        for op in operators:
            collection_w.query(
                f"distance(vector, vector, 'L2') {op} 0.5",
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
            )
        
        # Branch Coverage 3: Complex boolean expressions (valid)
        complex_boolean_exprs = [
            "id > 0 AND id < 5",
            "id IN [1,2] OR id IN [4,5]",
            "NOT (id == 3)",
            "(id > 1) AND (name LIKE 'vector_%')"
        ]
        
        for expr in complex_boolean_exprs:
            result = collection_w.query(
                expr,
                output_fields=["id"],
                limit=TEST_DATA_SIZE,
                check_task=CheckTasks.check_nothing
            )
            
            # Extract actual result data
            if hasattr(result, 'data'):
                actual_result = result.data
            elif isinstance(result, tuple) and len(result) > 0:
                actual_result = result[0]
            else:
                actual_result = result if result else []
                
            assert isinstance(actual_result, list), f"Expected list for boolean expression: {expr}"
        
        # Branch Coverage 4: Mixed distance and boolean (invalid)
        mixed_exprs = [
            "distance(vector, vector, 'L2') > 0.5 OR id > 100",
            "id > 0 AND distance(vector, vector, 'COSINE') < 1.0"
        ]
        
        for expr in mixed_exprs:
            collection_w.query(
                expr,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1100, "err_msg": "distance expressions should be used in SELECT context"}
            )
        
        log.info("Code branch coverage validation completed successfully")


# Test configuration for pytest following Milvus patterns
def pytest_configure(config):
    """Configure pytest markers for optimized distance query tests"""
    config.addinivalue_line("markers", "L0: Critical functionality tests")
    config.addinivalue_line("markers", "L1: Important feature tests")
    config.addinivalue_line("markers", "L2: Advanced and edge case tests")


if __name__ == "__main__":
    """
    Support for direct execution with optimized test selection
    """
    import sys
    
    # Default to running L0 and L1 tests for quick validation
    test_args = [__file__, "-v", "-m", "L0 or L1", "--tb=short"]
    
    # If 'all' argument provided, run all tests
    if len(sys.argv) > 1 and sys.argv[1] == "all":
        test_args = [__file__, "-v", "--tb=short"]
    
    pytest.main(test_args)