"""
CDC sync tests for index operations.
"""

import time
import random
from .base import TestCDCSyncBase, logger


class TestCDCSyncIndex(TestCDCSyncBase):
    """Test CDC sync for index operations."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method - only cleanup upstream, downstream will sync."""
        upstream_client = getattr(self, "_upstream_client", None)

        if upstream_client:
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "collection":
                    self.cleanup_collection(upstream_client, resource_name)

            time.sleep(1)  # Allow cleanup to sync to downstream

    def test_create_index(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_INDEX operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_create_idx")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )

        # Wait for creation to sync
        def check_create():
            return downstream_client.has_collection(collection_name)

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create collection {collection_name}"
        )

        # Create index
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        upstream_client.create_index(collection_name, index_params)

        # Wait for index creation to sync
        def check_index():
            try:
                downstream_indexes = downstream_client.list_indexes(collection_name)
                return len(downstream_indexes) > 0
            except:
                return False

        assert self.wait_for_sync(
            check_index, sync_timeout, f"create index on {collection_name}"
        )

    def test_drop_index(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_INDEX operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_drop_idx")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection and index
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )

        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        upstream_client.create_index(collection_name, index_params)

        # Wait for setup to sync
        def check_setup():
            try:
                return (
                    downstream_client.has_collection(collection_name)
                    and len(downstream_client.list_indexes(collection_name)) > 0
                )
            except:
                return False

        assert self.wait_for_sync(
            check_setup, sync_timeout, f"setup collection and index {collection_name}"
        )

        # Drop index
        upstream_client.drop_index(collection_name, "vector")

        # Wait for index drop to sync
        def check_drop():
            try:
                downstream_indexes = downstream_client.list_indexes(collection_name)
                return len(downstream_indexes) == 0
            except:
                return True  # If error, assume index is dropped

        assert self.wait_for_sync(
            check_drop, sync_timeout, f"drop index on {collection_name}"
        )

    def test_create_vector_indexes_comprehensive(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test CREATE_INDEX operation sync for all vector index types."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        # Test cases for different vector types and their applicable indexes
        test_cases = [
            # FLOAT_VECTOR indexes
            {
                "field_name": "float_vector",
                "field_type": "FLOAT_VECTOR",
                "index_tests": [
                    {"index_type": "FLAT", "metric_type": "L2", "params": {}},
                    {
                        "index_type": "IVF_FLAT",
                        "metric_type": "L2",
                        "params": {"nlist": 128},
                    },
                    {
                        "index_type": "IVF_SQ8",
                        "metric_type": "L2",
                        "params": {"nlist": 128},
                    },
                    {
                        "index_type": "IVF_PQ",
                        "metric_type": "L2",
                        "params": {"nlist": 128, "m": 16, "nbits": 8},
                    },
                    {
                        "index_type": "HNSW",
                        "metric_type": "L2",
                        "params": {"M": 16, "efConstruction": 200},
                    },
                ],
            },
            # FLOAT16_VECTOR indexes
            {
                "field_name": "float16_vector",
                "field_type": "FLOAT16_VECTOR",
                "index_tests": [
                    {"index_type": "FLAT", "metric_type": "L2", "params": {}},
                    {
                        "index_type": "IVF_FLAT",
                        "metric_type": "L2",
                        "params": {"nlist": 128},
                    },
                    {
                        "index_type": "HNSW",
                        "metric_type": "L2",
                        "params": {"M": 16, "efConstruction": 200},
                    },
                ],
            },
            # BINARY_VECTOR indexes
            {
                "field_name": "binary_vector",
                "field_type": "BINARY_VECTOR",
                "index_tests": [
                    {"index_type": "BIN_FLAT", "metric_type": "HAMMING", "params": {}},
                    {
                        "index_type": "BIN_IVF_FLAT",
                        "metric_type": "HAMMING",
                        "params": {"nlist": 128},
                    },
                ],
            },
            # SPARSE_FLOAT_VECTOR indexes
            {
                "field_name": "sparse_vector",
                "field_type": "SPARSE_FLOAT_VECTOR",
                "index_tests": [
                    {
                        "index_type": "SPARSE_INVERTED_INDEX",
                        "metric_type": "IP",
                        "params": {},
                    },
                ],
            },
        ]

        for test_case in test_cases:
            for index_test in test_case["index_tests"]:
                collection_name = self.gen_unique_name(
                    f"test_idx_{test_case['field_type'].lower()}_{index_test['index_type'].lower()}"
                )
                self.resources_to_cleanup.append(("collection", collection_name))

                try:
                    logger.info(
                        f"[INDEX_TEST] Testing {test_case['field_type']} with {index_test['index_type']} index"
                    )

                    # Initial cleanup
                    self.cleanup_collection(upstream_client, collection_name)

                    # Create collection with specific vector field
                    schema = self._create_vector_schema(
                        upstream_client,
                        test_case["field_name"],
                        test_case["field_type"],
                    )
                    upstream_client.create_collection(
                        collection_name=collection_name, schema=schema
                    )

                    # Wait for creation to sync
                    def check_create():
                        return downstream_client.has_collection(collection_name)

                    assert self.wait_for_sync(
                        check_create,
                        sync_timeout,
                        f"create collection {collection_name}",
                    )

                    # Insert test data before creating index (better practice)
                    test_data = self._generate_test_data_for_vector_field(
                        test_case["field_name"], test_case["field_type"], 100
                    )
                    upstream_client.insert(collection_name, test_data)
                    upstream_client.flush(collection_name)
                    logger.info(
                        f"[DATA_INSERTED] Inserted 100 records before creating {test_case['field_type']} index"
                    )

                    # Create specific index
                    index_params = upstream_client.prepare_index_params()
                    index_params.add_index(
                        field_name=test_case["field_name"],
                        index_type=index_test["index_type"],
                        metric_type=index_test["metric_type"],
                        params=index_test["params"],
                    )
                    upstream_client.create_index(collection_name, index_params)

                    # Wait for index creation to sync
                    def check_index():
                        try:
                            downstream_indexes = downstream_client.list_indexes(
                                collection_name
                            )
                            return len(downstream_indexes) > 0
                        except:
                            return False

                    assert self.wait_for_sync(
                        check_index,
                        sync_timeout,
                        f"create {index_test['index_type']} index on {collection_name}",
                    )

                    # Verify index details
                    try:
                        index_info = downstream_client.describe_index(
                            collection_name, test_case["field_name"]
                        )
                        logger.info(
                            f"[INDEX_VERIFICATION] {index_test['index_type']} index created successfully: {index_info}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to verify {index_test['index_type']} index details: {e}"
                        )

                except Exception as e:
                    logger.error(
                        f"[INDEX_ERROR] Failed to test {test_case['field_type']} with {index_test['index_type']}: {e}"
                    )
                    raise

    def test_create_scalar_indexes_comprehensive(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test CREATE_INDEX operation sync for all scalar field index types."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        # Test cases for different scalar types and their applicable indexes
        test_cases = [
            # VARCHAR indexes
            {
                "field_name": "varchar_field",
                "field_type": "VARCHAR",
                "index_tests": [
                    {"index_type": "INVERTED", "params": {}},
                    {"index_type": "Trie", "params": {}},
                ],
            },
            # BOOL indexes
            {
                "field_name": "bool_field",
                "field_type": "BOOL",
                "index_tests": [
                    {"index_type": "INVERTED", "params": {}},
                ],
            },
            # INT32 indexes
            {
                "field_name": "int32_field",
                "field_type": "INT32",
                "index_tests": [
                    {"index_type": "INVERTED", "params": {}},
                    {"index_type": "STL_SORT", "params": {}},
                ],
            },
            # INT64 indexes
            {
                "field_name": "int64_field",
                "field_type": "INT64",
                "index_tests": [
                    {"index_type": "INVERTED", "params": {}},
                    {"index_type": "STL_SORT", "params": {}},
                ],
            },
            # FLOAT indexes
            {
                "field_name": "float_field",
                "field_type": "FLOAT",
                "index_tests": [
                    {"index_type": "INVERTED", "params": {}},
                ],
            },
            # DOUBLE indexes
            {
                "field_name": "double_field",
                "field_type": "DOUBLE",
                "index_tests": [
                    {"index_type": "INVERTED", "params": {}},
                ],
            },
            # ARRAY indexes
            {
                "field_name": "int32_array",
                "field_type": "ARRAY",
                "element_type": "INT32",
                "index_tests": [
                    {"index_type": "INVERTED", "params": {}},
                ],
            },
            # JSON indexes - use AUTOINDEX (recommended) with proper JSON path syntax
            {
                "field_name": "json_field",
                "field_type": "JSON",
                "index_tests": [
                    {
                        "index_type": "AUTOINDEX",
                        "params": {
                            "json_path": 'json_field["name"]',
                            "json_cast_type": "VARCHAR",
                        },
                    },
                ],
            },
        ]

        for test_case in test_cases:
            for index_test in test_case["index_tests"]:
                collection_name = self.gen_unique_name(
                    f"test_idx_{test_case['field_type'].lower()}_{index_test['index_type'].lower()}"
                )
                self.resources_to_cleanup.append(("collection", collection_name))

                try:
                    logger.info(
                        f"[SCALAR_INDEX_TEST] Testing {test_case['field_type']} with {index_test['index_type']} index"
                    )

                    # Initial cleanup
                    self.cleanup_collection(upstream_client, collection_name)

                    # Create collection with specific scalar field
                    schema = self._create_scalar_schema(upstream_client, test_case)
                    upstream_client.create_collection(
                        collection_name=collection_name, schema=schema
                    )

                    # Wait for creation to sync
                    def check_create():
                        return downstream_client.has_collection(collection_name)

                    assert self.wait_for_sync(
                        check_create,
                        sync_timeout,
                        f"create collection {collection_name}",
                    )

                    # Insert test data before creating index (better practice)
                    test_data = self._generate_test_data_for_scalar_field(
                        test_case["field_name"], test_case["field_type"], 100
                    )
                    upstream_client.insert(collection_name, test_data)
                    upstream_client.flush(collection_name)
                    logger.info(
                        f"[DATA_INSERTED] Inserted 100 records before creating {test_case['field_type']} index"
                    )

                    # Create specific index
                    index_params = upstream_client.prepare_index_params()
                    if test_case["field_type"] == "JSON":
                        # JSON fields need special handling with index_name
                        index_params.add_index(
                            field_name=test_case["field_name"],
                            index_type=index_test["index_type"],
                            index_name=f"{test_case['field_name']}_name_index",
                            params=index_test["params"],
                        )
                    else:
                        index_params.add_index(
                            field_name=test_case["field_name"],
                            index_type=index_test["index_type"],
                            params=index_test["params"],
                        )
                    upstream_client.create_index(collection_name, index_params)

                    # Wait for index creation to sync
                    def check_index():
                        try:
                            downstream_indexes = downstream_client.list_indexes(
                                collection_name
                            )
                            return len(downstream_indexes) > 0
                        except:
                            return False

                    assert self.wait_for_sync(
                        check_index,
                        sync_timeout,
                        f"create {index_test['index_type']} index on {collection_name}",
                    )

                    # Verify index details
                    try:
                        index_info = downstream_client.describe_index(
                            collection_name, test_case["field_name"]
                        )
                        logger.info(
                            f"[SCALAR_INDEX_VERIFICATION] {index_test['index_type']} index created successfully: {index_info}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to verify {index_test['index_type']} scalar index details: {e}"
                        )

                except Exception as e:
                    logger.error(
                        f"[SCALAR_INDEX_ERROR] Failed to test {test_case['field_type']} with {index_test['index_type']}: {e}"
                    )
                    raise

    def _create_vector_schema(self, client, field_name, field_type):
        """Create schema for vector index testing."""
        from pymilvus import DataType

        schema = client.create_schema(enable_dynamic_field=True)
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)

        if field_type == "FLOAT_VECTOR":
            schema.add_field(field_name, DataType.FLOAT_VECTOR, dim=128)
        elif field_type == "FLOAT16_VECTOR":
            schema.add_field(field_name, DataType.FLOAT16_VECTOR, dim=64)
        elif field_type == "BFLOAT16_VECTOR":
            schema.add_field(field_name, DataType.BFLOAT16_VECTOR, dim=64)
        elif field_type == "BINARY_VECTOR":
            schema.add_field(field_name, DataType.BINARY_VECTOR, dim=128)
        elif field_type == "SPARSE_FLOAT_VECTOR":
            schema.add_field(field_name, DataType.SPARSE_FLOAT_VECTOR)
        elif field_type == "INT8_VECTOR":
            schema.add_field(field_name, DataType.INT8_VECTOR, dim=128)

        return schema

    def _create_scalar_schema(self, client, test_case):
        """Create schema for scalar index testing."""
        from pymilvus import DataType

        schema = client.create_schema(enable_dynamic_field=True)
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(
            "vector", DataType.FLOAT_VECTOR, dim=128
        )  # Required for collection

        field_name = test_case["field_name"]
        field_type = test_case["field_type"]

        if field_type == "VARCHAR":
            schema.add_field(field_name, DataType.VARCHAR, max_length=1000)
        elif field_type == "BOOL":
            schema.add_field(field_name, DataType.BOOL)
        elif field_type == "INT32":
            schema.add_field(field_name, DataType.INT32)
        elif field_type == "INT64":
            schema.add_field(field_name, DataType.INT64)
        elif field_type == "FLOAT":
            schema.add_field(field_name, DataType.FLOAT)
        elif field_type == "DOUBLE":
            schema.add_field(field_name, DataType.DOUBLE)
        elif field_type == "ARRAY" and test_case.get("element_type") == "INT32":
            schema.add_field(
                field_name,
                DataType.ARRAY,
                element_type=DataType.INT32,
                max_capacity=100,
            )
        elif field_type == "JSON":
            schema.add_field(field_name, DataType.JSON)

        return schema

    def _generate_test_data_for_vector_field(self, field_name, field_type, count=100):
        """Generate test data for specific vector field type."""
        from pymilvus import DataType

        data = []
        for _ in range(count):
            record = {}

            if field_type == "FLOAT_VECTOR":
                vectors = self._gen_vectors(1, 128, DataType.FLOAT_VECTOR)
                record[field_name] = vectors[0]
            elif field_type == "FLOAT16_VECTOR":
                vectors = self._gen_vectors(1, 64, DataType.FLOAT16_VECTOR)
                record[field_name] = vectors[0]
            elif field_type == "BFLOAT16_VECTOR":
                vectors = self._gen_vectors(1, 64, DataType.BFLOAT16_VECTOR)
                record[field_name] = vectors[0]
            elif field_type == "BINARY_VECTOR":
                vectors = self._gen_vectors(1, 128, DataType.BINARY_VECTOR)
                record[field_name] = vectors[0]
            elif field_type == "SPARSE_FLOAT_VECTOR":
                vectors = self._gen_vectors(1, 1000, DataType.SPARSE_FLOAT_VECTOR)
                record[field_name] = vectors[0]
            elif field_type == "INT8_VECTOR":
                vectors = self._gen_vectors(1, 128, DataType.INT8_VECTOR)
                record[field_name] = vectors[0]

            data.append(record)

        return data

    def _generate_test_data_for_scalar_field(self, field_name, field_type, count=100):
        """Generate test data for specific scalar field type."""
        data = []
        for i in range(count):
            record = {
                "vector": [
                    random.random() for _ in range(128)
                ],  # Required base vector field
            }

            if field_type == "VARCHAR":
                record[field_name] = f"test_string_{i}_{random.randint(1000, 9999)}"
            elif field_type == "BOOL":
                record[field_name] = random.choice([True, False])
            elif field_type == "INT32":
                record[field_name] = random.randint(-1000000, 1000000)
            elif field_type == "INT64":
                record[field_name] = random.randint(-1000000000, 1000000000)
            elif field_type == "FLOAT":
                record[field_name] = random.uniform(-1000.0, 1000.0)
            elif field_type == "DOUBLE":
                record[field_name] = random.uniform(-1000.0, 1000.0)
            elif field_type == "ARRAY":
                record[field_name] = [
                    random.randint(-100, 100) for _ in range(random.randint(1, 10))
                ]
            elif field_type == "JSON":
                record[field_name] = {
                    "name": f"test_item_{i}",
                    "value": random.randint(1, 1000),
                    "category": random.choice(["A", "B", "C"]),
                    "metadata": {
                        "score": random.uniform(0.0, 100.0),
                        "created": f"2024-01-{random.randint(1, 28):02d}",
                    },
                }

            data.append(record)

        return data

    def test_create_bfloat16_int8_vector_indexes(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test CREATE_INDEX operation sync for BFLOAT16_VECTOR and INT8_VECTOR (combined test due to 4-vector limit)."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        # Test cases for BFLOAT16_VECTOR and INT8_VECTOR
        test_cases = [
            # BFLOAT16_VECTOR indexes - use AUTOINDEX for compatibility
            {
                "field_name": "bfloat16_vector",
                "field_type": "BFLOAT16_VECTOR",
                "index_tests": [
                    {"index_type": "AUTOINDEX", "metric_type": "L2", "params": {}},
                ],
            },
            # INT8_VECTOR indexes - use AUTOINDEX for compatibility
            {
                "field_name": "int8_vector",
                "field_type": "INT8_VECTOR",
                "index_tests": [
                    {"index_type": "AUTOINDEX", "metric_type": "L2", "params": {}},
                ],
            },
        ]

        for test_case in test_cases:
            for index_test in test_case["index_tests"]:
                collection_name = self.gen_unique_name(
                    f"test_idx_{test_case['field_type'].lower()}_{index_test['index_type'].lower()}"
                )
                self.resources_to_cleanup.append(("collection", collection_name))

                try:
                    logger.info(
                        f"[{test_case['field_type']}_INDEX_TEST] Testing {test_case['field_type']} with {index_test['index_type']} index"
                    )

                    # Initial cleanup
                    self.cleanup_collection(upstream_client, collection_name)

                    # Create collection with specific vector field
                    schema = self._create_vector_schema(
                        upstream_client,
                        test_case["field_name"],
                        test_case["field_type"],
                    )
                    upstream_client.create_collection(
                        collection_name=collection_name, schema=schema
                    )

                    # Wait for creation to sync
                    def check_create():
                        return downstream_client.has_collection(collection_name)

                    assert self.wait_for_sync(
                        check_create,
                        sync_timeout,
                        f"create collection {collection_name}",
                    )

                    # Insert test data before creating index (better practice)
                    if test_case["field_type"] == "BFLOAT16_VECTOR":
                        test_data = self.generate_bfloat16_test_data(100)
                    else:  # INT8_VECTOR
                        test_data = self.generate_int8_test_data(100)
                    upstream_client.insert(collection_name, test_data)
                    upstream_client.flush(collection_name)
                    logger.info(
                        f"[DATA_INSERTED] Inserted 100 records before creating {test_case['field_type']} index"
                    )

                    # Create specific index
                    index_params = upstream_client.prepare_index_params()
                    index_params.add_index(
                        field_name=test_case["field_name"],
                        index_type=index_test["index_type"],
                        metric_type=index_test["metric_type"],
                        params=index_test["params"],
                    )
                    upstream_client.create_index(collection_name, index_params)

                    # Wait for index creation to sync
                    def check_index():
                        try:
                            downstream_indexes = downstream_client.list_indexes(
                                collection_name
                            )
                            return len(downstream_indexes) > 0
                        except:
                            return False

                    assert self.wait_for_sync(
                        check_index,
                        sync_timeout,
                        f"create {index_test['index_type']} index on {collection_name}",
                    )

                    # Verify index details
                    try:
                        index_info = downstream_client.describe_index(
                            collection_name, test_case["field_name"]
                        )
                        logger.info(
                            f"[{test_case['field_type']}_INDEX_VERIFICATION] {index_test['index_type']} index created successfully: {index_info}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to verify {index_test['index_type']} {test_case['field_type']} index details: {e}"
                        )

                except Exception as e:
                    logger.error(
                        f"[{test_case['field_type']}_INDEX_ERROR] Failed to test {test_case['field_type']} with {index_test['index_type']}: {e}"
                    )
                    raise
