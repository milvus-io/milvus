"""
CDC sync tests for advanced schema features (dynamic fields, nullable, default values,
partition keys, clustering keys, and combinations thereof).
"""

import time
import random
from pymilvus import DataType
from .base import TestCDCSyncBase, logger


class TestCDCSyncSchemaFeatures(TestCDCSyncBase):
    """Test CDC sync for advanced schema features."""

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

    def test_dynamic_schema_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test that dynamic schema fields are correctly replicated via CDC."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_dynamic_schema", max_length=50)

        self.log_test_start("test_dynamic_schema_sync", "DYNAMIC_SCHEMA", collection_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Create dynamic schema collection
            self.log_operation("CREATE_COLLECTION", "collection", collection_name, "upstream")
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_dynamic_schema(upstream_client),
            )

            # Create HNSW index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Wait for collection creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Insert 200 rows with extra dynamic fields
            extra_fields = {"extra_int": int, "extra_str": str, "extra_float": float}
            test_data = self.generate_dynamic_data(200, extra_fields=extra_fields)
            self.log_data_operation("INSERT", collection_name, len(test_data), "- dynamic schema data")
            upstream_client.insert(collection_name, test_data)
            upstream_client.flush(collection_name)

            # Query extra_int > 0 on upstream
            self.log_sync_verification("DYNAMIC_SCHEMA", collection_name, "extra_int > 0 count matches downstream")
            upstream_result = upstream_client.query(
                collection_name=collection_name,
                filter="extra_int > 0",
                output_fields=["count(*)"],
            )
            upstream_count = upstream_result[0]["count(*)"] if upstream_result else 0
            logger.info(f"[UPSTREAM] extra_int > 0 count: {upstream_count}")

            # Wait for sync and verify downstream count matches
            def check_data():
                try:
                    down_result = downstream_client.query(
                        collection_name=collection_name,
                        filter="extra_int > 0",
                        output_fields=["count(*)"],
                    )
                    down_count = down_result[0]["count(*)"] if down_result else 0
                    logger.info(f"[SYNC_PROGRESS] downstream extra_int > 0 count: {down_count}/{upstream_count}")
                    return down_count == upstream_count
                except Exception as e:
                    logger.warning(f"Dynamic schema sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data, sync_timeout, f"dynamic schema data sync {collection_name}"
            )
            assert sync_success, (
                f"Dynamic schema data failed to sync to downstream for {collection_name}"
            )

            # Verify data sampling for extra fields
            match, mismatch, details = self.verify_data_sampling(
                upstream_client,
                downstream_client,
                collection_name,
                sample_ratio=0.1,
                output_fields=["id", "varchar_field", "extra_int", "extra_str", "extra_float"],
            )
            logger.info(f"[VERIFY] Dynamic schema sampling: match={match}, mismatch={mismatch}")
            assert mismatch == 0, (
                f"Dynamic field data mismatch detected: {details}"
            )

            duration = time.time() - start_time
            self.log_test_end("test_dynamic_schema_sync", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] test_dynamic_schema_sync failed: {e}")
            self.log_test_end("test_dynamic_schema_sync", False, duration)
            raise

    def test_nullable_fields_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test that nullable field values (including NULLs) are correctly replicated via CDC."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_nullable_flds", max_length=50)

        self.log_test_start("test_nullable_fields_sync", "NULLABLE_FIELDS", collection_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Create nullable schema collection
            self.log_operation("CREATE_COLLECTION", "collection", collection_name, "upstream")
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_nullable_schema(upstream_client),
            )

            # Create index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Wait for collection creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Insert 200 rows with null_ratio=0.3
            test_data = self.generate_nullable_data(200, null_ratio=0.3)
            self.log_data_operation("INSERT", collection_name, len(test_data), "- nullable data null_ratio=0.3")
            upstream_client.insert(collection_name, test_data)
            upstream_client.flush(collection_name)

            # Count nulls and not-nulls for nullable_int64 on upstream
            null_result = upstream_client.query(
                collection_name=collection_name,
                filter="nullable_int64 == null",
                output_fields=["count(*)"],
            )
            not_null_result = upstream_client.query(
                collection_name=collection_name,
                filter="nullable_int64 != null",
                output_fields=["count(*)"],
            )
            upstream_null_count = null_result[0]["count(*)"] if null_result else 0
            upstream_not_null_count = not_null_result[0]["count(*)"] if not_null_result else 0
            logger.info(
                f"[UPSTREAM] nullable_int64 null={upstream_null_count}, not_null={upstream_not_null_count}"
            )

            self.log_sync_verification("NULLABLE_FIELDS", collection_name, "null counts match downstream")

            # Wait for sync and verify null/not-null counts match on downstream
            def check_null_counts():
                try:
                    d_null = downstream_client.query(
                        collection_name=collection_name,
                        filter="nullable_int64 == null",
                        output_fields=["count(*)"],
                    )
                    d_not_null = downstream_client.query(
                        collection_name=collection_name,
                        filter="nullable_int64 != null",
                        output_fields=["count(*)"],
                    )
                    d_null_count = d_null[0]["count(*)"] if d_null else 0
                    d_not_null_count = d_not_null[0]["count(*)"] if d_not_null else 0
                    logger.info(
                        f"[SYNC_PROGRESS] downstream nullable_int64 null={d_null_count}/{upstream_null_count}, "
                        f"not_null={d_not_null_count}/{upstream_not_null_count}"
                    )
                    return (
                        d_null_count == upstream_null_count
                        and d_not_null_count == upstream_not_null_count
                    )
                except Exception as e:
                    logger.warning(f"Nullable sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_null_counts, sync_timeout, f"nullable fields sync {collection_name}"
            )
            assert sync_success, (
                f"Nullable field counts failed to sync to downstream for {collection_name}"
            )

            duration = time.time() - start_time
            self.log_test_end("test_nullable_fields_sync", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] test_nullable_fields_sync failed: {e}")
            self.log_test_end("test_nullable_fields_sync", False, duration)
            raise

    def test_default_values_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test that default field values are applied and replicated correctly via CDC."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_default_vals", max_length=50)

        self.log_test_start("test_default_values_sync", "DEFAULT_VALUES", collection_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Create default values schema collection
            self.log_operation("CREATE_COLLECTION", "collection", collection_name, "upstream")
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_default_values_schema(upstream_client),
            )

            # Create index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Wait for collection creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Insert 100 rows providing ONLY float_vector — default fields are omitted
            test_data = [
                {"float_vector": [random.random() for _ in range(128)]}
                for _ in range(100)
            ]
            self.log_data_operation("INSERT", collection_name, len(test_data), "- only float_vector provided")
            upstream_client.insert(collection_name, test_data)
            upstream_client.flush(collection_name)

            # Query default_varchar == "default" on upstream — should be 100
            upstream_result = upstream_client.query(
                collection_name=collection_name,
                filter='default_varchar == "default"',
                output_fields=["count(*)"],
            )
            upstream_count = upstream_result[0]["count(*)"] if upstream_result else 0
            logger.info(f'[UPSTREAM] default_varchar == "default" count: {upstream_count}')
            assert upstream_count == 100, (
                f"Expected 100 rows with default varchar on upstream, got {upstream_count}"
            )

            self.log_sync_verification("DEFAULT_VALUES", collection_name, 'default_varchar == "default" count=100 on downstream')

            # Wait for sync and verify same count on downstream
            def check_defaults():
                try:
                    down_result = downstream_client.query(
                        collection_name=collection_name,
                        filter='default_varchar == "default"',
                        output_fields=["count(*)"],
                    )
                    down_count = down_result[0]["count(*)"] if down_result else 0
                    logger.info(f'[SYNC_PROGRESS] downstream default_varchar count: {down_count}/100')
                    return down_count == 100
                except Exception as e:
                    logger.warning(f"Default values sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_defaults, sync_timeout, f"default values sync {collection_name}"
            )
            assert sync_success, (
                f"Default value data failed to sync to downstream for {collection_name}"
            )

            duration = time.time() - start_time
            self.log_test_end("test_default_values_sync", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] test_default_values_sync failed: {e}")
            self.log_test_end("test_default_values_sync", False, duration)
            raise

    def test_partition_key_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test that partition key schema and data are correctly replicated via CDC."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_part_key", max_length=50)

        self.log_test_start("test_partition_key_sync", "PARTITION_KEY", collection_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Create partition key schema (VarChar key)
            self.log_operation("CREATE_COLLECTION", "collection", collection_name, "upstream")
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_partition_key_schema(upstream_client, key_type="VarChar"),
            )

            # Create index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Wait for collection creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Insert 500 rows with random categories as partition key values
            categories = ["cat_A", "cat_B", "cat_C", "cat_D", "cat_E"]
            test_data = [
                {
                    "float_vector": [random.random() for _ in range(128)],
                    "partition_key_field": random.choice(categories),
                    "data_field": f"data_{i}_{random.randint(1000, 9999)}",
                }
                for i in range(500)
            ]
            self.log_data_operation("INSERT", collection_name, len(test_data), "- partition key data")
            upstream_client.insert(collection_name, test_data)
            upstream_client.flush(collection_name)

            self.log_sync_verification("PARTITION_KEY", collection_name, "count=500 and partition_key field on downstream")

            # Wait for sync and verify count=500 on downstream
            def check_data():
                try:
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {count}/500")
                    return count >= 500
                except Exception as e:
                    logger.warning(f"Partition key sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data, sync_timeout, f"partition key data sync {collection_name}"
            )
            assert sync_success, (
                f"Partition key data failed to sync to downstream for {collection_name}"
            )

            # Verify partition_key_field is present in downstream describe_collection
            downstream_info = downstream_client.describe_collection(collection_name)
            downstream_fields = [
                f["name"] for f in downstream_info.get("fields", [])
            ]
            logger.info(f"[VERIFY] Downstream fields: {downstream_fields}")
            assert "partition_key_field" in downstream_fields, (
                f"partition_key_field not found in downstream collection schema: {downstream_fields}"
            )

            duration = time.time() - start_time
            self.log_test_end("test_partition_key_sync", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] test_partition_key_sync failed: {e}")
            self.log_test_end("test_partition_key_sync", False, duration)
            raise

    def test_clustering_key_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test that clustering key schema and data are correctly replicated via CDC."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_cluster_key", max_length=50)

        self.log_test_start("test_clustering_key_sync", "CLUSTERING_KEY", collection_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Create clustering key schema
            self.log_operation("CREATE_COLLECTION", "collection", collection_name, "upstream")
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_clustering_key_schema(upstream_client),
            )

            # Create index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Wait for collection creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Insert 300 rows
            test_data = [
                {
                    "float_vector": [random.random() for _ in range(128)],
                    "clustering_key_field": random.randint(0, 1000),
                    "data_field": f"data_{i}_{random.randint(1000, 9999)}",
                }
                for i in range(300)
            ]
            self.log_data_operation("INSERT", collection_name, len(test_data), "- clustering key data")
            upstream_client.insert(collection_name, test_data)
            upstream_client.flush(collection_name)

            self.log_sync_verification("CLUSTERING_KEY", collection_name, "count=300 and clustering_key field on downstream")

            # Wait for sync and verify count=300 on downstream
            def check_data():
                try:
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {count}/300")
                    return count >= 300
                except Exception as e:
                    logger.warning(f"Clustering key sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data, sync_timeout, f"clustering key data sync {collection_name}"
            )
            assert sync_success, (
                f"Clustering key data failed to sync to downstream for {collection_name}"
            )

            # Verify clustering_key_field is present in downstream describe_collection
            downstream_info = downstream_client.describe_collection(collection_name)
            downstream_fields = [
                f["name"] for f in downstream_info.get("fields", [])
            ]
            logger.info(f"[VERIFY] Downstream fields: {downstream_fields}")
            assert "clustering_key_field" in downstream_fields, (
                f"clustering_key_field not found in downstream collection schema: {downstream_fields}"
            )

            duration = time.time() - start_time
            self.log_test_end("test_clustering_key_sync", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] test_clustering_key_sync failed: {e}")
            self.log_test_end("test_clustering_key_sync", False, duration)
            raise

    def test_nullable_with_defaults(self, upstream_client, downstream_client, sync_timeout):
        """Test that nullable fields combined with default values sync correctly via CDC.

        Inserts 100 rows in three patterns:
          i%3==0: explicit values provided for both fields
          i%3==1: None values (explicit null) provided for both fields
          i%3==2: fields omitted entirely (uses default / null)
        """
        start_time = time.time()
        collection_name = self.gen_unique_name("test_null_default", max_length=50)

        self.log_test_start("test_nullable_with_defaults", "NULLABLE_WITH_DEFAULTS", collection_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Build custom schema: nullable_with_default (INT64, nullable, default=42)
            #                      nullable_no_default   (VARCHAR, nullable)
            schema = upstream_client.create_schema()
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=128)
            schema.add_field(
                "nullable_with_default",
                DataType.INT64,
                nullable=True,
                default_value=42,
            )
            schema.add_field(
                "nullable_no_default",
                DataType.VARCHAR,
                max_length=256,
                nullable=True,
            )

            self.log_operation("CREATE_COLLECTION", "collection", collection_name, "upstream")
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=schema,
            )

            # Create index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Wait for collection creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Insert 100 rows across 3 patterns
            test_data = []
            for i in range(100):
                record = {"float_vector": [random.random() for _ in range(128)]}
                if i % 3 == 0:
                    # Explicit values
                    record["nullable_with_default"] = random.randint(100, 999)
                    record["nullable_no_default"] = f"explicit_{i}"
                elif i % 3 == 1:
                    # Explicit None (null)
                    record["nullable_with_default"] = None
                    record["nullable_no_default"] = None
                # i%3==2: omit both fields — server applies default/null
                test_data.append(record)

            self.log_data_operation("INSERT", collection_name, len(test_data), "- nullable+default mixed pattern")
            upstream_client.insert(collection_name, test_data)
            upstream_client.flush(collection_name)

            self.log_sync_verification("NULLABLE_WITH_DEFAULTS", collection_name, "total count=100 on downstream")

            # Wait for sync and verify total count on downstream
            def check_data():
                try:
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {count}/100")
                    return count >= 100
                except Exception as e:
                    logger.warning(f"Nullable+defaults sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data, sync_timeout, f"nullable+defaults sync {collection_name}"
            )
            assert sync_success, (
                f"Nullable-with-defaults data failed to sync to downstream for {collection_name}"
            )

            duration = time.time() - start_time
            self.log_test_end("test_nullable_with_defaults", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] test_nullable_with_defaults failed: {e}")
            self.log_test_end("test_nullable_with_defaults", False, duration)
            raise

    def test_dynamic_with_partition_key(self, upstream_client, downstream_client, sync_timeout):
        """Test that dynamic fields combined with a partition key schema sync correctly via CDC."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_dyn_part_key", max_length=50)

        self.log_test_start("test_dynamic_with_partition_key", "DYNAMIC_WITH_PARTITION_KEY", collection_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Build schema: enable_dynamic_field=True + VARCHAR partition key
            schema = upstream_client.create_schema(enable_dynamic_field=True)
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=128)
            schema.add_field(
                "pk_field",
                DataType.VARCHAR,
                max_length=64,
                is_partition_key=True,
            )

            self.log_operation("CREATE_COLLECTION", "collection", collection_name, "upstream")
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=schema,
            )

            # Create index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Wait for collection creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Insert 300 rows with dynamic fields + partition key values
            partitions = ["region_A", "region_B", "region_C", "region_D"]
            test_data = [
                {
                    "float_vector": [random.random() for _ in range(128)],
                    "pk_field": random.choice(partitions),
                    # dynamic fields
                    "dynamic_num": random.randint(1, 10000),
                    "dynamic_tag": f"tag_{i % 10}",
                    "dynamic_score": random.uniform(0.0, 100.0),
                }
                for i in range(300)
            ]
            self.log_data_operation("INSERT", collection_name, len(test_data), "- dynamic+partition_key data")
            upstream_client.insert(collection_name, test_data)
            upstream_client.flush(collection_name)

            # Query dynamic_num > 0 on upstream
            upstream_result = upstream_client.query(
                collection_name=collection_name,
                filter="dynamic_num > 0",
                output_fields=["count(*)"],
            )
            upstream_count = upstream_result[0]["count(*)"] if upstream_result else 0
            logger.info(f"[UPSTREAM] dynamic_num > 0 count: {upstream_count}")

            self.log_sync_verification(
                "DYNAMIC_WITH_PARTITION_KEY",
                collection_name,
                f"dynamic_num > 0 count={upstream_count} on downstream",
            )

            # Wait for sync and verify the count matches on downstream
            def check_data():
                try:
                    down_result = downstream_client.query(
                        collection_name=collection_name,
                        filter="dynamic_num > 0",
                        output_fields=["count(*)"],
                    )
                    down_count = down_result[0]["count(*)"] if down_result else 0
                    logger.info(
                        f"[SYNC_PROGRESS] downstream dynamic_num > 0 count: {down_count}/{upstream_count}"
                    )
                    return down_count == upstream_count
                except Exception as e:
                    logger.warning(f"Dynamic+partition_key sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data, sync_timeout, f"dynamic+partition_key sync {collection_name}"
            )
            assert sync_success, (
                f"Dynamic+partition_key data failed to sync to downstream for {collection_name}"
            )

            duration = time.time() - start_time
            self.log_test_end("test_dynamic_with_partition_key", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] test_dynamic_with_partition_key failed: {e}")
            self.log_test_end("test_dynamic_with_partition_key", False, duration)
            raise
