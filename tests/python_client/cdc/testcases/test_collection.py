"""
CDC sync tests for collection DDL operations.
"""

import time
import pytest
from pymilvus import DataType, Collection
from .base import TestCDCSyncBase, logger


class TestCDCSyncCollectionDDL(TestCDCSyncBase):
    """Test CDC sync for collection DDL operations."""

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

    def test_create_collection(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_COLLECTION operation sync."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_create")

        # Log test start
        self.log_test_start(
            "test_create_collection", "CREATE_COLLECTION", collection_name
        )

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Log operation
            self.log_operation(
                "CREATE_COLLECTION", "collection", collection_name, "upstream"
            )

            # Create collection in upstream
            schema = self.create_default_schema(upstream_client)
            logger.info(f"[SCHEMA] Collection schema: {schema}")

            upstream_client.create_collection(
                collection_name=collection_name, schema=schema
            )

            # Verify upstream creation
            upstream_exists = upstream_client.has_collection(collection_name)
            self.log_resource_state(
                "collection",
                collection_name,
                "exists" if upstream_exists else "missing",
                "upstream",
            )
            assert upstream_exists, (
                f"Collection {collection_name} not created in upstream"
            )

            # Log sync verification start
            self.log_sync_verification(
                "CREATE_COLLECTION", collection_name, "exists in downstream"
            )

            # Wait for sync to downstream
            def check_sync():
                exists = downstream_client.has_collection(collection_name)
                if exists:
                    self.log_resource_state(
                        "collection",
                        collection_name,
                        "exists",
                        "downstream",
                        "Sync confirmed",
                    )
                return exists

            sync_success = self.wait_for_sync(
                check_sync, sync_timeout, f"create collection {collection_name}"
            )
            assert sync_success, (
                f"Collection {collection_name} failed to sync to downstream"
            )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_create_collection", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_create_collection", False, duration)
            raise

    def test_drop_collection(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_COLLECTION operation sync."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_drop")

        # Log test start
        self.log_test_start("test_drop_collection", "DROP_COLLECTION", collection_name)

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create collection first
            self.log_operation(
                "CREATE_COLLECTION", "collection", collection_name, "upstream"
            )
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

            # Drop collection in upstream
            self.log_operation(
                "DROP_COLLECTION", "collection", collection_name, "upstream"
            )
            upstream_client.drop_collection(collection_name)

            # Verify upstream drop
            upstream_exists = upstream_client.has_collection(collection_name)
            self.log_resource_state(
                "collection",
                collection_name,
                "missing" if not upstream_exists else "exists",
                "upstream",
            )
            assert not upstream_exists, (
                f"Collection {collection_name} still exists in upstream after drop"
            )

            # Log sync verification start
            self.log_sync_verification(
                "DROP_COLLECTION", collection_name, "missing from downstream"
            )

            # Wait for drop to sync
            def check_drop():
                exists = downstream_client.has_collection(collection_name)
                if not exists:
                    self.log_resource_state(
                        "collection",
                        collection_name,
                        "missing",
                        "downstream",
                        "Drop synced",
                    )
                return not exists

            sync_success = self.wait_for_sync(
                check_drop, sync_timeout, f"drop collection {collection_name}"
            )
            assert sync_success, (
                f"Collection {collection_name} drop failed to sync to downstream"
            )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_drop_collection", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_drop_collection", False, duration)
            raise

    def test_rename_collection(self, upstream_client, downstream_client, sync_timeout):
        """Test RENAME_COLLECTION operation sync."""
        start_time = time.time()

        old_name = self.gen_unique_name("test_col_rename_old")
        new_name = self.gen_unique_name("test_col_rename_new")

        # Log test start
        self.log_test_start(
            "test_rename_collection", "RENAME_COLLECTION", f"{old_name} -> {new_name}"
        )

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", old_name))
        self.resources_to_cleanup.append(("collection", new_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, old_name)
            self.cleanup_collection(upstream_client, new_name)

            # Create collection first
            self.log_operation("CREATE_COLLECTION", "collection", old_name, "upstream")
            upstream_client.create_collection(
                collection_name=old_name,
                schema=self.create_default_schema(upstream_client),
            )

            # Wait for creation to sync
            def check_create():
                return downstream_client.has_collection(old_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {old_name}"
            )

            # Rename collection
            rename_start_time = time.time()
            self.log_operation(
                "RENAME_COLLECTION",
                "collection",
                f"{old_name} -> {new_name}",
                "upstream",
            )

            try:
                upstream_client.rename_collection(old_name, new_name)
                rename_duration = time.time() - rename_start_time
                logger.info(
                    f"[SUCCESS] Rename operation completed in {rename_duration:.2f}s"
                )
            except Exception as e:
                rename_duration = time.time() - rename_start_time
                logger.error(
                    f"[FAILED] Rename operation failed after {rename_duration:.2f}s: {e}"
                )
                raise

            # Verify rename in upstream
            old_exists = upstream_client.has_collection(old_name)
            new_exists = upstream_client.has_collection(new_name)
            self.log_resource_state(
                "collection",
                old_name,
                "missing" if not old_exists else "exists",
                "upstream",
            )
            self.log_resource_state(
                "collection",
                new_name,
                "exists" if new_exists else "missing",
                "upstream",
            )

            assert not old_exists, (
                f"Old collection {old_name} still exists after rename"
            )
            assert new_exists, f"New collection {new_name} not found after rename"

            # Log sync verification start
            self.log_sync_verification(
                "RENAME_COLLECTION",
                f"{old_name} -> {new_name}",
                "completed in downstream",
            )

            # Wait for rename to sync
            def check_rename():
                return not downstream_client.has_collection(
                    old_name
                ) and downstream_client.has_collection(new_name)

            sync_success = self.wait_for_sync(
                check_rename,
                sync_timeout,
                f"rename collection {old_name} to {new_name}",
            )
            assert sync_success, (
                f"Collection rename from {old_name} to {new_name} failed to sync to downstream"
            )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_rename_collection", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_rename_collection", False, duration)
            raise


class TestCDCSyncCollectionManagement(TestCDCSyncBase):
    """Test CDC sync for collection management operations."""

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

    def test_add_collection_field(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test ADD_FIELD operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_add_field")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection
        schema = self.create_default_schema(upstream_client)
        upstream_client.create_collection(
            collection_name=collection_name, schema=schema, consistency_level="Strong"
        )
        assert self.wait_for_sync(
            lambda: upstream_client.has_collection(collection_name),
            sync_timeout,
            f"create collection {collection_name}",
        )

        # Add field
        upstream_client.add_collection_field(
            collection_name,
            field_name="new_field",
            data_type=DataType.INT64,
            nullable=True,
        )
        print(f"DEBUG: add field {collection_name}")
        res = upstream_client.describe_collection(collection_name)
        print(f"DEBUG: describe collection {collection_name}: {res}")

        # Wait for addition to sync
        def check_add():
            res = downstream_client.describe_collection(collection_name)
            logger.info(
                f"DEBUG: describe collection in downstream {collection_name}: {res}"
            )
            return "new_field" in [field["name"] for field in res["fields"]]

        assert self.wait_for_sync(
            check_add, sync_timeout, f"add field {collection_name}"
        )

    def test_load_collection(self, upstream_client, downstream_client, sync_timeout):
        """Test LOAD_COLLECTION operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_load")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection with proper schema
        schema = self.create_default_schema(upstream_client)
        upstream_client.create_collection(
            collection_name=collection_name, schema=schema, consistency_level="Strong"
        )

        # Create index (required for loading)
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)

        # Wait for creation to sync
        def check_create():
            return downstream_client.has_collection(collection_name)

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create collection {collection_name}"
        )

        # Load collection
        upstream_client.load_collection(collection_name)

        # Wait for load to sync
        def check_load():
            try:
                # Try to perform a search to verify the collection is loaded
                query_vector = [[0.1] * 128]  # dummy vector
                downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[],
                )
                return True
            except:
                return False

        assert self.wait_for_sync(
            check_load, sync_timeout, f"load collection {collection_name}"
        )

    @pytest.mark.skip(reason="skip multi-replica test")
    def test_load_collection_multi_replicas(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test LOAD_COLLECTION operation with multiple replicas sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_multi_replicas")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection with proper schema
        schema = self.create_default_schema(upstream_client)
        upstream_client.create_collection(
            collection_name=collection_name, schema=schema, consistency_level="Strong"
        )

        # Create index (required for loading)
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)

        # Wait for creation to sync
        def check_create():
            return downstream_client.has_collection(collection_name)

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create collection {collection_name}"
        )

        # Load collection with 2 replicas
        replica_number = 2
        logger.info(
            f"Loading collection {collection_name} with {replica_number} replicas"
        )
        upstream_client.load_collection(collection_name, replica_number=replica_number)

        # Verify upstream load with replicas and check replica count
        def verify_upstream_replicas():
            try:
                # Create Collection object to get replica information
                upstream_collection = Collection(
                    name=collection_name, using=upstream_client._using
                )

                # Get replicas information
                replicas = upstream_collection.get_replicas()
                actual_replica_count = len(replicas.groups)
                logger.info(
                    f"Upstream collection {collection_name} has {actual_replica_count} replicas"
                )
                logger.info(f"Replica details: {replicas}")

                # Verify replica count matches expected
                if actual_replica_count != replica_number:
                    logger.warning(
                        f"Expected {replica_number} replicas, but found {actual_replica_count}"
                    )
                    return False

                # Try to perform a search to verify the collection is loaded
                query_vector = [[0.1] * 128]
                upstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[],
                )
                logger.info(
                    f"Upstream collection {collection_name} loaded successfully with {actual_replica_count} replicas"
                )
                return True
            except Exception as e:
                logger.warning(f"Upstream load verification failed: {e}")
                return False

        assert self.wait_for_sync(
            verify_upstream_replicas,
            sync_timeout,
            f"load collection {collection_name} with {replica_number} replicas in upstream",
        )

        # Wait for load with replicas to sync to downstream
        def check_downstream_replicas():
            try:
                # Create Collection object to get replica information
                downstream_collection = Collection(
                    name=collection_name, using=downstream_client._using
                )

                # Get replicas information
                replicas = downstream_collection.get_replicas()
                actual_replica_count = len(replicas.groups)
                logger.info(
                    f"Downstream collection {collection_name} has {actual_replica_count} replicas"
                )
                logger.info(f"Replica details: {replicas}")

                # Verify replica count matches expected
                if actual_replica_count != replica_number:
                    logger.warning(
                        f"Expected {replica_number} replicas in downstream, but found {actual_replica_count}"
                    )
                    return False

                # Try to perform a search to verify the collection is loaded in downstream
                query_vector = [[0.1] * 128]
                downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[],
                )
                logger.info(
                    f"Downstream collection {collection_name} loaded successfully with {actual_replica_count} replicas"
                )
                return True
            except Exception as e:
                logger.warning(f"Downstream load check failed: {e}")
                return False

        assert self.wait_for_sync(
            check_downstream_replicas,
            sync_timeout,
            f"load collection {collection_name} with {replica_number} replicas in downstream",
        )

        logger.info(
            f"Successfully verified multi-replica load sync for collection {collection_name}"
        )
        logger.info(f"Both upstream and downstream have {replica_number} replicas")

        # Now test release with multiple replicas
        logger.info(
            f"Testing release operation for multi-replica collection {collection_name}"
        )
        upstream_client.release_collection(collection_name)

        # Verify upstream release
        def verify_upstream_release():
            try:
                # Try to search - should fail if released
                query_vector = [[0.1] * 128]
                upstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[],
                )
                logger.warning(
                    f"Upstream collection {collection_name} is still loaded (search succeeded)"
                )
                return False  # If search succeeds, collection is still loaded
            except Exception as e:
                logger.info(
                    f"Upstream collection {collection_name} released successfully (search failed as expected): {e}"
                )
                return True  # If search fails, collection is released

        assert self.wait_for_sync(
            verify_upstream_release,
            sync_timeout,
            f"release collection {collection_name} in upstream",
        )

        # Wait for release to sync to downstream
        def check_downstream_release():
            try:
                # Try to search - should fail if released
                query_vector = [[0.1] * 128]
                downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[],
                )
                logger.warning(
                    f"Downstream collection {collection_name} is still loaded (search succeeded)"
                )
                return False  # If search succeeds, collection is still loaded
            except Exception as e:
                logger.info(
                    f"Downstream collection {collection_name} released successfully (search failed as expected): {e}"
                )
                return True  # If search fails, collection is released

        assert self.wait_for_sync(
            check_downstream_release,
            sync_timeout,
            f"release collection {collection_name} in downstream",
        )

        logger.info(
            f"Successfully verified multi-replica release sync for collection {collection_name}"
        )
        logger.info(
            f"Both upstream and downstream released the collection with {replica_number} replicas"
        )

    def test_release_collection(self, upstream_client, downstream_client, sync_timeout):
        """Test RELEASE_COLLECTION operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_release")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection with proper schema
        schema = self.create_default_schema(upstream_client)
        upstream_client.create_collection(
            collection_name=collection_name, schema=schema, consistency_level="Strong"
        )

        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)
        upstream_client.load_collection(collection_name)

        # Wait for setup to sync
        def check_setup():
            try:
                query_vector = [[0.1] * 128]
                downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[],
                )
                return True
            except:
                return False

        assert self.wait_for_sync(
            check_setup, sync_timeout, f"setup and load collection {collection_name}"
        )

        # Release collection
        upstream_client.release_collection(collection_name)

        # Wait for release to sync
        def check_release():
            try:
                # Try to search - should fail if released
                query_vector = [[0.1] * 128]
                downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[],
                )
                return False  # If search succeeds, collection is still loaded
            except:
                return True  # If search fails, collection is released

        assert self.wait_for_sync(
            check_release, sync_timeout, f"release collection {collection_name}"
        )

    def test_flush(self, upstream_client, downstream_client, sync_timeout):
        """Test FLUSH operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_flush")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection with proper schema
        schema = self.create_default_schema(upstream_client)
        upstream_client.create_collection(
            collection_name=collection_name, schema=schema, consistency_level="Strong"
        )

        # Wait for creation to sync
        def check_create():
            return downstream_client.has_collection(collection_name)

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create collection {collection_name}"
        )

        # Insert data (without immediate flush)
        test_data = self.generate_test_data(5000)
        insert_result = upstream_client.insert(collection_name, test_data)

        # Verify data is not visible before flush
        stats_before = upstream_client.get_collection_stats(collection_name)
        logger.info(f"Stats before flush: {stats_before}")

        # Flush collection
        upstream_client.flush(collection_name)

        # Wait for flush data to be visible with timeout
        expected_count = (
            insert_result.get("insert_count", 100) if insert_result else 100
        )

        def check_flush_stats():
            try:
                stats = upstream_client.get_collection_stats(collection_name)
                logger.info(
                    f"DEBUG: get collection stats in upstream {collection_name}: {stats}"
                )
                row_count = stats.get("row_count", 0)
                logger.info(
                    f"Current row count: {row_count}, expected: {expected_count}"
                )
                return row_count >= expected_count
            except Exception as e:
                logger.warning(f"Error checking stats: {e}")
                return False

        # Use timeout for waiting flush stats to update
        timeout = 30  # 30 seconds timeout
        assert self.wait_for_sync(
            check_flush_stats,
            timeout,
            f"flush data visible in stats (expected: {expected_count})",
        )

        # Get final stats after flush
        stats_after = upstream_client.get_collection_stats(collection_name)
        logger.info(f"Stats after flush: {stats_after}")

        # Wait for flush to sync downstream
        def check_flush():
            try:
                downstream_stats = downstream_client.get_collection_stats(
                    collection_name
                )
                logger.info(
                    f"DEBUG: get collection stats in downstream {collection_name}: {downstream_stats}"
                )
                return downstream_stats.get("row_count", 0) >= 100
            except:
                return False

        assert self.wait_for_sync(
            check_flush, sync_timeout, f"flush collection {collection_name}"
        )

    def test_load_collection_with_load_fields(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test LOAD_COLLECTION operation with load_fields parameter sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_load_fields")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection with comprehensive schema (has multiple fields)
        schema = self.create_comprehensive_schema(upstream_client)
        upstream_client.create_collection(
            collection_name=collection_name, schema=schema, consistency_level="Strong"
        )

        # Create index for float_vector field (required for loading)
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="float_vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)

        # Wait for creation to sync
        def check_create():
            return downstream_client.has_collection(collection_name)

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create collection {collection_name}"
        )

        # Insert some test data
        test_data = self.generate_comprehensive_test_data(100)
        upstream_client.insert(collection_name, test_data)
        upstream_client.flush(collection_name)

        # Load collection with specific fields only (float_vector + id + varchar_field)
        load_fields = ["float_vector", "id", "varchar_field"]
        upstream_client.load_collection(collection_name, load_fields=load_fields)

        # Verify upstream load operation succeeded
        def verify_upstream_load():
            try:
                # Try to search to verify collection is loaded
                query_vector = [[0.1] * 128]
                upstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=["varchar_field"],
                    anns_field="float_vector",
                )
                return True
            except Exception as e:
                logger.warning(f"Upstream load verification failed: {e}")
                return False

        assert self.wait_for_sync(
            verify_upstream_load,
            sync_timeout,
            f"verify upstream load with load_fields in {collection_name}",
        )

        # Verify downstream sync - collection should be loaded and searchable
        def check_downstream_load_sync():
            try:
                query_vector = [[0.1] * 128]
                result = downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=["varchar_field"],
                    anns_field="float_vector",
                )
                return len(result) > 0 and len(result[0]) >= 0
            except Exception as e:
                logger.warning(f"Downstream load sync check failed: {e}")
                return False

        assert self.wait_for_sync(
            check_downstream_load_sync,
            sync_timeout,
            f"verify downstream load sync for {collection_name}",
        )

        # Additional verification: test that both loaded and unloaded fields can be output
        # (since load_fields only affects memory usage, not field accessibility)
        def verify_all_fields_accessible():
            try:
                query_vector = [[0.1] * 128]
                # Test accessing both loaded and unloaded fields
                result1 = downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=["varchar_field"],  # loaded field
                    anns_field="float_vector",
                )

                result2 = downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    output_fields=[
                        "float_field"
                    ],  # unloaded field (but should still be accessible)
                    anns_field="float_vector",
                )

                return len(result1) > 0 and len(result2) > 0
            except Exception as e:
                logger.warning(f"Field accessibility verification failed: {e}")
                return False

        assert self.wait_for_sync(
            verify_all_fields_accessible,
            sync_timeout,
            f"verify all fields accessible in {collection_name}",
        )

        logger.info(
            f"Successfully tested load_collection with load_fields: {load_fields}"
        )
        logger.info("Verified CDC sync of load operation with load_fields parameter")
