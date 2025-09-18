"""
CDC sync tests for data manipulation operations.
"""

import time
import random
from pymilvus import DataType
from .base import TestCDCSyncBase, logger


class TestCDCSyncDML(TestCDCSyncBase):
    """Test CDC sync for data manipulation operations."""

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

    def test_insert(self, upstream_client, downstream_client, sync_timeout):
        """Test INSERT operation sync."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_insert")

        # Log test start
        self.log_test_start("test_insert", "INSERT", collection_name)

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create collection
            self.log_operation(
                "CREATE_COLLECTION", "collection", collection_name, "upstream"
            )
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_default_schema(upstream_client),
            )

            # Create index and load collection for querying
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector", index_type="AUTOINDEX", metric_type="L2"
            )
            upstream_client.create_index(
                collection_name=collection_name, index_params=index_params
            )
            upstream_client.load_collection(collection_name)

            # Wait for creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Generate and insert data
            test_data = self.generate_test_data(100)
            logger.info(f"[GENERATED] Generated test data: {len(test_data)} records")

            self.log_data_operation(
                "INSERT", collection_name, len(test_data), "- starting data insertion"
            )

            result = upstream_client.insert(collection_name, test_data)
            inserted_count = result.get("insert_count", len(test_data))

            self.log_data_operation(
                "INSERT",
                collection_name,
                inserted_count,
                "- insertion completed upstream",
            )

            # Flush to ensure data is persisted
            logger.info(f"[FLUSH] Flushing collection {collection_name} in upstream")
            upstream_client.flush(collection_name)

            # Log sync verification start
            self.log_sync_verification(
                "INSERT", collection_name, f"{inserted_count} records in downstream"
            )

            # Wait for data sync by querying actual data
            def check_data():
                try:
                    # Query data to verify insertion
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",  # Get all records
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0

                    if count >= inserted_count:
                        logger.info(
                            f"[SYNC_OK] Data sync confirmed: {count} records found in downstream"
                        )
                    else:
                        logger.info(
                            f"[SYNC_PROGRESS] Data sync in progress: {count}/{inserted_count} records in downstream"
                        )

                    return count >= inserted_count
                except Exception as e:
                    logger.warning(f"Data sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data, sync_timeout, f"insert data to {collection_name}"
            )
            assert sync_success, (
                f"Data insertion failed to sync to downstream for {collection_name}"
            )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_insert", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_insert", False, duration)
            raise

    def test_delete(self, upstream_client, downstream_client, sync_timeout):
        """Test DELETE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_delete")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection and insert data
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )

        # Create index and load collection for querying
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(
            collection_name=collection_name, index_params=index_params
        )
        upstream_client.load_collection(collection_name)

        test_data = self.generate_test_data(100)
        upstream_client.insert(collection_name, test_data)
        upstream_client.flush(collection_name)

        # Wait for initial data sync by querying
        def check_data():
            try:
                result = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["count(*)"],
                )
                count = result[0]["count(*)"] if result else 0
                return count >= 100
            except:
                return False

        assert self.wait_for_sync(
            check_data, sync_timeout, f"initial data sync {collection_name}"
        )

        # Get some actual IDs to delete instead of assuming sequential IDs
        existing_records = upstream_client.query(
            collection_name=collection_name, filter="", output_fields=["id"], limit=10
        )
        delete_ids = [record["id"] for record in existing_records]

        # Delete some data using the actual IDs
        if delete_ids:
            upstream_client.delete(collection_name, filter=f"id in {delete_ids}")
            upstream_client.flush(collection_name)

        # Wait for delete to sync by querying remaining data
        def check_delete():
            if not delete_ids:  # No records to delete
                return True

            try:
                # Query for the deleted records - should return empty
                deleted_result = downstream_client.query(
                    collection_name=collection_name,
                    filter=f"id in {delete_ids}",
                    output_fields=["id"],
                )
                # Query total count
                count_result = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["count(*)"],
                )
                deleted_count = len(deleted_result) if deleted_result else 0
                total_count = count_result[0]["count(*)"] if count_result else 0
                expected_count = 100 - len(delete_ids)

                # Verify deleted records are gone and total count is correct
                return deleted_count == 0 and total_count == expected_count
            except Exception as e:
                logger.warning(f"Delete sync check failed: {e}")
                return False

        if delete_ids:
            assert self.wait_for_sync(
                check_delete, sync_timeout, f"delete data from {collection_name}"
            )
        else:
            logger.warning("No records found to delete, skipping delete test")

    def test_upsert(self, upstream_client, downstream_client, sync_timeout):
        """Test UPSERT operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_upsert")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection with manual ID schema for upsert operations
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_manual_id_schema(upstream_client),
        )

        # Create index and load collection for querying
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(
            collection_name=collection_name, index_params=index_params
        )
        upstream_client.load_collection(collection_name)

        initial_data = self.generate_test_data_with_id(50, start_id=1)
        upstream_client.insert(collection_name, initial_data)
        upstream_client.flush(collection_name)

        # Wait for initial data sync
        def check_initial():
            try:
                result = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["count(*)"],
                )
                count = result[0]["count(*)"] if result else 0
                return count >= 50
            except:
                return False

        assert self.wait_for_sync(
            check_initial, sync_timeout, f"initial data sync {collection_name}"
        )

        # Prepare upsert data - update first 25 existing records (IDs 1-25) + insert 25 new records (IDs 51-75)
        upsert_data = []

        # Update existing records (IDs 1-25)
        for i in range(1, 26):
            upsert_data.append(
                {
                    "id": i,
                    "vector": [random.random() for _ in range(128)],
                    "text": f"updated_text_{i}",
                    "number": i + 1000,
                    "metadata": {"type": "updated", "value": i + 1000},
                }
            )

        # Insert new records (IDs 51-75)
        for i in range(51, 76):
            upsert_data.append(
                {
                    "id": i,
                    "vector": [random.random() for _ in range(128)],
                    "text": f"new_text_{i}",
                    "number": i + 2000,
                    "metadata": {"type": "new", "value": i + 2000},
                }
            )

        upstream_client.upsert(collection_name, upsert_data)
        upstream_client.flush(collection_name)

        # Log upstream results before checking downstream sync
        logger.info("[UPSTREAM_CHECK] Checking upstream results after upsert...")
        try:
            upstream_count = upstream_client.query(
                collection_name=collection_name, filter="", output_fields=["count(*)"]
            )
            upstream_total = upstream_count[0]["count(*)"] if upstream_count else 0
            logger.info(f"[UPSTREAM_CHECK] Total count in upstream: {upstream_total}")

            upstream_updated = upstream_client.query(
                collection_name=collection_name,
                filter="number >= 1001 and number <= 1025",
                output_fields=["id", "number", "text"],
            )
            logger.info(
                f"[UPSTREAM_CHECK] Updated records in upstream: {len(upstream_updated)} found"
            )
            if upstream_updated:
                logger.info(
                    f"[UPSTREAM_CHECK] Sample updated record: {upstream_updated[0]}"
                )

            upstream_new = upstream_client.query(
                collection_name=collection_name,
                filter="number >= 2051 and number <= 2075",
                output_fields=["id", "number", "text"],
            )
            logger.info(
                f"[UPSTREAM_CHECK] New records in upstream: {len(upstream_new)} found"
            )
            if upstream_new:
                logger.info(f"[UPSTREAM_CHECK] Sample new record: {upstream_new[0]}")
        except Exception as e:
            logger.error(f"[UPSTREAM_CHECK] Failed to check upstream: {e}")

        # Wait for upsert to sync by verifying updated data
        def check_upsert():
            try:
                # Check total count (should be 75: 50 original + 25 new)
                count_result = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["count(*)"],
                    consistency_level="Strong",
                )
                total_count = count_result[0]["count(*)"] if count_result else 0
                logger.info(
                    f"[DOWNSTREAM_CHECK] Total count in downstream: {total_count} (expected: 75)"
                )

                # Check if updated records exist with new values (number >= 1001 and <= 1025)
                updated_result = downstream_client.query(
                    collection_name=collection_name,
                    filter="number >= 1001 and number <= 1025",  # Updated numbers for IDs 1-25
                    output_fields=["id", "number", "text"],
                    consistency_level="Strong",
                )
                updated_count = len(updated_result) if updated_result else 0
                logger.info(
                    f"[DOWNSTREAM_CHECK] Updated records in downstream: {updated_count} found (expected: 25)"
                )
                if updated_result and len(updated_result) > 0:
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Sample updated record: {updated_result[0]}"
                    )

                # Check if new records exist (number >= 2051 and <= 2075)
                new_result = downstream_client.query(
                    collection_name=collection_name,
                    filter="number >= 2051 and number <= 2075",  # New numbers for IDs 51-75
                    output_fields=["id", "number", "text"],
                    consistency_level="Strong",
                )
                new_count = len(new_result) if new_result else 0
                logger.info(
                    f"[DOWNSTREAM_CHECK] New records in downstream: {new_count} found (expected: 25)"
                )
                if new_result and len(new_result) > 0:
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Sample new record: {new_result[0]}"
                    )

                # Log detailed results
                success = total_count >= 75 and updated_count >= 25 and new_count >= 25
                logger.info(
                    f"[DOWNSTREAM_CHECK] Sync check result: total={total_count}>=75: {total_count >= 75}, updated={updated_count}>=25: {updated_count >= 25}, new={new_count}>=25: {new_count >= 25}, overall: {success}"
                )

                # Verify total count, updated records, and new records
                return success
            except Exception as e:
                logger.warning(f"[DOWNSTREAM_CHECK] Upsert sync check failed: {e}")
                return False

        assert self.wait_for_sync(
            check_upsert, sync_timeout, f"upsert data to {collection_name}"
        )

    def test_insert_comprehensive_data_types(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test INSERT operation sync with comprehensive data types."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_insert_comprehensive")

        # Log test start
        self.log_test_start(
            "test_insert_comprehensive_data_types",
            "INSERT_COMPREHENSIVE",
            collection_name,
        )

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create collection with comprehensive schema
            self.log_operation(
                "CREATE_COLLECTION", "collection", collection_name, "upstream"
            )
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_comprehensive_schema(upstream_client),
            )

            # Create indexes for vector fields (max 4 vector fields)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="float16_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="binary_vector", index_type="BIN_FLAT", metric_type="HAMMING"
            )
            index_params.add_index(
                field_name="sparse_vector",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
            )
            upstream_client.create_index(
                collection_name=collection_name, index_params=index_params
            )
            upstream_client.load_collection(collection_name)

            # Wait for creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Generate and insert comprehensive test data
            test_data = self.generate_comprehensive_test_data(50)
            logger.info(
                f"[GENERATED] Generated comprehensive test data: {len(test_data)} records"
            )

            self.log_data_operation(
                "INSERT",
                collection_name,
                len(test_data),
                "- starting comprehensive data insertion",
            )

            result = upstream_client.insert(collection_name, test_data)
            inserted_count = result.get("insert_count", len(test_data))

            self.log_data_operation(
                "INSERT",
                collection_name,
                inserted_count,
                "- comprehensive insertion completed upstream",
            )

            # Flush to ensure data is persisted
            logger.info(f"[FLUSH] Flushing collection {collection_name} in upstream")
            upstream_client.flush(collection_name)

            # Log sync verification start
            self.log_sync_verification(
                "INSERT",
                collection_name,
                f"{inserted_count} comprehensive records in downstream",
            )

            # Wait for data sync by querying actual data
            def check_data():
                try:
                    # Query data to verify insertion
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",  # Get all records
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0

                    if count >= inserted_count:
                        logger.info(
                            f"[SYNC_OK] Comprehensive data sync confirmed: {count} records found in downstream"
                        )
                    else:
                        logger.info(
                            f"[SYNC_PROGRESS] Comprehensive data sync in progress: {count}/{inserted_count} records in downstream"
                        )

                    return count >= inserted_count
                except Exception as e:
                    logger.warning(f"Comprehensive data sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data,
                sync_timeout,
                f"insert comprehensive data to {collection_name}",
            )
            assert sync_success, (
                f"Comprehensive data insertion failed to sync to downstream for {collection_name}"
            )

            # Verify specific data types by querying some records
            try:
                sample_records = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["id", "bool_field", "varchar_field", "json_field"],
                    limit=5,
                )
                logger.info(
                    f"[VERIFICATION] Sample comprehensive records synced: {len(sample_records)} found"
                )
                if sample_records:
                    logger.info(f"[VERIFICATION] Sample record: {sample_records[0]}")
            except Exception as e:
                logger.warning(f"Failed to verify comprehensive data types: {e}")

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_insert_comprehensive_data_types", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_insert_comprehensive_data_types", False, duration)
            raise

    def test_upsert_comprehensive_data_types(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test UPSERT operation sync with comprehensive data types."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_upsert_comprehensive")

        # Log test start
        self.log_test_start(
            "test_upsert_comprehensive_data_types",
            "UPSERT_COMPREHENSIVE",
            collection_name,
        )

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create collection with comprehensive manual ID schema
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_comprehensive_manual_id_schema(upstream_client),
            )

            # Create indexes for vector fields (max 4 vector fields)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="float16_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="binary_vector", index_type="BIN_FLAT", metric_type="HAMMING"
            )
            index_params.add_index(
                field_name="sparse_vector",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
            )
            upstream_client.create_index(
                collection_name=collection_name, index_params=index_params
            )
            upstream_client.load_collection(collection_name)

            # Insert initial comprehensive data
            initial_data = self.generate_comprehensive_test_data_with_id(30, start_id=1)
            upstream_client.insert(collection_name, initial_data)
            upstream_client.flush(collection_name)

            # Wait for initial data sync
            def check_initial():
                try:
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0
                    return count >= 30
                except:
                    return False

            assert self.wait_for_sync(
                check_initial,
                sync_timeout,
                f"initial comprehensive data sync {collection_name}",
            )

            # Prepare comprehensive upsert data - update first 15 existing records + insert 15 new records
            upsert_data = []

            # Update existing records (IDs 1-15) with new comprehensive data
            update_data = self.generate_comprehensive_test_data_with_id(15, start_id=1)
            for record in update_data:
                record["varchar_field"] = f"updated_{record['varchar_field']}"
                record["json_field"]["status"] = "updated"
            upsert_data.extend(update_data)

            # Insert new records (IDs 31-45)
            new_data = self.generate_comprehensive_test_data_with_id(15, start_id=31)
            for record in new_data:
                record["varchar_field"] = f"new_{record['varchar_field']}"
                record["json_field"]["status"] = "new"
            upsert_data.extend(new_data)

            self.log_data_operation(
                "UPSERT",
                collection_name,
                len(upsert_data),
                "- starting comprehensive upsert",
            )

            upstream_client.upsert(collection_name, upsert_data)
            upstream_client.flush(collection_name)

            # Wait for upsert to sync by verifying updated data
            def check_upsert():
                try:
                    # Check total count (should be 45: 30 original + 15 new)
                    count_result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                        consistency_level="Strong",
                    )
                    total_count = count_result[0]["count(*)"] if count_result else 0
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Total comprehensive count in downstream: {total_count} (expected: 45)"
                    )

                    # Check if updated records exist with "updated_" prefix
                    updated_result = downstream_client.query(
                        collection_name=collection_name,
                        filter='varchar_field like "updated_%"',
                        output_fields=["id", "varchar_field"],
                        consistency_level="Strong",
                    )
                    updated_count = len(updated_result) if updated_result else 0
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Updated comprehensive records in downstream: {updated_count} found (expected: 15)"
                    )

                    # Check if new records exist with "new_" prefix
                    new_result = downstream_client.query(
                        collection_name=collection_name,
                        filter='varchar_field like "new_%"',
                        output_fields=["id", "varchar_field"],
                        consistency_level="Strong",
                    )
                    new_count = len(new_result) if new_result else 0
                    logger.info(
                        f"[DOWNSTREAM_CHECK] New comprehensive records in downstream: {new_count} found (expected: 15)"
                    )

                    # Verify total count, updated records, and new records
                    success = (
                        total_count >= 45 and updated_count >= 15 and new_count >= 15
                    )
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Comprehensive upsert check result: total={total_count}>=45: {total_count >= 45}, updated={updated_count}>=15: {updated_count >= 15}, new={new_count}>=15: {new_count >= 15}, overall: {success}"
                    )

                    return success
                except Exception as e:
                    logger.warning(
                        f"[DOWNSTREAM_CHECK] Comprehensive upsert sync check failed: {e}"
                    )
                    return False

            assert self.wait_for_sync(
                check_upsert,
                sync_timeout,
                f"upsert comprehensive data to {collection_name}",
            )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_upsert_comprehensive_data_types", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_upsert_comprehensive_data_types", False, duration)
            raise

    def test_insert_comprehensive_alt_data_types(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test INSERT operation sync with alternative comprehensive data types (BFLOAT16 + INT8)."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_insert_alt")

        # Log test start
        self.log_test_start(
            "test_insert_comprehensive_alt_data_types",
            "INSERT_ALT_COMPREHENSIVE",
            collection_name,
        )

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create collection with alternative comprehensive schema
            self.log_operation(
                "CREATE_COLLECTION", "collection", collection_name, "upstream"
            )
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_comprehensive_schema_alt(upstream_client),
            )

            # Create indexes for vector fields (alternative set) - use AUTOINDEX for compatibility
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="bfloat16_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="int8_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="sparse_vector",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
            )
            upstream_client.create_index(
                collection_name=collection_name, index_params=index_params
            )
            upstream_client.load_collection(collection_name)

            # Wait for creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Generate and insert alternative comprehensive test data
            test_data = self.generate_comprehensive_test_data_alt(50)
            logger.info(
                f"[GENERATED] Generated alternative comprehensive test data: {len(test_data)} records"
            )

            self.log_data_operation(
                "INSERT",
                collection_name,
                len(test_data),
                "- starting alternative comprehensive data insertion",
            )

            result = upstream_client.insert(collection_name, test_data)
            inserted_count = result.get("insert_count", len(test_data))

            self.log_data_operation(
                "INSERT",
                collection_name,
                inserted_count,
                "- alternative comprehensive insertion completed upstream",
            )

            # Flush to ensure data is persisted
            logger.info(f"[FLUSH] Flushing collection {collection_name} in upstream")
            upstream_client.flush(collection_name)

            # Log sync verification start
            self.log_sync_verification(
                "INSERT",
                collection_name,
                f"{inserted_count} alternative comprehensive records in downstream",
            )

            # Wait for data sync by querying actual data
            def check_data():
                try:
                    # Query data to verify insertion
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",  # Get all records
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0

                    if count >= inserted_count:
                        logger.info(
                            f"[SYNC_OK] Alternative comprehensive data sync confirmed: {count} records found in downstream"
                        )
                    else:
                        logger.info(
                            f"[SYNC_PROGRESS] Alternative comprehensive data sync in progress: {count}/{inserted_count} records in downstream"
                        )

                    return count >= inserted_count
                except Exception as e:
                    logger.warning(
                        f"Alternative comprehensive data sync check failed: {e}"
                    )
                    return False

            sync_success = self.wait_for_sync(
                check_data,
                sync_timeout,
                f"insert alternative comprehensive data to {collection_name}",
            )
            assert sync_success, (
                f"Alternative comprehensive data insertion failed to sync to downstream for {collection_name}"
            )

            # Verify specific alternative data types by querying some records
            try:
                sample_records = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["id", "bool_field", "varchar_field", "json_field"],
                    limit=5,
                )
                logger.info(
                    f"[VERIFICATION] Sample alternative comprehensive records synced: {len(sample_records)} found"
                )
                if sample_records:
                    logger.info(f"[VERIFICATION] Sample record: {sample_records[0]}")
            except Exception as e:
                logger.warning(
                    f"Failed to verify alternative comprehensive data types: {e}"
                )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end(
                "test_insert_comprehensive_alt_data_types", True, duration
            )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end(
                "test_insert_comprehensive_alt_data_types", False, duration
            )
            raise

    def test_upsert_comprehensive_alt_data_types(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test UPSERT operation sync with alternative comprehensive data types (BFLOAT16 + INT8)."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_upsert_alt")

        # Log test start
        self.log_test_start(
            "test_upsert_comprehensive_alt_data_types",
            "UPSERT_ALT_COMPREHENSIVE",
            collection_name,
        )

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create collection with alternative comprehensive manual ID schema
            upstream_client.create_collection(
                collection_name=collection_name,
                schema=self.create_comprehensive_manual_id_schema_alt(upstream_client),
            )

            # Create indexes for vector fields (alternative set) - use AUTOINDEX for compatibility
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="float_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="bfloat16_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="int8_vector", index_type="AUTOINDEX", metric_type="L2"
            )
            index_params.add_index(
                field_name="sparse_vector",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
            )
            upstream_client.create_index(
                collection_name=collection_name, index_params=index_params
            )
            upstream_client.load_collection(collection_name)

            # Insert initial alternative comprehensive data
            initial_data = self.generate_comprehensive_test_data_alt_with_id(
                30, start_id=1
            )
            upstream_client.insert(collection_name, initial_data)
            upstream_client.flush(collection_name)

            # Wait for initial data sync
            def check_initial():
                try:
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0
                    return count >= 30
                except:
                    return False

            assert self.wait_for_sync(
                check_initial,
                sync_timeout,
                f"initial alternative comprehensive data sync {collection_name}",
            )

            # Prepare alternative comprehensive upsert data - update first 15 existing records + insert 15 new records
            upsert_data = []

            # Update existing records (IDs 1-15) with new alternative comprehensive data
            update_data = self.generate_comprehensive_test_data_alt_with_id(
                15, start_id=1
            )
            for record in update_data:
                record["varchar_field"] = f"updated_{record['varchar_field']}"
                record["json_field"]["status"] = "updated"
            upsert_data.extend(update_data)

            # Insert new records (IDs 31-45)
            new_data = self.generate_comprehensive_test_data_alt_with_id(
                15, start_id=31
            )
            for record in new_data:
                record["varchar_field"] = f"new_{record['varchar_field']}"
                record["json_field"]["status"] = "new"
            upsert_data.extend(new_data)

            self.log_data_operation(
                "UPSERT",
                collection_name,
                len(upsert_data),
                "- starting alternative comprehensive upsert",
            )

            upstream_client.upsert(collection_name, upsert_data)
            upstream_client.flush(collection_name)

            # Wait for upsert to sync by verifying updated data
            def check_upsert():
                try:
                    # Check total count (should be 45: 30 original + 15 new)
                    count_result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                        consistency_level="Strong",
                    )
                    total_count = count_result[0]["count(*)"] if count_result else 0
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Total alternative comprehensive count in downstream: {total_count} (expected: 45)"
                    )

                    # Check if updated records exist with "updated_" prefix
                    updated_result = downstream_client.query(
                        collection_name=collection_name,
                        filter='varchar_field like "updated_%"',
                        output_fields=["id", "varchar_field"],
                        consistency_level="Strong",
                    )
                    updated_count = len(updated_result) if updated_result else 0
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Updated alternative comprehensive records in downstream: {updated_count} found (expected: 15)"
                    )

                    # Check if new records exist with "new_" prefix
                    new_result = downstream_client.query(
                        collection_name=collection_name,
                        filter='varchar_field like "new_%"',
                        output_fields=["id", "varchar_field"],
                        consistency_level="Strong",
                    )
                    new_count = len(new_result) if new_result else 0
                    logger.info(
                        f"[DOWNSTREAM_CHECK] New alternative comprehensive records in downstream: {new_count} found (expected: 15)"
                    )

                    # Verify total count, updated records, and new records
                    success = (
                        total_count >= 45 and updated_count >= 15 and new_count >= 15
                    )
                    logger.info(
                        f"[DOWNSTREAM_CHECK] Alternative comprehensive upsert check result: total={total_count}>=45: {total_count >= 45}, updated={updated_count}>=15: {updated_count >= 15}, new={new_count}>=15: {new_count >= 15}, overall: {success}"
                    )

                    return success
                except Exception as e:
                    logger.warning(
                        f"[DOWNSTREAM_CHECK] Alternative comprehensive upsert sync check failed: {e}"
                    )
                    return False

            assert self.wait_for_sync(
                check_upsert,
                sync_timeout,
                f"upsert alternative comprehensive data to {collection_name}",
            )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end(
                "test_upsert_comprehensive_alt_data_types", True, duration
            )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end(
                "test_upsert_comprehensive_alt_data_types", False, duration
            )
            raise

    def test_insert_auto_id_consistency(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test INSERT operation with auto_id to verify upstream and downstream ID consistency."""
        start_time = time.time()
        collection_name = self.gen_unique_name("test_col_auto_id")

        # Log test start
        self.log_test_start(
            "test_insert_auto_id_consistency", "INSERT_AUTO_ID", collection_name
        )

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create collection with auto_id schema
            self.log_operation(
                "CREATE_COLLECTION", "collection", collection_name, "upstream"
            )
            schema = upstream_client.create_schema(enable_dynamic_field=True)
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=128)

            upstream_client.create_collection(
                collection_name=collection_name, schema=schema
            )

            # Create index and load collection for querying
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector", index_type="AUTOINDEX", metric_type="L2"
            )
            upstream_client.create_index(
                collection_name=collection_name, index_params=index_params
            )
            upstream_client.load_collection(collection_name)

            # Wait for creation to sync
            def check_create():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {collection_name}"
            )

            # Generate and insert data (without id field, as it will be auto-generated)
            test_data = [
                {
                    "vector": [random.random() for _ in range(128)],
                    "text": f"test_text_{i}",
                    "number": i,
                    "metadata": {"type": "test", "value": i},
                }
                for i in range(100)
            ]
            logger.info(
                f"[GENERATED] Generated test data: {len(test_data)} records (without id)"
            )

            self.log_data_operation(
                "INSERT",
                collection_name,
                len(test_data),
                "- starting data insertion with auto_id",
            )

            result = upstream_client.insert(collection_name, test_data)
            inserted_count = result.get("insert_count", len(test_data))

            self.log_data_operation(
                "INSERT",
                collection_name,
                inserted_count,
                "- insertion completed upstream",
            )

            # Flush to ensure data is persisted
            logger.info(f"[FLUSH] Flushing collection {collection_name} in upstream")
            upstream_client.flush(collection_name)

            # Query upstream to get auto-generated IDs
            logger.info("[QUERY] Querying upstream to get auto-generated IDs")
            upstream_records = upstream_client.query(
                collection_name=collection_name,
                filter="",
                output_fields=["id", "text", "number"],
                limit=10000,
            )

            upstream_ids = sorted([record["id"] for record in upstream_records])
            logger.info(
                f"[UPSTREAM] Retrieved {len(upstream_ids)} records from upstream"
            )
            logger.info(
                f"[UPSTREAM] ID range: {min(upstream_ids)} to {max(upstream_ids)}"
            )

            # Log sync verification start
            self.log_sync_verification(
                "INSERT_AUTO_ID",
                collection_name,
                f"{inserted_count} records with matching IDs in downstream",
            )

            # Wait for data sync by querying actual data
            def check_data():
                try:
                    # Query data to verify insertion
                    result = downstream_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    count = result[0]["count(*)"] if result else 0

                    if count >= inserted_count:
                        logger.info(
                            f"[SYNC_OK] Data sync confirmed: {count} records found in downstream"
                        )
                    else:
                        logger.info(
                            f"[SYNC_PROGRESS] Data sync in progress: {count}/{inserted_count} records in downstream"
                        )

                    return count >= inserted_count
                except Exception as e:
                    logger.warning(f"Data sync check failed: {e}")
                    return False

            sync_success = self.wait_for_sync(
                check_data, sync_timeout, f"insert data to {collection_name}"
            )
            assert sync_success, (
                f"Data insertion failed to sync to downstream for {collection_name}"
            )

            # Verify ID consistency between upstream and downstream
            logger.info(
                "[VERIFICATION] Verifying ID consistency between upstream and downstream"
            )

            downstream_records = downstream_client.query(
                collection_name=collection_name,
                filter="",
                output_fields=["id", "text", "number"],
                limit=10000,
            )

            downstream_ids = sorted([record["id"] for record in downstream_records])
            logger.info(
                f"[DOWNSTREAM] Retrieved {len(downstream_ids)} records from downstream"
            )
            logger.info(
                f"[DOWNSTREAM] ID range: {min(downstream_ids)} to {max(downstream_ids)}"
            )

            # Compare IDs
            assert len(upstream_ids) == len(downstream_ids), (
                f"ID count mismatch: upstream={len(upstream_ids)}, downstream={len(downstream_ids)}"
            )

            assert upstream_ids == downstream_ids, (
                "ID mismatch detected between upstream and downstream"
            )

            logger.info(
                f"[SUCCESS] ID consistency verified: {len(upstream_ids)} IDs match between upstream and downstream"
            )

            # Verify some specific records to ensure data integrity
            upstream_records_dict = {rec["id"]: rec for rec in upstream_records}
            downstream_records_dict = {rec["id"]: rec for rec in downstream_records}

            sample_ids = random.sample(upstream_ids, min(10, len(upstream_ids)))
            mismatches = []

            for sample_id in sample_ids:
                upstream_rec = upstream_records_dict[sample_id]
                downstream_rec = downstream_records_dict[sample_id]

                if (
                    upstream_rec["text"] != downstream_rec["text"]
                    or upstream_rec["number"] != downstream_rec["number"]
                ):
                    mismatches.append(sample_id)
                    logger.warning(
                        f"[MISMATCH] Data mismatch for ID {sample_id}: "
                        f"upstream={upstream_rec}, downstream={downstream_rec}"
                    )

            assert len(mismatches) == 0, (
                f"Data mismatch detected for {len(mismatches)} records with IDs: {mismatches}"
            )

            logger.info(
                f"[VERIFICATION] Data integrity verified for {len(sample_ids)} sample records"
            )

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_insert_auto_id_consistency", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_insert_auto_id_consistency", False, duration)
            raise
