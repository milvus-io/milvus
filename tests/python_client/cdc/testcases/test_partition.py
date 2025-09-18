"""
CDC sync tests for partition operations.
"""

import time
from .base import TestCDCSyncBase


class TestCDCSyncPartition(TestCDCSyncBase):
    """Test CDC sync for partition operations."""

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

    def test_create_partition(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_PARTITION operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_part_create")
        partition_name = self.gen_unique_name("test_part_create")
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

        # Create partition
        upstream_client.create_partition(collection_name, partition_name)

        # Verify partition exists in upstream
        upstream_partitions = upstream_client.list_partitions(collection_name)
        assert partition_name in upstream_partitions

        # Wait for partition sync to downstream
        def check_partition():
            try:
                downstream_partitions = downstream_client.list_partitions(
                    collection_name
                )
                return partition_name in downstream_partitions
            except:
                return False

        assert self.wait_for_sync(
            check_partition, sync_timeout, f"create partition {partition_name}"
        )

    def test_drop_partition(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_PARTITION operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_part_drop")
        partition_name = self.gen_unique_name("test_part_drop")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection and partition
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )
        upstream_client.create_partition(collection_name, partition_name)

        # Wait for setup to sync
        def check_setup():
            try:
                return downstream_client.has_collection(
                    collection_name
                ) and partition_name in downstream_client.list_partitions(
                    collection_name
                )
            except:
                return False

        assert self.wait_for_sync(
            check_setup,
            sync_timeout,
            f"setup collection and partition {collection_name}",
        )

        # Drop partition
        upstream_client.drop_partition(collection_name, partition_name)

        # Verify partition is dropped in upstream
        upstream_partitions = upstream_client.list_partitions(collection_name)
        assert partition_name not in upstream_partitions

        # Wait for drop to sync to downstream
        def check_drop():
            try:
                downstream_partitions = downstream_client.list_partitions(
                    collection_name
                )
                return partition_name not in downstream_partitions
            except:
                return True  # If error, assume partition is dropped

        assert self.wait_for_sync(
            check_drop, sync_timeout, f"drop partition {partition_name}"
        )

    def test_load_partition(self, upstream_client, downstream_client, sync_timeout):
        """Test LOAD_PARTITION operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_part_load")
        partition_name = self.gen_unique_name("test_part_load")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection, partition, and index
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )
        upstream_client.create_partition(collection_name, partition_name)

        # Create index and load collection (required for querying/searching)
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)

        # Wait for setup to sync
        def check_setup():
            try:
                return downstream_client.has_collection(
                    collection_name
                ) and partition_name in downstream_client.list_partitions(
                    collection_name
                )
            except:
                return False

        assert self.wait_for_sync(
            check_setup,
            sync_timeout,
            f"setup collection and partition {collection_name}",
        )

        # Load partition
        upstream_client.load_partitions(collection_name, [partition_name])
        # check partition load state in upstream
        upstream_load_state = upstream_client.get_load_state(
            collection_name, partition_name
        )
        load_state = str(upstream_load_state["state"])
        print(f"DEBUG: partition load state in upstream: {load_state}")

        # Wait for load to sync
        def check_load():
            try:
                # Check partition load state
                load_state = downstream_client.get_load_state(
                    collection_name=collection_name, partition_name=partition_name
                )
                print(
                    f"DEBUG: partition load state in check_load: {load_state['state']}"
                )
                return "Loaded" == str(load_state["state"])
            except Exception as e:
                print(f"DEBUG: get_load_state exception in check_load: {e}")
                return False

        assert self.wait_for_sync(
            check_load, sync_timeout, f"load partition {partition_name}"
        )

    def test_release_partition(self, upstream_client, downstream_client, sync_timeout):
        """Test RELEASE_PARTITION operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_part_release")
        partition_name = self.gen_unique_name("test_part_release")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection, partition, index, and load
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )
        upstream_client.create_partition(collection_name, partition_name)

        # Create index and load collection (required for querying/searching)
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)
        upstream_client.load_collection(collection_name)
        upstream_client.load_partitions(collection_name, [partition_name])
        for p_name in [partition_name, None]:
            data = [{"vector": [0.1] * 128, "id": i} for i in range(100)]
            upstream_client.insert(collection_name, data, partition_name=p_name)

        # Wait for setup to sync
        def check_setup():
            try:
                query_vector = [[0.1] * 128]
                downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    partition_names=[partition_name],
                    output_fields=[],
                )
                return True
            except:
                return False

        assert self.wait_for_sync(
            check_setup, sync_timeout, f"setup and load partition {partition_name}"
        )

        # Release partition
        upstream_client.release_partitions(collection_name, [partition_name])

        # check partition load state in upstream
        upstream_load_state = upstream_client.get_load_state(
            collection_name, partition_name
        )
        load_state = str(upstream_load_state["state"])
        print(f"DEBUG: partition load state in upstream: {load_state}")

        # check partition load state in upstream
        def check_release():
            try:
                query_vector = [[0.1] * 128]
                res = upstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    partition_names=[partition_name],
                    output_fields=[],
                )
                print(
                    f"DEBUG: released partition {partition_name} can still be searched: {res}"
                )
                print(
                    f"DEBUG: released partition {partition_name} can still be searched"
                )
                return False
            except:
                print(f"DEBUG: released partition {partition_name} cannot be searched")
                return True

        assert self.wait_for_sync(
            check_release, sync_timeout, f"release partition {partition_name}"
        )

        # check partition load state in downstream
        def check_release():
            try:
                query_vector = [[0.1] * 128]
                downstream_client.search(
                    collection_name=collection_name,
                    data=query_vector,
                    limit=1,
                    partition_names=[partition_name],
                    output_fields=[],
                )
                print(
                    f"DEBUG: released partition {partition_name} can still be searched"
                )
                return False
            except:
                print(f"DEBUG: released partition {partition_name} cannot be searched")
                return True

        assert self.wait_for_sync(
            check_release, sync_timeout, f"release partition {partition_name}"
        )

    def test_partition_insert(self, upstream_client, downstream_client, sync_timeout):
        """Test INSERT operation to partition sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_part_insert")
        partition_name = self.gen_unique_name("test_part_insert")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection and partition
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )
        upstream_client.create_partition(collection_name, partition_name)

        # Create index and load collection (required for querying/searching)
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)
        upstream_client.load_collection(collection_name)

        # Wait for setup to sync
        def check_setup():
            try:
                return downstream_client.has_collection(
                    collection_name
                ) and partition_name in downstream_client.list_partitions(
                    collection_name
                )
            except:
                return False

        assert self.wait_for_sync(
            check_setup,
            sync_timeout,
            f"setup collection and partition {collection_name}",
        )

        # Insert data to specific partition
        test_data = self.generate_test_data(100)
        result = upstream_client.insert(
            collection_name, test_data, partition_name=partition_name
        )
        inserted_count = result.get("insert_count", len(test_data))

        # Flush to ensure data is persisted
        upstream_client.flush(collection_name)

        # Wait for data sync to downstream partition by querying
        def check_data():
            try:
                # Query data in specific partition
                result = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["count(*)"],
                    partition_names=[partition_name],
                )
                count = result[0]["count(*)"] if result else 0
                return count >= inserted_count
            except:
                return False

        assert self.wait_for_sync(
            check_data, sync_timeout, f"insert data to partition {partition_name}"
        )

    def test_partition_delete(self, upstream_client, downstream_client, sync_timeout):
        """Test DELETE operation from partition sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_part_delete")
        partition_name = self.gen_unique_name("test_part_delete")
        self.resources_to_cleanup.append(("collection", collection_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection and partition
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
            consistency_level="Strong",
        )
        upstream_client.create_partition(collection_name, partition_name)

        # Create index and load collection (required for querying/searching)
        index_params = upstream_client.prepare_index_params()
        index_params.add_index(
            field_name="vector", index_type="AUTOINDEX", metric_type="L2"
        )
        upstream_client.create_index(collection_name, index_params)
        upstream_client.load_collection(collection_name)

        # Wait for setup to sync
        def check_setup():
            try:
                return downstream_client.has_collection(
                    collection_name
                ) and partition_name in downstream_client.list_partitions(
                    collection_name
                )
            except:
                return False

        assert self.wait_for_sync(
            check_setup,
            sync_timeout,
            f"setup collection and partition {collection_name}",
        )

        # Insert data to partition
        test_data = self.generate_test_data(100)
        upstream_client.insert(
            collection_name, test_data, partition_name=partition_name
        )
        upstream_client.flush(collection_name)

        # Wait for initial data sync by querying partition
        def check_data():
            try:
                result = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["count(*)"],
                    partition_names=[partition_name],
                )
                count = result[0]["count(*)"] if result else 0
                return count >= 100
            except:
                return False

        assert self.wait_for_sync(
            check_data, sync_timeout, f"initial data sync to partition {partition_name}"
        )

        # Delete some data from partition
        delete_ids = list(range(20))  # Delete first 20 records
        upstream_client.delete(
            collection_name, filter=f"id in {delete_ids}", partition_name=partition_name
        )
        upstream_client.flush(collection_name)
        deleted_result = upstream_client.query(
            collection_name=collection_name,
            filter=f"id in {delete_ids}",
            output_fields=["id"],
            partition_names=[partition_name],
        )
        total_count = upstream_client.query(
            collection_name=collection_name,
            filter="",
            output_fields=["count(*)"],
            partition_names=[partition_name],
        )
        total_count = total_count[0]["count(*)"] if total_count else 0
        print(f"DEBUG: deleted_result in upstream: {deleted_result}")
        print(f"DEBUG: total_count in upstream: {total_count}")

        # Wait for delete to sync by querying partition
        def check_delete():
            try:
                # Query for the deleted records in partition - should return empty
                deleted_result = downstream_client.query(
                    collection_name=collection_name,
                    filter=f"id in {delete_ids}",
                    output_fields=["id"],
                    partition_names=[partition_name],
                )
                # Query total count in partition
                count_result = downstream_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["count(*)"],
                    partition_names=[partition_name],
                )
                deleted_count = len(deleted_result) if deleted_result else 0
                total_count = count_result[0]["count(*)"] if count_result else 0

                print(f"DEBUG: deleted_result in check_delete: {deleted_result}")
                print(f"DEBUG: count_result in check_delete: {count_result}")
                print(
                    f"DEBUG: deleted_count: {deleted_count}, total_count: {total_count}"
                )

                # Verify deleted records are gone and total count is correct in partition
                return deleted_count == 0 and total_count == 80
            except Exception as e:
                print(f"DEBUG: query exception in check_delete: {e}")
                return False

        assert self.wait_for_sync(
            check_delete, sync_timeout, f"delete data from partition {partition_name}"
        )
