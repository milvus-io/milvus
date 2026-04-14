"""
CDC sync tests for collection property operations.
"""

import time
from .base import TestCDCSyncBase, logger


class TestCDCSyncCollectionProperties(TestCDCSyncBase):
    """Test CDC sync for collection property (alter/drop) operations."""

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

    def _create_basic_collection(self, client, c_name):
        """Create a default schema collection, insert 100 rows, and create HNSW index."""
        schema = self.create_default_schema(client)
        client.create_collection(
            collection_name=c_name,
            schema=schema,
            consistency_level="Strong",
        )

        # Insert 100 rows
        test_data = self.generate_test_data(100)
        client.insert(c_name, test_data)

        # Create HNSW index on vector field
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="L2",
            params={"M": 8, "efConstruction": 64},
        )
        client.create_index(c_name, index_params)

    def test_ttl_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test ALTER_COLLECTION_PROPERTIES (TTL) sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        c_name = self.gen_unique_name("test_col_ttl")
        self.resources_to_cleanup.append(("collection", c_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, c_name)

        # Create collection
        self._create_basic_collection(upstream_client, c_name)

        # Wait for collection to sync downstream
        def check_create():
            return downstream_client.has_collection(c_name)

        assert self.wait_for_sync(check_create, sync_timeout, f"create collection {c_name}")

        # Alter collection TTL property
        upstream_client.alter_collection_properties(
            collection_name=c_name,
            properties={"collection.ttl.seconds": "3600"},
        )

        # Wait for property to sync downstream
        def check_ttl():
            try:
                desc = downstream_client.describe_collection(c_name)
                props = desc.get("properties", {})
                logger.info(f"Downstream collection properties: {props}")
                return str(props.get("collection.ttl.seconds", "")) == "3600"
            except Exception as e:
                logger.warning(f"Check TTL sync failed: {e}")
                return False

        assert self.wait_for_sync(check_ttl, sync_timeout, f"TTL property sync for {c_name}")

    def test_mmap_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test ALTER_COLLECTION_PROPERTIES (mmap.enabled) sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        c_name = self.gen_unique_name("test_col_mmap")
        self.resources_to_cleanup.append(("collection", c_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, c_name)

        # Create collection
        self._create_basic_collection(upstream_client, c_name)

        # Wait for collection to sync downstream
        def check_create():
            return downstream_client.has_collection(c_name)

        assert self.wait_for_sync(check_create, sync_timeout, f"create collection {c_name}")

        # Alter mmap.enabled property
        upstream_client.alter_collection_properties(
            collection_name=c_name,
            properties={"mmap.enabled": "true"},
        )

        # Wait for property to sync downstream
        def check_mmap():
            try:
                desc = downstream_client.describe_collection(c_name)
                props = desc.get("properties", {})
                logger.info(f"Downstream collection properties: {props}")
                return str(props.get("mmap.enabled", "")).lower() == "true"
            except Exception as e:
                logger.warning(f"Check mmap sync failed: {e}")
                return False

        assert self.wait_for_sync(check_mmap, sync_timeout, f"mmap property sync for {c_name}")

    def test_autocompaction_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test ALTER_COLLECTION_PROPERTIES (autocompaction.enabled) sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        c_name = self.gen_unique_name("test_col_autocomp")
        self.resources_to_cleanup.append(("collection", c_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, c_name)

        # Create collection
        self._create_basic_collection(upstream_client, c_name)

        # Wait for collection to sync downstream
        def check_create():
            return downstream_client.has_collection(c_name)

        assert self.wait_for_sync(check_create, sync_timeout, f"create collection {c_name}")

        # Alter autocompaction property
        upstream_client.alter_collection_properties(
            collection_name=c_name,
            properties={"collection.autocompaction.enabled": "true"},
        )

        # Wait for property to sync downstream
        def check_autocomp():
            try:
                desc = downstream_client.describe_collection(c_name)
                props = desc.get("properties", {})
                logger.info(f"Downstream collection properties: {props}")
                return str(props.get("collection.autocompaction.enabled", "")).lower() == "true"
            except Exception as e:
                logger.warning(f"Check autocompaction sync failed: {e}")
                return False

        assert self.wait_for_sync(
            check_autocomp, sync_timeout, f"autocompaction property sync for {c_name}"
        )

    def test_alter_multiple_properties(self, upstream_client, downstream_client, sync_timeout):
        """Test ALTER_COLLECTION_PROPERTIES with multiple properties at once sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        c_name = self.gen_unique_name("test_col_multi_props")
        self.resources_to_cleanup.append(("collection", c_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, c_name)

        # Create collection
        self._create_basic_collection(upstream_client, c_name)

        # Wait for collection to sync downstream
        def check_create():
            return downstream_client.has_collection(c_name)

        assert self.wait_for_sync(check_create, sync_timeout, f"create collection {c_name}")

        # Alter all 3 properties at once
        upstream_client.alter_collection_properties(
            collection_name=c_name,
            properties={
                "collection.ttl.seconds": "3600",
                "mmap.enabled": "true",
                "collection.autocompaction.enabled": "true",
            },
        )

        # Wait for all 3 properties to sync downstream
        def check_all_props():
            try:
                desc = downstream_client.describe_collection(c_name)
                props = desc.get("properties", {})
                logger.info(f"Downstream collection properties: {props}")
                ttl_ok = str(props.get("collection.ttl.seconds", "")) == "3600"
                mmap_ok = str(props.get("mmap.enabled", "")).lower() == "true"
                autocomp_ok = str(props.get("collection.autocompaction.enabled", "")).lower() == "true"
                return ttl_ok and mmap_ok and autocomp_ok
            except Exception as e:
                logger.warning(f"Check all properties sync failed: {e}")
                return False

        assert self.wait_for_sync(
            check_all_props, sync_timeout, f"all properties sync for {c_name}"
        )

    def test_drop_properties_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_COLLECTION_PROPERTIES — set TTL + mmap, drop TTL, verify TTL gone and mmap remains."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        c_name = self.gen_unique_name("test_col_drop_props")
        self.resources_to_cleanup.append(("collection", c_name))

        # Initial cleanup
        self.cleanup_collection(upstream_client, c_name)

        # Create collection
        self._create_basic_collection(upstream_client, c_name)

        # Wait for collection to sync downstream
        def check_create():
            return downstream_client.has_collection(c_name)

        assert self.wait_for_sync(check_create, sync_timeout, f"create collection {c_name}")

        # Set TTL and mmap properties
        upstream_client.alter_collection_properties(
            collection_name=c_name,
            properties={
                "collection.ttl.seconds": "3600",
                "mmap.enabled": "true",
            },
        )

        # Wait for both properties to sync downstream
        def check_props_set():
            try:
                desc = downstream_client.describe_collection(c_name)
                props = desc.get("properties", {})
                ttl_set = str(props.get("collection.ttl.seconds", "")) == "3600"
                mmap_set = str(props.get("mmap.enabled", "")).lower() == "true"
                return ttl_set and mmap_set
            except Exception as e:
                logger.warning(f"Check properties set failed: {e}")
                return False

        assert self.wait_for_sync(check_props_set, sync_timeout, f"set properties for {c_name}")

        # Drop TTL property
        upstream_client.drop_collection_properties(
            collection_name=c_name,
            property_keys=["collection.ttl.seconds"],
        )

        # Wait for TTL to be gone but mmap to remain on downstream
        def check_drop_ttl():
            try:
                desc = downstream_client.describe_collection(c_name)
                props = desc.get("properties", {})
                logger.info(f"Downstream collection properties after drop: {props}")
                ttl_gone = "collection.ttl.seconds" not in props
                mmap_remains = str(props.get("mmap.enabled", "")).lower() == "true"
                return ttl_gone and mmap_remains
            except Exception as e:
                logger.warning(f"Check drop TTL sync failed: {e}")
                return False

        assert self.wait_for_sync(
            check_drop_ttl, sync_timeout, f"drop TTL property sync for {c_name}"
        )
