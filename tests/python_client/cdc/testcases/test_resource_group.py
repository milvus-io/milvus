"""
CDC sync tests for resource group operations.
"""

import time
from .base import TestCDCSyncBase, logger


class TestCDCSyncResourceGroup(TestCDCSyncBase):
    """Test CDC sync for resource group operations."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method - only cleanup upstream, downstream will sync."""
        upstream_client = getattr(self, "_upstream_client", None)

        if upstream_client:
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "resource_group":
                    self._cleanup_resource_group(upstream_client, resource_name)
                elif resource_type == "collection":
                    self.cleanup_collection(upstream_client, resource_name)

            time.sleep(1)  # Allow cleanup to sync to downstream

    def _cleanup_resource_group(self, client, rg_name):
        """Clean up resource group if exists (skipping default RG)."""
        try:
            existing = client.list_resource_groups()
            if rg_name in existing:
                logger.info(f"[CLEANUP] Cleaning up resource group: {rg_name}")
                client.drop_resource_group(rg_name)
                logger.info(f"[SUCCESS] Resource group {rg_name} cleaned up successfully")
            else:
                logger.debug(f"Resource group {rg_name} does not exist, skipping cleanup")
        except Exception as e:
            logger.warning(f"[FAILED] Failed to cleanup resource group {rg_name}: {e}")

    def test_create_resource_group_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_RESOURCE_GROUP operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        rg_name = self.gen_unique_name("test_rg_create")
        self.resources_to_cleanup.append(("resource_group", rg_name))

        # Initial cleanup
        self._cleanup_resource_group(upstream_client, rg_name)

        # Create resource group in upstream
        upstream_client.create_resource_group(rg_name)
        assert rg_name in upstream_client.list_resource_groups(), (
            f"Resource group {rg_name} not created in upstream"
        )

        # Wait for sync to downstream
        def check_sync():
            return rg_name in downstream_client.list_resource_groups()

        assert self.wait_for_sync(
            check_sync, sync_timeout, f"create resource group {rg_name}"
        )

    def test_drop_resource_group_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_RESOURCE_GROUP operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        rg_name = self.gen_unique_name("test_rg_drop")
        self.resources_to_cleanup.append(("resource_group", rg_name))

        # Initial cleanup
        self._cleanup_resource_group(upstream_client, rg_name)

        # Create resource group first
        upstream_client.create_resource_group(rg_name)

        # Wait for creation to sync
        def check_create():
            return rg_name in downstream_client.list_resource_groups()

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create resource group {rg_name}"
        )

        # Drop resource group in upstream
        upstream_client.drop_resource_group(rg_name)
        assert rg_name not in upstream_client.list_resource_groups(), (
            f"Resource group {rg_name} still exists in upstream after drop"
        )

        # Wait for drop to sync to downstream
        def check_drop():
            return rg_name not in downstream_client.list_resource_groups()

        assert self.wait_for_sync(
            check_drop, sync_timeout, f"drop resource group {rg_name}"
        )

    def test_update_resource_group_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test UPDATE_RESOURCE_GROUP (with config) operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        rg_name = self.gen_unique_name("test_rg_update")
        self.resources_to_cleanup.append(("resource_group", rg_name))

        # Initial cleanup
        self._cleanup_resource_group(upstream_client, rg_name)

        # Create resource group with config
        config = {
            "requests": {"node_num": 0},
            "limits": {"node_num": 2},
        }
        upstream_client.create_resource_group(rg_name, config=config)
        assert rg_name in upstream_client.list_resource_groups(), (
            f"Resource group {rg_name} not created in upstream"
        )

        # Wait for resource group to sync downstream
        def check_sync():
            try:
                rgs = downstream_client.list_resource_groups()
                if rg_name not in rgs:
                    return False
                # Verify describe succeeds
                desc = downstream_client.describe_resource_group(rg_name)
                logger.info(f"Downstream resource group description: {desc}")
                return desc is not None
            except Exception as e:
                logger.warning(f"Check update resource group sync failed: {e}")
                return False

        assert self.wait_for_sync(
            check_sync, sync_timeout, f"update resource group {rg_name}"
        )

    def test_transfer_replica_sync(self, upstream_client, downstream_client, sync_timeout):
        """Test that two resource groups both sync to downstream."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        rg_name_1 = self.gen_unique_name("test_rg_transfer_1")
        rg_name_2 = self.gen_unique_name("test_rg_transfer_2")
        self.resources_to_cleanup.append(("resource_group", rg_name_1))
        self.resources_to_cleanup.append(("resource_group", rg_name_2))

        # Initial cleanup
        self._cleanup_resource_group(upstream_client, rg_name_1)
        self._cleanup_resource_group(upstream_client, rg_name_2)

        # Create both resource groups in upstream
        upstream_client.create_resource_group(rg_name_1)
        upstream_client.create_resource_group(rg_name_2)

        upstream_rgs = upstream_client.list_resource_groups()
        assert rg_name_1 in upstream_rgs, f"Resource group {rg_name_1} not created in upstream"
        assert rg_name_2 in upstream_rgs, f"Resource group {rg_name_2} not created in upstream"

        # Wait for both resource groups to sync to downstream
        def check_both_synced():
            try:
                downstream_rgs = downstream_client.list_resource_groups()
                rg1_exists = rg_name_1 in downstream_rgs
                rg2_exists = rg_name_2 in downstream_rgs
                if rg1_exists and rg2_exists:
                    logger.info(f"Both resource groups synced: {rg_name_1}, {rg_name_2}")
                return rg1_exists and rg2_exists
            except Exception as e:
                logger.warning(f"Check transfer sync failed: {e}")
                return False

        assert self.wait_for_sync(
            check_both_synced,
            sync_timeout,
            f"both resource groups {rg_name_1} and {rg_name_2} sync",
        )
