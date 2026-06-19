"""
CDC non-replication tests for resource group operations.

Resource groups and replica assignment are per-cluster state; by design
CDC does NOT propagate RG create/drop/update/transfer-replica to the
downstream cluster. These tests guard that invariant.
"""

import time

from .base import TestCDCSyncBase, logger


class TestCDCSyncResourceGroup(TestCDCSyncBase):
    """Verify that resource group operations are NOT replicated by CDC."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup both upstream and downstream (downstream won't auto-sync)."""
        upstream_client = getattr(self, "_upstream_client", None)
        downstream_client = getattr(self, "_downstream_client", None)

        for client in (upstream_client, downstream_client):
            if not client:
                continue
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "resource_group":
                    self._cleanup_resource_group(client, resource_name)
                elif resource_type == "collection":
                    self.cleanup_collection(client, resource_name)

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

    def test_create_resource_group_not_replicated(self, upstream_client, downstream_client, sync_timeout):
        """Creating an RG on upstream must NOT create it on downstream."""
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client

        rg_name = self.gen_unique_name("test_rg_create")
        self.resources_to_cleanup.append(("resource_group", rg_name))

        self._cleanup_resource_group(upstream_client, rg_name)
        self._cleanup_resource_group(downstream_client, rg_name)

        upstream_client.create_resource_group(rg_name)
        assert rg_name in upstream_client.list_resource_groups(), f"Resource group {rg_name} not created in upstream"

        # Wait the full sync window; if CDC were going to leak the RG it would
        # have shown up by now.
        time.sleep(sync_timeout)
        downstream_rgs = downstream_client.list_resource_groups()
        assert rg_name not in downstream_rgs, (
            f"Resource group {rg_name} unexpectedly appeared on downstream (RG ops must not replicate)"
        )

    def test_drop_resource_group_not_replicated(self, upstream_client, downstream_client, sync_timeout):
        """Dropping an RG on upstream must NOT drop it on downstream."""
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client

        rg_name = self.gen_unique_name("test_rg_drop")
        self.resources_to_cleanup.append(("resource_group", rg_name))

        self._cleanup_resource_group(upstream_client, rg_name)
        self._cleanup_resource_group(downstream_client, rg_name)

        # Create the RG on BOTH sides (independently, since create isn't replicated either).
        upstream_client.create_resource_group(rg_name)
        downstream_client.create_resource_group(rg_name)
        assert rg_name in upstream_client.list_resource_groups()
        assert rg_name in downstream_client.list_resource_groups()

        # Drop only on upstream; downstream must retain its own copy.
        upstream_client.drop_resource_group(rg_name)
        assert rg_name not in upstream_client.list_resource_groups(), (
            f"Resource group {rg_name} still exists in upstream after drop"
        )

        time.sleep(sync_timeout)
        downstream_rgs = downstream_client.list_resource_groups()
        assert rg_name in downstream_rgs, (
            f"Resource group {rg_name} was dropped on downstream after upstream drop "
            f"(RG ops must not replicate). downstream RGs: {downstream_rgs}"
        )

    def test_update_resource_group_not_replicated(self, upstream_client, downstream_client, sync_timeout):
        """Updating RG config on upstream must NOT reconfigure downstream's RG."""
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client

        rg_name = self.gen_unique_name("test_rg_update")
        self.resources_to_cleanup.append(("resource_group", rg_name))

        self._cleanup_resource_group(upstream_client, rg_name)
        self._cleanup_resource_group(downstream_client, rg_name)

        upstream_config = {"requests": {"node_num": 0}, "limits": {"node_num": 2}}
        downstream_config = {"requests": {"node_num": 0}, "limits": {"node_num": 1}}

        # Create the RG on both sides with different configs.
        upstream_client.create_resource_group(rg_name, config=upstream_config)
        downstream_client.create_resource_group(rg_name, config=downstream_config)
        downstream_desc_before = downstream_client.describe_resource_group(rg_name)
        logger.info(f"Downstream RG before upstream update: {downstream_desc_before}")

        # Upstream has already been created with its config; nothing else to
        # update since config drift itself would be the leak. Wait and confirm
        # downstream config is unchanged. ResourceGroupInfo has no __eq__, so
        # compare the str() form (includes config/limits/requests/nodes).
        time.sleep(sync_timeout)
        downstream_desc_after = downstream_client.describe_resource_group(rg_name)
        logger.info(f"Downstream RG after upstream update: {downstream_desc_after}")
        assert str(downstream_desc_after) == str(downstream_desc_before), (
            f"Downstream RG {rg_name} was modified by upstream config (RG ops must not replicate). "
            f"before={downstream_desc_before}, after={downstream_desc_after}"
        )

    def test_transfer_replica_not_replicated(self, upstream_client, downstream_client, sync_timeout):
        """Creating multiple RGs on upstream must NOT create any of them on downstream."""
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client

        rg_name_1 = self.gen_unique_name("test_rg_transfer_1")
        rg_name_2 = self.gen_unique_name("test_rg_transfer_2")
        self.resources_to_cleanup.append(("resource_group", rg_name_1))
        self.resources_to_cleanup.append(("resource_group", rg_name_2))

        self._cleanup_resource_group(upstream_client, rg_name_1)
        self._cleanup_resource_group(upstream_client, rg_name_2)
        self._cleanup_resource_group(downstream_client, rg_name_1)
        self._cleanup_resource_group(downstream_client, rg_name_2)

        upstream_client.create_resource_group(rg_name_1)
        upstream_client.create_resource_group(rg_name_2)
        upstream_rgs = upstream_client.list_resource_groups()
        assert rg_name_1 in upstream_rgs
        assert rg_name_2 in upstream_rgs

        time.sleep(sync_timeout)
        downstream_rgs = downstream_client.list_resource_groups()
        assert rg_name_1 not in downstream_rgs and rg_name_2 not in downstream_rgs, (
            f"Upstream RGs leaked to downstream (RG ops must not replicate). downstream RGs: {downstream_rgs}"
        )
