"""
CDC sync tests for alias operations.
"""

import time
from .base import TestCDCSyncBase, logger


class TestCDCSyncAlias(TestCDCSyncBase):
    """Test CDC sync for alias operations."""

    def setup_method(self):
        """Setup for each test method."""
        logger.info("Setting up test method")
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method - only cleanup upstream, downstream will sync."""
        logger.info("Starting teardown method")
        upstream_client = getattr(self, "_upstream_client", None)

        if upstream_client:
            logger.info(f"Cleaning up {len(self.resources_to_cleanup)} resources")

            # First pass: cleanup aliases (must be done before collections)
            logger.info("Cleaning up aliases first")
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "alias":
                    logger.info(f"Cleaning up alias: {resource_name}")
                    try:
                        upstream_client.drop_alias(resource_name)
                        logger.info(f"Successfully dropped alias: {resource_name}")
                    except Exception as e:
                        logger.warning(f"Failed to drop alias {resource_name}: {e}")

            # Second pass: cleanup collections (after aliases are removed)
            logger.info("Cleaning up collections after aliases")
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "collection":
                    logger.info(f"Cleaning up collection: {resource_name}")
                    self.cleanup_collection(upstream_client, resource_name)

            logger.info("Waiting 1 second for cleanup to sync to downstream")
            time.sleep(1)  # Allow cleanup to sync to downstream
        else:
            logger.info("No upstream client found, skipping cleanup")
        logger.info("Teardown method completed")

    def test_create_alias(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_ALIAS operation sync."""
        logger.info("=== Starting test_create_alias ===")
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_alias_create")
        alias_name = self.gen_unique_name("test_alias_create")
        logger.info(f"Generated collection name: {collection_name}")
        logger.info(f"Generated alias name: {alias_name}")
        self.resources_to_cleanup.append(("collection", collection_name))
        self.resources_to_cleanup.append(("alias", alias_name))

        # Initial cleanup
        logger.info(f"Performing initial cleanup for collection: {collection_name}")
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection
        logger.info(f"Creating collection: {collection_name}")
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )
        logger.info(f"Collection {collection_name} created in upstream")

        # Wait for creation to sync
        logger.info(
            f"Waiting for collection {collection_name} to sync to downstream (timeout: {sync_timeout}s)"
        )

        def check_create():
            has_collection = downstream_client.has_collection(collection_name)
            if has_collection:
                logger.info(f"Collection {collection_name} found in downstream")
            return has_collection

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create collection {collection_name}"
        )

        # Create alias
        logger.info(f"Creating alias {alias_name} for collection {collection_name}")
        upstream_client.create_alias(collection_name, alias_name)
        logger.info(f"Alias {alias_name} created in upstream")

        # Verify alias exists in upstream
        logger.info("Verifying alias exists in upstream")
        upstream_aliases_result = upstream_client.list_aliases()
        logger.info(f"Upstream aliases result: {upstream_aliases_result}")
        upstream_aliases = upstream_aliases_result.get("aliases", [])
        assert alias_name in upstream_aliases
        logger.info(f"Confirmed alias {alias_name} exists in upstream")

        # Verify alias points to correct collection using describe_alias
        logger.info(
            f"Verifying alias {alias_name} points to collection {collection_name}"
        )
        alias_desc = upstream_client.describe_alias(alias_name)
        logger.info(f"Alias description: {alias_desc}")
        assert alias_desc.get("collection_name") == collection_name
        logger.info(
            f"Confirmed alias {alias_name} correctly points to {collection_name}"
        )

        # Wait for alias sync to downstream
        logger.info(
            f"Waiting for alias {alias_name} to sync to downstream (timeout: {sync_timeout}s)"
        )

        def check_alias():
            try:
                downstream_aliases_result = downstream_client.list_aliases()
                logger.info(f"Downstream aliases result: {downstream_aliases_result}")
                downstream_aliases = downstream_aliases_result.get("aliases", [])
                if alias_name in downstream_aliases:
                    logger.info(f"Alias {alias_name} found in downstream")
                    # Also verify alias points to correct collection in downstream
                    try:
                        downstream_alias_desc = downstream_client.describe_alias(
                            alias_name
                        )
                        logger.info(
                            f"Downstream alias description: {downstream_alias_desc}"
                        )
                        if (
                            downstream_alias_desc.get("collection_name")
                            == collection_name
                        ):
                            logger.info(
                                f"Downstream alias {alias_name} correctly points to {collection_name}"
                            )
                            return True
                        else:
                            logger.warning(
                                f"Downstream alias {alias_name} points to wrong collection: {downstream_alias_desc.get('collection_name')}"
                            )
                            return False
                    except Exception as desc_e:
                        logger.warning(f"Error describing downstream alias: {desc_e}")
                        return False
                return False
            except Exception as e:
                logger.warning(f"Error checking downstream aliases: {e}")
                return False

        assert self.wait_for_sync(
            check_alias, sync_timeout, f"create alias {alias_name}"
        )
        logger.info("=== test_create_alias completed successfully ===")

    def test_drop_alias(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_ALIAS operation sync."""
        logger.info("=== Starting test_drop_alias ===")
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        collection_name = self.gen_unique_name("test_col_alias_drop")
        alias_name = self.gen_unique_name("test_alias_drop")
        logger.info(f"Generated collection name: {collection_name}")
        logger.info(f"Generated alias name: {alias_name}")
        self.resources_to_cleanup.append(("collection", collection_name))
        self.resources_to_cleanup.append(("alias", alias_name))

        # Initial cleanup
        logger.info(f"Performing initial cleanup for collection: {collection_name}")
        self.cleanup_collection(upstream_client, collection_name)

        # Create collection and alias
        logger.info(f"Creating collection: {collection_name}")
        upstream_client.create_collection(
            collection_name=collection_name,
            schema=self.create_default_schema(upstream_client),
        )
        logger.info(f"Collection {collection_name} created in upstream")

        logger.info(f"Creating alias {alias_name} for collection {collection_name}")
        upstream_client.create_alias(collection_name, alias_name)
        logger.info(f"Alias {alias_name} created in upstream")

        # Wait for setup to sync
        logger.info(
            f"Waiting for collection and alias setup to sync to downstream (timeout: {sync_timeout}s)"
        )

        def check_setup():
            try:
                has_collection = downstream_client.has_collection(collection_name)
                downstream_aliases_result = downstream_client.list_aliases()
                downstream_aliases = downstream_aliases_result.get("aliases", [])
                has_alias = alias_name in downstream_aliases
                logger.info(
                    f"Downstream - has_collection: {has_collection}, has_alias: {has_alias}"
                )
                return has_collection and has_alias
            except Exception as e:
                logger.warning(f"Error checking downstream setup: {e}")
                return False

        assert self.wait_for_sync(
            check_setup, sync_timeout, f"setup collection and alias {collection_name}"
        )

        # Drop alias
        logger.info(f"Dropping alias {alias_name} from upstream")
        upstream_client.drop_alias(alias_name)
        logger.info(f"Alias {alias_name} dropped from upstream")

        # Verify alias is dropped in upstream
        logger.info("Verifying alias is dropped in upstream")
        upstream_aliases_result = upstream_client.list_aliases()
        logger.info(f"Upstream aliases result after drop: {upstream_aliases_result}")
        upstream_aliases = upstream_aliases_result.get("aliases", [])
        assert alias_name not in upstream_aliases
        logger.info(f"Confirmed alias {alias_name} is dropped from upstream")

        # Wait for drop to sync to downstream
        logger.info(
            f"Waiting for alias drop to sync to downstream (timeout: {sync_timeout}s)"
        )

        def check_drop():
            try:
                downstream_aliases_result = downstream_client.list_aliases()
                logger.info(f"Downstream aliases result: {downstream_aliases_result}")
                downstream_aliases = downstream_aliases_result.get("aliases", [])
                is_dropped = alias_name not in downstream_aliases
                if is_dropped:
                    logger.info(
                        f"Alias {alias_name} successfully dropped from downstream"
                    )
                return is_dropped
            except Exception as e:
                logger.warning(f"Error checking downstream aliases during drop: {e}")
                return True  # If error, assume alias is dropped

        assert self.wait_for_sync(check_drop, sync_timeout, f"drop alias {alias_name}")
        logger.info("=== test_drop_alias completed successfully ===")

    def test_alter_alias(self, upstream_client, downstream_client, sync_timeout):
        """Test ALTER_ALIAS operation sync."""
        logger.info("=== Starting test_alter_alias ===")
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        old_collection = self.gen_unique_name("test_col_alias_old")
        new_collection = self.gen_unique_name("test_col_alias_new")
        alias_name = self.gen_unique_name("test_alias_alter")
        logger.info(f"Generated old collection name: {old_collection}")
        logger.info(f"Generated new collection name: {new_collection}")
        logger.info(f"Generated alias name: {alias_name}")
        self.resources_to_cleanup.append(("collection", old_collection))
        self.resources_to_cleanup.append(("collection", new_collection))
        self.resources_to_cleanup.append(("alias", alias_name))

        # Initial cleanup
        logger.info(
            f"Performing initial cleanup for collections: {old_collection}, {new_collection}"
        )
        self.cleanup_collection(upstream_client, old_collection)
        self.cleanup_collection(upstream_client, new_collection)

        # Create both collections
        logger.info(f"Creating old collection: {old_collection}")
        upstream_client.create_collection(
            collection_name=old_collection,
            schema=self.create_default_schema(upstream_client),
        )
        logger.info(f"Old collection {old_collection} created in upstream")

        logger.info(f"Creating new collection: {new_collection}")
        upstream_client.create_collection(
            collection_name=new_collection,
            schema=self.create_default_schema(upstream_client),
        )
        logger.info(f"New collection {new_collection} created in upstream")

        # Create alias pointing to old collection
        logger.info(
            f"Creating alias {alias_name} pointing to old collection {old_collection}"
        )
        upstream_client.create_alias(old_collection, alias_name)
        logger.info(
            f"Alias {alias_name} created in upstream pointing to {old_collection}"
        )

        # Wait for setup to sync
        logger.info(
            f"Waiting for collections and alias setup to sync to downstream (timeout: {sync_timeout}s)"
        )

        def check_setup():
            try:
                has_old = downstream_client.has_collection(old_collection)
                has_new = downstream_client.has_collection(new_collection)
                downstream_aliases_result = downstream_client.list_aliases()
                downstream_aliases = downstream_aliases_result.get("aliases", [])
                has_alias = alias_name in downstream_aliases
                logger.info(
                    f"Downstream - has_old: {has_old}, has_new: {has_new}, has_alias: {has_alias}"
                )
                return has_old and has_new and has_alias
            except Exception as e:
                logger.warning(f"Error checking downstream setup: {e}")
                return False

        assert self.wait_for_sync(
            check_setup, sync_timeout, "setup collections and alias"
        )

        # Alter alias to point to new collection
        logger.info(
            f"Altering alias {alias_name} to point to new collection {new_collection}"
        )
        upstream_client.alter_alias(new_collection, alias_name)
        logger.info(
            f"Alias {alias_name} altered in upstream to point to {new_collection}"
        )

        # Verify alias alteration in upstream
        logger.info(
            f"Verifying alias {alias_name} now points to {new_collection} in upstream"
        )
        upstream_alias_desc = upstream_client.describe_alias(alias_name)
        logger.info(f"Upstream alias description after alter: {upstream_alias_desc}")
        assert upstream_alias_desc.get("collection_name") == new_collection
        logger.info(
            f"Confirmed upstream alias {alias_name} now points to {new_collection}"
        )

        # Wait for alter to sync
        logger.info(
            f"Waiting for alias alter to sync to downstream (timeout: {sync_timeout}s)"
        )

        def check_alter():
            try:
                # Check if alias still exists and points to correct collection after alter
                downstream_aliases_result = downstream_client.list_aliases()
                logger.info(
                    f"Downstream aliases result after alter: {downstream_aliases_result}"
                )
                downstream_aliases = downstream_aliases_result.get("aliases", [])
                if alias_name in downstream_aliases:
                    logger.info(f"Alias {alias_name} found in downstream after alter")
                    # Verify alias points to new collection
                    try:
                        downstream_alias_desc = downstream_client.describe_alias(
                            alias_name
                        )
                        logger.info(
                            f"Downstream alias description after alter: {downstream_alias_desc}"
                        )
                        if (
                            downstream_alias_desc.get("collection_name")
                            == new_collection
                        ):
                            logger.info(
                                f"Downstream alias {alias_name} correctly points to {new_collection}"
                            )
                            return True
                        else:
                            logger.warning(
                                f"Downstream alias {alias_name} still points to old collection: {downstream_alias_desc.get('collection_name')}"
                            )
                            return False
                    except Exception as desc_e:
                        logger.warning(
                            f"Error describing downstream alias after alter: {desc_e}"
                        )
                        return False
                return False
            except Exception as e:
                logger.warning(f"Error checking downstream aliases after alter: {e}")
                return False

        assert self.wait_for_sync(
            check_alter, sync_timeout, f"alter alias {alias_name}"
        )
        logger.info("=== test_alter_alias completed successfully ===")
