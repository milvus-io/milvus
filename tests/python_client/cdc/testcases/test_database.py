"""
CDC sync tests for database operations.
"""

import time
from .base import TestCDCSyncBase, logger


class TestCDCSyncDatabase(TestCDCSyncBase):
    """Test CDC sync for database operations."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method - only cleanup upstream, downstream will sync."""
        upstream_client = getattr(self, "_upstream_client", None)

        if upstream_client:
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "database":
                    self.cleanup_database(upstream_client, resource_name)

            time.sleep(1)  # Allow cleanup to sync to downstream

    def test_create_database(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_DATABASE operation sync."""
        start_time = time.time()
        db_name = self.gen_unique_name("test_db_create")

        # Log test start
        self.log_test_start("test_create_database", "CREATE_DATABASE", db_name)

        # Store upstream client for teardown
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("database", db_name))

        try:
            # Initial cleanup
            self.cleanup_database(upstream_client, db_name)

            # Log operation
            self.log_operation("CREATE_DATABASE", "database", db_name, "upstream")

            # Create database in upstream
            upstream_client.create_database(db_name)

            # Verify upstream creation
            upstream_databases = upstream_client.list_databases()
            upstream_exists = db_name in upstream_databases
            self.log_resource_state(
                "database",
                db_name,
                "exists" if upstream_exists else "missing",
                "upstream",
            )
            assert upstream_exists, f"Database {db_name} not created in upstream"

            # Log sync verification start
            self.log_sync_verification(
                "CREATE_DATABASE", db_name, "exists in downstream"
            )

            # Wait for sync to downstream
            def check_sync():
                downstream_databases = downstream_client.list_databases()
                exists = db_name in downstream_databases
                if exists:
                    self.log_resource_state(
                        "database", db_name, "exists", "downstream", "Sync confirmed"
                    )
                return exists

            sync_success = self.wait_for_sync(
                check_sync, sync_timeout, f"create database {db_name}"
            )
            assert sync_success, f"Database {db_name} failed to sync to downstream"

            # Log test success
            duration = time.time() - start_time
            self.log_test_end("test_create_database", True, duration)

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[ERROR] Test failed with error: {e}")
            self.log_test_end("test_create_database", False, duration)
            raise

    def test_drop_database(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_DATABASE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        db_name = self.gen_unique_name("test_db_drop")
        self.resources_to_cleanup.append(("database", db_name))

        # Initial cleanup
        self.cleanup_database(upstream_client, db_name)

        # Create database in upstream first
        upstream_client.create_database(db_name)

        # Wait for creation to sync
        def check_create():
            return db_name in downstream_client.list_databases()

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create database {db_name}"
        )

        # Drop database in upstream
        upstream_client.drop_database(db_name)
        assert db_name not in upstream_client.list_databases()

        # Wait for drop to sync to downstream
        def check_drop():
            return db_name not in downstream_client.list_databases()

        assert self.wait_for_sync(check_drop, sync_timeout, f"drop database {db_name}")

    def test_alter_database_properties(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test ALTER_DATABASE_PROPERTIES operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        db_name = self.gen_unique_name("test_db_alter_props")
        self.resources_to_cleanup.append(("database", db_name))

        # Initial cleanup
        self.cleanup_database(upstream_client, db_name)

        # Create database in upstream first
        upstream_client.create_database(db_name)

        # Wait for creation to sync
        def check_create():
            return db_name in downstream_client.list_databases()

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create database {db_name}"
        )

        # Set multiple database properties
        properties_to_set = {
            "database.max.collections": 100,
            "database.diskQuota.mb": 1024,
        }

        upstream_client.alter_database_properties(
            db_name=db_name, properties=properties_to_set
        )

        # Wait for alter properties to sync
        def check_alter_properties():
            try:
                # Verify database exists
                if db_name not in downstream_client.list_databases():
                    return False

                # Verify properties are synced correctly
                downstream_props = downstream_client.describe_database(db_name)
                logger.info(f"Downstream database properties: {downstream_props}")
                for key, expected_value in properties_to_set.items():
                    if key not in downstream_props or str(downstream_props[key]) != str(
                        expected_value
                    ):
                        return False
                return True
            except:
                return False

        assert self.wait_for_sync(
            check_alter_properties, sync_timeout, f"alter database properties {db_name}"
        )
        logger.info(f"Database properties altered successfully for {db_name}")

    def test_drop_database_properties(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test DROP_DATABASE_PROPERTIES operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        db_name = self.gen_unique_name("test_db_drop_props")
        self.resources_to_cleanup.append(("database", db_name))

        # Initial cleanup
        self.cleanup_database(upstream_client, db_name)

        # Create database in upstream first
        upstream_client.create_database(db_name)

        # Wait for creation to sync
        def check_create():
            return db_name in downstream_client.list_databases()

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create database {db_name}"
        )

        # First set some properties
        properties_to_set = {
            "database.max.collections": 200,
            "database.diskQuota.mb": 2048,
        }

        upstream_client.alter_database_properties(
            db_name=db_name, properties=properties_to_set
        )

        # Wait for initial properties to sync
        time.sleep(3)  # Allow properties to be set

        # Drop specific database properties (only drop one, keep the other)
        property_keys_to_drop = ["database.max.collections"]

        upstream_client.drop_database_properties(
            db_name=db_name, property_keys=property_keys_to_drop
        )

        # Wait for drop properties to sync
        def check_drop_properties():
            try:
                # Verify database exists
                if db_name not in downstream_client.list_databases():
                    return False

                # Verify properties are synced correctly after drop
                downstream_props = downstream_client.describe_database(db_name)

                logger.info(f"Downstream database properties: {downstream_props}")
                # Verify dropped property is not present
                if "database.max.collections" in downstream_props:
                    return False

                # Verify non-dropped property is still present with correct value
                if (
                    "database.diskQuota.mb" not in downstream_props
                    or str(downstream_props["database.diskQuota.mb"]) != "2048"
                ):
                    return False

                return True
            except:
                return False

        assert self.wait_for_sync(
            check_drop_properties, sync_timeout, f"drop database properties {db_name}"
        )
        logger.info(f"Database properties dropped successfully for {db_name}")
