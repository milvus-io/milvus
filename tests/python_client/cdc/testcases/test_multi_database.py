"""
CDC sync tests for multi-database operations.
"""

import time

from pymilvus import MilvusClient

from .base import TestCDCSyncBase, logger


class TestCDCSyncMultiDatabase(TestCDCSyncBase):
    """Test CDC sync for operations across multiple databases."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method - only cleanup upstream, downstream will sync."""
        upstream_uri = getattr(self, "_upstream_uri", None)
        upstream_token = getattr(self, "_upstream_token", None)

        if upstream_uri:
            # Re-create a default-db client for cleanup
            try:
                client = MilvusClient(uri=upstream_uri, token=upstream_token)
            except Exception as e:
                logger.warning(f"[CLEANUP] Failed to create upstream client: {e}")
                return

            # Clean up collections in databases first, then databases
            for resource_type, resource_data in self.resources_to_cleanup:
                if resource_type == "collection_in_db":
                    db_name, c_name = resource_data
                    try:
                        db_client = MilvusClient(
                            uri=upstream_uri,
                            token=upstream_token,
                            db_name=db_name,
                        )
                        if db_client.has_collection(c_name):
                            logger.info(f"[CLEANUP] Dropping collection {c_name} in db {db_name}")
                            db_client.drop_collection(c_name)
                        db_client.close()
                    except Exception as e:
                        logger.warning(f"[CLEANUP] Failed to drop collection {c_name} in db {db_name}: {e}")

            for resource_type, resource_data in self.resources_to_cleanup:
                if resource_type == "database":
                    db_name = resource_data
                    self.cleanup_database(client, db_name)

            client.close()
            time.sleep(1)  # Allow cleanup to sync to downstream

    def test_create_collections_in_multiple_dbs(
        self,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Test creating collections in multiple databases syncs to downstream."""
        self._upstream_uri = upstream_uri
        self._upstream_token = upstream_token

        db_name_1 = self.gen_unique_name("test_mdb_db1")
        db_name_2 = self.gen_unique_name("test_mdb_db2")
        c_name_1 = self.gen_unique_name("test_mdb_col1")
        c_name_2 = self.gen_unique_name("test_mdb_col2")

        self.resources_to_cleanup.append(("collection_in_db", (db_name_1, c_name_1)))
        self.resources_to_cleanup.append(("collection_in_db", (db_name_2, c_name_2)))
        self.resources_to_cleanup.append(("database", db_name_1))
        self.resources_to_cleanup.append(("database", db_name_2))

        # Initial cleanup
        self.cleanup_database(upstream_client, db_name_1)
        self.cleanup_database(upstream_client, db_name_2)

        # Create databases
        upstream_client.create_database(db_name_1)
        upstream_client.create_database(db_name_2)

        # Create DB-scoped clients
        up_db1_client = MilvusClient(uri=upstream_uri, token=upstream_token, db_name=db_name_1)
        up_db2_client = MilvusClient(uri=upstream_uri, token=upstream_token, db_name=db_name_2)

        # Create collection in db1 with 100 rows
        schema1 = self.create_default_schema(up_db1_client)
        up_db1_client.create_collection(collection_name=c_name_1, schema=schema1, consistency_level="Strong")
        up_db1_client.insert(c_name_1, self.generate_test_data(100))
        up_db1_client.flush(c_name_1)

        # Create collection in db2 with 200 rows
        schema2 = self.create_default_schema(up_db2_client)
        up_db2_client.create_collection(collection_name=c_name_2, schema=schema2, consistency_level="Strong")
        up_db2_client.insert(c_name_2, self.generate_test_data(200))
        up_db2_client.flush(c_name_2)

        up_db1_client.close()
        up_db2_client.close()

        # Wait for both databases and collections to appear on downstream
        def check_db1_collection():
            try:
                if db_name_1 not in downstream_client.list_databases():
                    return False
                dn_db1 = MilvusClient(uri=downstream_uri, token=downstream_token, db_name=db_name_1)
                exists = dn_db1.has_collection(c_name_1)
                if exists:
                    stats = dn_db1.get_collection_stats(c_name_1)
                    logger.info(f"Downstream db1 collection stats: {stats}")
                dn_db1.close()
                return exists
            except Exception as e:
                logger.warning(f"Check db1 collection failed: {e}")
                return False

        assert self.wait_for_sync(check_db1_collection, sync_timeout, f"collection {c_name_1} in {db_name_1}")

        def check_db2_collection():
            try:
                if db_name_2 not in downstream_client.list_databases():
                    return False
                dn_db2 = MilvusClient(uri=downstream_uri, token=downstream_token, db_name=db_name_2)
                exists = dn_db2.has_collection(c_name_2)
                if exists:
                    stats = dn_db2.get_collection_stats(c_name_2)
                    logger.info(f"Downstream db2 collection stats: {stats}")
                dn_db2.close()
                return exists
            except Exception as e:
                logger.warning(f"Check db2 collection failed: {e}")
                return False

        assert self.wait_for_sync(check_db2_collection, sync_timeout, f"collection {c_name_2} in {db_name_2}")

        # Verify row counts
        dn_db1 = MilvusClient(uri=downstream_uri, token=downstream_token, db_name=db_name_1)
        dn_db2 = MilvusClient(uri=downstream_uri, token=downstream_token, db_name=db_name_2)
        try:
            stats1 = dn_db1.get_collection_stats(c_name_1)
            stats2 = dn_db2.get_collection_stats(c_name_2)
            logger.info(f"DB1 collection row count: {stats1.get('row_count')}")
            logger.info(f"DB2 collection row count: {stats2.get('row_count')}")
            assert stats1.get("row_count", 0) >= 100, (
                f"Expected >= 100 rows in {c_name_1}, got {stats1.get('row_count')}"
            )
            assert stats2.get("row_count", 0) >= 200, (
                f"Expected >= 200 rows in {c_name_2}, got {stats2.get('row_count')}"
            )
        finally:
            dn_db1.close()
            dn_db2.close()

    def test_drop_db_with_collections(
        self,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Test drop DB with collection syncs to downstream (DB gone from downstream)."""
        self._upstream_uri = upstream_uri
        self._upstream_token = upstream_token

        db_name = self.gen_unique_name("test_mdb_drop_db")
        c_name = self.gen_unique_name("test_mdb_drop_col")

        self.resources_to_cleanup.append(("collection_in_db", (db_name, c_name)))
        self.resources_to_cleanup.append(("database", db_name))

        # Initial cleanup
        self.cleanup_database(upstream_client, db_name)

        # Create database and collection
        upstream_client.create_database(db_name)
        up_db_client = MilvusClient(uri=upstream_uri, token=upstream_token, db_name=db_name)
        schema = self.create_default_schema(up_db_client)
        up_db_client.create_collection(collection_name=c_name, schema=schema, consistency_level="Strong")
        up_db_client.insert(c_name, self.generate_test_data(50))
        up_db_client.flush(c_name)
        up_db_client.close()

        # Wait for DB + collection to sync to downstream
        def check_created():
            try:
                if db_name not in downstream_client.list_databases():
                    return False
                dn = MilvusClient(uri=downstream_uri, token=downstream_token, db_name=db_name)
                exists = dn.has_collection(c_name)
                dn.close()
                return exists
            except Exception as e:
                logger.warning(f"Check DB+collection created: {e}")
                return False

        assert self.wait_for_sync(check_created, sync_timeout, f"create db {db_name} with collection")

        # Drop collection then DB in upstream
        up_db_client2 = MilvusClient(uri=upstream_uri, token=upstream_token, db_name=db_name)
        up_db_client2.drop_collection(c_name)
        up_db_client2.close()
        upstream_client.drop_database(db_name)

        assert db_name not in upstream_client.list_databases(), (
            f"Database {db_name} still exists in upstream after drop"
        )

        # Wait for DB to be gone on downstream
        def check_db_dropped():
            return db_name not in downstream_client.list_databases()

        assert self.wait_for_sync(check_db_dropped, sync_timeout, f"drop database {db_name}")

    def test_cross_db_operations(
        self,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Test cross-DB operations: create DB, collection, insert, alter DB properties, create 2nd collection."""
        self._upstream_uri = upstream_uri
        self._upstream_token = upstream_token

        db_name = self.gen_unique_name("test_mdb_cross_db")
        c_name_1 = self.gen_unique_name("test_mdb_cross_col1")
        c_name_2 = self.gen_unique_name("test_mdb_cross_col2")

        self.resources_to_cleanup.append(("collection_in_db", (db_name, c_name_1)))
        self.resources_to_cleanup.append(("collection_in_db", (db_name, c_name_2)))
        self.resources_to_cleanup.append(("database", db_name))

        # Initial cleanup
        self.cleanup_database(upstream_client, db_name)

        # Step 1: Create database
        upstream_client.create_database(db_name)

        # Step 2: Create DB-scoped client and 1st collection with insert
        up_db_client = MilvusClient(uri=upstream_uri, token=upstream_token, db_name=db_name)
        schema1 = self.create_default_schema(up_db_client)
        up_db_client.create_collection(collection_name=c_name_1, schema=schema1, consistency_level="Strong")
        up_db_client.insert(c_name_1, self.generate_test_data(100))
        up_db_client.flush(c_name_1)

        # Step 3: Alter DB properties
        upstream_client.alter_database_properties(
            db_name=db_name,
            properties={"database.max.collections": 10},
        )

        # Step 4: Create 2nd collection in the same DB
        schema2 = self.create_default_schema(up_db_client)
        up_db_client.create_collection(collection_name=c_name_2, schema=schema2, consistency_level="Strong")
        up_db_client.close()

        # Wait for all operations to sync to downstream

        # Check database exists on downstream
        def check_db_exists():
            return db_name in downstream_client.list_databases()

        assert self.wait_for_sync(check_db_exists, sync_timeout, f"create database {db_name}")

        # Check 1st collection exists on downstream
        def check_col1():
            try:
                dn = MilvusClient(uri=downstream_uri, token=downstream_token, db_name=db_name)
                exists = dn.has_collection(c_name_1)
                dn.close()
                return exists
            except Exception as e:
                logger.warning(f"Check col1 sync failed: {e}")
                return False

        assert self.wait_for_sync(check_col1, sync_timeout, f"collection {c_name_1} in {db_name}")

        # Check 2nd collection exists on downstream
        def check_col2():
            try:
                dn = MilvusClient(uri=downstream_uri, token=downstream_token, db_name=db_name)
                exists = dn.has_collection(c_name_2)
                dn.close()
                return exists
            except Exception as e:
                logger.warning(f"Check col2 sync failed: {e}")
                return False

        assert self.wait_for_sync(check_col2, sync_timeout, f"collection {c_name_2} in {db_name}")

        # Check DB properties synced
        def check_db_props():
            try:
                if db_name not in downstream_client.list_databases():
                    return False
                props = downstream_client.describe_database(db_name)
                logger.info(f"Downstream database properties: {props}")
                return str(props.get("database.max.collections", "")) == "10"
            except Exception as e:
                logger.warning(f"Check DB properties sync failed: {e}")
                return False

        assert self.wait_for_sync(check_db_props, sync_timeout, f"DB properties sync for {db_name}")
