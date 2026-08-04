#!/usr/bin/env python3

import importlib.util
import os
import pathlib
import tempfile
import types
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("catalog_transfer_e2e.py")
SPEC = importlib.util.spec_from_file_location("catalog_transfer_e2e", MODULE_PATH)
catalog_transfer_e2e = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(catalog_transfer_e2e)


class FakeConnections:
    def __init__(self):
        self.connected = []

    def connect(self, alias, uri, **kwargs):
        self.connected.append((alias, uri, kwargs))


class FakeUtility:
    def __init__(self):
        self.calls = []

    def list_collections(self, using):
        self.calls.append(using)
        return []


class FakeDB:
    def __init__(self):
        self.used = []

    def list_database(self, using):
        return ["catalog_runtime_e2e_db"]

    def create_database(self, db_name, using):
        raise AssertionError("database should already exist in this test")

    def using_database(self, db_name, using):
        self.used.append((db_name, using))


class CatalogTransferE2ETest(unittest.TestCase):
    def test_wait_uses_returned_utility_client(self):
        connections = FakeConnections()
        utility = FakeUtility()
        original = catalog_transfer_e2e.require_pymilvus
        catalog_transfer_e2e.require_pymilvus = lambda: (
            object,
                object,
                object,
                object,
                connections,
                object,
                utility,
            )
        try:
            code = catalog_transfer_e2e.wait(
                types.SimpleNamespace(alias="src", uri="http://127.0.0.1:19530", timeout=1)
            )
        finally:
            catalog_transfer_e2e.require_pymilvus = original

        self.assertEqual(code, 0)
        self.assertEqual(connections.connected, [("src", "http://127.0.0.1:19530", {})])
        self.assertEqual(utility.calls, ["src"])

    def test_writer_fails_if_run_file_is_removed_before_source_rejection(self):
        class FakeCollection:
            def __init__(self, name, using):
                self.name = name
                self.using = using

        connections = FakeConnections()
        original_require = catalog_transfer_e2e.require_pymilvus
        original_insert = catalog_transfer_e2e.insert_rows

        with tempfile.TemporaryDirectory() as tmp:
            run_file = os.path.join(tmp, "writer.run")
            ready_file = os.path.join(tmp, "writer.ready")
            error_file = os.path.join(tmp, "writer.error")
            pathlib.Path(run_file).write_text("run\n", encoding="utf-8")

            def fake_insert(collection, start, count, dim, partition):
                os.remove(run_file)

            catalog_transfer_e2e.require_pymilvus = lambda: (
                FakeCollection,
                object,
                object,
                object,
                connections,
                object,
                object,
            )
            catalog_transfer_e2e.insert_rows = fake_insert
            try:
                code = catalog_transfer_e2e.writer(
                    types.SimpleNamespace(
                        source_uri="http://127.0.0.1:19530",
                        db_name="default",
                        collection="catalog_transfer_demo",
                        run_file=run_file,
                        ready_file=ready_file,
                        error_file=error_file,
                        start_id=100000,
                        batch_rows=2,
                        dim=4,
                        partition="p_transfer",
                        interval=0,
                    )
                )
            finally:
                catalog_transfer_e2e.require_pymilvus = original_require
                catalog_transfer_e2e.insert_rows = original_insert

        self.assertEqual(code, 1)
        self.assertEqual(connections.connected, [("src-writer", "http://127.0.0.1:19530", {"db_name": "default"})])

    def test_seed_drops_alias_before_collection(self):
        calls = []

        class UtilityWithAlias:
            def has_collection(self, collection, using):
                calls.append(("has_collection", using, collection))
                return True

            def drop_alias(self, alias, using):
                calls.append(("drop_alias", using, alias))

            def drop_collection(self, collection, using):
                calls.append(("drop_collection", using, collection))

            def create_alias(self, collection, alias, using):
                calls.append(("create_alias", using, collection, alias))

        class FakeCollection:
            def __init__(self, name, schema=None, using=None, shards_num=None, consistency_level=None):
                calls.append(("collection", using, name, schema is not None))
                self.schema = object()

            def create_partition(self, partition):
                calls.append(("create_partition", partition))

            def create_index(self, field, params):
                calls.append(("create_index", field))

            def flush(self):
                calls.append(("flush",))

        connections = FakeConnections()
        fake_db = FakeDB()
        utility = UtilityWithAlias()
        original_require = catalog_transfer_e2e.require_pymilvus
        original_insert = catalog_transfer_e2e.insert_rows
        try:
            catalog_transfer_e2e.require_pymilvus = lambda: (
                FakeCollection,
                lambda *args, **kwargs: object(),
                types.SimpleNamespace(INT64=1, FLOAT_VECTOR=2),
                lambda *args, **kwargs: object(),
                connections,
                fake_db,
                utility,
            )
            catalog_transfer_e2e.insert_rows = lambda *args, **kwargs: calls.append(("insert_rows",))
            code = catalog_transfer_e2e.seed(
                types.SimpleNamespace(
                    source_uri="http://127.0.0.1:19530",
                    target_uri="http://127.0.0.1:19630",
                    db_name="catalog_runtime_e2e_db",
                    collection="catalog_cutover_transfer_runtime_e2e",
                    alias_name="catalog_cutover_transfer_runtime_e2e_alias",
                    partition="p_transfer",
                    dim=4,
                    rows=20,
                    skip_target_db=False,
                )
            )
        finally:
            catalog_transfer_e2e.require_pymilvus = original_require
            catalog_transfer_e2e.insert_rows = original_insert

        self.assertEqual(code, 0)
        self.assertLess(
            calls.index(("drop_alias", "src", "catalog_cutover_transfer_runtime_e2e_alias")),
            calls.index(("drop_collection", "src", "catalog_cutover_transfer_runtime_e2e")),
        )
        self.assertLess(
            calls.index(("drop_alias", "dst", "catalog_cutover_transfer_runtime_e2e_alias")),
            calls.index(("drop_collection", "dst", "catalog_cutover_transfer_runtime_e2e")),
        )

    def test_seed_can_leave_target_database_absent_for_transfer_materialization(self):
        class Utility:
            def has_collection(self, collection, using):
                return False

            def drop_alias(self, alias, using):
                pass

            def create_alias(self, collection, alias, using):
                pass

        class FakeCollection:
            def __init__(self, name, schema=None, using=None, shards_num=None, consistency_level=None):
                self.schema = object()

            def create_partition(self, partition):
                pass

            def create_index(self, field, params):
                pass

            def flush(self):
                pass

        connections = FakeConnections()
        fake_db = FakeDB()
        original_require = catalog_transfer_e2e.require_pymilvus
        original_insert = catalog_transfer_e2e.insert_rows
        try:
            catalog_transfer_e2e.require_pymilvus = lambda: (
                FakeCollection,
                lambda *args, **kwargs: object(),
                types.SimpleNamespace(INT64=1, FLOAT_VECTOR=2),
                lambda *args, **kwargs: object(),
                connections,
                fake_db,
                Utility(),
            )
            catalog_transfer_e2e.insert_rows = lambda *args, **kwargs: None
            code = catalog_transfer_e2e.seed(
                types.SimpleNamespace(
                    source_uri="http://127.0.0.1:19530",
                    target_uri="http://127.0.0.1:19630",
                    db_name="catalog_runtime_e2e_db",
                    collection="catalog_cutover_transfer_runtime_e2e",
                    alias_name="catalog_cutover_transfer_runtime_e2e_alias",
                    partition="p_transfer",
                    dim=4,
                    rows=20,
                    skip_target_db=True,
                )
            )
        finally:
            catalog_transfer_e2e.require_pymilvus = original_require
            catalog_transfer_e2e.insert_rows = original_insert

        self.assertEqual(code, 0)
        self.assertEqual(fake_db.used, [("catalog_runtime_e2e_db", "src")])


if __name__ == "__main__":
    unittest.main()
