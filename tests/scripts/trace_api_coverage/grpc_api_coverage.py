#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import ApiResult, add_common_args, finalize_results, grpc_trace_kwargs, run_case, write_json


DECLARED_GRPC_APIS = [
    "add_collection_field",
    "add_collection_function",
    "add_file_resource",
    "add_privileges_to_group",
    "alter_alias",
    "alter_collection_field",
    "alter_collection_function",
    "alter_collection_properties",
    "alter_database_properties",
    "alter_index_properties",
    "close",
    "compact",
    "create_alias",
    "create_collection",
    "create_database",
    "create_field_schema",
    "create_index",
    "create_partition",
    "create_privilege_group",
    "create_resource_group",
    "create_role",
    "create_schema",
    "create_snapshot",
    "create_struct_field_schema",
    "create_user",
    "delete",
    "describe_alias",
    "describe_collection",
    "describe_database",
    "describe_index",
    "describe_replica",
    "describe_resource_group",
    "describe_role",
    "describe_snapshot",
    "describe_user",
    "drop_alias",
    "drop_collection",
    "drop_collection_function",
    "drop_collection_properties",
    "drop_database",
    "drop_database_properties",
    "drop_index",
    "drop_index_properties",
    "drop_partition",
    "drop_privilege_group",
    "drop_resource_group",
    "drop_role",
    "drop_snapshot",
    "drop_user",
    "flush",
    "flush_all",
    "get",
    "get_collection_stats",
    "get_compaction_plans",
    "get_compaction_state",
    "get_flush_all_state",
    "get_load_state",
    "get_partition_stats",
    "get_restore_snapshot_state",
    "get_server_type",
    "get_server_version",
    "grant_privilege",
    "grant_privilege_v2",
    "grant_role",
    "has_collection",
    "has_partition",
    "hybrid_search",
    "insert",
    "list_aliases",
    "list_collections",
    "list_databases",
    "list_file_resources",
    "list_indexes",
    "list_loaded_segments",
    "list_partitions",
    "list_persistent_segments",
    "list_privilege_groups",
    "list_resource_groups",
    "list_restore_snapshot_jobs",
    "list_roles",
    "list_snapshots",
    "list_users",
    "load_collection",
    "load_partitions",
    "optimize",
    "prepare_index_params",
    "query",
    "query_iterator",
    "refresh_load",
    "release_collection",
    "release_partitions",
    "remove_file_resource",
    "remove_privileges_from_group",
    "rename_collection",
    "restore_snapshot",
    "revoke_privilege",
    "revoke_privilege_v2",
    "revoke_role",
    "run_analyzer",
    "search",
    "search_iterator",
    "transfer_replica",
    "truncate_collection",
    "update_password",
    "update_replicate_configuration",
    "update_resource_groups",
    "upsert",
    "use_database",
    "using_database",
]


class GrpcTraceSuite:
    def __init__(self, uri: str, token: str, prefix: str):
        from pymilvus import MilvusClient

        self.client = MilvusClient(uri=uri, token=token)
        self.prefix = prefix
        self.collection = f"{prefix}_grpc"
        self.collection_renamed = f"{prefix}_grpc_renamed"
        self.partition = f"{prefix}_part"
        self.alias = f"{prefix}_alias"
        self.role = f"{prefix}_role"
        self.user = f"{prefix}_user"
        self.db = f"{prefix}_db"
        self.rg = f"{prefix}_rg"
        self.privilege_group = f"{prefix}_priv_group"
        self.snapshot = f"{prefix}_snapshot"

    def setup(self) -> None:
        self._drop_ignore(self.collection)
        self._drop_ignore(self.collection_renamed)
        self.client.create_collection(self.collection, dimension=4, auto_id=False, metric_type="COSINE")

    def cleanup(self) -> None:
        for alias in [self.alias]:
            self._call_ignore(lambda: self.client.drop_alias(alias))
        for name in [self.collection_renamed, self.collection]:
            self._drop_ignore(name)
        self._call_ignore(lambda: self.client.drop_database(self.db))
        self._call_ignore(lambda: self.client.drop_role(self.role, force_drop=True))
        self._call_ignore(lambda: self.client.drop_user(self.user))
        self._call_ignore(lambda: self.client.drop_resource_group(self.rg))
        self._call_ignore(lambda: self.client.drop_privilege_group(self.privilege_group))

    def _drop_ignore(self, name: str) -> None:
        self._call_ignore(lambda: self.client.drop_collection(name))

    def _call_ignore(self, fn: Callable[[], object]) -> None:
        try:
            fn()
        except Exception:
            pass

    def kw(self, trace_id: str) -> dict[str, str]:
        return grpc_trace_kwargs(trace_id)

    def rows(self, start: int = 0, count: int = 4) -> list[dict]:
        return [
            {"id": start + i, "vector": [random.random() for _ in range(4)], "text": f"row-{start + i}"}
            for i in range(count)
        ]

    def cases(self) -> list[tuple[str, Callable[[str], object]]]:
        c = self.client
        collection = self.collection
        part = self.partition

        def index_params():
            params = c.prepare_index_params()
            params.add_index("vector", index_type="AUTOINDEX", metric_type="COSINE")
            return params

        return [
            ("get_server_version", lambda t: c.get_server_version(**self.kw(t))),
            ("get_server_type", lambda t: c.get_server_type(**self.kw(t))),
            ("list_databases", lambda t: c.list_databases(**self.kw(t))),
            ("create_database", lambda t: c.create_database(self.db, **self.kw(t))),
            ("describe_database", lambda t: c.describe_database(self.db, **self.kw(t))),
            ("alter_database_properties", lambda t: c.alter_database_properties(self.db, {"database.replica.number": "1"}, **self.kw(t))),
            ("drop_database_properties", lambda t: c.drop_database_properties(self.db, ["database.replica.number"], **self.kw(t))),
            ("using_database", lambda t: c.using_database("default", **self.kw(t))),
            ("use_database", lambda t: c.use_database("default", **self.kw(t))),
            ("create_schema", lambda _t: c.create_schema(auto_id=False, enable_dynamic_field=True)),
            ("create_field_schema", lambda _t: c.create_field_schema("dummy", "INT64", is_primary=True)),
            ("list_collections", lambda t: c.list_collections(**self.kw(t))),
            ("has_collection", lambda t: c.has_collection(collection, **self.kw(t))),
            ("describe_collection", lambda t: c.describe_collection(collection, **self.kw(t))),
            ("insert", lambda t: c.insert(collection, self.rows(0), **self.kw(t))),
            ("upsert", lambda t: c.upsert(collection, self.rows(2), **self.kw(t))),
            ("flush", lambda t: c.flush(collection, **self.kw(t))),
            ("create_index", lambda t: c.create_index(collection, index_params(), **self.kw(t))),
            ("list_indexes", lambda t: c.list_indexes(collection, **self.kw(t))),
            ("describe_index", lambda t: c.describe_index(collection, "vector", **self.kw(t))),
            ("load_collection", lambda t: c.load_collection(collection, **self.kw(t))),
            ("get_load_state", lambda t: c.get_load_state(collection, **self.kw(t))),
            ("get_collection_stats", lambda t: c.get_collection_stats(collection, **self.kw(t))),
            ("get", lambda t: c.get(collection, [0, 1], output_fields=["id"], **self.kw(t))),
            ("query", lambda t: c.query(collection, filter="id >= 0", output_fields=["id"], **self.kw(t))),
            ("search", lambda t: c.search(collection, data=[[0.1, 0.2, 0.3, 0.4]], limit=2, output_fields=["id"], **self.kw(t))),
            ("query_iterator", lambda t: close_iterator(c.query_iterator(collection, batch_size=2, filter="id >= 0", output_fields=["id"], **self.kw(t)))),
            ("search_iterator", lambda t: close_iterator(c.search_iterator(collection, data=[0.1, 0.2, 0.3, 0.4], batch_size=2, limit=2, output_fields=["id"], **self.kw(t)))),
            ("delete", lambda t: c.delete(collection, ids=[3], **self.kw(t))),
            ("create_partition", lambda t: c.create_partition(collection, part, **self.kw(t))),
            ("has_partition", lambda t: c.has_partition(collection, part, **self.kw(t))),
            ("list_partitions", lambda t: c.list_partitions(collection, **self.kw(t))),
            ("load_partitions", lambda t: c.load_partitions(collection, [part], **self.kw(t))),
            ("get_partition_stats", lambda t: c.get_partition_stats(collection, part, **self.kw(t))),
            ("release_partitions", lambda t: c.release_partitions(collection, [part], **self.kw(t))),
            ("drop_partition", lambda t: c.drop_partition(collection, part, **self.kw(t))),
            ("create_alias", lambda t: c.create_alias(collection, self.alias, **self.kw(t))),
            ("describe_alias", lambda t: c.describe_alias(self.alias, **self.kw(t))),
            ("list_aliases", lambda t: c.list_aliases(collection, **self.kw(t))),
            ("alter_alias", lambda t: c.alter_alias(collection, self.alias, **self.kw(t))),
            ("drop_alias", lambda t: c.drop_alias(self.alias, **self.kw(t))),
            ("compact", lambda t: c.compact(collection, **self.kw(t))),
            ("get_compaction_plans", lambda t: c.get_compaction_plans(collection, **self.kw(t))),
            ("optimize", lambda t: c.optimize(collection, **self.kw(t))),
            ("flush_all", lambda t: c.flush_all(**self.kw(t))),
            ("get_flush_all_state", lambda t: c.get_flush_all_state(int(time.time() * 1000), **self.kw(t))),
            ("list_loaded_segments", lambda t: c.list_loaded_segments(collection, **self.kw(t))),
            ("list_persistent_segments", lambda t: c.list_persistent_segments(collection, **self.kw(t))),
            ("release_collection", lambda t: c.release_collection(collection, **self.kw(t))),
            ("refresh_load", lambda t: c.refresh_load(collection, **self.kw(t))),
            ("create_resource_group", lambda t: c.create_resource_group(self.rg, **self.kw(t))),
            ("list_resource_groups", lambda t: c.list_resource_groups(**self.kw(t))),
            ("describe_resource_group", lambda t: c.describe_resource_group(self.rg, **self.kw(t))),
            ("drop_resource_group", lambda t: c.drop_resource_group(self.rg, **self.kw(t))),
            ("create_user", lambda t: c.create_user(self.user, "Milvus123", **self.kw(t))),
            ("describe_user", lambda t: c.describe_user(self.user, **self.kw(t))),
            ("list_users", lambda t: c.list_users(**self.kw(t))),
            ("update_password", lambda t: c.update_password(self.user, "Milvus123", "Milvus456", **self.kw(t))),
            ("drop_user", lambda t: c.drop_user(self.user, **self.kw(t))),
            ("create_role", lambda t: c.create_role(self.role, **self.kw(t))),
            ("list_roles", lambda t: c.list_roles(**self.kw(t))),
            ("describe_role", lambda t: c.describe_role(self.role, **self.kw(t))),
            ("grant_privilege", lambda t: c.grant_privilege(self.role, "Global", "DescribeCollection", "*", **self.kw(t))),
            ("revoke_privilege", lambda t: c.revoke_privilege(self.role, "Global", "DescribeCollection", "*", **self.kw(t))),
            ("drop_role", lambda t: c.drop_role(self.role, force_drop=True, **self.kw(t))),
            ("create_privilege_group", lambda t: c.create_privilege_group(self.privilege_group, **self.kw(t))),
            ("list_privilege_groups", lambda t: c.list_privilege_groups(**self.kw(t))),
            ("add_privileges_to_group", lambda t: c.add_privileges_to_group(self.privilege_group, ["DescribeCollection"], **self.kw(t))),
            ("remove_privileges_from_group", lambda t: c.remove_privileges_from_group(self.privilege_group, ["DescribeCollection"], **self.kw(t))),
            ("drop_privilege_group", lambda t: c.drop_privilege_group(self.privilege_group, **self.kw(t))),
            ("rename_collection", lambda t: c.rename_collection(collection, self.collection_renamed, **self.kw(t))),
            ("drop_collection", lambda t: c.drop_collection(self.collection_renamed, **self.kw(t))),
            ("run_analyzer", lambda t: c.run_analyzer(["hello milvus"], analyzer_params={"tokenizer": "standard"}, **self.kw(t))),
            ("close", lambda _t: c.close()),
        ]


def close_iterator(iterator: object) -> None:
    next_method = getattr(iterator, "next", None)
    if callable(next_method):
        next_method()
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run gRPC Milvus API trace coverage.")
    add_common_args(parser)
    parser.add_argument("--prefix", default=f"trace_{int(time.time())}")
    args = parser.parse_args()

    suite = GrpcTraceSuite(args.grpc_uri, args.token, args.prefix)
    results: list[ApiResult] = []
    try:
        suite.setup()
        for name, fn in suite.cases():
            if name not in DECLARED_GRPC_APIS:
                results.append(ApiResult(protocol="grpc", name=name, status="skipped", error="case is not in declared API list"))
                continue
            results.append(run_case("grpc", name, fn))
        executed = {result.name for result in results}
        for name in DECLARED_GRPC_APIS:
            if name not in executed:
                results.append(ApiResult(
                    protocol="grpc",
                    name=name,
                    status="skipped",
                    error="declared API has no executable fixture in this script yet",
                ))
    finally:
        suite.cleanup()

    report = finalize_results(args, results, DECLARED_GRPC_APIS)
    write_json(args.output, report)
    if report["summary"]["failed"] or (not args.skip_log_check and report["summary"]["log_matched"] < report["summary"]["ok"]):
        sys.exit(1)


if __name__ == "__main__":
    main()
