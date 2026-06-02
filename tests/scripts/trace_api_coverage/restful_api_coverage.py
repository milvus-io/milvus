#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Callable

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import ApiResult, add_common_args, finalize_results, run_case, trace_headers, write_json


DECLARED_REST_APIS = [
    "v2/vectordb/collections/create",
    "v2/vectordb/collections/describe",
    "v2/vectordb/collections/list",
    "v2/vectordb/collections/has",
    "v2/vectordb/collections/load",
    "v2/vectordb/collections/get_load_state",
    "v2/vectordb/collections/get_stats",
    "v2/vectordb/entities/insert",
    "v2/vectordb/entities/upsert",
    "v2/vectordb/entities/get",
    "v2/vectordb/entities/query",
    "v2/vectordb/entities/search",
    "v2/vectordb/entities/delete",
    "v2/vectordb/indexes/create",
    "v2/vectordb/indexes/list",
    "v2/vectordb/indexes/describe",
    "v2/vectordb/indexes/drop",
    "v2/vectordb/partitions/create",
    "v2/vectordb/partitions/list",
    "v2/vectordb/partitions/has",
    "v2/vectordb/partitions/get_stats",
    "v2/vectordb/partitions/load",
    "v2/vectordb/partitions/release",
    "v2/vectordb/partitions/drop",
    "v2/vectordb/aliases/create",
    "v2/vectordb/aliases/list",
    "v2/vectordb/aliases/describe",
    "v2/vectordb/aliases/alter",
    "v2/vectordb/aliases/drop",
    "v2/vectordb/collections/release",
    "v2/vectordb/collections/drop",
    "v2/vectordb/databases/create",
    "v2/vectordb/databases/list",
    "v2/vectordb/databases/describe",
    "v2/vectordb/databases/drop",
    "v2/vectordb/users/create",
    "v2/vectordb/users/list",
    "v2/vectordb/users/describe",
    "v2/vectordb/users/update_password",
    "v2/vectordb/users/drop",
    "v2/vectordb/roles/create",
    "v2/vectordb/roles/list",
    "v2/vectordb/roles/describe",
    "v2/vectordb/roles/grant",
    "v2/vectordb/roles/revoke",
    "v2/vectordb/roles/drop",
]


class RestTraceSuite:
    def __init__(self, uri: str, token: str, prefix: str):
        self.uri = uri.rstrip("/")
        self.token = token
        self.prefix = prefix
        self.collection = f"{prefix}_rest"
        self.partition = f"{prefix}_part"
        self.alias = f"{prefix}_alias"
        self.db = f"{prefix}_db"
        self.role = f"{prefix}_role"
        self.user = f"{prefix}_user"

    def request(self, trace_id: str, method: str, path: str, json_body: dict | None = None, params: dict | None = None) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Accept-Type-Allow-Int64": "true",
        }
        headers.update(trace_headers(trace_id))
        response = requests.request(
            method,
            f"{self.uri}/{path}",
            headers=headers,
            json=json_body,
            params=params,
            timeout=30,
        )
        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text}
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} HTTP {response.status_code}: {body}")
        code = body.get("code", body.get("Code", 0))
        if code not in (0, 200, "0"):
            raise RuntimeError(f"{method} {path} returned code {code}: {body}")
        return body

    def setup(self) -> None:
        self._ignore(lambda t: self.drop_collection(t))

    def cleanup(self) -> None:
        for fn in [self.drop_alias, self.drop_collection, self.drop_database, self.drop_role, self.drop_user]:
            self._ignore(fn)

    def _ignore(self, fn: Callable[[str], object]) -> None:
        try:
            fn("0" * 32)
        except Exception:
            pass

    def create_collection_payload(self) -> dict:
        return {
            "collectionName": self.collection,
            "dimension": 4,
            "metricType": "COSINE",
            "primaryField": "id",
            "vectorField": "vector",
            "autoId": False,
        }

    def rows(self) -> list[dict]:
        return [
            {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4], "text": "one"},
            {"id": 2, "vector": [0.2, 0.3, 0.4, 0.5], "text": "two"},
        ]

    def cases(self) -> list[tuple[str, Callable[[str], object]]]:
        c = self.collection
        p = self.partition
        return [
            ("v2/vectordb/collections/create", lambda t: self.request(t, "POST", "v2/vectordb/collections/create", self.create_collection_payload())),
            ("v2/vectordb/collections/list", lambda t: self.request(t, "POST", "v2/vectordb/collections/list", {})),
            ("v2/vectordb/collections/has", lambda t: self.request(t, "POST", "v2/vectordb/collections/has", {"collectionName": c})),
            ("v2/vectordb/collections/describe", lambda t: self.request(t, "POST", "v2/vectordb/collections/describe", {"collectionName": c})),
            ("v2/vectordb/entities/insert", lambda t: self.request(t, "POST", "v2/vectordb/entities/insert", {"collectionName": c, "data": self.rows()})),
            ("v2/vectordb/entities/upsert", lambda t: self.request(t, "POST", "v2/vectordb/entities/upsert", {"collectionName": c, "data": self.rows()})),
            ("v2/vectordb/collections/get_stats", lambda t: self.request(t, "POST", "v2/vectordb/collections/get_stats", {"collectionName": c})),
            ("v2/vectordb/indexes/create", lambda t: self.request(t, "POST", "v2/vectordb/indexes/create", {"collectionName": c, "indexParams": [{"fieldName": "vector", "indexName": "vector", "metricType": "COSINE"}]})),
            ("v2/vectordb/indexes/list", lambda t: self.request(t, "POST", "v2/vectordb/indexes/list", {"collectionName": c})),
            ("v2/vectordb/indexes/describe", lambda t: self.request(t, "POST", "v2/vectordb/indexes/describe", {"collectionName": c, "indexName": "vector"})),
            ("v2/vectordb/collections/load", lambda t: self.request(t, "POST", "v2/vectordb/collections/load", {"collectionName": c})),
            ("v2/vectordb/collections/get_load_state", lambda t: self.request(t, "POST", "v2/vectordb/collections/get_load_state", {"collectionName": c})),
            ("v2/vectordb/entities/get", lambda t: self.request(t, "POST", "v2/vectordb/entities/get", {"collectionName": c, "id": 1})),
            ("v2/vectordb/entities/query", lambda t: self.request(t, "POST", "v2/vectordb/entities/query", {"collectionName": c, "filter": "id >= 1", "outputFields": ["id"]})),
            ("v2/vectordb/entities/search", lambda t: self.request(t, "POST", "v2/vectordb/entities/search", {"collectionName": c, "data": [[0.1, 0.2, 0.3, 0.4]], "limit": 2, "outputFields": ["id"]})),
            ("v2/vectordb/entities/delete", lambda t: self.request(t, "POST", "v2/vectordb/entities/delete", {"collectionName": c, "id": 2})),
            ("v2/vectordb/partitions/create", lambda t: self.request(t, "POST", "v2/vectordb/partitions/create", {"collectionName": c, "partitionName": p})),
            ("v2/vectordb/partitions/list", lambda t: self.request(t, "POST", "v2/vectordb/partitions/list", {"collectionName": c})),
            ("v2/vectordb/partitions/has", lambda t: self.request(t, "POST", "v2/vectordb/partitions/has", {"collectionName": c, "partitionName": p})),
            ("v2/vectordb/partitions/get_stats", lambda t: self.request(t, "POST", "v2/vectordb/partitions/get_stats", {"collectionName": c, "partitionName": p})),
            ("v2/vectordb/partitions/load", lambda t: self.request(t, "POST", "v2/vectordb/partitions/load", {"collectionName": c, "partitionNames": [p]})),
            ("v2/vectordb/partitions/release", lambda t: self.request(t, "POST", "v2/vectordb/partitions/release", {"collectionName": c, "partitionNames": [p]})),
            ("v2/vectordb/partitions/drop", lambda t: self.request(t, "POST", "v2/vectordb/partitions/drop", {"collectionName": c, "partitionName": p})),
            ("v2/vectordb/aliases/create", lambda t: self.request(t, "POST", "v2/vectordb/aliases/create", {"collectionName": c, "aliasName": self.alias})),
            ("v2/vectordb/aliases/list", lambda t: self.request(t, "POST", "v2/vectordb/aliases/list", {"collectionName": c})),
            ("v2/vectordb/aliases/describe", lambda t: self.request(t, "POST", "v2/vectordb/aliases/describe", {"aliasName": self.alias})),
            ("v2/vectordb/aliases/alter", lambda t: self.request(t, "POST", "v2/vectordb/aliases/alter", {"collectionName": c, "aliasName": self.alias})),
            ("v2/vectordb/aliases/drop", lambda t: self.request(t, "POST", "v2/vectordb/aliases/drop", {"aliasName": self.alias})),
            ("v2/vectordb/indexes/drop", lambda t: self.request(t, "POST", "v2/vectordb/indexes/drop", {"collectionName": c, "indexName": "vector"})),
            ("v2/vectordb/collections/release", lambda t: self.request(t, "POST", "v2/vectordb/collections/release", {"collectionName": c})),
            ("v2/vectordb/databases/list", lambda t: self.request(t, "POST", "v2/vectordb/databases/list", {})),
            ("v2/vectordb/users/list", lambda t: self.request(t, "POST", "v2/vectordb/users/list", {})),
            ("v2/vectordb/roles/list", lambda t: self.request(t, "POST", "v2/vectordb/roles/list", {})),
            ("v2/vectordb/collections/drop", lambda t: self.request(t, "POST", "v2/vectordb/collections/drop", {"collectionName": c})),
        ]

    def drop_collection(self, trace_id: str) -> object:
        return self.request(trace_id, "POST", "v2/vectordb/collections/drop", {"collectionName": self.collection})

    def drop_alias(self, trace_id: str) -> object:
        return self.request(trace_id, "POST", "v2/vectordb/aliases/drop", {"aliasName": self.alias})

    def drop_database(self, trace_id: str) -> object:
        return self.request(trace_id, "POST", "v2/vectordb/databases/drop", {"dbName": self.db})

    def drop_role(self, trace_id: str) -> object:
        return self.request(trace_id, "POST", "v2/vectordb/roles/drop", {"roleName": self.role})

    def drop_user(self, trace_id: str) -> object:
        return self.request(trace_id, "POST", "v2/vectordb/users/drop", {"userName": self.user})


def main() -> None:
    parser = argparse.ArgumentParser(description="Run REST Milvus API trace coverage.")
    add_common_args(parser)
    parser.add_argument("--prefix", default=f"trace_{int(time.time())}")
    args = parser.parse_args()

    suite = RestTraceSuite(args.rest_uri, args.token, args.prefix)
    results: list[ApiResult] = []
    try:
        suite.setup()
        for name, fn in suite.cases():
            results.append(run_case("rest", name, fn))
        executed = {result.name for result in results}
        for name in DECLARED_REST_APIS:
            if name not in executed:
                results.append(ApiResult(
                    protocol="rest",
                    name=name,
                    status="skipped",
                    error="declared REST endpoint has no executable fixture in this script yet",
                ))
    finally:
        suite.cleanup()

    report = finalize_results(args, results, DECLARED_REST_APIS)
    write_json(args.output, report)
    if report["summary"]["failed"] or (not args.skip_log_check and report["summary"]["log_matched"] < report["summary"]["ok"]):
        sys.exit(1)


if __name__ == "__main__":
    main()
