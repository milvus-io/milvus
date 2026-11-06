# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Finite concurrent Struct histories with retained inputs and interval read oracles."""

import argparse
import copy
import hashlib
import importlib.metadata
import json
import os
import random
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

import pytest
from common.struct_array_workload_assertions import _equal, identity
from pymilvus import AnnSearchRequest, DataType, MilvusClient, RRFRanker
from pymilvus.exceptions import SchemaMismatchRetryableException

CORE = ("anchor", "score", "label", "emb", "tags")


@dataclass(frozen=True)
class WorkloadConfig:
    """Runner-neutral contract; budgets are per actor per schema epoch.

    Duration is an optional epoch deadline, checked between complete cycles.
    The operation budget remains a hard cap. Read/write ratio is controlled by
    actor counts and budgets; read_mix also controls the read operation weights.
    This retained-history oracle is bounded and is not a throughput benchmark.
    """

    writers: int = 2
    readers: int = 2
    write_budget: int = 18
    read_budget: int = 30
    duration_seconds: float | None = None
    read_mix: tuple[str, ...] = ("get", "query", "predicate", "element_search", "hybrid")
    schema_retries: int = 3
    request_timeout: float = 30
    expected_product: str | None = None

    def validate(self):
        assert 1 <= self.writers <= 16 and 1 <= self.readers <= 16
        assert self.write_budget >= 6 and self.write_budget % 6 == 0
        assert set(self.read_mix) == {"get", "query", "predicate", "element_search", "hybrid"}
        assert self.read_budget >= len(self.read_mix)
        assert self.read_budget % len(self.read_mix) == 0
        # The exact scan/search oracle retains the whole population; refuse
        # profiles that would exceed its fixed, explicit result bounds.
        assert self.writers * self.write_budget <= 180
        assert self.duration_seconds is None or self.duration_seconds > 0
        assert 0 <= self.schema_retries <= 10 and self.request_timeout > 0


def make_row(pk, revision, shape):
    """Generate inputs without consulting any server response."""
    clips = [
        {
            "anchor": j,
            "score": None if j % 2 == 0 else revision,
            "label": None if j % 2 else f"r{revision}",
            "emb": None if j % 2 == 0 else [float(pk % 97 + revision + j)] + [0.0] * 7,
            "tags": [None, [], [None], [revision, None], [1, 2]][j],
        }
        for j in range(5)
    ]
    if shape % 4 == 0:
        clips = None
    elif shape % 4 == 1:
        clips = []
    elif shape % 4 == 2:
        clips = [{key: None for key in CORE} for _ in range(3)]
    return {"id": pk, "revision": revision, "normal_vector": [float(pk + 1)] + [0.0] * 7, "clips": clips}


def project(row, schema):
    if row is None:
        return None
    result = copy.deepcopy(row)
    for element in result["clips"] or []:
        original = dict(element)
        element.clear()
        element.update({key: original[key] for key in CORE})
        for key, generation in schema.items():
            element[key] = original.get(f"{key}@{generation}")
    return result


class MixedHistory:
    def __init__(self, client, name, seed, smoke=False, evidence="/tmp/struct-mixed-evidence", config=None):
        self.client, self.name, self.seed = client, name, seed
        self.config = config or WorkloadConfig(write_budget=6 if smoke else 18, read_budget=10 if smoke else 30)
        self.config.validate()
        self.write_budget, self.read_budget = self.config.write_budget, self.config.read_budget
        self.path = Path(evidence)
        self.path.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.events, self.histories, self.reads, self.schemas = [], {}, [], []
        self.model, self.schema, self.deleted = {}, {}, set()
        self.current = [1000 + worker * 100000 for worker in range(self.config.writers)]
        self.schema_ready = threading.Event()
        self.errors = []
        self.epoch = -1
        self.journal = (self.path / f"{name}.jsonl").open("w")

    def emit(self, event):
        with self.lock:
            event = {"collection": self.name, "epoch": self.epoch, **event}
            self.events.append(event)
            encoded = json.dumps(event, default=str)
            self.journal.write(encoded + "\n")
            self.journal.flush()
            print("STRUCT_MIXED " + encoded, flush=True)
        return event

    def call(self, op, function, **details):
        start = time.monotonic_ns()
        self.emit({"event": "start", "op": op, "start": start, **details})
        rpc_start = time.monotonic_ns()
        try:
            result = function()
        except Exception as exc:
            self.emit(
                {
                    "event": "end",
                    "op": op,
                    "start": start,
                    "end": time.monotonic_ns(),
                    "rpc_start": rpc_start,
                    "outcome": "rejected_schema"
                    if isinstance(exc, SchemaMismatchRetryableException)
                    else "uncertain"
                    if details.get("write")
                    else "error",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "error_code": getattr(exc, "code", None),
                    "error_message": getattr(exc, "message", None),
                    **details,
                }
            )
            raise
        end = time.monotonic_ns()
        self.emit(
            {
                "event": "end",
                "op": op,
                "start": start,
                "rpc_start": rpc_start,
                "end": end,
                "outcome": "ack",
                "result": result,
                **details,
            }
        )
        return result, rpc_start, end

    def create(self):
        build = identity(self.client)
        assert build["git_commit"], build
        if self.config.expected_product is not None:
            assert self.config.expected_product.startswith(build["git_commit"]), build
        sdk = importlib.metadata.distribution("pymilvus")
        direct = json.loads(sdk.read_text("direct_url.json") or "{}")
        self.emit(
            {
                "event": "identity",
                "product": build,
                "sdk_version": sdk.version,
                "sdk_source": direct,
                "seed": self.seed,
                "config": asdict(self.config),
                "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            }
        )
        schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("revision", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=8)
        child = self.client.create_struct_field_schema()
        child.add_field("anchor", DataType.INT64, nullable=True)
        child.add_field("score", DataType.INT64, nullable=True)
        child.add_field("label", DataType.VARCHAR, max_length=64, nullable=True)
        child.add_field("emb", DataType.FLOAT_VECTOR, dim=8, nullable=True)
        child.add_field(
            "tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4, nullable=True, element_nullable=True
        )
        schema.add_field(
            "clips", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=child, max_capacity=8, nullable=True
        )
        indexes = self.client.prepare_index_params()
        for field in ("normal_vector", "clips[emb]"):
            indexes.add_index(field, index_type="FLAT", metric_type="L2")
        self.call(
            "create",
            lambda: self.client.create_collection(
                self.name, schema=schema, index_params=indexes, consistency_level="Strong", timeout=120
            ),
        )
        rows = [make_row(pk, 0, pk) for pk in range(16)]
        self.call(
            "seed_sealed",
            lambda: self.client.insert(self.name, rows[:8], timeout=self.config.request_timeout),
            payload=rows[:8],
        )
        self.call("seed_flush", lambda: self.client.flush(self.name, timeout=120))
        self.call(
            "seed_growing",
            lambda: self.client.insert(self.name, rows[8:], timeout=self.config.request_timeout),
            payload=rows[8:],
        )
        self.model = {row["id"]: row for row in rows}
        self.schemas.append({"start": 0, "end": 0, "schema": {}})
        self.verify("seed")

    def write(self, op, pk, payload, after, method, partial=False):
        before = copy.deepcopy(self.model.get(pk))
        start = time.monotonic_ns()
        entry = {
            "op": op,
            "pk": pk,
            "start": start,
            "end": None,
            "before": before,
            "after": copy.deepcopy(after),
            "outcome": "pending",
            "payload": copy.deepcopy(payload),
        }
        with self.lock:
            self.histories.setdefault(pk, []).append(entry)
        try:
            for attempt in range(self.config.schema_retries + 1):
                try:
                    result, _, end = self.call(
                        op,
                        method,
                        write=True,
                        pk=pk,
                        payload=payload,
                        before=before,
                        after=after,
                        partial=partial,
                        attempt=attempt,
                    )
                    break
                except SchemaMismatchRetryableException:
                    if attempt == self.config.schema_retries:
                        raise
                    assert self.schema_ready.wait(timeout=60), "DDL did not finish before schema retry"
                    observed, _, _ = self.call(
                        "schema_rejection_readback",
                        lambda: self.client.get(
                            self.name,
                            [pk],
                            output_fields=["*"],
                            consistency_level="Strong",
                            timeout=self.config.request_timeout,
                        ),
                        pk=pk,
                    )
                    expected = project(before, self.schema)
                    _equal(observed, [] if expected is None else [expected])
                    self.emit({"event": "schema_rejection_verified", "pk": pk, "attempt": attempt})
                    time.sleep(0.1)
            count_key = (
                "delete_count" if op == "delete" else "insert_count" if op in ("insert", "reinsert") else "upsert_count"
            )
            assert result[count_key] == 1, result
            with self.lock:
                entry.update(end=end, outcome="ack")
                if after is None:
                    self.model.pop(pk, None)
                    self.deleted.add(pk)
                else:
                    self.model[pk] = copy.deepcopy(after)
                    self.deleted.discard(pk)
            # Verify transient partial-update states before this owner advances
            # the key; other writers, readers and DDL remain concurrent.
            rows, read_start, read_end = self.call(
                "writer_readback",
                lambda: self.client.get(
                    self.name,
                    [pk],
                    output_fields=["*"],
                    consistency_level="Strong",
                    timeout=self.config.request_timeout,
                ),
                pk=pk,
                read=True,
            )
            with self.lock:
                self.reads.append({"pk": pk, "start": read_start, "end": read_end, "rows": copy.deepcopy(rows)})
        except Exception:
            with self.lock:
                if entry["outcome"] != "ack":
                    entry.update(end=time.monotonic_ns(), outcome="uncertain")
            raise

    def writer(self, worker, barrier):
        rng = random.Random(self.seed * 100 + self.epoch * 2 + worker)
        barrier.wait(timeout=self.config.request_timeout)
        started = time.monotonic()
        for step in range(self.write_budget):
            if step and step % 6 == 0 and self.config.duration_seconds is not None:
                if time.monotonic() - started >= self.config.duration_seconds:
                    break
            time.sleep(rng.uniform(0.002, 0.012))
            pk = 1000 + worker * 100000 + self.epoch * 100 + step // 6
            with self.lock:
                self.current[worker] = pk
            kind = step % 6
            revision = self.epoch * 100 + step + 1
            full = make_row(pk, revision, self.seed + step // 6 + self.epoch)
            if kind in (0, 5):
                self.write(
                    "insert" if kind == 0 else "reinsert",
                    pk,
                    [full],
                    full,
                    lambda: self.client.insert(self.name, [full], timeout=self.config.request_timeout),
                )
            elif kind == 1:
                self.write(
                    "full_upsert",
                    pk,
                    [full],
                    full,
                    lambda: self.client.upsert(self.name, [full], timeout=self.config.request_timeout),
                )
            elif kind == 2:
                patch = {"id": pk, "revision": revision}
                after = copy.deepcopy(self.model[pk])
                after.update(patch)
                self.write(
                    "partial_omit",
                    pk,
                    [patch],
                    after,
                    lambda: self.client.upsert(
                        self.name, [patch], partial_update=True, timeout=self.config.request_timeout
                    ),
                    True,
                )
            elif kind == 3:
                # Omitted children must become NULL, not inherit the previous Struct.
                clips = [None, [], [{key: None for key in CORE}], [{"anchor": 0, "tags": [None, 7]}]][self.epoch % 4]
                patch = {"id": pk, "revision": revision, "clips": clips}
                after = copy.deepcopy(self.model[pk])
                after.update(copy.deepcopy(patch))
                for element in after["clips"] or []:
                    for key in CORE:
                        element.setdefault(key, None)
                self.write(
                    "partial_replace",
                    pk,
                    [patch],
                    after,
                    lambda: self.client.upsert(
                        self.name, [patch], partial_update=True, timeout=self.config.request_timeout
                    ),
                    True,
                )
            else:
                self.write(
                    "delete",
                    pk,
                    [pk],
                    None,
                    lambda: self.client.delete(self.name, ids=[pk], timeout=self.config.request_timeout),
                )

    def reader(self, worker, barrier):
        barrier.wait(timeout=self.config.request_timeout)
        started = time.monotonic()
        for step in range(self.read_budget):
            if step and step % len(self.config.read_mix) == 0 and self.config.duration_seconds is not None:
                if time.monotonic() - started >= self.config.duration_seconds:
                    break
            operation = self.config.read_mix[step % len(self.config.read_mix)]
            if operation in ("get", "query"):
                with self.lock:
                    pk = self.current[(worker + step) % self.config.writers]
                method = (
                    (
                        lambda: self.client.get(
                            self.name, [pk], output_fields=["*"], consistency_level="Strong", timeout=30
                        )
                    )
                    if operation == "get"
                    else (
                        lambda: self.client.query(
                            self.name, filter=f"id == {pk}", output_fields=["*"], consistency_level="Strong", timeout=30
                        )
                    )
                )
                result, start, end = self.call(operation, method, pk=pk, read=True)
                with self.lock:
                    self.reads.append({"pk": pk, "start": start, "end": end, "rows": copy.deepcopy(result)})
            elif operation == "predicate":
                self.predicates(canaries=True, selector=(step // 5 + worker) % 4)
            else:
                self.search(canaries=True, hybrid=operation == "hybrid")

    def predicates(self, canaries, selector=0):
        with self.lock:
            source = {pk: row for pk, row in self.model.items() if not canaries or pk < 16}
        expression, predicate = [
            ("$[score] IS NULL", lambda e: e["score"] is None),
            ("$[tags] IS NULL", lambda e: e["tags"] is None),
            ("array_length($[tags]) == 0", lambda e: e["tags"] == []),
            ("array_contains($[tags], 1)", lambda e: e["tags"] is not None and 1 in e["tags"]),
        ][selector]
        expected = {
            (pk, offset)
            for pk, row in source.items()
            for offset, element in enumerate(row["clips"] or [])
            if predicate(element)
        }
        prefix = "id < 16 and " if canaries else ""
        rows, _, _ = self.call(
            "predicate",
            lambda: self.client.query(
                self.name,
                filter=f"{prefix}element_filter(clips, {expression})",
                output_fields=["id"],
                limit=4000,
                consistency_level="Strong",
                timeout=self.config.request_timeout,
            ),
            read=True,
            expression=expression,
        )
        assert len(rows) == len(expected) and {(r["id"], r["offset"]) for r in rows} == expected

    def search(self, canaries, hybrid=False, field="emb"):
        # Only immutable PKs are ranked during concurrency; all PKs are checked after draining.
        with self.lock:
            source = {pk: row for pk, row in self.model.items() if not canaries or pk < 16}
        query = [0.25] + [0.0] * 7
        projected = {pk: project(row, self.schema) for pk, row in source.items()}
        distances = {
            (pk, j): sum((a - b) ** 2 for a, b in zip(query, element[field]))
            for pk, row in projected.items()
            for j, element in enumerate(row["clips"] or [])
            if element[field] is not None
        }
        expr = "id < 16" if canaries else "id >= 0"
        if not hybrid:
            hits, _, _ = self.call(
                "element_search",
                lambda: self.client.search(
                    self.name,
                    [query],
                    anns_field=f"clips[{field}]",
                    filter=expr,
                    limit=1000,
                    search_params={"metric_type": "L2"},
                    consistency_level="Strong",
                    timeout=self.config.request_timeout,
                ),
                read=True,
                field=field,
            )
            hits = hits[0]
            assert len(hits) == len(distances)
            assert {(h["id"], h["offset"]) for h in hits} == set(distances)
            for hit in hits:
                assert hit["distance"] == pytest.approx(distances[hit["id"], hit["offset"]], abs=2e-4, rel=2e-6)
            assert [h["distance"] for h in hits] == sorted(h["distance"] for h in hits)
            return
        normal = sorted(
            projected, key=lambda pk: sum((a - b) ** 2 for a, b in zip(query, projected[pk]["normal_vector"]))
        )
        best = {
            pk: min(value for (key, _), value in distances.items() if key == pk)
            for pk in projected
            if any(key == pk for key, _ in distances)
        }
        child = sorted(best, key=best.get)
        scores = {pk: 1 / (60 + rank) for rank, pk in enumerate(normal, 1)}
        for rank, pk in enumerate(child, 1):
            scores[pk] += 1 / (60 + rank)
        requests = [
            AnnSearchRequest([query], f, {"metric_type": "L2"}, 1000, expr=expr)
            for f in ("normal_vector", "clips[emb]")
        ]
        hits, _, _ = self.call(
            "hybrid",
            lambda: self.client.hybrid_search(
                self.name, requests, RRFRanker(k=60), limit=1000, consistency_level="Strong", timeout=30
            ),
            read=True,
        )
        hits = hits[0]
        assert len(hits) == len(scores) and {h["id"] for h in hits} == set(scores)
        for hit in hits:
            assert hit["distance"] == pytest.approx(scores[hit["id"]], abs=1e-6)

    def evolve(self, barrier):
        next_schema = dict(self.schema)
        if self.epoch in (0, 1, 2, 4):
            field, dtype, options, generation = {
                0: ("added", DataType.INT64, {}, 1),
                1: (
                    "newtags",
                    DataType.ARRAY,
                    {"element_type": DataType.INT64, "max_capacity": 4, "element_nullable": True},
                    1,
                ),
                2: ("newvec", DataType.FLOAT_VECTOR, {"dim": 8}, 1),
                4: ("added", DataType.INT64, {}, 2),
            }[self.epoch]
            next_schema[field] = generation

            def method():
                return self.client.add_collection_field(
                    self.name, f"clips[{field}]", dtype, nullable=True, timeout=60, **options
                )

            op = f"add_{field}"
        elif self.epoch == 3:
            del next_schema["added"]

            def method():
                return self.client.drop_collection_field(self.name, field_name="clips[added]", timeout=60)

            op = "drop_added"
        else:

            def method():
                return self.client.flush(self.name, timeout=120)

            op = "concurrent_flush"
        barrier.wait(timeout=self.config.request_timeout)
        _, start, end = self.call(op, method, ddl=True)
        self.schemas.append({"start": start, "end": end, "schema": next_schema})
        self.schema = next_schema
        self.schema_ready.set()

    def check_reads(self):
        for read in self.reads:
            history = self.histories.get(read["pk"], [])
            completed = [h for h in history if h["outcome"] == "ack" and h["end"] < read["start"]]
            base = completed[-1] if completed else None
            candidates = [base["after"] if base else None]
            candidates.extend(h["after"] for h in history if h["start"] <= read["end"] and h["end"] >= read["start"])
            completed_schema = [s for s in self.schemas if s["end"] < read["start"]]
            schemas = [completed_schema[-1]["schema"]]
            schemas.extend(s["schema"] for s in self.schemas if s["start"] <= read["end"] and s["end"] >= read["start"])
            matches = False
            for candidate in candidates:
                for schema in schemas:
                    expected = [] if candidate is None else [project(candidate, schema)]
                    try:
                        _equal(read["rows"], expected)
                        matches = True
                    except AssertionError:
                        pass
            if not matches:
                self.emit({"event": "read_mismatch", "read": read, "candidates": candidates, "schemas": schemas})
            assert matches, read
        self.emit({"event": "interval_reads_verified", "count": len(self.reads)})

    def verify(self, phase):
        expected = {pk: project(row, self.schema) for pk, row in self.model.items()}
        rows, _, _ = self.call(
            "integrity_query",
            lambda: self.client.query(
                self.name, filter="id >= 0", output_fields=["*"], limit=4000, consistency_level="Strong", timeout=30
            ),
        )
        assert len(rows) == len(expected) and {r["id"] for r in rows} == set(expected)
        for row in rows:
            _equal(row, expected[row["id"]])
        all_keys = sorted(set(expected) | self.deleted | {9999999})
        for begin in range(0, len(all_keys), 50):
            batch = all_keys[begin : begin + 50]
            rows, _, _ = self.call(
                "integrity_get",
                lambda batch=batch: self.client.get(
                    self.name, batch, output_fields=["*"], consistency_level="Strong", timeout=30
                ),
            )
            assert {r["id"] for r in rows} == set(batch) & set(expected)
            for row in rows:
                _equal(row, expected[row["id"]])
        for selector in range(4):
            self.predicates(False, selector)
        offsets, _, _ = self.call(
            "all_offsets",
            lambda: self.client.query(
                self.name,
                filter="element_filter(clips, not ($[score] in []))",
                output_fields=["id"],
                limit=4000,
                consistency_level="Strong",
                timeout=self.config.request_timeout,
            ),
        )
        expected_offsets = {(pk, j) for pk, row in expected.items() for j, _ in enumerate(row["clips"] or [])}
        assert len(offsets) == len(expected_offsets)
        assert {(row["id"], row["offset"]) for row in offsets} == expected_offsets
        segments = self.client._get_connection().get_query_segment_info(self.name, timeout=self.config.request_timeout)
        self.emit({"event": "segments", "phase": phase, "segments": [str(segment) for segment in segments]})
        self.search(False)
        # The immutable set has unique rank distances; mutable rows may tie.
        self.search(True, hybrid=True)
        self.emit(
            {
                "event": "verified",
                "phase": phase,
                "rows": len(expected),
                "deleted": sorted(self.deleted),
                "schema": self.schema,
                "expected": expected,
            }
        )
        (self.path / f"{self.name}-{phase}-expected.json").write_text(json.dumps(expected))

    def reject_bad_wire(self):
        row = make_row(9999999, 1, 3)
        handler = self.client._get_connection()
        request = handler._prepare_row_insert_request(self.name, [row])
        struct = next(f for f in request.fields_data if f.field_name == "clips").struct_arrays
        score = next(f for f in struct.fields if f.field_name in ("score", "clips[score]"))
        del score.scalars.array_data.data[0].valid_data[-1]
        response, _, _ = self.call(
            "invalid_wire", lambda: handler._stub.Insert(request, timeout=self.config.request_timeout)
        )
        assert response.status.code == 1100, response.status
        assert "valid" in response.status.reason.lower(), response.status
        self.verify("rejected_unchanged")

    def run(self):
        failure = None
        try:
            self.create()
            self.reject_bad_wire()
            for epoch in range(6):
                self.epoch = epoch
                error_count = len(self.errors)
                self.schema_ready.clear()
                actors = self.config.writers + self.config.readers + 1
                barrier = threading.Barrier(actors)
                with ThreadPoolExecutor(max_workers=actors) as pool:
                    futures = [pool.submit(self.writer, w, barrier) for w in range(self.config.writers)]
                    futures += [pool.submit(self.reader, r, barrier) for r in range(self.config.readers)]
                    futures.append(pool.submit(self.evolve, barrier))
                    for future in futures:
                        try:
                            future.result(timeout=600)
                        except Exception as exc:
                            self.errors.append(
                                {
                                    "epoch": epoch,
                                    "type": type(exc).__name__,
                                    "error": str(exc),
                                    "code": getattr(exc, "code", None),
                                    "message": getattr(exc, "message", None),
                                }
                            )
                self.check_reads()
                self.verify(f"epoch_{epoch}")
                if len(self.errors) > error_count:
                    failed_calls = [
                        e
                        for e in self.events
                        if e["event"] == "end"
                        and e["epoch"] == epoch
                        and e["outcome"] not in ("ack", "rejected_schema")
                    ]
                    # Continue diagnostics only after exact quiescent verification,
                    # with unambiguous writes and errors confined to read RPCs.
                    # The history still fails below; no failed request is retried.
                    assert len(failed_calls) == len(self.errors) - error_count, self.errors
                    assert all(e.get("read") and e["outcome"] == "error" for e in failed_calls), self.errors
                    assert all(h["outcome"] == "ack" for hs in self.histories.values() for h in hs), self.errors
                    self.emit({"event": "continue_diagnostics", "errors": self.errors[error_count:]})
                if epoch in (0, 1, 2):
                    canary = copy.deepcopy(self.model[3])
                    key = ("added", "newtags", "newvec")[epoch]
                    canary["clips"][1][f"{key}@1"] = (913, [None, 19], [0.125] + [0.0] * 7)[epoch]
                    payload = project(canary, self.schema)
                    self.call(
                        "canary_update",
                        lambda: self.client.upsert(self.name, [payload], timeout=self.config.request_timeout),
                        payload=[payload],
                    )
                    self.model[3] = canary
                    if epoch == 0:
                        self.old_id = self.child_id("added")
                    if epoch == 2:
                        index = self.client.prepare_index_params()
                        index.add_index("clips[newvec]", index_type="FLAT", metric_type="L2")
                        self.call("index_newvec", lambda: self.client.create_index(self.name, index, timeout=120))
                if epoch == 4:
                    assert self.child_id("added") > self.old_id
            # Retain actual tombstones in the final oracle, in addition to delete/reinsert history.
            for worker in range(self.config.writers):
                pk = self.current[worker]
                self.write(
                    "delete",
                    pk,
                    [pk],
                    None,
                    lambda pk=pk: self.client.delete(self.name, ids=[pk], timeout=self.config.request_timeout),
                )
            self.check_reads()
            assert not self.errors, self.errors
        except Exception as exc:
            failure = exc
            self.emit({"event": "failure", "type": type(exc).__name__, "error": str(exc), "workers": self.errors})
        finally:
            # Always try reconciliation after all worker futures have drained.
            uncertain = [h for history in self.histories.values() for h in history if h["outcome"] != "ack"]
            for entry in uncertain:
                rows = self.client.get(
                    self.name, [entry["pk"]], output_fields=["*"], consistency_level="Strong", timeout=30
                )
                matches = []
                for label in ("before", "after"):
                    candidate = project(entry[label], self.schema)
                    try:
                        _equal(rows, [] if candidate is None else [candidate])
                        matches.append(label)
                    except AssertionError:
                        pass
                self.emit({"event": "uncertain_reconciliation", "entry": entry, "observed": rows, "matches": matches})
                if matches == ["after"]:
                    if entry["after"] is None:
                        self.model.pop(entry["pk"], None)
                        self.deleted.add(entry["pk"])
                    else:
                        self.model[entry["pk"]] = entry["after"]
            if self.model:
                try:
                    self.verify("drained")
                    self.call("final_flush", lambda: self.client.flush(self.name, timeout=120))
                    self.call("release", lambda: self.client.release_collection(self.name, timeout=60))
                    self.call("load", lambda: self.client.load_collection(self.name, timeout=120))
                    self.verify("reloaded")
                    if "newvec" in self.schema:
                        self.search(False, field="newvec")
                except Exception as exc:
                    failure = failure or exc
                    self.emit({"event": "final_verification_failure", "type": type(exc).__name__, "error": str(exc)})
            ends = [e for e in self.events if e["event"] == "end"]
            writes = [e for e in ends if e.get("write")]
            reads = [e for e in ends if e.get("read")]
            overlap = sum(any(w["rpc_start"] < r["end"] and r["rpc_start"] < w["end"] for w in writes) for r in reads)
            ddl_overlap = {
                e["op"] + f"_{e['epoch']}": sum(w["rpc_start"] < e["end"] and e["rpc_start"] < w["end"] for w in writes)
                for e in ends
                if e.get("ddl")
            }
            if not overlap or not all(ddl_overlap.values()):
                failure = failure or AssertionError("Required read/write or DDL/write overlap was not observed")
            summary = {
                "event": "summary",
                "seed": self.seed,
                "config": asdict(self.config),
                "outcome": "FAIL" if failure or uncertain else "PASS",
                "counts": dict(Counter(e["op"] for e in ends)),
                "read_write_overlap": overlap,
                "ddl_write_overlap": ddl_overlap,
                "uncertain_writes": len(uncertain),
                "acknowledged_writes": sum(
                    h["outcome"] == "ack" for history in self.histories.values() for h in history
                ),
                "outcomes": dict(Counter(e["outcome"] for e in ends)),
                "errors": self.errors,
            }
            self.emit(summary)
            (self.path / f"{self.name}-summary.json").write_text(json.dumps(summary, indent=2))
            self.journal.close()
            if failure is None:
                assert overlap > 0 and all(ddl_overlap.values()), summary
                assert not uncertain, summary
        if failure:
            raise failure
        return summary

    def child_id(self, field):
        desc = self.client.describe_collection(self.name, timeout=self.config.request_timeout)
        child = next(f for f in desc["fields"] if f["name"] == "clips")
        field_id = next(f["field_id"] for f in child["struct_fields"] if f["name"] == field)
        self.emit({"event": "field_id", "field": field, "id": field_id})
        return field_id


def run_workload(client, collection, seed, artifacts, config=None, cleanup=True):
    """Adapter entry point: caller supplies connection and resource ownership.

    Return the summary on success; raise on failure with the same journal and
    summary retained. Refuse pre-existing collections. No deployment, global
    cleanup, resource selection or performance SLO is implicit in this API.
    """
    if client.has_collection(collection, timeout=30):
        raise ValueError(f"Refusing to overwrite existing collection {collection}")
    driver = MixedHistory(client, collection, seed, evidence=artifacts, config=config)
    try:
        return driver.run()
    finally:
        if cleanup:
            client.drop_collection(collection, timeout=60)
            Path(artifacts, f"{collection}-cleanup.json").write_text(
                json.dumps({"collection": collection, "dropped": True})
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--collection", required=True, help="New, explicitly owned collection name")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--config", type=Path, help="WorkloadConfig JSON; token comes only from MILVUS_TOKEN")
    args = parser.parse_args()
    config = WorkloadConfig(**json.loads(args.config.read_text())) if args.config else WorkloadConfig()
    config.validate()
    client = MilvusClient(args.uri, token=os.environ.get("MILVUS_TOKEN", ""), timeout=config.request_timeout)
    try:
        run_workload(client, args.collection, args.seed, args.artifacts, config)
    finally:
        client.close()


if __name__ == "__main__":
    main()
