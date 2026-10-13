import copy
import threading
import time
from collections import Counter

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import AnnSearchRequest, DataType, RRFRanker
from pymilvus.client.types import SegmentState
from pymilvus.exceptions import MilvusException
from pymilvus.grpc_gen import milvus_pb2
from utils.util_log import test_log as log

READ_ACTIONS = ["query", "query_iterator", "search", "search_iterator", "hybrid_search"]
ALL_ACTIONS = READ_ACTIONS + ["insert", "upsert", "delete"]
OWNER = "owner == $current_principal"
FIELDS = ["id", "owner", "department", "level", "active", "payload", "projects", "note"]
VECTOR_TYPES = [
    DataType.FLOAT_VECTOR,
    DataType.BINARY_VECTOR,
    DataType.FLOAT16_VECTOR,
    DataType.BFLOAT16_VECTOR,
    DataType.INT8_VECTOR,
    DataType.SPARSE_FLOAT_VECTOR,
]
VECTOR_DIM = 128
# Keep mixed-segment iterator settings aligned with the Go SDK RLS suite.
ITERATOR_BATCH_SIZE = 127
HNSW_EF = 4096
SEGMENT_GROUPS = [("indexed", 0, 3000), ("unindexed", 3000, 500), ("growing", 3500, 500)]


def rls_distance(vector_type, query, vector):
    """Compute the expected score from the values actually encoded by the SDK."""
    if vector_type == DataType.BINARY_VECTOR:
        return (int.from_bytes(query, "little") ^ int.from_bytes(vector, "little")).bit_count()
    if vector_type == DataType.SPARSE_FLOAT_VECTOR:
        return sum(value * vector.get(index, 0.0) for index, value in query.items())
    delta = np.asarray(query, dtype=np.float32) - np.asarray(vector, dtype=np.float32)
    return float(np.dot(delta, delta))


def rls_rows(vector_type=DataType.FLOAT_VECTOR, varchar=False):
    vectors = cf.gen_vectors(4, VECTOR_DIM, vector_type)
    return [
        {
            "id": f"doc_{i}" if varchar else i,
            "owner": "alice" if i % 2 else "bob",
            "department": "eng" if i <= 2 else "sales",
            "level": i,
            "active": i != 3,
            "payload": {"revision": 0, "text": f"文档_{i}"},
            "projects": ["a"] if i <= 2 else ["b"],
            "note": None if i == 3 else f"note_{i}",
            "vector": vectors[i - 1],
        }
        for i in range(1, 5)
    ]


def rls_values(rows):
    return [{key: row[key] for key in FIELDS} for row in rows]


def rls_mixed_rows(vector_type, varchar):
    """Both owners alternate within every segment; PK ranges never overlap."""
    templates = rls_rows(vector_type)
    vectors = cf.gen_vectors(sum(count for _, _, count in SEGMENT_GROUPS), VECTOR_DIM, vector_type)
    rows = []
    for group, start, count in SEGMENT_GROUPS:
        for offset in range(count):
            row = copy.deepcopy(templates[offset % len(templates)])
            row["id"] = f"doc_{start + offset + 1}" if varchar else start + offset + 1
            row["payload"]["segment_group"] = group
            row["vector"] = vectors[start + offset]
            rows.append(row)
    fingerprints = []
    for row in rows:
        vector = row["vector"]
        if isinstance(vector, dict):
            fingerprints.append(tuple(sorted((index, float(np.float32(value))) for index, value in vector.items())))
        elif isinstance(vector, bytes):
            fingerprints.append(vector)
        else:
            fingerprints.append(np.asarray(vector, dtype=np.float32).tobytes())
    assert len(set(fingerprints)) == len(rows), "Generated vectors must remain unique after dtype conversion"
    return rows


class TestMilvusClientRLSIndependent(TestMilvusClientV2Base):
    """Each case owns its collections; no global RBAC cleanup is required."""

    skip_global_role_cleanup = True

    @pytest.fixture
    def rls_client(self, request):
        self.tear_down_collection_names = []
        client = self._client()
        request.addfinalizer(client.close)
        return client

    def _prepare_rls(
        self,
        client,
        vector_type=DataType.FLOAT_VECTOR,
        varchar=False,
        policy=True,
        name=None,
        force_teardown=True,
        mixed_segments=False,
    ):
        name = name or cf.gen_unique_str("client_rls")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        self.add_field(schema, "id", DataType.VARCHAR if varchar else DataType.INT64, is_primary=True)
        for field in ["owner", "department"]:
            self.add_field(schema, field, DataType.VARCHAR)
        self.add_field(schema, "level", DataType.INT64)
        self.add_field(schema, "active", DataType.BOOL)
        self.add_field(schema, "payload", DataType.JSON)
        self.add_field(schema, "projects", DataType.ARRAY, element_type=DataType.VARCHAR, max_length=256)
        self.add_field(schema, "note", DataType.VARCHAR, nullable=True)
        dimension = {} if vector_type == DataType.SPARSE_FLOAT_VECTOR else {"dim": VECTOR_DIM}
        self.add_field(schema, "vector", vector_type, **dimension)
        self.create_collection(
            client,
            name,
            schema=schema,
            properties={"rls.enabled": True},
            force_teardown=force_teardown,
            num_shards=1,
            consistency_level="Strong",
        )
        # The shared framework does not yet wrap the new RLS management APIs.
        if policy:
            client.create_row_policy(name, "owner", "permissive", ALL_ACTIONS, OWNER, OWNER, timeout=30)
        rows = rls_mixed_rows(vector_type, varchar) if mixed_segments else rls_rows(vector_type, varchar)
        if mixed_segments:
            self.alter_collection_properties(client, name, {"collection.autocompaction.enabled": "false"})
            for _, start, count in SEGMENT_GROUPS[:2]:
                self.insert(client, name, rows[start : start + count], skip_rls=True)
                self.flush(client, name)
        else:
            self.insert(client, name, rows, skip_rls=True)
        metric = "L2"
        index_type = "FLAT"
        if vector_type == DataType.BINARY_VECTOR:
            metric, index_type = "HAMMING", "BIN_FLAT"
        elif vector_type == DataType.SPARSE_FLOAT_VECTOR:
            metric, index_type = "IP", "SPARSE_INVERTED_INDEX"
        elif vector_type == DataType.INT8_VECTOR:
            index_type = "HNSW"
        if mixed_segments and index_type in ["FLAT", "BIN_FLAT"]:
            index_type = "BIN_IVF_FLAT" if vector_type == DataType.BINARY_VECTOR else "IVF_FLAT"
        build_params = {"M": 16, "efConstruction": 200} if index_type == "HNSW" else {}
        if "IVF" in index_type:
            build_params = {"nlist": 64}
        params = client.prepare_index_params()
        params.add_index(
            field_name="vector",
            index_type=index_type,
            metric_type=metric,
            params=build_params,
        )
        self.create_index(client, name, params)
        if mixed_segments:
            assert self.wait_for_index_ready(client, name, "vector", timeout=180)
        self.load_collection(client, name)
        if mixed_segments:
            self.insert(client, name, rows[3500:], skip_rls=True)
            self._assert_mixed_segments(client, name)
            # Strong reads verify exact membership as well as the 4000/3500 split.
            self._check_rows(client, name, rows, skip_rls=True)
            self._check_rows(client, name, rows[:3500], skip_rls=True, ignore_growing=True)
        return name, rows, metric

    def _assert_mixed_segments(self, client, name, expected_sealed_ids=None):
        """Check the two flushed batches; exact ignore_growing reads check the final batch."""
        deadline = time.monotonic() + 90
        loaded = []
        while time.monotonic() < deadline:
            # The shared framework has no segment-metadata wrapper.
            loaded = client.list_loaded_segments(name, timeout=10)
            if (
                len(loaded) == 2
                and sorted(segment.num_rows for segment in loaded) == [500, 3000]
                and all(segment.state == SegmentState.Sealed for segment in loaded)
            ):
                break
            threading.Event().wait(1)
        else:
            pytest.fail(f"Expected 3000-row and 500-row sealed segments, got {loaded}")
        sealed_ids = {segment.segment_id for segment in loaded}
        if expected_sealed_ids is not None:
            assert sealed_ids == expected_sealed_ids
        log.info("RLS mixed segments: loaded=%s", loaded)
        return sealed_ids

    def _check_rows(self, client, name, expected, **context):
        result, _ = self.query(
            client,
            name,
            filter="owner != ''",
            output_fields=FIELDS,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": len(expected), "output_fields": FIELDS},
            **context,
        )
        # Unique PKs allow an exact sorted comparison without the generic checker's quadratic matching.
        assert sorted(result, key=lambda row: row["id"]) == sorted(rls_values(expected), key=lambda row: row["id"])

    def _check_stored(self, client, name, rows):
        self._check_rows(client, name, rows, skip_rls=True)
        vectors, _ = self.query(client, name, filter="owner != ''", output_fields=["id", "vector"], skip_rls=True)
        assert sorted(row["id"] for row in vectors) == sorted(row["id"] for row in rows)
        expected = {row["id"]: row["vector"] for row in rows}
        for row in vectors:
            want = expected[row["id"]]
            got = row["vector"]
            if isinstance(want, bytes | np.ndarray):
                if isinstance(got, list) and len(got) == 1 and isinstance(got[0], bytes):
                    got = got[0]
                assert got == (want.tobytes() if isinstance(want, np.ndarray) else want)
            elif isinstance(want, dict):
                assert got == {index: float(np.float32(value)) for index, value in want.items()}
            else:
                np.testing.assert_array_equal(got, np.asarray(want, dtype=np.float32))
        for principal in ["alice", "bob", "nobody"]:
            self._check_rows(client, name, [r for r in rows if r["owner"] == principal], rls_principal=principal)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", VECTOR_TYPES, ids=lambda value: value.name)
    @pytest.mark.parametrize("varchar", [False, True], ids=["int64_pk", "varchar_pk"])
    def test_rls_schema_read_write_correctness(self, rls_client, vector_type, varchar):
        """
        target: RLS row data across vector and primary-key schemas
        method: insert, query, get, search, full/partial upsert and delete
        expected: exact authorized values and unchanged unrelated rows
        """
        client = rls_client
        name, rows, metric = self._prepare_rls(client, vector_type, varchar)
        for principal in ["alice", "bob", "nobody"]:
            expected = [r for r in rows if r["owner"] == principal]
            self._check_rows(client, name, expected, rls_principal=principal)
            got, _ = self.get(client, name, [r["id"] for r in rows], output_fields=FIELDS, rls_principal=principal)
            assert sorted(got, key=lambda r: r["id"]) == sorted(rls_values(expected), key=lambda r: r["id"])
            found, _ = self.search(
                client,
                name,
                [rows[0]["vector"]],
                limit=10,
                output_fields=FIELDS,
                search_params={"metric_type": metric},
                rls_principal=principal,
            )
            assert len(found) == 1
            assert sorted(hit["id"] for hit in found[0]) == sorted(r["id"] for r in expected)
            for hit in found[0]:
                row = next(r for r in expected if r["id"] == hit["id"])
                assert {k: hit["entity"][k] for k in FIELDS} == rls_values([row])[0]
                distance = rls_distance(vector_type, rows[0]["vector"], row["vector"])
                # Sparse IP postings use FP16 by default; allow its relative rounding bound.
                tolerance = 5e-4 if vector_type == DataType.SPARSE_FLOAT_VECTOR else 1e-5
                assert hit["distance"] == pytest.approx(distance, rel=tolerance, abs=1e-6)
        changed = copy.deepcopy(rows[0])
        changed.update(
            payload={"revision": 1, "text": "已更新"},
            note="updated",
            vector=cf.gen_vectors(1, VECTOR_DIM, vector_type)[0],
        )
        self.upsert(client, name, [changed], rls_principal="alice")
        rows[0] = changed
        self._check_stored(client, name, rows)
        self.upsert(
            client, name, [{"id": changed["id"], "note": "partial"}], partial_update=True, rls_principal="alice"
        )
        rows[0]["note"] = "partial"
        self._check_stored(client, name, rows)
        fresh = copy.deepcopy(changed)
        fresh["id"] = "doc_5" if varchar else 5
        fresh["vector"] = cf.gen_vectors(1, VECTOR_DIM, vector_type)[0]
        self.insert(client, name, [fresh], rls_principal="alice")
        rows.append(fresh)
        self._check_stored(client, name, rows)
        self.delete(client, name, ids=[changed["id"]], rls_principal="alice")
        rows = [r for r in rows if r["id"] != changed["id"]]
        self._check_stored(client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", VECTOR_TYPES, ids=lambda value: value.name)
    @pytest.mark.parametrize("varchar", [False, True], ids=["int64_pk", "varchar_pk"])
    def test_rls_mixed_segments_read_correctness(self, rls_client, vector_type, varchar):
        """
        target: each owner's data on indexed sealed, unindexed sealed and growing paths
        method: query/get/search/hybrid and exhaust query iteration across all groups
        expected: exact authorized membership and values in every group
        """
        client = rls_client
        name, rows, metric = self._prepare_rls(client, vector_type, varchar, mixed_segments=True)
        sentinels = [row for _, start, _ in SEGMENT_GROUPS for row in rows[start : start + 8]]
        sentinel_filter = f"id in {[row['id'] for row in sentinels]}"
        search_params = {
            "metric_type": metric,
            "params": {"ef": HNSW_EF} if vector_type == DataType.INT8_VECTOR else {"nprobe": 64},
        }
        vector = rows[0]["vector"]
        for principal in ["alice", "bob", "nobody"]:
            expected = [row for row in rows if row["owner"] == principal]
            self._check_rows(client, name, expected, rls_principal=principal)
            self._check_rows(
                client,
                name,
                [row for row in expected if row["payload"]["segment_group"] != "growing"],
                rls_principal=principal,
                ignore_growing=True,
            )
            for group, _, count in SEGMENT_GROUPS:
                group_rows = [row for row in expected if row["payload"]["segment_group"] == group]
                assert len(group_rows) == (0 if principal == "nobody" else count // 2)
                result, _ = self.query(
                    client,
                    name,
                    filter=f'payload["segment_group"] == "{group}"',
                    output_fields=["count(*)"],
                    rls_principal=principal,
                )
                assert result == [{"count(*)": len(group_rows)}]
            result, _ = self.get(
                client, name, [row["id"] for row in rows], output_fields=FIELDS, rls_principal=principal
            )
            assert sorted(result, key=lambda row: row["id"]) == sorted(rls_values(expected), key=lambda row: row["id"])
            selected = [row for row in sentinels if row["owner"] == principal]
            result, _ = self.search(
                client,
                name,
                [vector],
                filter=sentinel_filter,
                limit=24,
                output_fields=FIELDS,
                search_params=search_params,
                rls_principal=principal,
            )
            assert sorted((hit["entity"] for hit in result[0]), key=lambda row: row["id"]) == sorted(
                rls_values(selected), key=lambda row: row["id"]
            )
            vectors_by_id = {row["id"]: row["vector"] for row in selected}
            for hit in result[0]:
                distance = rls_distance(vector_type, vector, vectors_by_id[hit["id"]])
                # Sparse IP postings use FP16 by default; allow its relative rounding bound.
                tolerance = 5e-4 if vector_type == DataType.SPARSE_FLOAT_VECTOR else 1e-5
                assert hit["distance"] == pytest.approx(distance, rel=tolerance, abs=1e-6)
            requests = [AnnSearchRequest([vector], "vector", search_params, 24, expr=sentinel_filter)]
            result, _ = self.hybrid_search(
                client, name, requests, RRFRanker(), limit=24, output_fields=FIELDS, rls_principal=principal
            )
            assert sorted((hit["entity"] for hit in result[0]), key=lambda row: row["id"]) == sorted(
                rls_values(selected), key=lambda row: row["id"]
            )
            iterator, _ = self.query_iterator(
                client, name, batch_size=333, filter="owner != ''", output_fields=FIELDS, rls_principal=principal
            )
            try:
                found = []
                for _ in range(15):
                    page = iterator.next()
                    if not page:
                        break
                    found.extend(page)
                else:
                    pytest.fail("Mixed-segment query iterator did not reach EOF")
                assert sorted(found, key=lambda row: row["id"]) == sorted(
                    rls_values(expected), key=lambda row: row["id"]
                )
            finally:
                iterator.close()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", VECTOR_TYPES, ids=lambda value: value.name)
    @pytest.mark.parametrize("varchar", [False, True], ids=["int64_pk", "varchar_pk"])
    def test_rls_mixed_segments_search_iterator(self, rls_client, vector_type, varchar):
        """
        target: complete search iteration for every owner and segment path
        method: exhaust each group with principal/bypass and ignore_growing controls
        expected: exact authorized IDs and field values, no duplicates, and explicit EOF
        """
        client = rls_client
        name, rows, metric = self._prepare_rls(client, vector_type, varchar, mixed_segments=True)
        mismatches = []
        for principal in ["alice", "bob", "nobody", ""]:
            options = {"rls_principal": principal} if principal else {"skip_rls": True}
            for group, start, count in SEGMENT_GROUPS:
                candidates = rows[start : start + count]
                requested = [row["id"] for row in candidates]
                for ignore_growing in [False, True]:
                    expected = [
                        row
                        for row in candidates
                        if (not principal or row["owner"] == principal) and not (ignore_growing and group == "growing")
                    ]
                    expected_by_id = {row["id"]: row for row in rls_values(expected)}
                    context = f"principal={principal or 'SkipRLS'}, group={group}, ignore_growing={ignore_growing}"
                    # The binary index can reject either construction or the first page.
                    # Inspect both boundaries explicitly instead of accepting arbitrary errors.
                    iterator, created = self.search_iterator(
                        client,
                        name,
                        [candidates[0]["vector"]],
                        batch_size=ITERATOR_BATCH_SIZE,
                        limit=-1,
                        filter=f"id in {requested}",
                        output_fields=FIELDS,
                        search_params={
                            "metric_type": metric,
                            "params": {"ef": HNSW_EF} if vector_type == DataType.INT8_VECTOR else {"nprobe": 64},
                            "ignore_growing": ignore_growing,
                        },
                        check_task=CheckTasks.check_nothing,
                        **options,
                    )
                    if not created:
                        assert vector_type == DataType.BINARY_VECTOR and expected, (context, iterator)
                        assert "index not supported" in iterator.message, (context, iterator)
                        log.info("Binary indexed iteration unsupported: %s", context)
                        continue
                    found_ids = []
                    try:
                        for _ in range(count // ITERATOR_BATCH_SIZE + 3):
                            page = iterator.next()
                            if not page:
                                break
                            for hit in page:
                                assert hit.id in expected_by_id, (context, "Unauthorized or unexpected ID", hit.id)
                                actual_row = {field: hit.entity.get(field) for field in FIELDS}
                                assert actual_row == expected_by_id[hit.id], context
                                found_ids.append(hit.id)
                        else:
                            pytest.fail(f"Search iterator did not reach EOF: {context}")
                    except MilvusException as exc:
                        assert vector_type == DataType.BINARY_VECTOR and expected, (context, exc)
                        assert "index not supported" in str(exc), (context, exc)
                        log.info("Binary indexed iteration unsupported: %s", context)
                        continue
                    finally:
                        iterator.close()
                    actual = Counter(found_ids)
                    wanted = Counter(expected_by_id.keys())
                    if actual != wanted:
                        mismatches.append(
                            f"{context}: missing={list((wanted - actual).elements())}, "
                            f"unexpected_or_duplicate={list((actual - wanted).elements())}"
                        )
        assert not mismatches, "\n".join(mismatches)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", VECTOR_TYPES, ids=lambda value: value.name)
    @pytest.mark.parametrize("varchar", [False, True], ids=["int64_pk", "varchar_pk"])
    def test_rls_mixed_segments_write_correctness(self, rls_client, vector_type, varchar):
        """
        target: both owners' write correctness across all three original segment groups
        method: full/partial upsert, delete by IDs/filter, and insert for each owner
        expected: exact stored values after every write, including the other owner's data
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client, vector_type, varchar, mixed_segments=True)
        sealed_ids = self._assert_mixed_segments(client, name)
        expected_by_id = {row["id"]: copy.deepcopy(row) for row in rows}
        remaining_sealed = {row["id"]: row for row in rows[:3500]}
        for owner_offset, principal in enumerate(["alice", "bob"]):
            for operation_offset, operation in enumerate(["upsert", "partial_upsert", "delete_ids", "delete_filter"]):
                self._assert_mixed_segments(client, name, sealed_ids)
                # Different sentinels ensure each operation targets original rows in all three paths.
                targets = [rows[start + operation_offset * 2 + owner_offset] for _, start, _ in SEGMENT_GROUPS]
                ids = [row["id"] for row in targets]
                if operation == "upsert":
                    vectors = cf.gen_vectors(len(targets), VECTOR_DIM, vector_type)
                    changed = [
                        {**copy.deepcopy(row), "note": "full update", "vector": vector}
                        for row, vector in zip(targets, vectors)
                    ]
                    self.upsert(client, name, changed, rls_principal=principal)
                    expected_by_id.update({row["id"]: row for row in changed})
                elif operation == "partial_upsert":
                    self.upsert(
                        client,
                        name,
                        [{"id": row_id, "note": "partial update"} for row_id in ids],
                        partial_update=True,
                        rls_principal=principal,
                    )
                    for row_id in ids:
                        expected_by_id[row_id]["note"] = "partial update"
                else:
                    self.delete(
                        client,
                        name,
                        **({"ids": ids} if operation == "delete_ids" else {"filter": f"id in {ids}"}),
                        rls_principal=principal,
                    )
                    for row_id in ids:
                        del expected_by_id[row_id]
                self._check_stored(client, name, list(expected_by_id.values()))
                for row_id in ids:
                    remaining_sealed.pop(row_id, None)
                self._check_rows(client, name, list(remaining_sealed.values()), skip_rls=True, ignore_growing=True)
            fresh = copy.deepcopy(rows[owner_offset])
            fresh["id"] = f"doc_{4001 + owner_offset}" if varchar else 4001 + owner_offset
            fresh["vector"] = cf.gen_vectors(1, VECTOR_DIM, vector_type)[0]
            self.insert(client, name, [fresh], rls_principal=principal)
            expected_by_id[fresh["id"]] = fresh
            self._check_stored(client, name, list(expected_by_id.values()))
        self._assert_mixed_segments(client, name, sealed_ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("principal", ["alice", "bob", "nobody"])
    def test_rls_iterators_hybrid_and_count(self, rls_client, principal):
        """
        target: RLS context on paginated and combined reads
        method: exhaust both iterators and query hybrid/count/template paths
        expected: complete authorized membership without duplicate rows
        """
        client = rls_client
        name, rows, metric = self._prepare_rls(client)
        expected = [r for r in rows if r["owner"] == principal]
        ids = sorted(r["id"] for r in expected)
        self.query(
            client,
            name,
            filter="id in {ids}",
            filter_params={"ids": [1, 2, 3, 4]},
            output_fields=FIELDS,
            rls_principal=principal,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": rls_values(expected), "with_vec": False},
        )
        count, _ = self.query(client, name, filter="id >= 0", output_fields=["count(*)"], rls_principal=principal)
        assert count == [{"count(*)": len(expected)}]
        iterator, _ = self.query_iterator(
            client, name, batch_size=1, filter="id >= 0", output_fields=FIELDS, rls_principal=principal
        )
        try:
            got = []
            for _ in range(len(rows) + 1):
                page = iterator.next()
                if not page:
                    break
                got.extend(page)
            else:
                pytest.fail("Query iterator did not reach EOF")
            assert sorted(got, key=lambda r: r["id"]) == rls_values(expected)
        finally:
            iterator.close()
        vector = rows[0]["vector"]
        iterator, _ = self.search_iterator(
            client,
            name,
            [vector],
            batch_size=1,
            output_fields=["owner"],
            rls_principal=principal,
            search_params={"metric_type": metric},
        )
        try:
            got_ids = []
            for _ in range(len(rows) + 1):
                page = iterator.next()
                if not page:
                    break
                got_ids.extend(hit.id for hit in page)
                assert all(hit.entity.get("owner") == principal for hit in page)
            else:
                pytest.fail("Search iterator did not reach EOF")
            assert sorted(got_ids) == ids
        finally:
            iterator.close()
        requests = [AnnSearchRequest([vector], "vector", {"metric_type": metric}, 10) for _ in range(2)]
        result, _ = self.hybrid_search(
            client, name, requests, RRFRanker(), limit=10, output_fields=["owner"], rls_principal=principal
        )
        assert sorted(hit["id"] for hit in result[0]) == ids
        assert all(hit["entity"]["owner"] == principal for hit in result[0])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["insert", "upsert", "partial_upsert"])
    @pytest.mark.parametrize("mixed_segments", [False, True], ids=["small", "mixed_segments"])
    def test_rls_rejected_batch_preserves_rows(self, rls_client, operation, mixed_segments):
        """
        target: USING/CHECK rejection and batch atomicity
        method: submit a mixed-owner mutation and inspect all stored fields
        expected: RLS error and no partial mutation
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client, mixed_segments=mixed_segments)
        batch = copy.deepcopy(
            [row for _, start, _ in SEGMENT_GROUPS for row in rows[start : start + 2]] if mixed_segments else rows[:2]
        )
        if operation == "insert":
            for row in batch:
                row["id"] += len(rows)
        else:
            for row in batch:
                row["note"] = "must_not_commit"
        method = self.insert if operation == "insert" else self.upsert
        method(
            client,
            name,
            batch,
            rls_principal="alice",
            **({"partial_update": True} if operation == "partial_upsert" else {}),
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        self._check_stored(client, name, rows)
        if mixed_segments:
            self._assert_mixed_segments(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "operation",
        ALL_ACTIONS + ["get", "count", "partial_upsert", "delete_filter"],
    )
    @pytest.mark.parametrize("mode", ["omitted", "empty", "blank", "no_policy", "forced_skip"])
    def test_rls_request_context_rejected(self, rls_client, operation, mode):
        """
        target: request admission on every public read/write path
        method: omit identity/policy or request bypass on a forced collection
        expected: explicit rejection, unchanged policy and complete stored data
        """
        client = rls_client
        # Admission errors do not depend on segment shape; use the small fixture.
        name, rows, metric = self._prepare_rls(client, policy=mode != "no_policy")
        context = {
            "omitted": {},
            "empty": {"rls_principal": ""},
            "blank": {"rls_principal": " \t"},
            "no_policy": {"rls_principal": "alice"},
            "forced_skip": {"rls_principal": "alice", "skip_rls": True},
        }[mode]
        message = {"no_policy": "no applicable", "forced_skip": "rls.force"}.get(mode, "rls_principal")
        if mode == "forced_skip":
            self.alter_collection_properties(client, name, {"rls.force": True})
        policies = client.list_row_policies(name, timeout=30)
        error = {"check_task": CheckTasks.err_res, "check_items": {ct.err_msg: message}}
        vector = rows[0]["vector"]
        if operation in ["query_iterator", "search_iterator"]:
            # The wrappers check construction only; denial may occur on the first next().
            iterator = None
            try:
                with pytest.raises(MilvusException, match=message):
                    if operation == "query_iterator":
                        iterator = client.query_iterator(name, batch_size=1, filter="id >= 0", **context)
                    else:
                        iterator = client.search_iterator(
                            name, [vector], batch_size=1, search_params={"metric_type": metric}, **context
                        )
                    iterator.next()
            finally:
                if iterator is not None:
                    iterator.close()
        elif operation in ["query", "count"]:
            fields = ["count(*)"] if operation == "count" else FIELDS
            self.query(client, name, filter="id >= 0", output_fields=fields, **context, **error)
        elif operation == "get":
            self.get(client, name, [1, 2, 3, 4], **context, **error)
        elif operation == "search":
            self.search(client, name, [vector], search_params={"metric_type": metric}, **context, **error)
        elif operation == "hybrid_search":
            requests = [AnnSearchRequest([vector], "vector", {"metric_type": metric}, 4)]
            self.hybrid_search(client, name, requests, RRFRanker(), limit=4, **context, **error)
        elif operation == "insert":
            self.insert(client, name, [{**rows[0], "id": 5}], **context, **error)
        elif operation == "upsert":
            self.upsert(client, name, [{**rows[0], "note": "must_not_commit"}], **context, **error)
        elif operation == "partial_upsert":
            self.upsert(client, name, [{"id": 1, "note": "must_not_commit"}], partial_update=True, **context, **error)
        else:
            selector = {"ids": [1]} if operation == "delete" else {"filter": "id == 1"}
            self.delete(client, name, **selector, **context, **error)
        assert client.list_row_policies(name, timeout=30) == policies
        # Restore an authorized observation path after checking admission failed.
        if mode == "forced_skip":
            self._check_rows(client, name, rows[::2], rls_principal="alice")
            self.alter_collection_properties(client, name, {"rls.force": False})
        elif mode == "no_policy":
            client.create_row_policy(name, "owner", "permissive", ALL_ACTIONS, OWNER, OWNER, timeout=30)
        self._check_stored(client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_policy_lifecycle_and_action_scope(self, rls_client):
        """
        target: policy CRUD, replacement and per-action defaults
        method: create/update/drop a query-only policy and inspect metadata
        expected: current policy enforced immediately; other actions denied
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client, policy=False)
        assert client.list_row_policies(name, timeout=30) == []
        self.query(
            client,
            name,
            filter="id >= 0",
            rls_principal="alice",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        client.create_row_policy(name, "reader", "permissive", "query", OWNER, description="initial", timeout=30)
        before = client.list_row_policies(name, timeout=30)
        assert len(before) == 1
        assert before[0]["using_expr"] == OWNER
        assert before[0]["description"] == "initial"
        with pytest.raises(MilvusException, match="already exists"):
            client.create_row_policy(name, "reader", "permissive", "query", "true", timeout=30)
        self._check_rows(client, name, rows[::2], rls_principal="alice")
        self.search(
            client,
            name,
            [rows[0]["vector"]],
            rls_principal="alice",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        self.delete(
            client, name, ids=[1], rls_principal="alice", check_task=CheckTasks.err_res, check_items={ct.err_msg: "RLS"}
        )
        client.update_row_policy(name, "reader", "permissive", "query", "department == 'eng'", timeout=30)
        after = client.list_row_policies(name, timeout=30)
        assert after[0]["policy_name"] == before[0]["policy_name"]
        assert after[0]["description"] == ""
        assert after[0]["check_expr"] == ""
        self._check_rows(client, name, rows[:2], rls_principal="alice")
        client.drop_row_policy(name, "reader", timeout=30)
        client.drop_row_policy(name, "reader", timeout=30)
        assert client.list_row_policies(name, timeout=30) == []
        self.query(
            client,
            name,
            filter="id >= 0",
            rls_principal="alice",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        self._check_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "kind,action",
        [
            ("permissive", "query"),
            ("RowPolicyTypePermissive", ["Query"]),
            (milvus_pb2.RowPolicyTypePermissive, [milvus_pb2.Query]),
        ],
        ids=["short_name", "enum_name", "enum_number"],
    )
    def test_rls_policy_enum_inputs(self, rls_client, kind, action):
        """
        target: SDK enum input normalization
        method: create a policy using each supported type/action representation
        expected: identical metadata and enforcement
        """
        name, rows, _ = self._prepare_rls(rls_client, policy=False)
        rls_client.create_row_policy(name, "reader", kind, action, OWNER, timeout=30)
        policy = rls_client.list_row_policies(name, timeout=30)[0]
        assert policy["policy_type"] == "RowPolicyTypePermissive"
        assert policy["actions"] == ["Query"]
        self._check_rows(rls_client, name, rows[::2], rls_principal="alice")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", ["missing == 1", "level > 1", "owner == department"])
    def test_rls_invalid_update_preserves_policy(self, rls_client, expression):
        """
        target: rejected policy replacement
        method: update a valid policy with an unsupported expression
        expected: original metadata and visible rows remain unchanged
        """
        name, rows, _ = self._prepare_rls(rls_client)
        before = rls_client.list_row_policies(name, timeout=30)
        with pytest.raises(MilvusException):
            rls_client.update_row_policy(name, "owner", "permissive", ALL_ACTIONS, expression, OWNER, timeout=30)
        assert rls_client.list_row_policies(name, timeout=30) == before
        self._check_stored(rls_client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["create", "update"])
    @pytest.mark.parametrize(
        "changes,message",
        [
            pytest.param({"actions": []}, "actions", id="empty_actions"),
            pytest.param({"actions": [999]}, "action", id="invalid_action"),
            pytest.param({"policy_type": True}, "type", id="boolean_type"),
            pytest.param({"actions": [True]}, "action", id="boolean_action"),
            pytest.param({"actions": ["query", "query"], "check_expr": ""}, "duplicated", id="duplicate_action"),
            pytest.param({"actions": ALL_ACTIONS + ["query"]}, "max count", id="too_many_actions"),
            pytest.param({"actions": ["upsert"], "check_expr": ""}, "check_expr is required", id="missing_check"),
            pytest.param({"actions": ["upsert"], "using_expr": ""}, "using_expr is required", id="missing_using"),
            pytest.param({"actions": ["query"]}, "check_expr is not used", id="unused_check"),
            pytest.param({"actions": ["insert"]}, "using_expr is not used", id="unused_using"),
            pytest.param({"check_expr": "missing_field == 1"}, "RLS", id="invalid_check_field"),
            pytest.param({"check_expr": "level > 1"}, "RLS", id="unsupported_check_operator"),
            # Length boundaries use the default admission quotas from milvus.yaml.
            pytest.param({"using_expr": " " * 4096 + OWNER}, "using_expr exceeds", id="using_too_long"),
            pytest.param({"check_expr": " " * 4096 + OWNER}, "check_expr exceeds", id="check_too_long"),
            pytest.param({"description": "d" * 1025}, "description exceeds", id="description_too_long"),
        ],
    )
    def test_rls_rejected_policy_definition_preserves_state(self, rls_client, operation, changes, message):
        """
        target: SDK normalization and server policy admission
        method: submit invalid create/update definitions on an existing collection
        expected: the original complete policy and data remain unchanged
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client)
        before = client.list_row_policies(name, timeout=30)
        definition = {"policy_type": "permissive", "actions": ALL_ACTIONS, "using_expr": OWNER, "check_expr": OWNER}
        definition.update(changes)
        method = client.create_row_policy if operation == "create" else client.update_row_policy
        with pytest.raises(MilvusException, match=message):
            method(name, "rejected" if operation == "create" else "owner", **definition, timeout=30)
        assert client.list_row_policies(name, timeout=30) == before
        self._check_stored(client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_tags_types_merge_and_delete(self, rls_client):
        """
        target: principal-tag JSON types and incremental updates
        method: set, read, merge, delete selected keys and delete all tags
        expected: scalar/array types retained and omitted keys preserved
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client)
        tags = {
            "department": "研发",
            "level": 3,
            "ratio": 3.0,
            "projects": ["a", "b"],
            "integers": [1, 2],
            "numbers": [1.5, 2.5],
            "empty": [],
        }
        client.set_rls_principal_tags(name, "alice", tags, timeout=30)
        actual = client.get_rls_principal_tags(name, "alice", timeout=30)
        assert actual == tags
        assert type(actual["level"]) is int
        assert type(actual["ratio"]) is float
        assert client.list_rls_principals(name, timeout=30) == ["alice"]
        client.set_rls_principal_tags(name, "alice", {"department": "sales"}, timeout=30)
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == {**tags, "department": "sales"}
        client.delete_rls_principal_tags(name, "alice", ["department", "missing", "department"], timeout=30)
        tags.pop("department")
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == tags
        client.delete_rls_principal_tags(name, "alice", timeout=30)
        client.delete_rls_principal_tags(name, "alice", timeout=30)
        assert client.list_rls_principals(name, timeout=30) == []
        with pytest.raises(MilvusException, match="does not exist"):
            client.get_rls_principal_tags(name, "alice", timeout=30)
        self._check_rows(client, name, rows[::2], rls_principal="alice")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "tags",
        [
            {},
            {"x": True},
            {"x": None},
            {"x": {"nested": 1}},
            {"x": [[1]]},
            {"x": float("nan")},
            {"x": float("inf")},
            {"x": ["a", 1]},
            {"x": [9007199254740993, 1.0]},
            {" ": "x"},
            {"a'b": "x"},
            {"k" * 129: "x"},
            {"x": "v" * 1025},
            {"x": ["v" * 1025]},
            {"x": [1] * 1025},
        ],
        ids=[
            "empty",
            "boolean",
            "null",
            "object",
            "nested_array",
            "nan",
            "infinity",
            "mixed_array",
            "lossy_array",
            "blank_key",
            "reserved_key",
            "key_too_long",
            "value_too_long",
            "array_element_too_long",
            "array_too_long",
        ],
    )
    def test_rls_invalid_tags_preserve_binding(self, rls_client, tags):
        """
        target: SDK/server tag validation
        method: submit an invalid tag map after storing a valid binding
        expected: rejection leaves both stored binding and query decisions unchanged
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client, policy=False)
        client.create_row_policy(
            name, "department", "permissive", "query", "department == $current_principal_tags['department']", timeout=30
        )
        original = {"department": "eng", "keep": "original"}
        client.set_rls_principal_tags(name, "alice", original, timeout=30)
        # A valid change in the same request must not be partially committed.
        payload = {"department": "sales", **tags} if tags else tags
        with pytest.raises(MilvusException, match="tag"):
            client.set_rls_principal_tags(name, "alice", payload, timeout=30)
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == original
        assert client.list_rls_principals(name, timeout=30) == ["alice"]
        self._check_rows(client, name, rows[:2], rls_principal="alice")
        self._check_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("key", ["", " \t", "a'b"], ids=["empty", "blank", "reserved"])
    def test_rls_rejected_tag_delete_preserves_binding(self, rls_client, key):
        """
        target: tag deletion validation before mutation
        method: delete a valid and an invalid key in the same request
        expected: both original tags and their query decisions are preserved
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client, policy=False)
        client.create_row_policy(
            name, "department", "permissive", "query", "department == $current_principal_tags['department']", timeout=30
        )
        original = {"department": "eng", "keep": "original"}
        client.set_rls_principal_tags(name, "alice", original, timeout=30)
        with pytest.raises(MilvusException, match="tag"):
            client.delete_rls_principal_tags(name, "alice", ["department", key], timeout=30)
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == original
        self._check_rows(client, name, rows[:2], rls_principal="alice")
        self._check_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("principal", ["", " \t", "p" * 256], ids=["empty", "blank", "too_long"])
    def test_rls_rejected_principal_creates_no_binding(self, rls_client, principal):
        """
        target: principal identifier validation
        method: set tags for an invalid principal alongside an existing binding
        expected: rejection creates no principal record and preserves existing data
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client)
        client.set_rls_principal_tags(name, "alice", {"keep": "original"}, timeout=30)
        with pytest.raises(MilvusException, match="principal"):
            client.set_rls_principal_tags(name, principal, {"keep": "changed"}, timeout=30)
        assert client.list_rls_principals(name, timeout=30) == ["alice"]
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == {"keep": "original"}
        self._check_stored(client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target", ["collection", "database"])
    @pytest.mark.parametrize(
        "operation",
        [
            "create_policy",
            "update_policy",
            "drop_policy",
            "list_policies",
            "set_tags",
            "get_tags",
            "delete_tags",
            "list_principals",
        ],
    )
    def test_rls_management_missing_target_preserves_state(self, rls_client, operation, target):
        """
        target: collection/database resolution for every RLS management API
        method: address a missing collection or database while valid metadata exists
        expected: not-found errors without changing the existing collection
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client)
        before = client.list_row_policies(name, timeout=30)
        client.set_rls_principal_tags(name, "alice", {"keep": "original"}, timeout=30)
        collection = cf.gen_unique_str("missing_rls") if target == "collection" else name
        database = cf.gen_unique_str("missing_rls") if target == "database" else "default"
        calls = {
            "create_policy": lambda: client.create_row_policy(
                collection, "owner", "permissive", "query", "true", db_name=database, timeout=30
            ),
            "update_policy": lambda: client.update_row_policy(
                collection, "owner", "permissive", "query", "true", db_name=database, timeout=30
            ),
            "drop_policy": lambda: client.drop_row_policy(collection, "owner", db_name=database, timeout=30),
            "list_policies": lambda: client.list_row_policies(collection, db_name=database, timeout=30),
            "set_tags": lambda: client.set_rls_principal_tags(
                collection, "alice", {"keep": "changed"}, db_name=database, timeout=30
            ),
            "get_tags": lambda: client.get_rls_principal_tags(collection, "alice", db_name=database, timeout=30),
            "delete_tags": lambda: client.delete_rls_principal_tags(collection, "alice", db_name=database, timeout=30),
            "list_principals": lambda: client.list_rls_principals(collection, db_name=database, timeout=30),
        }
        with pytest.raises(MilvusException, match="not found"):
            calls[operation]()
        assert client.list_row_policies(name, timeout=30) == before
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == {"keep": "original"}
        self._check_stored(client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "operation", ["invalid_enabled", "invalid_force", "disable_while_forced", "drop_enabled_while_forced"]
    )
    def test_rls_rejected_property_update_preserves_state(self, rls_client, operation):
        """
        target: validation of an enforcement-property transition
        method: submit invalid booleans or disable only the enabled flag while forced
        expected: original properties, metadata and effective enforcement survive
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client)
        client.set_rls_principal_tags(name, "alice", {"keep": "original"}, timeout=30)
        self.alter_collection_properties(client, name, {"rls.force": True})
        before, _ = self.describe_collection(client, name)
        policies = client.list_row_policies(name, timeout=30)
        error = {"check_task": CheckTasks.err_res, "check_items": {ct.err_msg: "rls."}}
        if operation == "drop_enabled_while_forced":
            self.drop_collection_properties(client, name, ["rls.enabled"], **error)
        else:
            properties = {
                "invalid_enabled": {"rls.enabled": "invalid"},
                "invalid_force": {"rls.force": "invalid"},
                "disable_while_forced": {"rls.enabled": False},
            }[operation]
            self.alter_collection_properties(client, name, properties, **error)
        after, _ = self.describe_collection(client, name)
        assert after["properties"] == before["properties"]
        assert client.list_row_policies(name, timeout=30) == policies
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == {"keep": "original"}
        self.query(client, name, filter="id >= 0", skip_rls=True, **error)
        self._check_rows(client, name, rows[::2], rls_principal="alice")
        self._check_rows(client, name, rows[1::2], rls_principal="bob")
        self.alter_collection_properties(client, name, {"rls.force": False})
        self._check_stored(client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mixed_segments", [False, True], ids=["small", "mixed_segments"])
    def test_rls_sharing_revocation_and_restrictive_policy(self, rls_client, mixed_segments):
        """
        target: project sharing and permissive/restrictive composition
        method: grant/revoke memberships and apply an active-row restriction
        expected: exact visible sets update after every policy/tag change
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client, mixed_segments=mixed_segments)
        client.create_row_policy(
            name,
            "project",
            "permissive",
            "query",
            "array_contains_any(projects, $current_principal_tags['projects'])",
            timeout=30,
        )
        self._check_rows(client, name, rows[::2], rls_principal="alice")
        client.set_rls_principal_tags(name, "alice", {"projects": ["a", "b"]}, timeout=30)
        self._check_rows(client, name, rows, rls_principal="alice")
        client.create_row_policy(name, "active", "restrictive", "query", "active == true", timeout=30)
        self._check_rows(client, name, [row for row in rows if row["active"]], rls_principal="alice")
        client.set_rls_principal_tags(name, "alice", {"projects": ["b"]}, timeout=30)
        self._check_rows(
            client,
            name,
            [row for row in rows if row["active"] and (row["owner"] == "alice" or "b" in row["projects"])],
            rls_principal="alice",
        )
        client.delete_rls_principal_tags(name, "alice", ["projects"], timeout=30)
        self._check_rows(
            client, name, [row for row in rows if row["active"] and row["owner"] == "alice"], rls_principal="alice"
        )
        client.drop_row_policy(name, "owner", timeout=30)
        client.drop_row_policy(name, "project", timeout=30)
        # Restrictive-only composition is false; it returns no rows.
        self._check_rows(client, name, [], rls_principal="alice")
        self._check_rows(client, name, rows, skip_rls=True)
        if mixed_segments:
            self._assert_mixed_segments(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_toggle_and_forced_enforcement(self, rls_client):
        """
        target: enforcement properties and explicit administrative bypass
        method: disable/re-enable enforcement, then require forced enforcement
        expected: metadata survives toggles and force rejects bypass
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client)
        client.set_rls_principal_tags(name, "alice", {"department": "eng"}, timeout=30)
        policies = client.list_row_policies(name, timeout=30)
        self.alter_collection_properties(client, name, {"rls.enabled": False})
        self._check_rows(client, name, rows)
        assert client.list_row_policies(name, timeout=30) == policies
        assert client.get_rls_principal_tags(name, "alice", timeout=30) == {"department": "eng"}
        self.alter_collection_properties(client, name, {"rls.enabled": True})
        self._check_stored(client, name, rows)
        self.alter_collection_properties(client, name, {"rls.force": True})
        self.query(
            client,
            name,
            filter="id >= 0",
            skip_rls=True,
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        self._check_rows(client, name, rows[::2], rls_principal="alice")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "value,expected_ids",
        [(1, [1]), (1.0, [1]), ("1", []), (1.5, [])],
        ids=["integer", "integral_double", "string_mismatch", "fractional_double"],
    )
    def test_rls_numeric_tag_binding(self, rls_client, value, expected_ids):
        """
        target: JSON numeric tags and exact scalar binding
        method: use different tag types against an integer policy field
        expected: only lossless numeric matches grant access
        """
        name, rows, _ = self._prepare_rls(rls_client, policy=False)
        rls_client.create_row_policy(
            name, "level", "permissive", "query", "level == $current_principal_tags['level']", timeout=30
        )
        self._check_rows(rls_client, name, [], rls_principal="alice")
        rls_client.set_rls_principal_tags(name, "alice", {"level": value}, timeout=30)
        self._check_rows(rls_client, name, [r for r in rows if r["id"] in expected_ids], rls_principal="alice")
        rls_client.delete_rls_principal_tags(name, "alice", timeout=30)
        self._check_rows(rls_client, name, [], rls_principal="alice")

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_explicit_management_database_isolation(self, rls_client, request):
        """
        target: database override in RLS request bodies and RPC metadata
        method: manage identically named collections in default and a second database
        expected: policies and principal tags change only in the requested database
        """
        client = rls_client
        name, rows, _ = self._prepare_rls(client)
        database = cf.gen_unique_str("rls_db")
        self.create_database(client, database)
        request.addfinalizer(lambda: client.drop_database(database, timeout=30))
        other = self._client(db_name=database)
        request.addfinalizer(other.close)
        request.addfinalizer(lambda: other.drop_collection(name, timeout=30))
        self._prepare_rls(other, name=name, policy=False, force_teardown=False)
        assert client.list_row_policies(name, db_name=database, timeout=30) == []
        client.create_row_policy(
            name,
            "department",
            "permissive",
            "query",
            "department == $current_principal_tags['department']",
            db_name=database,
            timeout=30,
        )
        client.set_rls_principal_tags(name, "alice", {"department": "eng"}, db_name=database, timeout=30)
        assert client.list_rls_principals(name, db_name=database, timeout=30) == ["alice"]
        assert client.get_rls_principal_tags(name, "alice", db_name=database, timeout=30) == {"department": "eng"}
        assert client.list_rls_principals(name, timeout=30) == []
        assert client.list_row_policies(name, timeout=30)[0]["policy_name"] == "owner"
        self._check_rows(other, name, rows[:2], rls_principal="alice")
        self._check_rows(client, name, rows[::2], rls_principal="alice")
        client.update_row_policy(name, "department", "permissive", "query", OWNER, db_name=database, timeout=30)
        self._check_rows(other, name, rows[::2], rls_principal="alice")
        client.delete_rls_principal_tags(name, "alice", db_name=database, timeout=30)
        assert client.list_rls_principals(name, db_name=database, timeout=30) == []
        client.drop_row_policy(name, "department", db_name=database, timeout=30)
        assert client.list_row_policies(name, db_name=database, timeout=30) == []
        self._check_stored(client, name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["empty_actions", "invalid_check", "mixed_tags", "invalid_delete_key"])
    @pytest.mark.asyncio
    async def test_rls_async_rejected_metadata_preserves_state(self, rls_client, operation):
        """
        target: async SDK and RPC rejection propagation
        method: reject malformed policies and tag mutations, then inspect via sync APIs
        expected: a typed error with unchanged policy, binding and stored data
        """
        name, rows, _ = self._prepare_rls(rls_client)
        before = rls_client.list_row_policies(name, timeout=30)
        tags = {"department": "eng", "keep": "original"}
        rls_client.set_rls_principal_tags(name, "alice", tags, timeout=30)
        self.init_async_milvus_client()
        wrapper = self.async_milvus_client_wrap
        client = wrapper.async_milvus_client
        try:
            message = {"empty_actions": "actions", "invalid_check": "RLS"}.get(operation, "tag")
            with pytest.raises(MilvusException, match=message):
                if operation == "empty_actions":
                    await client.update_row_policy(name, "owner", "permissive", [], OWNER, OWNER, timeout=30)
                elif operation == "invalid_check":
                    await client.update_row_policy(
                        name, "owner", "permissive", ALL_ACTIONS, OWNER, "missing_field == 1", timeout=30
                    )
                elif operation == "mixed_tags":
                    await client.set_rls_principal_tags(
                        name, "alice", {"department": "sales", "bad": ["a", 1]}, timeout=30
                    )
                else:
                    await client.delete_rls_principal_tags(name, "alice", ["department", ""], timeout=30)
            assert rls_client.list_row_policies(name, timeout=30) == before
            assert rls_client.get_rls_principal_tags(name, "alice", timeout=30) == tags
            self._check_stored(rls_client, name, rows)
        finally:
            await wrapper.close()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.asyncio
    async def test_rls_async_management_and_reads(self, rls_client):
        """
        target: AsyncMilvusClient policy/tag APIs and read context
        method: create/update/drop policies, manage tags and query/get/search/hybrid
        expected: sync and async clients observe the same metadata and rows
        """
        name, rows, metric = self._prepare_rls(rls_client, policy=False)
        self.init_async_milvus_client()
        wrapper = self.async_milvus_client_wrap
        client = wrapper.async_milvus_client
        try:
            await client.create_row_policy(name, "owner", "permissive", ALL_ACTIONS, OWNER, OWNER, timeout=30)
            policies = await client.list_row_policies(name, timeout=30)
            assert policies == rls_client.list_row_policies(name, timeout=30)
            await client.set_rls_principal_tags(name, "alice", {"projects": ["a"], "level": 3}, timeout=30)
            assert await client.get_rls_principal_tags(name, "alice", timeout=30) == {"projects": ["a"], "level": 3}
            assert await client.list_rls_principals(name, timeout=30) == ["alice"]
            result, _ = await wrapper.query(name, filter="id >= 0", output_fields=FIELDS, rls_principal="alice")
            assert sorted(result, key=lambda r: r["id"]) == rls_values(rows[::2])
            result, _ = await wrapper.get(name, [1, 2, 3, 4], output_fields=FIELDS, rls_principal="bob")
            assert sorted(result, key=lambda r: r["id"]) == rls_values(rows[1::2])
            result, _ = await wrapper.search(
                name, [rows[0]["vector"]], limit=10, rls_principal="alice", search_params={"metric_type": metric}
            )
            assert sorted(hit["id"] for hit in result[0]) == [1, 3]
            requests = [AnnSearchRequest([rows[0]["vector"]], "vector", {"metric_type": metric}, 10)]
            result, _ = await wrapper.hybrid_search(name, requests, RRFRanker(), limit=10, rls_principal="bob")
            assert sorted(hit["id"] for hit in result[0]) == [2, 4]
            await client.update_row_policy(name, "owner", "permissive", "query", "department == 'eng'", timeout=30)
            self._check_rows(rls_client, name, rows[:2], rls_principal="alice")
            await client.delete_rls_principal_tags(name, "alice", ["projects"], timeout=30)
            assert await client.get_rls_principal_tags(name, "alice", timeout=30) == {"level": 3}
            await client.delete_rls_principal_tags(name, "alice", timeout=30)
            assert await client.list_rls_principals(name, timeout=30) == []
            await client.drop_row_policy(name, "owner", timeout=30)
            assert await client.list_row_policies(name, timeout=30) == []
            await wrapper.query(
                name,
                filter="id >= 0",
                rls_principal="alice",
                check_task=CheckTasks.err_res,
                check_items={ct.err_msg: "RLS"},
            )
        finally:
            await wrapper.close()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("principal", ["alice", "bob"])
    @pytest.mark.asyncio
    async def test_rls_async_mixed_segments(self, rls_client, principal):
        """
        target: async reads and writes with both owners in every segment shape
        method: query/get/search/hybrid all groups and mutate distinct original rows
        expected: exact visible rows and all unrelated stored values preserved
        """
        name, rows, metric = self._prepare_rls(rls_client, mixed_segments=True)
        sealed_ids = self._assert_mixed_segments(rls_client, name)
        self.init_async_milvus_client()
        wrapper = self.async_milvus_client_wrap
        expected = [row for row in rows if row["owner"] == principal]
        sentinels = [row for _, start, _ in SEGMENT_GROUPS for row in rows[start : start + 8]]
        selected = [row for row in sentinels if row["owner"] == principal]
        expression = f"id in {[row['id'] for row in sentinels]}"
        params = {"metric_type": metric, "params": {"nprobe": 64}}
        try:
            result, _ = await wrapper.query(name, filter="owner != ''", output_fields=FIELDS, rls_principal=principal)
            assert sorted(result, key=lambda row: row["id"]) == rls_values(expected)
            result, _ = await wrapper.get(
                name, [row["id"] for row in rows], output_fields=FIELDS, rls_principal=principal
            )
            assert sorted(result, key=lambda row: row["id"]) == rls_values(expected)
            result, _ = await wrapper.search(
                name,
                [rows[0]["vector"]],
                filter=expression,
                search_params=params,
                limit=24,
                output_fields=FIELDS,
                rls_principal=principal,
            )
            assert sorted((hit["entity"] for hit in result[0]), key=lambda row: row["id"]) == rls_values(selected)
            requests = [AnnSearchRequest([rows[0]["vector"]], "vector", params, 24, expr=expression)]
            result, _ = await wrapper.hybrid_search(
                name, requests, RRFRanker(), limit=24, output_fields=FIELDS, rls_principal=principal
            )
            assert sorted((hit["entity"] for hit in result[0]), key=lambda row: row["id"]) == rls_values(selected)
            expected_by_id = {row["id"]: copy.deepcopy(row) for row in rows}
            remaining_sealed = {row["id"]: row for row in rows[:3500]}
            owner_offset = 0 if principal == "alice" else 1
            for offset, operation in enumerate(["upsert", "partial_upsert", "delete_ids", "delete_filter"]):
                self._assert_mixed_segments(rls_client, name, sealed_ids)
                targets = [rows[start + offset * 2 + owner_offset] for _, start, _ in SEGMENT_GROUPS]
                ids = [row["id"] for row in targets]
                if operation == "upsert":
                    vectors = cf.gen_vectors(len(targets), VECTOR_DIM)
                    changed = [
                        {**copy.deepcopy(row), "note": "async full", "vector": vector}
                        for row, vector in zip(targets, vectors)
                    ]
                    await wrapper.upsert(name, changed, rls_principal=principal)
                    expected_by_id.update({row["id"]: row for row in changed})
                elif operation == "partial_upsert":
                    await wrapper.upsert(
                        name,
                        [{"id": row_id, "note": "async partial"} for row_id in ids],
                        partial_update=True,
                        rls_principal=principal,
                    )
                    for row_id in ids:
                        expected_by_id[row_id]["note"] = "async partial"
                else:
                    await wrapper.delete(
                        name,
                        **({"ids": ids} if operation == "delete_ids" else {"filter": f"id in {ids}"}),
                        rls_principal=principal,
                    )
                    for row_id in ids:
                        del expected_by_id[row_id]
                self._check_stored(rls_client, name, list(expected_by_id.values()))
                for row_id in ids:
                    remaining_sealed.pop(row_id, None)
                self._check_rows(rls_client, name, list(remaining_sealed.values()), skip_rls=True, ignore_growing=True)
            fresh = {
                **copy.deepcopy(expected[0]),
                "id": 4001,
                "vector": cf.gen_vectors(1, VECTOR_DIM)[0],
            }
            await wrapper.insert(name, [fresh], rls_principal=principal)
            expected_by_id[fresh["id"]] = fresh
            self._check_stored(rls_client, name, list(expected_by_id.values()))
            self._assert_mixed_segments(rls_client, name, sealed_ids)
        finally:
            await wrapper.close()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.asyncio
    async def test_rls_async_mutations_and_delete_status(self, rls_client):
        """
        target: async mutation context and delete Status propagation
        method: accept own writes, reject mixed batches and deny an ungranted delete
        expected: explicit errors and exact persisted state after every operation
        """
        name, rows, _ = self._prepare_rls(rls_client)
        self.init_async_milvus_client()
        wrapper = self.async_milvus_client_wrap
        client = wrapper.async_milvus_client
        try:
            fresh = copy.deepcopy(rows[0])
            fresh["id"] = 5
            fresh["vector"] = cf.gen_vectors(1, VECTOR_DIM)[0]
            result, _ = await wrapper.insert(name, [fresh], rls_principal="alice")
            assert result["insert_count"] == 1
            rows.append(fresh)
            self._check_stored(rls_client, name, rows)
            fresh = {**fresh, "note": "async update"}
            await wrapper.upsert(name, [fresh], rls_principal="alice")
            rows[-1] = fresh
            self._check_stored(rls_client, name, rows)
            await wrapper.insert(
                name,
                [{**rows[0], "id": 6}, {**rows[1], "id": 7}],
                rls_principal="alice",
                check_task=CheckTasks.err_res,
                check_items={ct.err_msg: "RLS"},
            )
            self._check_stored(rls_client, name, rows)
            await client.update_row_policy(
                name, "owner", "permissive", READ_ACTIONS + ["insert", "upsert"], OWNER, OWNER, timeout=30
            )
            await wrapper.delete(
                name, ids=[1], rls_principal="alice", check_task=CheckTasks.err_res, check_items={ct.err_msg: "RLS"}
            )
            self._check_stored(rls_client, name, rows)
            await client.update_row_policy(name, "owner", "permissive", ALL_ACTIONS, OWNER, OWNER, timeout=30)
            await wrapper.delete(name, ids=[5], rls_principal="alice")
            self._check_stored(rls_client, name, rows[:-1])
        finally:
            await wrapper.close()

    def _prepare_rls_spaces(self, client, enabled=True, configure=True, legacy=False):
        """Synthetic tenants and resources; each resource has chunks in all segment forms."""
        name = cf.gen_unique_str("rls_spaces")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        for field in ["resource", "content"] if legacy else ["tenant", "resource", "content"]:
            self.add_field(schema, field, DataType.VARCHAR, max_length=128)
        for field, capacity in [] if legacy else [("spaces", 1), ("access", 16)]:
            self.add_field(
                schema, field, DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=capacity, max_length=128
            )
        self.add_field(schema, "vector", DataType.FLOAT_VECTOR, dim=VECTOR_DIM)
        self.create_collection(
            client,
            name,
            schema=schema,
            num_shards=1,
            consistency_level="Strong",
            properties={"rls.enabled": enabled, "collection.autocompaction.enabled": "false"},
        )
        locations = [
            ("tenant_0", "common"),
            ("tenant_0", "red"),
            ("tenant_0", "blue"),
            ("tenant_1", "red"),
            ("tenant_2", "personal"),
        ]
        vectors = cf.gen_vectors(4000, VECTOR_DIM)
        assert len({np.asarray(v, dtype=np.float32).tobytes() for v in vectors}) == 4000
        rows = []
        for i, vector in enumerate(vectors):
            tenant, space = locations[i % 5]
            rows.append(
                {
                    "id": i + 1,
                    "tenant": tenant,
                    "spaces": [f"{tenant}/{space}"],
                    "resource": f"resource_{i % 5}",
                    "access": [],
                    "content": f"chunk_{i}",
                    "vector": vector,
                }
            )
        if legacy:
            rows = [
                {key: value for key, value in row.items() if key not in ["tenant", "spaces", "access"]} for row in rows
            ]
        for _, start, count in SEGMENT_GROUPS[:2]:
            self.insert(client, name, rows[start : start + count], skip_rls=True)
            self.flush(client, name)
        params = client.prepare_index_params()
        params.add_index("vector", index_type="IVF_FLAT", metric_type="L2", params={"nlist": 64})
        self.create_index(client, name, params)
        assert self.wait_for_index_ready(client, name, "vector", timeout=180)
        self.load_collection(client, name)
        self.insert(client, name, rows[3500:], skip_rls=True)
        self._assert_mixed_segments(client, name)
        self._check_space_rows(client, name, rows, skip_rls=True)
        self._check_space_rows(client, name, rows[:3500], skip_rls=True, ignore_growing=True)
        if configure:
            self._configure_rls_spaces(client, name)
        return name, rows

    def _configure_rls_spaces(self, client, name):
        wall = "tenant == $current_principal_tags['tenant']"
        write = "array_contains_any(spaces, $current_principal_tags['write_spaces'])"
        client.create_row_policy(name, "tenant_wall", "restrictive", ALL_ACTIONS, wall, wall)
        client.create_row_policy(name, "space_writer", "permissive", ALL_ACTIONS, write, write)
        client.create_row_policy(
            name,
            "space_reader",
            "permissive",
            READ_ACTIONS,
            "array_contains_any(spaces, $current_principal_tags['read_spaces'])",
        )
        client.create_row_policy(
            name, "direct_reader", "permissive", READ_ACTIONS, "array_contains(access, $current_principal)"
        )
        client.create_row_policy(
            name,
            "group_reader",
            "permissive",
            READ_ACTIONS,
            "array_contains_any(access, $current_principal_tags['groups'])",
        )
        members = {
            "member_0": ("tenant_0", ["tenant_0/common"], ["tenant_0/red"]),
            "member_1": ("tenant_0", ["tenant_0/common"], ["tenant_0/blue"]),
            "member_2": ("tenant_0", ["tenant_0/common"], ["tenant_0/red", "tenant_0/blue"]),
            "member_3": ("tenant_1", [], ["tenant_1/red"]),
            "visitor": ("tenant_0", ["tenant_0/common", "tenant_0/red"], []),
            "personal": ("tenant_2", [], ["tenant_2/personal"]),
        }
        for principal, (tenant, read, write) in members.items():
            client.set_rls_principal_tags(
                name, principal, {"tenant": tenant, "read_spaces": read, "write_spaces": write}
            )

    def _check_space_rows(self, client, name, expected, **context):
        result, _ = self.query(client, name, filter="id >= 0", output_fields=["*"], **context)
        assert len(result) == len(expected)
        expected_by_id = {row["id"]: row for row in expected}
        assert sorted(row["id"] for row in result) == sorted(expected_by_id)
        for row in result:
            want = expected_by_id[row["id"]]
            assert {key: value for key, value in row.items() if key != "vector"} == {
                key: value for key, value in want.items() if key != "vector"
            }
            np.testing.assert_array_equal(row["vector"], np.asarray(want["vector"], dtype=np.float32))

    def _check_space_reads(self, client, name, rows, principal, resources):
        """Expected resource sets are explicit, independent of policy expression evaluation."""
        expected = [row for row in rows if row["resource"] in resources]
        ids = sorted(row["id"] for row in expected)
        self._check_space_rows(client, name, expected, rls_principal=principal)
        count, _ = self.query(client, name, filter="id >= 0", output_fields=["count(*)"], rls_principal=principal)
        assert count == [{"count(*)": len(ids)}]
        got, _ = self.get(
            client, name, [row["id"] for row in rows], output_fields=["resource"], rls_principal=principal
        )
        assert sorted(row["id"] for row in got) == ids
        # A small explicit selection spans every resource in every segment form.
        # Full Query/QueryIterator above/below validate every chunk of each resource.
        selected = [row for row in rows if any(start < row["id"] <= start + 10 for _, start, _ in SEGMENT_GROUPS)]
        expr = f"id in {[row['id'] for row in selected]}"
        selected_ids = sorted(row["id"] for row in selected if row["resource"] in resources)
        params = {"metric_type": "L2", "params": {"nprobe": 64}}
        vector = rows[0]["vector"]
        result, _ = self.search(
            client,
            name,
            [vector],
            filter=expr,
            limit=30,
            search_params=params,
            output_fields=["resource"],
            rls_principal=principal,
        )
        assert sorted(hit["id"] for hit in result[0]) == selected_ids
        requests = [
            AnnSearchRequest([vector], "vector", params, 30, expr=expr),
            AnnSearchRequest([vector], "vector", params, 30, expr=f"({expr}) and tenant == 'tenant_1'"),
        ]
        result, _ = self.hybrid_search(
            client, name, requests, RRFRanker(), limit=30, output_fields=["resource"], rls_principal=principal
        )
        assert sorted(hit["id"] for hit in result[0]) == selected_ids
        for operation in ["query_iterator", "search_iterator"]:
            if operation == "query_iterator":
                iterator, _ = self.query_iterator(
                    client, name, filter="id >= 0", batch_size=127, output_fields=["id"], rls_principal=principal
                )
                want = ids
            else:
                iterator, _ = self.search_iterator(
                    client, name, [vector], filter=expr, batch_size=7, search_params=params, rls_principal=principal
                )
                want = selected_ids
            try:
                got = []
                for _ in range(len(rows) // 7 + 2):
                    page = iterator.next()
                    if not page:
                        break
                    got.extend(row["id"] if operation == "query_iterator" else row.id for row in page)
                else:
                    pytest.fail(f"{operation} did not terminate")
                assert sorted(got) == want, (principal, operation)
            finally:
                iterator.close()

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_space_read_matrix(self, rls_client):
        """
        target: tenant walls, public readers, space writers and private personal spaces
        method: exercise every read API with all identities in three segment forms
        expected: exact resource and chunk membership, including an empty unknown identity
        """
        name, rows = self._prepare_rls_spaces(rls_client)
        for principal, resources in {
            "member_0": ["resource_0", "resource_1"],
            "member_1": ["resource_0", "resource_2"],
            "member_2": ["resource_0", "resource_1", "resource_2"],
            "member_3": ["resource_3"],
            "visitor": ["resource_0", "resource_1"],
            "personal": ["resource_4"],
            "unknown": [],
        }.items():
            self._check_space_reads(rls_client, name, rows, principal, resources)
        # A foreign space accidentally present in a membership list cannot cross the tenant wall.
        rls_client.set_rls_principal_tags(name, "member_0", {"write_spaces": ["tenant_0/red", "tenant_1/red"]})
        self._check_space_reads(rls_client, name, rows, "member_0", ["resource_0", "resource_1"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["insert", "upsert", "partial_upsert", "delete_ids", "delete_filter", "move"])
    def test_rls_space_write_matrix(self, rls_client, operation):
        """
        target: separate read/write memberships and old/new space checks
        method: mutate chunks in every segment as each member and a read-only visitor
        expected: exact accepted data; rejected writes preserve every stored field
        """
        client = rls_client
        name, original = self._prepare_rls_spaces(client)
        expected = copy.deepcopy(original)
        principals = ["member_0", "member_1", "member_2", "member_3", "visitor", "unknown"]
        for position, principal in enumerate(principals):
            # Use untouched chunks for each identity; restoring rows would move them to growing segments.
            target_ids = [2 + position * 5 + start for _, start, _ in SEGMENT_GROUPS]
            targets = [row for row in original if row["id"] in target_ids]
            if operation == "move":
                targets = [row for row in original if row["resource"] == "resource_1"]
            changed = [{**copy.deepcopy(row), "content": "updated"} for row in targets]
            allowed = principal in (["member_1", "member_2"] if operation == "insert" else ["member_0", "member_2"])
            if operation == "insert":
                for row, vector in zip(changed, cf.gen_vectors(len(changed), VECTOR_DIM)):
                    row.update(id=row["id"] + 10000, spaces=["tenant_0/blue"], vector=vector)
            elif operation == "move":
                allowed = principal == "member_2"
                for row in changed:
                    row["spaces"] = ["tenant_0/blue"]
            error = {} if allowed else {"check_task": CheckTasks.err_res, "check_items": {ct.err_msg: "RLS"}}
            if operation.startswith("delete"):
                ids = [row["id"] for row in targets] + [1, 3001, 3501]
                result, _ = self.delete(
                    client,
                    name,
                    **({"ids": ids} if operation == "delete_ids" else {"filter": f"id in {ids}"}),
                    rls_principal=principal,
                )
                assert result["delete_count"] == (len(targets) if allowed else 0)
                if allowed:
                    expected = [row for row in expected if row["id"] not in {r["id"] for r in targets}]
            else:
                method = self.insert if operation == "insert" else self.upsert
                payload = (
                    [{"id": row["id"], "content": row["content"]} for row in changed]
                    if operation == "partial_upsert"
                    else changed
                )
                method(
                    client,
                    name,
                    payload,
                    rls_principal=principal,
                    **({"partial_update": True} if operation == "partial_upsert" else {}),
                    **error,
                )
                if allowed:
                    if operation == "insert":
                        expected.extend(changed)
                    else:
                        replacements = {row["id"]: row for row in changed}
                        expected = [replacements.get(row["id"], row) for row in expected]
            self._check_space_rows(client, name, expected, skip_rls=True)
            if operation == "move" and allowed:
                for reader, resources in [
                    ("member_0", ["resource_0"]),
                    ("member_1", ["resource_0", "resource_1", "resource_2"]),
                ]:
                    self._check_space_reads(client, name, expected, reader, resources)
            if operation == "insert" and allowed:
                self.delete(client, name, ids=[row["id"] for row in changed], skip_rls=True)
                expected = [row for row in expected if row["id"] <= 4000]
            if operation == "move" and allowed:
                self.upsert(client, name, targets, skip_rls=True)
                expected = copy.deepcopy(original)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["insert", "upsert", "partial_upsert"])
    def test_rls_space_invalid_destination_preserves_rows(self, rls_client, operation):
        """
        target: single-space schema and restrictive CHECK admission
        method: write multiple spaces, no space, a foreign tenant or a read-only public space
        expected: rejection without partial changes to any resource
        """
        client = rls_client
        name, rows = self._prepare_rls_spaces(client)
        for changes in [
            {"spaces": ["tenant_0/red", "tenant_0/blue"]},
            {"spaces": []},
            {"tenant": "tenant_1"},
            {"spaces": ["tenant_0/common"]},
        ]:
            changed = [{**copy.deepcopy(rows[i]), **changes} for i in [1, 3001, 3501]]
            if operation == "insert":
                for row in changed:
                    row["id"] += 10000
            method = self.insert if operation == "insert" else self.upsert
            # Array cardinality may be rejected by either SDK or Server; both must preserve data.
            method(
                client,
                name,
                changed,
                rls_principal="member_0",
                **({"partial_update": True} if operation == "partial_upsert" else {}),
                check_task=CheckTasks.err_res,
                check_items={ct.err_msg: "capacity" if len(changes.get("spaces", [])) > 1 else "RLS"},
            )
            self._check_space_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_space_membership_lifecycle(self, rls_client):
        """
        target: membership replacement, downgrade, public editing and removal
        method: change tags while keeping resource rows and policies unchanged
        expected: acknowledged memberships immediately control reads and writes
        """
        client = rls_client
        name, rows = self._prepare_rls_spaces(client)
        policies = client.list_row_policies(name)
        for write, resources in [
            ([], ["resource_0"]),
            (["tenant_0/red"], ["resource_0", "resource_1"]),
            (["tenant_0/red", "tenant_0/blue"], ["resource_0", "resource_1", "resource_2"]),
            (["tenant_0/blue"], ["resource_0", "resource_2"]),
            ([], ["resource_0"]),
        ]:
            for _ in range(2):
                client.set_rls_principal_tags(name, "member_0", {"write_spaces": write})
            assert client.get_rls_principal_tags(name, "member_0") == {
                "tenant": "tenant_0",
                "read_spaces": ["tenant_0/common"],
                "write_spaces": write,
            }
            self._check_space_reads(client, name, rows, "member_0", resources)
            self.upsert(
                client,
                name,
                [{"id": 2, "content": rows[1]["content"]}],
                partial_update=True,
                rls_principal="member_0",
                **(
                    {}
                    if "tenant_0/red" in write
                    else {"check_task": CheckTasks.err_res, "check_items": {ct.err_msg: "RLS"}}
                ),
            )
        client.set_rls_principal_tags(name, "member_0", {"write_spaces": ["tenant_0/common"]})
        self.upsert(client, name, [rows[0]], rls_principal="member_0")
        client.set_rls_principal_tags(name, "member_0", {"write_spaces": []})
        self.upsert(
            client,
            name,
            [rows[0]],
            rls_principal="member_0",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        client.delete_rls_principal_tags(name, "member_0")
        self._check_space_reads(client, name, rows, "member_0", [])
        self.insert(
            client,
            name,
            [{**rows[1], "id": 5001}],
            rls_principal="member_0",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        assert client.list_row_policies(name) == policies
        self._check_space_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_space_add_nullable_fields_existing_data(self, rls_client):
        """
        target: migrate a legacy collection without permission fields
        method: add nullable fields, partially backfill, synchronize policies, then enable RLS
        expected: NULL is not an implicit grant; backfill changes visibility without losing content
        """
        client = rls_client
        name, rows = self._prepare_rls_spaces(client, enabled=False, configure=False, legacy=True)
        description, _ = self.describe_collection(client, name)
        assert not {"tenant", "spaces", "access"} & {field["name"] for field in description["fields"]}
        self.add_collection_field(client, name, "tenant", DataType.VARCHAR, max_length=128, nullable=True)
        for field, capacity in [("spaces", 1), ("access", 16)]:
            self.add_collection_field(
                client,
                name,
                field,
                DataType.ARRAY,
                element_type=DataType.VARCHAR,
                max_length=128,
                max_capacity=capacity,
                nullable=True,
            )
        self.release_collection(client, name)
        self.load_collection(client, name)
        description, _ = self.describe_collection(client, name)
        for field in description["fields"]:
            if field["name"] in ["tenant", "spaces", "access"]:
                assert field["nullable"] is True
        rows = [{**row, "tenant": None, "spaces": None, "access": None} for row in rows]
        self._check_space_rows(client, name, rows)
        # Schema changes flush growing segments; all original rows are now sealed.
        loaded = client.list_loaded_segments(name)
        assert sorted(segment.num_rows for segment in loaded) == [500, 500, 3000]
        assert all(segment.state == SegmentState.Sealed for segment in loaded)
        self._check_space_rows(client, name, rows, ignore_growing=True)

        # Backfill creates new row versions. Untouched legacy chunks retain implicit NULLs.
        values = {
            "resource_1": {"tenant": "tenant_0", "spaces": ["tenant_0/red"], "access": None},
            "resource_2": {"tenant": "tenant_0", "spaces": None, "access": ["member_1"]},
            "resource_3": {"tenant": None, "spaces": ["tenant_0/red"], "access": ["member_0"]},
            "resource_4": {"tenant": "tenant_0", "spaces": [], "access": []},
        }
        updates = [{"id": row["id"], **values[row["resource"]]} for row in rows if row["resource"] in values]
        self.upsert(client, name, updates, partial_update=True)
        rows = [{**row, **values.get(row["resource"], {})} for row in rows]
        self._check_space_rows(client, name, rows)
        self._configure_rls_spaces(client, name)
        self._check_space_rows(client, name, rows, rls_principal="unknown")
        self.alter_collection_properties(client, name, {"rls.enabled": True})
        for principal, resources in {"member_0": ["resource_1"], "member_1": ["resource_2"], "unknown": []}.items():
            self._check_space_reads(client, name, rows, principal, resources)

        # NULL access does not block a space writer. NULL tenant/space fails its write checks.
        for field in ["tenant", "spaces"]:
            self.upsert(
                client,
                name,
                [{"id": 2, field: None}],
                partial_update=True,
                rls_principal="member_0",
                check_task=CheckTasks.err_res,
                check_items={ct.err_msg: "RLS"},
            )
            fresh = {**rows[1], "id": 5001, field: None, "vector": cf.gen_vectors(1, VECTOR_DIM)[0]}
            self.insert(
                client,
                name,
                [fresh],
                rls_principal="member_0",
                check_task=CheckTasks.err_res,
                check_items={ct.err_msg: "RLS"},
            )
            self._check_space_rows(client, name, rows, skip_rls=True)
        omitted = {key: value for key, value in fresh.items() if key not in ["tenant", "spaces", "access"]}
        self.insert(
            client,
            name,
            [omitted],
            rls_principal="member_0",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        fresh = {**rows[1], "id": 5001, "vector": cf.gen_vectors(1, VECTOR_DIM)[0]}
        self.insert(client, name, [fresh], rls_principal="member_0")
        self.upsert(client, name, [{**fresh, "content": "full_update"}], rls_principal="member_0")
        self.upsert(
            client,
            name,
            [{"id": 5001, "content": "partial_update", "access": None}],
            partial_update=True,
            rls_principal="member_0",
        )
        self._check_space_rows(client, name, rows + [{**fresh, "content": "partial_update"}], skip_rls=True)
        result, _ = self.delete(client, name, ids=[5001, 1, 3001, 3501], rls_principal="member_0")
        assert result["delete_count"] == 1
        self._check_space_rows(client, name, rows, skip_rls=True)

        # A trusted migration job can resume backfill after enforcement is enabled.
        updates = [
            {"id": row["id"], "tenant": "tenant_0", "spaces": ["tenant_0/red"]}
            for row in rows
            if row["resource"] == "resource_0"
        ]
        self.upsert(client, name, updates, partial_update=True, skip_rls=True)
        rows = [
            {**row, "tenant": "tenant_0", "spaces": ["tenant_0/red"]} if row["resource"] == "resource_0" else row
            for row in rows
        ]
        self._check_space_reads(client, name, rows, "member_0", ["resource_0", "resource_1"])
        self._check_space_reads(client, name, rows, "member_1", ["resource_2"])
        self.release_collection(client, name)
        self.load_collection(client, name)
        self._check_space_reads(client, name, rows, "member_0", ["resource_0", "resource_1"])
        self._check_space_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_space_resource_sharing_lifecycle(self, rls_client):
        """
        target: direct and group sharing over every chunk of one resource
        method: grant two independent read routes, revoke them separately, and remove membership
        expected: readers cannot edit ACLs and the final revocation hides all chunks
        """
        client = rls_client
        name, rows = self._prepare_rls_spaces(client)
        shared = [{**row, "access": ["member_1", "group_0"]} for row in rows if row["resource"] == "resource_1"]
        self.upsert(
            client,
            name,
            shared,
            rls_principal="member_1",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        self._check_space_rows(client, name, rows, skip_rls=True)
        self.upsert(client, name, shared, rls_principal="member_0")
        replacements = {row["id"]: row for row in shared}
        rows = [replacements.get(row["id"], row) for row in rows]
        client.set_rls_principal_tags(name, "member_1", {"groups": ["group_0"]})
        self._check_space_reads(client, name, rows, "member_1", ["resource_0", "resource_1", "resource_2"])
        self.upsert(
            client,
            name,
            [{"id": 2, "access": ["visitor"]}],
            partial_update=True,
            rls_principal="member_1",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        shared = [{**row, "access": ["group_0"]} for row in shared]
        self.upsert(client, name, shared, rls_principal="member_0")
        replacements = {row["id"]: row for row in shared}
        rows = [replacements.get(row["id"], row) for row in rows]
        self._check_space_reads(client, name, rows, "member_1", ["resource_0", "resource_1", "resource_2"])
        client.delete_rls_principal_tags(name, "member_1", ["groups"])
        self._check_space_reads(client, name, rows, "member_1", ["resource_0", "resource_2"])
        client.set_rls_principal_tags(name, "member_1", {"groups": ["group_0"]})
        client.delete_rls_principal_tags(name, "member_1")
        self._check_space_reads(client, name, rows, "member_1", [])
        self._check_space_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("tenant_wall", [True, False], ids=["tenant_scoped", "cross_tenant"])
    def test_rls_space_link_lifecycle(self, rls_client, tenant_wall):
        """
        target: read-only link identities and their revocation contract
        method: grant a synthetic link; revoke its tags or row ACL according to the policy model
        expected: exact linked chunks, no writes, and effective final revocation
        """
        client = rls_client
        name, rows = self._prepare_rls_spaces(client)
        link = "link_0"
        client.set_rls_principal_tags(name, link, {"tenant": "tenant_0"})
        shared = [{**row, "access": [link]} for row in rows if row["resource"] in ["resource_1", "resource_3"]]
        self.upsert(client, name, shared, skip_rls=True)
        replacements = {row["id"]: row for row in shared}
        rows = [replacements.get(row["id"], row) for row in rows]
        if not tenant_wall:
            client.drop_row_policy(name, "tenant_wall")
            client.set_rls_principal_tags(name, "global_member", {"read_spaces": ["tenant_0/red", "tenant_1/red"]})
            self._check_space_reads(client, name, rows, "global_member", ["resource_1", "resource_3"])
        expected = ["resource_1"] if tenant_wall else ["resource_1", "resource_3"]
        self._check_space_reads(client, name, rows, link, expected)
        self.insert(
            client,
            name,
            [{**rows[1], "id": 5001}],
            rls_principal=link,
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        self.upsert(
            client, name, [rows[1]], rls_principal=link, check_task=CheckTasks.err_res, check_items={ct.err_msg: "RLS"}
        )
        result, _ = self.delete(client, name, filter="id >= 0", rls_principal=link)
        assert result["delete_count"] == 0
        client.delete_rls_principal_tags(name, link)
        self._check_space_reads(client, name, rows, link, [] if tenant_wall else expected)
        shared = [{**row, "access": []} for row in shared]
        self.upsert(client, name, shared, skip_rls=True)
        replacements = {row["id"]: row for row in shared}
        rows = [replacements.get(row["id"], row) for row in rows]
        self._check_space_reads(client, name, rows, link, [])
        self._check_space_rows(client, name, rows, skip_rls=True)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rls_space_enable_existing_data(self, rls_client):
        """
        target: staged activation and rollback with preconfigured memberships
        method: prepare policies while disabled, enable, roll back, update tags and re-enable
        expected: data stays intact and the latest memberships take effect
        """
        client = rls_client
        name, rows = self._prepare_rls_spaces(client, enabled=False, configure=False)
        assert client.list_row_policies(name) == []
        assert client.list_rls_principals(name) == []
        self._check_space_rows(client, name, rows)
        self._configure_rls_spaces(client, name)
        policies = client.list_row_policies(name)
        self._check_space_rows(client, name, rows, rls_principal="unknown")
        self.alter_collection_properties(client, name, {"rls.enabled": True})
        self.query(
            client, name, filter="id >= 0", check_task=CheckTasks.err_res, check_items={ct.err_msg: "rls_principal"}
        )
        for principal, resources in {
            "member_0": ["resource_0", "resource_1"],
            "member_1": ["resource_0", "resource_2"],
            "member_2": ["resource_0", "resource_1", "resource_2"],
            "member_3": ["resource_3"],
            "unknown": [],
        }.items():
            self._check_space_reads(client, name, rows, principal, resources)
        for row_id in [2, 3002, 3502]:
            changed = {**rows[row_id - 1], "content": "upgraded_full"}
            self.upsert(client, name, [changed], rls_principal="member_0")
            self.upsert(
                client,
                name,
                [{"id": row_id, "content": "upgraded_partial"}],
                partial_update=True,
                rls_principal="member_0",
            )
            rows[row_id - 1] = {**changed, "content": "upgraded_partial"}
            self.upsert(
                client,
                name,
                [changed],
                rls_principal="member_1",
                check_task=CheckTasks.err_res,
                check_items={ct.err_msg: "RLS"},
            )
        fresh = {**rows[1], "id": 5001, "vector": cf.gen_vectors(1, VECTOR_DIM)[0]}
        self.insert(client, name, [fresh], rls_principal="member_0")
        self.insert(
            client,
            name,
            [{**rows[2], "id": 5002}],
            rls_principal="member_0",
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "RLS"},
        )
        self.insert(
            client,
            name,
            [{**fresh, "id": 5003}],
            check_task=CheckTasks.err_res,
            check_items={ct.err_msg: "rls_principal"},
        )
        result, _ = self.delete(client, name, ids=[5001, 3], rls_principal="member_0")
        assert result["delete_count"] == 1
        self._check_space_rows(client, name, rows, skip_rls=True)

        self.alter_collection_properties(client, name, {"rls.enabled": False})
        client.set_rls_principal_tags(name, "member_0", {"write_spaces": ["tenant_0/blue"]})
        self._check_space_rows(client, name, rows)
        self.alter_collection_properties(client, name, {"rls.enabled": True})
        self._check_space_reads(client, name, rows, "member_0", ["resource_0", "resource_2"])
        assert client.list_row_policies(name) == policies
        self._check_space_rows(client, name, rows, skip_rls=True)
