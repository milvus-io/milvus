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

"""Struct Array search E2E coverage."""

import copy
import json
import os
import time
from urllib.parse import urlsplit

import allure
import numpy as np
import pytest
import requests
from base.client_v2_base import TestMilvusClientV2Base
from base.struct_array import MetadataStructArrayTestBase, StructArrayTestBase, TypedStructArrayTestBase
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_data import (
    DIM,
    assert_values_equal,
    decode_vector,
    make_metadata_row,
    make_nullable_elements,
    make_nullable_row,
    make_typed_vector,
    normalized_vector,
    nullable_children,
    vector_score,
    vector_score_tolerance,
)
from pymilvus import AnnSearchRequest, DataType, RRFRanker
from pymilvus.client.embedding_list import EmbeddingList
from utils.util_log import test_log as log


class TestMilvusClientStructArrayGrowingSealedSearch(MetadataStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("metric", ["L2", "COSINE"])
    def test_struct_array_observed_mixed_segment_search(self, host, token, metric):
        """
        target: verify indexed sealed, unindexed sealed and growing StructArray searches together
        method: observe runtime segment/index metadata around exact data and element-ranking checks
        expected: three physical populations remain present and every PK group contributes correct hits
        """
        client = self._client()
        hostname = urlsplit(host if "://" in host else f"http://{host}").hostname
        admin_uri = os.environ.get("STRUCT_ARRAY_ADMIN_URI", f"http://{hostname}:9091")
        assert token and ":" in token, "L3 requires administrator credentials for read-only segment metadata"
        auth = tuple(token.split(":", 1))

        def admin(path, params=None):
            response = requests.get(f"{admin_uri}/api/v1/{path}", auth=auth, params=params, timeout=15)
            response.raise_for_status()
            return response.json()

        config = admin("_cluster/configs")
        threshold = int(config["indexcoord.segment.minsegmentnumrowstoenableindex"].split("[")[0])
        assert 500 < threshold <= 3000, threshold
        name = cf.gen_unique_str("struct_segments")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("revision", DataType.INT64)
        schema.add_field("bucket", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_field("note", DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field(
            "clips",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=self.required_child_schema(client),
            max_capacity=8,
            nullable=True,
        )
        self.create_collection(
            client, name, schema=schema, shards_num=1, properties={"collection.autocompaction.enabled": "false"}
        )
        rows = [make_metadata_row(schema, i) for i in range(4000)]
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        self.insert(client, name, rows[:3000])
        self.flush(client, name)
        self.insert(client, name, rows[3000:3500])
        self.flush(client, name)
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_name="normal_idx", index_type="FLAT", metric_type="L2")
        indexes.add_index(
            "clips[emb]",
            index_name="clips_idx",
            index_type="HNSW",
            metric_type=metric,
            params={"M": 16, "efConstruction": 200},
        )
        self.create_index(client, name, indexes)
        self.load_collection(client, name)
        self.insert(client, name, rows[3500:])
        description, _ = self.describe_collection(client, name)
        collection_id = description["collection_id"]
        emb_id = next(
            field["field_id"]
            for parent in description["fields"]
            for field in parent.get("struct_fields", [])
            if field["name"] == "emb"
        )

        def observe(phase):
            deadline = time.monotonic() + 90
            snapshot = None
            while time.monotonic() < deadline:
                snapshot = admin("_qn/segments", {"collection_id": collection_id, "in": "qn"}) or []
                snapshot = [item for item in snapshot if str(item["collection_id"]) == str(collection_id)]
                sealed = [item for item in snapshot if item.get("state", "").lower() == "sealed"]
                growing = [item for item in snapshot if item.get("state", "").lower() == "growing"]
                big = [item for item in sealed if int(item.get("loaded_insert_row_count", 0)) == 3000]
                small = [item for item in sealed if int(item.get("loaded_insert_row_count", 0)) == 500]
                live = [item for item in growing if int(item.get("loaded_insert_row_count", 0)) == 500]
                indexed = [
                    item
                    for item in big
                    if any(
                        int(field["field_id"]) == emb_id
                        and field.get("is_loaded") in (True, "true")
                        and int(field.get("index_size", 0)) > 0
                        for field in item.get("index_fields", [])
                    )
                ]
                # DataCoord allocates build IDs even for below-threshold tasks that
                # finish without artifacts. QueryNode reports these metadata entries
                # as loaded too; size plus raw-data availability distinguish this path.
                unindexed = [
                    item
                    for item in small
                    if any(
                        int(field["field_id"]) == emb_id
                        and int(field.get("index_size", 0)) == 0
                        and field.get("has_raw_data") is True
                        for field in item.get("index_fields", [])
                    )
                ]
                if indexed and unindexed and live:
                    log.info(
                        "STRUCT_SEGMENT_PROOF %s",
                        json.dumps(
                            {"phase": phase, "collection": name, "threshold": threshold, "segments": snapshot},
                            sort_keys=True,
                        ),
                    )
                    return
                time.sleep(1)
            raise AssertionError(f"Physical three-path fixture not established ({phase}): {snapshot}")

        observe("before_search")
        self.assert_metadata_rows(client, name, model)
        self.assert_required_predicates(client, name, model)
        self.assert_element_search(client, name, model, metric, "HNSW")
        # Each selected row has exactly two elements; all six must survive global reduction.
        selected = [3, 3006, 3503]
        assert all(len(model[key]["clips"]) == 2 for key in selected)
        queries = [model[key]["clips"][0]["emb"] for key in selected]
        hits, _ = self.search(
            client,
            name,
            data=queries,
            anns_field="clips[emb]",
            filter=f"id in {selected}",
            limit=6,
            consistency_level="Strong",
            search_params={"metric_type": metric, "params": {"ef": 512}},
        )
        expected_keys = {(key, offset) for key in selected for offset in range(2)}
        assert len(hits) == 3
        for query, result in zip(queries, hits):
            assert len(result) == 6 and {(hit["id"], hit["offset"]) for hit in result} == expected_keys
            for hit in result:
                expected = vector_score(query, model[hit["id"]]["clips"][hit["offset"]]["emb"], metric)
                assert hit["distance"] == pytest.approx(expected, abs=2e-4, rel=2e-4)
        observe("after_search")


class TestMilvusClientStructArrayNullableVectorCoordinates(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW", "DISKANN"])
    def test_nullable_vector_search_keeps_logical_coordinates(self, metric, index_type):
        """
        target: exclude null vectors without renumbering logical struct element positions
        method: independent flattened-vector oracle before/after indexing and mutations
        expected: legal unique coordinates, exact distances and exact or high-recall top-k
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children(), child_index=index_type, metric=metric)
        rows = [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(1200)]
        # A separate all-null vector population still owns logical element slots.
        for row in rows[:100]:
            for element in row["clips"] or []:
                element["emb"] = None
        model = self.insert_rows_with_model(client, name, rows)
        for phase in range(3):
            for query in [normalized_vector(99999), normalized_vector(77777)]:
                for filtered in (False, True):
                    scored = {}
                    for pk, row in model.items():
                        for offset, element in enumerate(row["clips"] or []):
                            if element["emb"] is None or (filtered and element["score"] is not None):
                                continue
                            a, b = np.asarray(query, dtype=np.float64), np.asarray(element["emb"], dtype=np.float64)
                            score = float(np.sum((a - b) ** 2)) if metric == "L2" else float(np.dot(a, b))
                            if metric == "COSINE":
                                score /= float(np.linalg.norm(a) * np.linalg.norm(b))
                            scored[(pk, offset)] = score
                    result, _ = self.search(
                        client,
                        name,
                        [query],
                        anns_field="clips[emb]",
                        limit=20,
                        filter="element_filter(clips, $[score] IS NULL)" if filtered else "",
                        search_params={"metric_type": metric, "params": {"ef": 200, "search_list": 200}},
                        output_fields=["clips"],
                    )
                    hits = result[0]
                    assert len(hits) == min(20, len(scored))
                    coordinates = [(h["id"], h["offset"]) for h in hits]
                    assert len(set(coordinates)) == len(coordinates)
                    assert set(coordinates).issubset(scored)
                    for hit, coordinate in zip(hits, coordinates):
                        assert hit["distance"] == pytest.approx(scored[coordinate], abs=2e-5, rel=2e-5)
                        assert_values_equal(hit["entity"]["clips"], model[hit["id"]]["clips"])
                    ranking = sorted(scored, key=scored.get, reverse=metric != "L2")[: len(hits)]
                    if index_type == "FLAT":
                        assert set(coordinates) == set(ranking)
                    else:
                        assert len(set(coordinates) & set(ranking)) / len(ranking) >= 0.95
                    distances = [h["distance"] for h in hits]
                    assert distances == sorted(distances, reverse=metric != "L2")
            self.assert_rows(client, name, model)
            if phase == 0:
                self.reload_collection(client, name)
                assert self.wait_for_index_ready(client, name, index_name="clips[emb]", timeout=300)
            elif phase == 1:
                deleted = list(range(0, 1200, 13))
                self.delete(client, name, ids=deleted)
                for pk in deleted:
                    del model[pk]
                changes = [
                    make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n, 5), revision=5)
                    for n in range(1, 1200, 11)
                ]
                self.upsert(client, name, changes)
                model.update({r["id"]: r for r in changes})
                self.reload_collection(client, name)


class TestMilvusClientStructArraySearchValidation(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_cannot_cross_struct_boundaries(self):
        """
        target: reject element filters that refer to another Struct with identical child names
        method: opposing NULL masks in two structs, cross-filter rejection and same-struct control
        expected: explicit same-struct restriction and exact coordinates from the allowed search
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        other = client.create_struct_field_schema()
        other.add_field("score", DataType.INT64, nullable=True)
        other.add_field("emb", DataType.FLOAT_VECTOR, dim=8, nullable=True)
        client.add_collection_struct_field(name, "other", other, max_capacity=8, nullable=True)
        index = client.prepare_index_params()
        index.add_index("other[emb]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, index)
        rows = [
            make_nullable_row(
                schema, n, [{"anchor": 0, "score": None, "label": None, "emb": normalized_vector(n), "tags": None}]
            )
            for n in range(4)
        ]
        for row in rows:
            row["other"] = [{"score": 1, "emb": normalized_vector(row["id"] + 10)}]
        model = self.insert_rows_with_model(client, name, rows)
        self.reload_collection(client, name)
        result, ok = self.search(
            client,
            name,
            [normalized_vector(1)],
            anns_field="clips[emb]",
            limit=10,
            filter="element_filter(other, $[score] IS NULL)",
            search_params={"metric_type": "L2"},
            check_task=CheckTasks.check_nothing,
        )
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "same struct" in str(result), result
        hits = client.search(
            name,
            [normalized_vector(1)],
            anns_field="clips[emb]",
            limit=10,
            filter="element_filter(clips, $[score] IS NULL)",
            search_params={"metric_type": "L2"},
        )[0]
        assert {(hit["id"], hit["offset"]) for hit in hits} == {(n, 0) for n in range(4)}
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric", ["MAX_SIM", "MAX_SIM_L2", "MAX_SIM_IP", "MAX_SIM_COSINE"])
    def test_max_sim_family_rejects_nullable_vector_index(self, metric):
        """
        target: reject each floating-point MAX_SIM family member on nullable vectors
        method: remove index while released, reject unsupported metric, rebuild a valid index
        expected: explicit nullable-vector restriction and full data preserved through recovery
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(12)],
        )
        self.release_collection(client, name)
        self.drop_index(client, name, index_name="clips[emb]")
        index = client.prepare_index_params()
        index.add_index("clips[emb]", index_type="HNSW", metric_type=metric, params={"M": 16, "efConstruction": 64})
        result, ok = self.create_index(client, name, index, check_task=CheckTasks.check_nothing)
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "element" in str(result).lower() and "nullable" in str(result).lower(), result
        index = client.prepare_index_params()
        index.add_index("clips[emb]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, index)
        self.load_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("surface", ["search", "hybrid"])
    def test_embedding_list_rejected_at_nullable_vector_search_boundary(self, surface):
        """
        target: reject embedding-list placeholders before metric/ranking processing
        method: Search and HybridSearch with embedding lists, followed by an ordinary-vector control
        expected: exact nullable embedding-list rejection and valid element search remains usable
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(12)],
        )
        embeddings = EmbeddingList()
        embeddings.add(normalized_vector(1))
        if surface == "search":
            result, ok = self.search(
                client,
                name,
                [embeddings],
                anns_field="clips[emb]",
                limit=5,
                search_params={"metric_type": "L2"},
                check_task=CheckTasks.check_nothing,
            )
        else:
            requests = [AnnSearchRequest([embeddings], "clips[emb]", {"metric_type": "L2"}, 5)]
            result, ok = self.hybrid_search(
                client, name, requests, RRFRanker(), limit=5, check_task=CheckTasks.check_nothing
            )
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "embedding-list search is not supported for element-nullable" in str(result), result
        hits = client.search(
            name, [normalized_vector(1)], anns_field="clips[emb]", limit=5, search_params={"metric_type": "L2"}
        )
        assert hits[0]
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_nested_scalar_index_rejected(self):
        """
        target: reject scalar indexing on a nested Array child
        method: attempt INVERTED indexing and verify index inventory and full data
        expected: parameter rejection, no orphan index, successful supported query
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(12)],
        )
        before = set(client.list_indexes(name))
        index = client.prepare_index_params()
        index.add_index("clips[tags]", index_type="INVERTED")
        result, ok = self.create_index(client, name, index, check_task=CheckTasks.check_nothing)
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "indexing recursive ARRAY field clips[tags] is not supported" in str(result), result
        assert set(client.list_indexes(name)) == before
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partial_load_excludes_unindexed_new_vector(self):
        """
        target: require an index only when loading a newly added vector child
        method: immediate read, full-load rejection, partial-load control, index and full reload
        expected: NULL backfill is immediate and partial loading ordinary fields remains usable
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(12)],
        )
        self.add_collection_field(client, name, "clips[newvec]", DataType.FLOAT_VECTOR, dim=8, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["newvec"] = None
        self.assert_rows(client, name, model)
        self.release_collection(client, name)
        result, ok = self.load_collection(client, name, check_task=CheckTasks.check_nothing)
        assert not ok and "index" in str(result).lower(), result
        self.load_collection(client, name, load_fields=["id", "revision", "normal_vector"])
        actual = client.query(name, filter="id >= 0", output_fields=["id", "revision"], consistency_level="Strong")
        assert {row["id"]: row["revision"] for row in actual} == {pk: row["revision"] for pk, row in model.items()}
        self.release_collection(client, name)
        index = client.prepare_index_params()
        index.add_index("clips[newvec]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, index)
        self.load_collection(client, name)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    def test_heterogeneous_hybrid_has_independent_rrf_oracle(self):
        """
        target: fuse ordinary rows with nullable struct elements using row identities
        method: deterministic FLAT distances, independent best-element collapse and RRF calculation
        expected: every PK and fused score matches the independent model before/after reload
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        rows = []
        for n in range(10):
            row = make_nullable_row(
                schema,
                n,
                [
                    {
                        "anchor": j,
                        "score": None,
                        "label": None,
                        "emb": None if j == 0 or n == 9 else [float(20 - n * 2 + j)] + [0.0] * 7,
                        "tags": [None],
                    }
                    for j in range(3)
                ],
            )
            row["normal_vector"] = [float(n + 1)] + [0.0] * 7
            rows.append(row)
        model = self.insert_rows_with_model(client, name, rows)
        query = [0.0] * 8
        normal_order = sorted(model, key=lambda pk: sum(v * v for v in model[pk]["normal_vector"]))
        child_distances = {
            pk: min(sum(v * v for v in e["emb"]) for e in row["clips"] if e["emb"] is not None)
            for pk, row in model.items()
            if any(e["emb"] is not None for e in row["clips"])
        }
        child_order = sorted(child_distances, key=child_distances.get)
        scores = {pk: 1 / (60 + rank) for rank, pk in enumerate(normal_order, 1)}
        for rank, pk in enumerate(child_order, 1):
            scores[pk] += 1 / (60 + rank)
        for phase in range(2):
            requests = [
                AnnSearchRequest([query], field, {"metric_type": "L2"}, 100)
                for field in ("normal_vector", "clips[emb]")
            ]
            hits = client.hybrid_search(name, requests, RRFRanker(k=60), limit=10, output_fields=["clips"])[0]
            assert len(hits) == len(scores) and {h["id"] for h in hits} == set(scores)
            assert [h["distance"] for h in hits] == sorted((h["distance"] for h in hits), reverse=True)
            for hit in hits:
                assert hit["distance"] == pytest.approx(scores[hit["id"]], abs=1e-6)
            self.assert_rows(client, name, model)
            if phase == 0:
                self.reload_collection(client, name)


DISKANN_SCORE_PROFILES = {
    (DataType.FLOAT16_VECTOR, "DISKANN", "IP"),
    (DataType.BFLOAT16_VECTOR, "DISKANN", "IP"),
    (DataType.BFLOAT16_VECTOR, "DISKANN", "COSINE"),
}

DISKANN_SCORE_ISSUE = "https://github.com/zilliztech/knowhere/issues/1956"


VECTOR_CASES = [
    (dtype, kind, metric)
    for dtype in (DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR)
    for kind in ("FLAT", "HNSW", "DISKANN")
    for metric in ("L2", "IP", "COSINE")
] + [(DataType.INT8_VECTOR, "HNSW", metric) for metric in ("L2", "IP", "COSINE")]


VECTOR_CASES += [
    (DataType.BINARY_VECTOR, kind, metric) for kind in ("BIN_FLAT", "BIN_IVF_FLAT") for metric in ("HAMMING", "JACCARD")
]


VECTOR_CASES += [(DataType.FLOAT_VECTOR, "IVF_FLAT", metric) for metric in ("L2", "IP", "COSINE")]


class DiskannScoreAccuracyMismatch(AssertionError):
    """Strict score-budget deviation tracked by zilliztech/knowhere#1956."""


class TestMilvusClientStructArrayVectorIndexMetric(TypedStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "dtype,kind,metric",
        [
            pytest.param(
                d,
                i,
                m,
                id=f"{d.name}-{i}-{m}",
                marks=pytest.mark.xfail(
                    strict=True,
                    raises=DiskannScoreAccuracyMismatch,
                    reason=f"{DISKANN_SCORE_ISSUE}: typed DISKANN score budget awaits an accuracy-contract decision",
                )
                if (d, i, m) in DISKANN_SCORE_PROFILES
                else (),
            )
            for d, i, m in VECTOR_CASES
        ],
    )
    def test_typed_nullable_vector_index_metric_mutations(self, dtype, kind, metric):
        """
        target: preserve nullable coordinates and typed scores across index and mutation paths
        method: independent vector model, indexed population, delete/replacement and reload
        expected: complete values, legal offsets, bounded ANN score error and tie-aware top-k recall
        """
        client = self._client()
        score_deviations = [] if (dtype, kind, metric) in DISKANN_SCORE_PROFILES else None
        name, schema = self.create_typed_collection(client, dtype, kind, metric)
        model = self.insert_rows_with_model(client, name, self.make_typed_rows(schema), split=False)
        self.assert_typed_search(client, name, model, dtype, kind, metric, score_deviations)
        for phase in range(2):
            self.reload_collection(client, name)
            assert self.wait_for_index_ready(client, name, index_name="clips[emb]", timeout=300)
            info = client.describe_index(name, "clips[emb]")
            log.info(f"STRUCT_TYPED_INDEX phase={phase} collection={name} info={info}")
            assert info["index_type"] == kind, info
            assert int(info["indexed_rows"]) >= 1000, info
            self.assert_typed_rows(client, name, model, dtype)
            self.assert_typed_search(client, name, model, dtype, kind, metric, score_deviations)
            if phase == 0:
                deleted = list(range(0, 1200, 13))
                self.delete(client, name, ids=deleted)
                for pk in deleted:
                    del model[pk]
                patches = []
                for pk in range(20, 1200, 11):
                    patch = make_nullable_row(
                        schema,
                        pk,
                        [
                            {"anchor": 0, "score": None, "label": "updated", "emb": None, "tags": None},
                            {
                                "anchor": 1,
                                "score": None,
                                "label": None,
                                "emb": make_typed_vector(dtype, pk + 50000),
                                "tags": [None],
                            },
                        ],
                        revision=1,
                    )
                    patches.append(patch)
                    model[pk] = copy.deepcopy(patch)
                self.upsert(client, name, patches, partial_update=True)
        if score_deviations:
            detail = json.dumps(score_deviations)
            allure.attach(detail, name="typed DISKANN score deviations", attachment_type=allure.attachment_type.JSON)
            raise DiskannScoreAccuracyMismatch(f"{DISKANN_SCORE_ISSUE}: {detail}")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("rows", [128, 1200], ids=["small-sealed", "indexed-control"])
    def test_sealed_nullable_vectors_preserve_logical_coordinates(self, rows):
        """
        target: separate small sealed brute-force remapping from indexed element remapping
        method: identical nullable vectors before/after flush; retain loaded segment metadata
        expected: no NULL coordinate or wrong score for either population
        """
        client = self._client()
        dtype = DataType.FLOAT_VECTOR
        name, schema = self.create_typed_collection(client, dtype, "HNSW", "L2")
        model = self.insert_rows_with_model(client, name, self.make_typed_rows(schema)[:rows], split=False)
        self.assert_typed_search(client, name, model, dtype, "HNSW", "L2")
        self.reload_collection(client, name)
        segments = client._get_connection().get_query_segment_info(name, timeout=30)
        log.info(f"STRUCT_SEALED_PATH collection={name} expected_rows={rows} segments={segments}")
        self.assert_typed_rows(client, name, model, dtype)
        self.assert_typed_search(client, name, model, dtype, "HNSW", "L2")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "dtype,metric",
        [(DataType.FLOAT16_VECTOR, "IP"), (DataType.BFLOAT16_VECTOR, "IP"), (DataType.BFLOAT16_VECTOR, "COSINE")],
        ids=["FLOAT16-IP", "BFLOAT16-IP", "BFLOAT16-COSINE"],
    )
    def test_typed_diskann_matches_flattened_top_level_control(self, dtype, metric):
        """
        target: separate nullable Struct coordinate errors from typed DiskANN score precision
        method: index identical valid vectors as Struct elements and ordinary rows, then reload
        expected: matching top-k coordinates and scores; retain exact-input score deviations
        """
        client = self._client()
        name, schema = self.create_typed_collection(client, dtype, "DISKANN", metric)
        model = self.insert_rows_with_model(client, name, self.make_typed_rows(schema), split=False)
        flat = cf.gen_unique_str("struct_diskann_flattened_control")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("elem_offset", DataType.INT64)
        schema.add_field("emb", dtype, dim=32)
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("emb", index_type="DISKANN", metric_type=metric)
        self.create_collection(client, flat, schema=schema, index_params=indexes, consistency_level="Strong")
        rows = [
            {"id": pk * 8 + offset, "elem_offset": offset, "emb": element["emb"]}
            for pk, row in model.items()
            for offset, element in enumerate(row["clips"] or [])
            if element["emb"] is not None
        ]
        self.insert(client, flat, rows)
        query = make_typed_vector(dtype, 900001)
        decoded = decode_vector(query, dtype)
        for phase in range(2):
            for collection, field in ((name, "clips[emb]"), (flat, "emb")):
                self.reload_collection(client, collection)
                assert self.wait_for_index_ready(client, collection, index_name=field, timeout=300)
                assert int(client.describe_index(collection, field)["indexed_rows"]) >= 1000
            self.assert_typed_rows(client, name, model, dtype)
            for filtered in (False, True):
                struct_hits, _ = self.search(
                    client,
                    name,
                    [query],
                    anns_field="clips[emb]",
                    limit=20,
                    filter="element_filter(clips, $[score] IS NULL)" if filtered else "",
                    search_params={"metric_type": metric, "params": {"search_list": 200}},
                )
                flat_hits, _ = self.search(
                    client,
                    flat,
                    [query],
                    anns_field="emb",
                    limit=20,
                    filter="elem_offset < 2" if filtered else "",
                    search_params={"metric_type": metric, "params": {"search_list": 200}},
                )
                actual = {(h["id"], h["offset"]): h["distance"] for h in struct_hits[0]}
                control = {(h["id"] // 8, h["id"] % 8): h["distance"] for h in flat_hits[0]}
                assert len(actual) == len(control) == 20
                assert actual.keys() == control.keys(), (actual, control)
                deviations = []
                for coordinate, distance in actual.items():
                    assert distance == pytest.approx(control[coordinate], abs=1e-5, rel=1e-6)
                    raw = model[coordinate[0]]["clips"][coordinate[1]]["emb"]
                    assert raw is not None
                    value = decode_vector(raw, dtype)
                    exact = float(np.dot(decoded, value))
                    if metric == "COSINE":
                        exact /= np.linalg.norm(decoded) * np.linalg.norm(value)
                    deviations.append(abs(distance - exact))
                log.info(
                    f"STRUCT_DISKANN_PRECISION_CONTROL dtype={dtype.name} metric={metric} "
                    f"phase={phase} filtered={filtered} max_input_score_deviation={max(deviations)}"
                )


PRECISION_DIM = 64

KINDS = [
    pytest.param(DataType.FLOAT16_VECTOR, "L2", id="float16_l2"),
    pytest.param(DataType.BFLOAT16_VECTOR, "IP", id="bfloat16_ip"),
    pytest.param(DataType.INT8_VECTOR, "COSINE", id="int8_cosine"),
    pytest.param(DataType.BINARY_VECTOR, "HAMMING", id="binary_hamming"),
]


def vector(kind, seed):
    rng = np.random.RandomState(seed)
    if kind == DataType.BINARY_VECTOR:
        return rng.randint(0, 256, PRECISION_DIM // 8, dtype=np.uint8).tobytes()
    if kind == DataType.INT8_VECTOR:
        return rng.randint(-100, 101, PRECISION_DIM, dtype=np.int8)
    dtype = np.float16 if kind == DataType.FLOAT16_VECTOR else cf.bfloat16
    return rng.normal(size=PRECISION_DIM).astype(dtype)


def decoded(value, kind):
    # Current SDK exposes binary children as [bytes] and BF16 children as uint16 bits.
    if kind == DataType.BINARY_VECTOR and isinstance(value, list) and len(value) == 1:
        value = value[0]
    if kind == DataType.BFLOAT16_VECTOR and np.asarray(value).dtype == np.uint16:
        return np.asarray(value, dtype=np.uint16).view(cf.bfloat16).astype(np.float64)
    if isinstance(value, bytes):
        dtype = {
            DataType.BINARY_VECTOR: np.uint8,
            DataType.FLOAT16_VECTOR: np.float16,
            DataType.BFLOAT16_VECTOR: cf.bfloat16,
            DataType.INT8_VECTOR: np.int8,
        }[kind]
        return np.frombuffer(value, dtype=dtype).astype(np.float64)
    return np.asarray(value, dtype=np.float64)


def distance(left, right, kind, metric):
    left, right = decoded(left, kind), decoded(right, kind)
    if metric == "HAMMING":
        return int(np.unpackbits(np.bitwise_xor(left.astype(np.uint8), right.astype(np.uint8))).sum())
    if metric == "L2":
        return float(np.sum((left - right) ** 2))
    dot = float(np.dot(left, right))
    return dot if metric == "IP" else dot / float(np.linalg.norm(left) * np.linalg.norm(right))


def row(number, kind, revision=0):
    shape = (number + revision) % 7
    elements = None if shape == 0 else []
    if shape > 1:
        elements = [
            {"value": number + offset + revision, "emb": vector(kind, number * 31 + offset + revision * 100003)}
            for offset in range(shape)
        ]
    return {
        "id": number,
        "revision": revision,
        "normal_vector": [float(number % 13)] * PRECISION_DIM,
        "clips": elements,
    }


class TestMilvusClientStructArrayVectorScores(TestMilvusClientV2Base):
    def create_precision_collection(self, client, kind, metric, index_type):
        name = cf.gen_unique_str("struct_precision")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("revision", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=PRECISION_DIM)
        struct, _ = self.create_struct_field_schema(client)
        struct.add_field("value", DataType.INT64)
        struct.add_field("emb", kind, dim=PRECISION_DIM)
        schema.add_field(
            "clips", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct, max_capacity=8, nullable=True
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_type="FLAT", metric_type="L2")
        indexes.add_index(
            "clips[emb]",
            index_type=index_type,
            metric_type=metric,
            params={"M": 16, "efConstruction": 200} if index_type == "HNSW" else {},
        )
        self.create_collection(client, name, schema=schema, index_params=indexes)
        return name

    def assert_precision_rows_and_search(self, client, name, model, kind, metric, index_type):
        index, _ = self.describe_index(client, name, "clips[emb]")
        tolerance = vector_score_tolerance(index, kind, abs_tol=3e-3, rel_tol=3e-3)
        rows, _ = self.query(
            client,
            name,
            filter="id >= 0",
            output_fields=["id", "revision", "clips"],
            limit=len(model) + 10,
            consistency_level="Strong",
        )
        actual = {item["id"]: item for item in rows}
        assert len(rows) == len(model) and set(actual) == set(model)
        for pk, expected in model.items():
            got = actual[pk]
            assert got["revision"] == expected["revision"]
            if expected["clips"] is None:
                assert got["clips"] is None
                continue
            assert len(got["clips"]) == len(expected["clips"])
            for left, right in zip(got["clips"], expected["clips"]):
                assert left["value"] == right["value"]
                np.testing.assert_array_equal(decoded(left["emb"], kind), decoded(right["emb"], kind))
        filters = [
            ("", lambda pk, element: True),
            ("element_filter(clips, $[value] % 3 == 0)", lambda pk, element: element["value"] % 3 == 0),
            (
                "id >= 700 && element_filter(clips, $[value] % 3 == 1)",
                lambda pk, element: pk >= 700 and element["value"] % 3 == 1,
            ),
        ]
        queries = [vector(kind, seed) for seed in (19, 59, 997)]
        ascending = metric in ("L2", "HAMMING")
        for expression, accept in filters:
            results, _ = self.search(
                client,
                name,
                data=queries,
                anns_field="clips[emb]",
                filter=expression,
                limit=20,
                consistency_level="Strong",
                output_fields=["revision"],
                search_params={"metric_type": metric, "params": {"ef": 512} if index_type == "HNSW" else {}},
            )
            assert len(results) == len(queries)
            for query, hits in zip(queries, results):
                truth = {
                    (pk, offset): distance(query, element["emb"], kind, metric)
                    for pk, item in model.items()
                    for offset, element in enumerate(item["clips"] or [])
                    if accept(pk, element)
                }
                ordered = sorted(truth.values(), reverse=not ascending)
                keys = [(hit["id"], hit["offset"]) for hit in hits]
                assert len(keys) == 20 and len(set(keys)) == 20
                scores = []
                for hit, key in zip(hits, keys):
                    assert key in truth
                    assert hit["distance"] == pytest.approx(truth[key], **tolerance), {
                        "coordinate": key,
                        "metric": metric,
                        "tolerance": tolerance,
                        "actual": hit["distance"],
                        "expected": truth[key],
                    }
                    assert hit["entity"]["revision"] == model[key[0]]["revision"]
                    scores.append(hit["distance"])
                assert scores == sorted(scores, reverse=not ascending)
                cutoff = ordered[19]

                def at_cutoff(score):
                    return score <= cutoff + 1e-7 if ascending else score >= cutoff - 1e-7

                if index_type != "HNSW":
                    assert all(at_cutoff(truth[key]) for key in keys)
                    strict = (
                        {key for key, score in truth.items() if score < cutoff - 1e-7}
                        if ascending
                        else {key for key, score in truth.items() if score > cutoff + 1e-7}
                    )
                    assert strict <= set(keys)
                else:
                    # Tie-aware quality threshold for this fixed corpus, not a universal ANN guarantee.
                    assert sum(at_cutoff(truth[key]) for key in keys) / 20 >= 0.95

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_int8_flat_rejection_preserves_data_and_recovers(self):
        """
        target: reject the unsupported INT8/FLAT combination without changing stored rows
        method: replace an existing index with FLAT, assert rejection, restore HNSW and verify
        expected: error 1100, no failed index left behind, exact rows and rankings after recovery
        """
        client = self._client()
        kind, metric = DataType.INT8_VECTOR, "COSINE"
        name = self.create_precision_collection(client, kind, metric, "HNSW")
        rows = [row(i, kind) for i in range(1100)]
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        self.insert(client, name, rows)
        self.flush(client, name)
        self.release_collection(client, name)
        names, _ = self.list_indexes(client, name, field_name="clips[emb]")
        assert len(names) == 1
        self.drop_index(client, name, names[0])
        invalid, _ = self.prepare_index_params(client)
        invalid.add_index("clips[emb]", index_name="unsupported_flat", index_type="FLAT", metric_type=metric)
        self.create_index(
            client,
            name,
            invalid,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "can't build with this index FLAT"},
        )
        names, _ = self.list_indexes(client, name, field_name="clips[emb]")
        assert names == []
        valid, _ = self.prepare_index_params(client)
        valid.add_index("clips[emb]", index_type="HNSW", metric_type=metric, params={"M": 16, "efConstruction": 200})
        self.create_index(client, name, valid)
        self.load_collection(client, name)
        self.assert_precision_rows_and_search(client, name, model, kind, metric, "HNSW")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "kind,metric,algorithm",
        [
            pytest.param(p.values[0], p.values[1], algorithm, id=f"{algorithm}-{p.id}")
            for p in KINDS
            for algorithm in ("exact", "hnsw")
            if not (p.values[0] == DataType.INT8_VECTOR and algorithm == "exact")
        ],
    )
    def test_struct_array_vector_precision_offsets_after_mutations(self, kind, metric, algorithm):
        """
        target: preserve typed vector bytes, nullable row shapes, element coordinates and scores
        method: compare 1100 rows and independent rankings, then replace/delete/reload and repeat
        expected: exact vector payloads and valid tie-aware top-k across FLAT and HNSW paths
        """
        client = self._client()
        index_type = "HNSW" if algorithm == "hnsw" else ("BIN_FLAT" if kind == DataType.BINARY_VECTOR else "FLAT")
        name = self.create_precision_collection(client, kind, metric, index_type)
        rows = [row(i, kind) for i in range(1100)]
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        self.insert(client, name, rows)
        self.assert_precision_rows_and_search(client, name, model, kind, metric, index_type)
        self.flush(client, name)
        self.release_collection(client, name)
        self.load_collection(client, name)
        self.assert_precision_rows_and_search(client, name, model, kind, metric, index_type)
        removed = list(range(0, 1100, 11))
        self.delete(client, name, ids=removed)
        for pk in removed:
            del model[pk]
        updates = [row(i, kind, revision=3) for i in range(3, 1150, 17)]
        self.upsert(client, name, updates)
        model.update({item["id"]: copy.deepcopy(item) for item in updates})
        self.flush(client, name)
        self.release_collection(client, name)
        self.load_collection(client, name)
        self.assert_precision_rows_and_search(client, name, model, kind, metric, index_type)
