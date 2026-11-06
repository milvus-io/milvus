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

"""Struct array vector index metric regression cases."""

import copy
import json

import allure
import numpy as np
import pytest
from common import common_func as cf
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import (
    _record,
)
from common.struct_array_typed_vector_helpers import (
    VECTOR_CASES,
    DiskannScoreAccuracyMismatch,
    StructArrayTypedVectorHelpersBase,
    _decode,
    _typed_vector,
)
from pymilvus import DataType
from utils.util_log import test_log as log

DISKANN_SCORE_PROFILES = {
    (DataType.FLOAT16_VECTOR, "DISKANN", "IP"),
    (DataType.BFLOAT16_VECTOR, "DISKANN", "IP"),
    (DataType.BFLOAT16_VECTOR, "DISKANN", "COSINE"),
}
DISKANN_SCORE_ISSUE = "https://github.com/zilliztech/knowhere/issues/1956"


class TestMilvusClientStructArrayVectorIndexMetric(StructArrayTypedVectorHelpersBase):
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
        expected: complete values, legal offsets, exact scores and tie-aware top-k recall
        """
        client = self._client()
        score_deviations = [] if (dtype, kind, metric) in DISKANN_SCORE_PROFILES else None
        name = self._typed_collection(client, dtype, kind, metric)
        model = self._seed(client, name, self._typed_rows(dtype), split=False)
        self._typed_search(client, name, model, dtype, kind, metric, score_deviations)
        for phase in range(2):
            self._reload(client, name)
            assert self.wait_for_index_ready(client, name, index_name="clips[emb]", timeout=300)
            info = client.describe_index(name, "clips[emb]")
            log.info(f"STRUCT_TYPED_INDEX phase={phase} collection={name} info={info}")
            assert info["index_type"] == kind, info
            assert int(info["indexed_rows"]) >= 1000, info
            self._typed_check(client, name, model, dtype)
            self._typed_search(client, name, model, dtype, kind, metric, score_deviations)
            if phase == 0:
                deleted = list(range(0, 1200, 13))
                self.delete(client, name, ids=deleted)
                for pk in deleted:
                    del model[pk]
                patches = []
                for pk in range(20, 1200, 11):
                    patch = _record(
                        pk,
                        [
                            {"anchor": 0, "score": None, "label": "updated", "emb": None, "tags": None},
                            {
                                "anchor": 1,
                                "score": None,
                                "label": None,
                                "emb": _typed_vector(dtype, pk + 50000),
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
        name = self._typed_collection(client, dtype, "HNSW", "L2")
        model = self._seed(client, name, self._typed_rows(dtype)[:rows], split=False)
        self._typed_search(client, name, model, dtype, "HNSW", "L2")
        self._reload(client, name)
        segments = client._get_connection().get_query_segment_info(name, timeout=30)
        log.info(f"STRUCT_SEALED_PATH collection={name} expected_rows={rows} segments={segments}")
        self._typed_check(client, name, model, dtype)
        self._typed_search(client, name, model, dtype, "HNSW", "L2")

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
        name = self._typed_collection(client, dtype, "DISKANN", metric)
        model = self._seed(client, name, self._typed_rows(dtype), split=False)
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
        query = _typed_vector(dtype, 900001)
        decoded = _decode(query, dtype)
        for phase in range(2):
            for collection, field in ((name, "clips[emb]"), (flat, "emb")):
                self._reload(client, collection)
                assert self.wait_for_index_ready(client, collection, index_name=field, timeout=300)
                assert int(client.describe_index(collection, field)["indexed_rows"]) >= 1000
            self._typed_check(client, name, model, dtype)
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
                    value = _decode(raw, dtype)
                    exact = float(np.dot(decoded, value))
                    if metric == "COSINE":
                        exact /= np.linalg.norm(decoded) * np.linalg.norm(value)
                    deviations.append(abs(distance - exact))
                log.info(
                    f"STRUCT_DISKANN_PRECISION_CONTROL dtype={dtype.name} metric={metric} "
                    f"phase={phase} filtered={filtered} max_input_score_deviation={max(deviations)}"
                )
