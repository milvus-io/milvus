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

"""Struct array indexed query regression cases."""

import copy

import pytest
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_row_model import (
    StructArrayRowModelBase,
    _row,
)
from pymilvus import DataType


class TestMilvusClientStructArrayIndexedQuery(StructArrayRowModelBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "value_type,index_type",
        [
            pytest.param(DataType.INT8, None, id="int8_scan"),
            pytest.param(DataType.INT8, "BITMAP", id="int8_bitmap"),
            pytest.param(DataType.INT8, "STL_SORT", id="int8_sort"),
            pytest.param(DataType.INT64, "INVERTED", id="int64_inverted"),
            pytest.param(DataType.INT64, "STL_SORT", id="int64_sort"),
            pytest.param(DataType.INT64, "AUTOINDEX", id="int64_auto"),
            pytest.param(DataType.DOUBLE, "INVERTED", id="double_inverted"),
            pytest.param(DataType.DOUBLE, "STL_SORT", id="double_sort"),
            pytest.param(DataType.VARCHAR, "BITMAP", id="varchar_bitmap"),
            pytest.param(DataType.VARCHAR, "INVERTED", id="varchar_inverted"),
            pytest.param(DataType.VARCHAR, "STL_SORT", id="varchar_sort"),
            pytest.param(DataType.BOOL, "BITMAP", id="bool_bitmap"),
        ],
    )
    def test_struct_array_scalar_index_three_valued_truth(self, value_type, index_type):
        """
        target: preserve NULL/empty and same-element truth across scalar index paths
        method: enumerate 18 predicates over 1100 rows, mutate shapes, then reload and compare
        expected: complete PK sets match a local three-valued semantic model in every phase
        """
        client = self._client()
        name = self._create_contract_collection(client, value_type=value_type, vectors=False, scalar_index=index_type)
        rows = [_row(i, value_type=value_type, vectors=False) for i in range(1100)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        self._assert_predicates(client, name, model, value_type)
        self._reload(client, name)
        if index_type:
            assert self.wait_for_index_ready(client, name, index_name="value_idx", timeout=300)
        self._assert_predicates(client, name, model, value_type)
        changes = [_row(i, revision=3, value_type=value_type, vectors=False) for i in range(0, 1100, 11)]
        self.upsert(client, name, changes)
        model.update({row["id"]: copy.deepcopy(row) for row in changes})
        self._reload(client, name)
        self._assert_model(client, name, model)
        self._assert_predicates(client, name, model, value_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW"])
    def test_struct_array_element_coordinates_after_mutation(self, metric, index_type):
        """
        target: preserve logical offsets, filtering and scores through mutable nullable arrays
        method: search independent flattened vectors before flush, after indexing, and after delete/upsert/reload
        expected: valid unique coordinates and distances; exact FLAT top-k or bounded HNSW recall
        """
        client = self._client()
        name = self._create_contract_collection(client, vector_index=index_type, metric=metric)
        rows = [_row(i) for i in range(1100)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        self._assert_element_search(client, name, model, metric, index_type)
        self._reload(client, name)
        self._assert_element_search(client, name, model, metric, index_type)
        deleted = list(range(0, 1100, 9))
        self.delete(client, name, ids=deleted)
        for pk in deleted:
            del model[pk]
        changes = [_row(i, revision=5) for i in range(1, 1100, 7)]
        self.upsert(client, name, changes)
        model.update({row["id"]: copy.deepcopy(row) for row in changes})
        self._reload(client, name)
        self._assert_model(client, name, model)
        self._assert_element_search(client, name, model, metric, index_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("value_type", [DataType.INT64, DataType.VARCHAR], ids=["int64", "varchar"])
    def test_rebuild_index_and_toggle_mmap_preserve_mutated_rows(self, value_type):
        """
        target: preserve mutation history through repeated scalar-index and mmap transitions
        method: replace arrays, change mmap and rebuild BITMAP/INVERTED/STL_SORT indexes
        expected: projection and all three-valued predicates remain equal to retained inputs
        """
        client = self._client()
        name = self._create_contract_collection(client, vectors=False, value_type=value_type, scalar_index="BITMAP")
        rows = [_row(i, vectors=False, value_type=value_type) for i in range(1100)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        for revision, index_type in enumerate(["INVERTED", "STL_SORT", "BITMAP"], 1):
            changes = [_row(i, revision, value_type=value_type, vectors=False) for i in range(revision, 1100, 13)]
            self.upsert(client, name, changes)
            model.update({row["id"]: row for row in changes})
            self.flush(client, name)
            self.release_collection(client, name)
            self.drop_index(client, name, index_name="value_idx")
            self.alter_collection_properties(client, name, properties={"mmap.enabled": revision % 2 == 1})
            indexes, _ = self.prepare_index_params(client)
            indexes.add_index("clips[value]", index_name="value_idx", index_type=index_type)
            self.create_index(client, name, indexes)
            assert self.wait_for_index_ready(client, name, index_name="value_idx", timeout=300)
            self.load_collection(client, name)
            self._assert_model(client, name, model)
            self._assert_predicates(client, name, model, value_type)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expression",
        [
            "MATCH_LEAST(clips, $[value] > 0, threshold=0)",
            "MATCH_EXACT(clips, $[value] > 0, threshold=-1)",
            "MATCH_MOST(clips, $[value] > 0, threshold=-1)",
        ],
    )
    def test_struct_array_invalid_match_threshold_is_rejected(self, expression):
        """
        target: reject invalid MATCH thresholds without affecting subsequent valid queries
        method: issue malformed query, verify full retained data and valid predicate results
        expected: parameter error followed by correct complete query results
        """
        client = self._client()
        name = self._create_contract_collection(client, vectors=False)
        rows = [_row(i, vectors=False) for i in range(14)]
        model = {row["id"]: row for row in rows}
        self.insert(client, name, rows)
        error, _ = self.query(
            client, name, filter=expression, check_task=CheckTasks.err_res, check_items={ct.err_msg: ""}
        )
        assert error.code == 1100
        self._assert_model(client, name, model)
        self._assert_predicates(client, name, model)
