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

"""Struct array growing sealed search regression cases."""

import copy
import json
import os
import time
from urllib.parse import urlsplit

import pytest
import requests
from common import common_func as cf
from common.common_type import CaseLabel
from common.struct_array_row_model import DIM, StructArrayRowModelBase, _row, _score
from pymilvus import DataType
from utils.util_log import test_log as log


class TestMilvusClientStructArrayGrowingSealedSearch(StructArrayRowModelBase):
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
            struct_schema=self._struct_schema(client),
            max_capacity=8,
            nullable=True,
        )
        self.create_collection(
            client, name, schema=schema, shards_num=1, properties={"collection.autocompaction.enabled": "false"}
        )
        rows = [_row(i) for i in range(4000)]
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
        self._assert_model(client, name, model)
        self._assert_predicates(client, name, model)
        self._assert_element_search(client, name, model, metric, "HNSW")
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
                expected = _score(query, model[hit["id"]]["clips"][hit["offset"]]["emb"], metric)
                assert hit["distance"] == pytest.approx(expected, abs=2e-4, rel=2e-4)
        observe("after_search")
