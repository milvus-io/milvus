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

"""Struct array query null reduction regression cases."""

import copy
import json

import pytest
from common import common_func as cf
from common.common_type import CaseLabel
from common.struct_array_protobuf_helpers import _fields
from common.struct_array_row_model import DIM, StructArrayRowModelBase, _assert_value, _row
from pymilvus import DataType
from pymilvus.client.prepare import Prepare
from utils.util_log import test_log as log


class TestMilvusClientStructArrayQueryNullReduction(StructArrayRowModelBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    @pytest.mark.parametrize("tail", ["all_null", "mixed"])
    def test_struct_array_query_reduction_preserves_all_null_validity(self, pk_type, tail):
        """
        target: retain row masks when reducing all-null and mixed vector/struct query outputs
        method: create four write populations, query cross-population subsets through SDK and raw RPC
        expected: exact rows and legacy/current validity masks agree before and after reload
        """
        client = self._client()
        name = cf.gen_unique_str("struct_reduction")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(
            "id",
            DataType.INT64 if pk_type == "int" else DataType.VARCHAR,
            is_primary=True,
            **({"max_length": 64} if pk_type == "str" else {}),
        )
        schema.add_field("revision", DataType.INT64)
        schema.add_field("bucket", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=DIM, nullable=True)
        schema.add_field("note", DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field(
            "clips",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=self._struct_schema(client),
            max_capacity=8,
            nullable=True,
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_type="FLAT", metric_type="L2")
        indexes.add_index("clips[emb]", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=2,
            properties={"collection.autocompaction.enabled": "false"},
        )
        rows = [_row(number, pk_type=pk_type) for number in range(44)]
        for number, item in enumerate(rows):
            if number // 11 in (0, 2) or (number // 11 == 3 and tail == "all_null"):
                item.update(normal_vector=None, clips=None, note=None)
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        for batch in range(4):
            self.insert(client, name, rows[batch * 11 : (batch + 1) * 11])
            if batch < 3:
                self.flush(client, name)
        groups = [
            list(range(11)) + list(range(22, 33)) + list(range(33, 44)),
            list(range(44)),
            [0, 11, 22, 33],
            [1, 12, 23, 34],
        ]
        for phase in ("before_reload", "after_reload"):
            if phase == "after_reload":
                self._reload(client, name)
            for numbers in groups:
                keys = [rows[number]["id"] for number in numbers]
                expression = f"id in {json.dumps(keys)}"
                expected = {key: model[key] for key in keys}
                decoded, _ = self.query(
                    client, name, filter=expression, output_fields=["*"], limit=100, consistency_level="Strong"
                )
                assert len(decoded) == len(expected) and {item["id"] for item in decoded} == set(expected)
                for item in decoded:
                    _assert_value(item, expected[item["id"]])
                request = Prepare.query_request(
                    name,
                    expression,
                    ["id", "normal_vector", "note", "clips"],
                    [],
                    use_default_consistency=False,
                    consistency_level=0,
                    limit=100,
                )
                response = client._get_connection()._stub.Query(request, timeout=30)
                assert response.status.code == 0, response.status
                fields = dict(_fields(response))
                ids = fields["id"].scalars.long_data.data if pk_type == "int" else fields["id"].scalars.string_data.data
                assert len(ids) == len(expected) and set(ids) == set(expected)
                for path in ("normal_vector", "note", "clips[value]", "clips[tag]", "clips[flag]", "clips[emb]"):
                    field = fields[path]
                    root = path.split("[")[0]
                    validity = [expected[key][root] is not None for key in ids]
                    assert list(field.valid_data) == validity, (phase, path, list(field.valid_data), validity)
                    wrapper = field.WhichOneof("field")
                    if wrapper:
                        assert list(getattr(field, wrapper).valid_data) == validity, (phase, path, field)
                    else:
                        assert not any(validity), (phase, path)
                log.info(
                    "STRUCT_REDUCTION_VALIDITY phase=%s pk=%s tail=%s rows=%s all_null=%s",
                    phase,
                    pk_type,
                    tail,
                    len(ids),
                    all(expected[key]["normal_vector"] is None for key in ids),
                )
