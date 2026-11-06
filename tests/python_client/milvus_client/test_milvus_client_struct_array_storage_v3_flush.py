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

"""Struct array storage v3 flush regression cases."""

import pytest
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_nullable_helpers import StructArrayNullableBase, _child, _record
from pymilvus import DataType
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status
from pymilvus.grpc_gen import milvus_pb2, schema_pb2


class TestMilvusClientStructArrayStorageV3Flush(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("entrance", ["create", "add_struct", "add_child"])
    @pytest.mark.parametrize("kind", ["scalar", "vector", "nested"])
    def test_growing_source_flush_requires_supported_storage_version(self, request, entrance, kind):
        """
        target: reject native-list schema changes only under V3 growing-source flush
        method: independently configured deployment, three DDL entrances and three layouts
        expected: precise configuration rejection without metadata changes, or durable NULL backfill
        """
        mode = request.config.getoption("--struct-storage-mode")
        if mode is None:
            pytest.skip("Requires explicit --struct-storage-mode and a verified isolated deployment")
        client = self._client()
        dtype, options = {
            "scalar": (DataType.INT64, {"nullable": True}),
            "vector": (DataType.FLOAT_VECTOR, {"nullable": True, "dim": 8}),
            "nested": (
                DataType.ARRAY,
                {"nullable": True, "element_type": DataType.INT64, "element_nullable": True, "max_capacity": 4},
            ),
        }[kind]
        children = client.create_struct_field_schema()
        children.add_field("value", dtype, **options)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field(
            "extra", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=children, nullable=True, max_capacity=8
        )
        if entrance == "create":
            name = cf.gen_unique_str("struct_storage_guard")
            try:
                raw = Prepare.create_collection_request(name, schema)
                status = client._get_connection()._stub.CreateCollection(raw, timeout=30)
                if mode == "v3-gsf":
                    assert status.code == 1100 and "growing-source flush is enabled" in status.reason, status
                    assert not client.has_collection(name)
                else:
                    check_status(status)
                    assert client.has_collection(name)
            finally:
                client.drop_collection(name)
            # A schema without nullable elements must still be admitted with the guard enabled.
            control = self._create(client, {"anchor": _child(DataType.INT64, False)})
            model = self._seed(client, control, [_record(n, [{"anchor": n}]) for n in range(6)])
            self._reload(client, control)
            self._check(client, control, model)
            return

        name = self._create(client, {"anchor": _child(DataType.INT64, False)})
        model = self._seed(
            client, name, [_record(n, None if n == 0 else [] if n == 1 else [{"anchor": n}]) for n in range(6)]
        )
        before = client.describe_collection(name)
        if entrance == "add_struct":
            wire = schema_pb2.CollectionSchema.FromString(Prepare.create_collection_request("unused", schema).schema)
            raw = milvus_pb2.AddCollectionStructFieldRequest(
                collection_name=name, struct_array_field_schema=wire.struct_array_fields[0]
            )
            status = client._get_connection()._stub.AddCollectionStructField(raw, timeout=30)
            if mode == "v3-gsf":
                assert status.code == 1100 and "growing-source flush is enabled" in status.reason, status
            else:
                check_status(status)
                for row in model.values():
                    row["extra"] = None
        else:
            result, succeeded = self.add_collection_field(
                client, name, "clips[value]", dtype, **options, check_task=CheckTasks.check_nothing
            )
            if mode == "v3-gsf":
                assert not succeeded and getattr(result, "code", None) == 1100, result
                assert "growing-source flush is enabled" in str(result), result
            else:
                assert succeeded, result
                for row in model.values():
                    for element in row["clips"] or []:
                        element["value"] = None
        if mode == "v3-gsf":
            assert client.describe_collection(name) == before
        elif kind == "vector":
            indexes = client.prepare_index_params()
            indexes.add_index(
                "extra[value]" if entrance == "add_struct" else "clips[value]", index_type="FLAT", metric_type="L2"
            )
            self.create_index(client, name, indexes)
        self._reload(client, name)
        self._check(client, name, model)
