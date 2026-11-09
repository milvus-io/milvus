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

"""Struct Array schema E2E coverage."""

import copy
import secrets
import time

import grpc
import pytest
from base.struct_array import StructArrayTestBase, TypedStructArrayTestBase
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_data import (
    SCALAR_IDS,
    SCALARS,
    child_schema,
    make_nullable_elements,
    make_nullable_row,
    make_predicate_rows,
    make_typed_vector,
    normalized_vector,
    nullable_children,
    scalar_boundary_value,
)
from pymilvus import CollectionSchema, DataType, FieldSchema, MilvusClient
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status
from pymilvus.exceptions import MilvusException
from pymilvus.grpc_gen import milvus_pb2, schema_pb2
from utils.util_log import test_log as log


class TestMilvusClientStructArraySchemaValidation(StructArrayTestBase):
    @staticmethod
    def _nested_wire(client, name):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        children = client.create_struct_field_schema()
        children.add_field(
            "tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4, nullable=True, element_nullable=True
        )
        schema.add_field(
            "clips", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=children, nullable=True, max_capacity=8
        )
        request = Prepare.create_collection_request(name, schema)
        return request, schema_pb2.CollectionSchema.FromString(request.schema)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("parent_nullable", [False, True])
    @pytest.mark.parametrize("kind", ["scalar", "vector", "nested"])
    def test_added_child_inherits_parent_and_rejects_capacity_conflict(self, kind, parent_nullable):
        """
        target: preserve parent nullability/capacity while independently retaining nested capacity
        method: reject an explicit conflicting outer capacity, then add with omitted capacity and conflicting row flag
        expected: conflicting true row flag is rejected on a required parent; accepted fields inherit parent metadata
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children(), parent_nullable=parent_nullable)
        rows = [make_nullable_row(n, make_nullable_elements(n)) for n in range(12)]
        if not parent_nullable:
            for row in rows:
                if row["clips"] is None:
                    row["clips"] = []
        model = self.insert_rows_with_model(client, name, rows)
        before = client.describe_collection(name)
        dtype, options = {
            "scalar": (DataType.INT64, {}),
            "vector": (DataType.FLOAT_VECTOR, {"dim": 8}),
            "nested": (DataType.ARRAY, {"element_type": DataType.INT64, "max_capacity": 4, "element_nullable": True}),
        }[kind]
        field = Prepare.prepare_collection_field(FieldSchema("clips[added]", dtype, nullable=True, **options))
        good = Prepare.alter_collection_schema_request(name, field_schema=field)
        raw_field = good.action.add_request.field_infos[0].field_schema
        raw_field.nullable = not parent_nullable
        if kind == "nested":
            raw_field.type_schema.nullable = not parent_nullable
        bad = copy.deepcopy(good)
        conflicting = bad.action.add_request.field_infos[0].field_schema
        params = conflicting.type_schema.type_params if kind == "nested" else conflicting.type_params
        params.add(key="max_capacity", value="7")
        handler = client._get_connection()
        status = handler._stub.AlterCollectionSchema(bad, timeout=30).alter_status
        assert status.code == 1100 and "but struct field clips uses 8" in status.reason, status
        assert client.describe_collection(name) == before
        self.assert_rows(client, name, model)
        if not parent_nullable:
            status = handler._stub.AlterCollectionSchema(good, timeout=30).alter_status
            assert (
                status.code == 1100
                and "sub-field in non-nullable struct cannot be nullable individually" in status.reason
            ), status
            assert client.describe_collection(name) == before
            self.assert_rows(client, name, model)
            raw_field.nullable = False
            if kind == "nested":
                raw_field.type_schema.nullable = False
        check_status(handler._stub.AlterCollectionSchema(good, timeout=30).alter_status)
        from pymilvus.grpc_gen import milvus_pb2

        response = handler._stub.DescribeCollection(milvus_pb2.DescribeCollectionRequest(collection_name=name))
        parent = next(f for f in response.schema.struct_array_fields if f.name == "clips")
        added = parent.fields[-1]
        assert added.name == "added" and added.fieldID > max(f.fieldID for f in parent.fields[:-1])
        assert added.nullable == parent.nullable == parent_nullable
        params = added.type_schema.type_params if kind == "nested" else added.type_params
        assert {p.key: p.value for p in params}["max_capacity"] == "8"
        if kind == "nested":
            assert added.type_schema.nullable == parent_nullable
            assert {p.key: p.value for p in added.type_schema.array_element.type_params}["max_capacity"] == "4"
        if kind == "vector":
            indexes = client.prepare_index_params()
            indexes.add_index("clips[added]", index_type="FLAT", metric_type="L2")
            client.create_index(name, indexes)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("kind", ["element_nullable", "nested"])
    def test_add_top_level_native_array_rejected(self, kind):
        """
        target: reject native-list placement at AddCollectionField as well as Create
        method: raw schema alteration with a top-level element-nullable or nested array
        expected: precise placement rejection, unchanged schema/data, ordinary Array control succeeds
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(12)]
        )
        before = client.describe_collection(name)
        if kind == "nested":
            _, wire = self._nested_wire(client, name)
            field = copy.deepcopy(wire.struct_array_fields[0].fields[0])
            field.name = "new_array"
            field.nullable = True
            field.element_nullable = False
            field.type_schema.array_element.nullable = False
        else:
            field = Prepare.prepare_collection_field(
                FieldSchema("new_array", DataType.ARRAY, element_type=DataType.INT64, nullable=True, max_capacity=8)
            )
            field.element_nullable = True
        raw = Prepare.alter_collection_schema_request(name, field_schema=field)
        status = client._get_connection()._stub.AlterCollectionSchema(raw, timeout=30).alter_status
        assert status.code == 1100, status
        reason = "element_nullable is only supported" if kind == "element_nullable" else "nested array can only"
        assert reason in status.reason, status
        assert client.describe_collection(name) == before
        self.assert_rows(client, name, model)
        client.add_collection_field(
            name, "new_array", DataType.ARRAY, element_type=DataType.INT64, max_capacity=8, nullable=True
        )
        for row in model.values():
            row["new_array"] = None
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("level", ["outer", "inner"])
    def test_struct_capacity_respects_configured_proxy_limit(self, request, level):
        """
        target: obey an explicitly configured capacity instead of hardcoding 4096
        method: schema limits at max-1/max/max+1, full NULL payload and reload at accepted boundaries
        expected: effective maximum appears in rejection and accepted values retain every NULL slot
        """
        maximum = request.config.getoption("--struct-max-array-capacity")
        if maximum is None:
            pytest.skip("Requires an isolated deployment with verified proxy.maxArrayCapacity")
        assert maximum > 4096
        client = self._client()
        for capacity in (maximum - 1, maximum, maximum + 1):
            name = cf.gen_unique_str("struct_custom_capacity")
            raw, wire = self._nested_wire(client, name)
            field = wire.struct_array_fields[0].fields[0]
            if level == "outer":
                field.type_params[0].value = str(capacity)
                field.type_schema.type_params[0].value = str(capacity)
            else:
                field.type_schema.array_element.type_params[0].value = str(capacity)
            raw.schema = wire.SerializeToString()
            try:
                status = client._get_connection()._stub.CreateCollection(raw, timeout=30)
                if capacity > maximum:
                    assert status.code == 1100 and f"(0, {maximum}]" in status.reason, status
                    assert not client.has_collection(name)
                    continue
                check_status(status)
                index = client.prepare_index_params()
                index.add_index("vector", index_type="FLAT", metric_type="L2")
                client.create_index(name, index)
                client.load_collection(name)
                clips = [{"tags": None}] * capacity if level == "outer" else [{"tags": [None] * capacity}]
                client.insert(name, [{"id": 1, "vector": [0.25] * 8, "clips": clips}])
                self.reload_collection(client, name)
                rows = client.query(name, filter="id >= 0", output_fields=["id", "clips"], consistency_level="Strong")
                assert list(rows) == [{"id": 1, "clips": clips}]
            finally:
                client.drop_collection(name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "violation,reason",
        [
            ("top_nullable", "element_nullable"),
            ("top_nested", "nested array"),
            ("vector_leaf", "element type floatvector is not supported"),
            ("struct_leaf", "element type struct is not supported"),
            ("deeper", "nested"),
        ],
    )
    def test_invalid_native_list_schema_reaches_proxy(self, violation, reason):
        """
        target: enforce placement, leaf type and nesting-depth limits at CreateCollection
        method: mutate the serialized type tree and retry the unchanged valid schema
        expected: the invalid schema is absent and the valid control is admitted
        """
        client = self._client()
        name = cf.gen_unique_str("struct_type_guard")
        good, wire = self._nested_wire(client, name)
        bad = copy.deepcopy(good)
        field = wire.struct_array_fields[0].fields[0]
        if violation == "top_nullable":
            top = wire.fields.add(
                name="plain_array", data_type=DataType.ARRAY, element_type=DataType.INT64, element_nullable=True
            )
            top.type_params.add(key="max_capacity", value="8")
        elif violation == "top_nested":
            top = wire.fields.add()
            top.CopyFrom(field)
            top.name = "nested_top"
            top.element_nullable = False
        else:
            leaf = field.type_schema.array_element.array_element
            if violation == "deeper":
                previous = copy.deepcopy(leaf)
                leaf.ClearField("leaf_type")
                leaf.array_element.CopyFrom(previous)
                leaf.type_params.add(key="max_capacity", value="4")
            else:
                leaf.leaf_type = DataType.FLOAT_VECTOR if violation == "vector_leaf" else DataType.STRUCT
        bad.schema = wire.SerializeToString()
        try:
            status = client._get_connection()._stub.CreateCollection(bad, timeout=30)
            assert status.code == 1100 and reason in status.reason.lower(), status
            assert not client.has_collection(name)
            check_status(client._get_connection()._stub.CreateCollection(good, timeout=30))
            assert client.has_collection(name)
        finally:
            client.drop_collection(name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("level", ["outer", "inner"])
    @pytest.mark.parametrize("capacity", [0, 1, 4096, 4097])
    def test_nested_array_capacity_inherits_or_rejects_conflict(self, level, capacity):
        """
        target: enforce default capacity bounds and count NULL entries as array elements
        method: raw Create RPC, exact-boundary insertion and one-extra NULL rejection
        expected: 0/max+1 rejected, 1/max accepted, rejected writes leave the retained row unchanged
        """
        client = self._client()
        name = cf.gen_unique_str("struct_capacity_guard")
        request, wire = self._nested_wire(client, name)
        field = wire.struct_array_fields[0].fields[0]
        if level == "outer":
            field.type_params[0].value = str(capacity)
            field.type_schema.type_params[0].value = str(capacity)
        else:
            field.type_schema.array_element.type_params[0].value = str(capacity)
        request.schema = wire.SerializeToString()
        try:
            status = client._get_connection()._stub.CreateCollection(request, timeout=30)
            if capacity in (0, 4097):
                assert status.code == 1100 and "capacity" in status.reason.lower(), status
                assert not client.has_collection(name)
                return
            check_status(status)
            index = client.prepare_index_params()
            index.add_index("vector", index_type="FLAT", metric_type="L2")
            client.create_index(name, index)
            client.load_collection(name)
            clips = [{"tags": None}] * capacity if level == "outer" else [{"tags": [None] * capacity}]
            row = {"id": 1, "vector": [0.25] * 8, "clips": clips}
            client.insert(name, [row])
            bad = copy.deepcopy(row)
            bad["id"] = 2
            if level == "outer":
                bad["clips"].append({"tags": None})
            else:
                bad["clips"][0]["tags"].append(None)
            handler = client._get_connection()
            raw = handler._prepare_row_insert_request(name, [bad])
            rejected = handler._stub.Insert(raw, timeout=30).status
            assert rejected.code == 1100, rejected
            actual = client.query(name, filter="id >= 0", output_fields=["id", "clips"], consistency_level="Strong")
            assert list(actual) == [{"id": 1, "clips": clips}]
            self.reload_collection(client, name)
            actual = client.query(name, filter="id >= 0", output_fields=["id", "clips"], consistency_level="Strong")
            assert list(actual) == [{"id": 1, "clips": clips}]
        finally:
            client.drop_collection(name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "violation,reason",
        [
            ("required", "element_nullable=true"),
            ("default", "default value"),
            ("is_primary_key", "primary key"),
            ("autoID", "autoID"),
            ("is_partition_key", "partition key"),
            ("is_clustering_key", "clustering key"),
            ("is_function_output", "function output"),
            ("is_dynamic", "protected role"),
            ("external_field", "external field"),
            ("index", "binding an index"),
            ("function", "combined with a function"),
            ("multiple", "only one field info"),
        ],
    )
    def test_add_subfield_rejects_invalid_schema_without_mutation(self, violation, reason):
        """
        target: enforce independent AddCollectionField guards at the server boundary
        method: mutate a valid nullable-child request, compare schema/data and retry the control
        expected: intended validation reason, no partial DDL, ordered NULL backfill on valid add
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(12)]
        )
        before = client.describe_collection(name)
        field = Prepare.prepare_collection_field(FieldSchema("clips[added]", DataType.INT64, nullable=True))
        good = Prepare.alter_collection_schema_request(name, field_schema=field)
        bad = copy.deepcopy(good)
        add = bad.action.add_request
        field = add.field_infos[0].field_schema
        if violation == "required":
            field.element_nullable = False
        elif violation == "default":
            field.default_value.long_data = 7
        elif violation == "external_field":
            field.external_field = "external_source"
        elif violation == "index":
            add.field_infos[0].index_name = "bound_index"
        elif violation == "function":
            add.func_schema.add(name="bound_function")
        elif violation == "multiple":
            other = copy.deepcopy(add.field_infos[0])
            other.field_schema.name = "clips[other]"
            add.field_infos.append(other)
        else:
            setattr(field, violation, True)
        handler = client._get_connection()
        status = handler._stub.AlterCollectionSchema(bad, timeout=30).alter_status
        assert status.code == 1100 and reason in status.reason, status
        assert client.describe_collection(name) == before
        self.assert_rows(client, name, model)
        check_status(handler._stub.AlterCollectionSchema(good, timeout=30).alter_status)
        parent = next(f for f in client.describe_collection(name)["fields"] if f["name"] == "clips")
        old_parent = next(f for f in before["fields"] if f["name"] == "clips")
        assert [f["name"] for f in parent["struct_fields"]] == [f["name"] for f in old_parent["struct_fields"]] + [
            "added"
        ]
        assert parent["nullable"] == old_parent["nullable"]
        assert parent["params"] == old_parent["params"]
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("property_name", ["enable_match", "enable_analyzer"])
    @pytest.mark.parametrize("entrance", ["create", "add"])
    def test_struct_text_features_rejected_with_false_control(self, property_name, entrance):
        """
        target: reject text-match/analyzer features on struct children at Create and Add
        method: alter a valid wire schema after SDK serialization and retry with explicit false
        expected: specific server rejection, unchanged collection state and successful false control
        """
        client = self._client()
        if entrance == "add":
            name = self.create_struct_collection(client, nullable_children())
            model = self.insert_rows_with_model(
                client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(6)]
            )
            before = client.describe_collection(name)
            field = Prepare.prepare_collection_field(
                FieldSchema("clips[text]", DataType.VARCHAR, max_length=32, nullable=True)
            )
            request = Prepare.alter_collection_schema_request(name, field_schema=field)
            field = request.action.add_request.field_infos[0].field_schema
            field.type_params.add(key=property_name, value="true")
            status = client._get_connection()._stub.AlterCollectionSchema(request, timeout=30).alter_status
            assert status.code == 1100 and property_name in status.reason, status
            assert client.describe_collection(name) == before
            self.assert_rows(client, name, model)
            field.type_params[-1].value = "false"
            check_status(client._get_connection()._stub.AlterCollectionSchema(request, timeout=30).alter_status)
            for row in model.values():
                for element in row["clips"] or []:
                    element["text"] = None
            self.reload_collection(client, name)
            self.assert_rows(client, name, model)
        else:
            name = cf.gen_unique_str("struct_text_guard")
            schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
            children = client.create_struct_field_schema()
            children.add_field("text", DataType.VARCHAR, max_length=32, nullable=True)
            schema.add_field(
                "clips",
                DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=children,
                nullable=True,
                max_capacity=8,
            )
            request = Prepare.create_collection_request(name, schema)
            wire = schema_pb2.CollectionSchema.FromString(request.schema)
            field = wire.struct_array_fields[0].fields[0]
            field.type_params.add(key=property_name, value="true")
            request.schema = wire.SerializeToString()
            try:
                status = client._get_connection()._stub.CreateCollection(request, timeout=30)
                assert status.code == 1100 and property_name in status.reason, status
                assert not client.has_collection(name)
                field.type_params[-1].value = "false"
                request.schema = wire.SerializeToString()
                check_status(client._get_connection()._stub.CreateCollection(request, timeout=30))
                assert client.has_collection(name)
            finally:
                client.drop_collection(name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [False, True])
    def test_append_whole_struct_requires_nullable(self, nullable):
        """
        target: require nullable parents when appending an entire Struct
        method: send the actual AddCollectionStructField RPC and compare historical rows
        expected: required parent rejected atomically; nullable parent backfills NULL through reload
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(6)]
        )
        before = client.describe_collection(name)
        children = client.create_struct_field_schema()
        children.add_field("value", DataType.INT64, nullable=True)
        # Build the parent using the collection serializer so the raw RPC tests server validation.
        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "extra",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=children,
            nullable=nullable,
            max_capacity=3,
        )
        wire = schema_pb2.CollectionSchema.FromString(Prepare.create_collection_request("unused", schema).schema)
        from pymilvus.grpc_gen import milvus_pb2

        request = milvus_pb2.AddCollectionStructFieldRequest(
            collection_name=name, struct_array_field_schema=wire.struct_array_fields[0]
        )
        status = client._get_connection()._stub.AddCollectionStructField(request, timeout=30)
        if not nullable:
            assert status.code == 1100 and "must be nullable" in status.reason, status
            assert client.describe_collection(name) == before
        else:
            check_status(status)
            for row in model.values():
                row["extra"] = None
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_utf8_byte_boundary_counts_bytes_not_characters(self):
        """
        target: enforce a VARCHAR child limit in UTF-8 bytes
        method: write five/six-byte values with NULL elements, reject seven bytes through raw RPC
        expected: exact byte boundary survives reload and rejected new PK stays absent
        """
        client = self._client()
        name = self.create_struct_collection(client, {"text": child_schema(DataType.VARCHAR, max_length=6)})
        rows = [make_nullable_row(n, [{"text": value}, {"text": None}]) for n, value in enumerate(["雪ab", "雪雪"])]
        model = self.insert_rows_with_model(client, name, rows)
        bad = make_nullable_row(100, [{"text": "雪雪a"}])
        handler = client._get_connection()
        status = handler._stub.Insert(handler._prepare_row_insert_request(name, [bad]), timeout=30).status
        assert status.code == 1100 and "length" in status.reason, status
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("guard", ["last_child", "last_original", "last_vector"])
    def test_drop_protection_reasons_are_independently_reachable(self, guard):
        """
        target: reach each last-child/original/vector deletion guard independently
        method: construct distinct valid schemas and compare complete metadata after rejection
        expected: the intended protection reason and unchanged schema/index inventory
        """
        client = self._client()
        if guard != "last_vector":
            name = self.create_struct_collection(client, {"anchor": child_schema(DataType.INT64, False)})
            if guard == "last_original":
                self.add_collection_field(client, name, "clips[added]", DataType.INT64, nullable=True)
            target = "clips[anchor]"
        else:
            name = cf.gen_unique_str("struct_last_vector")
            schema = client.create_schema(auto_id=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            children = client.create_struct_field_schema()
            children.add_field("anchor", DataType.INT64)
            children.add_field("emb", DataType.FLOAT_VECTOR, dim=8, nullable=True)
            schema.add_field(
                "clips",
                DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=children,
                max_capacity=8,
                nullable=True,
            )
            self.create_collection(client, name, schema=schema)
            target = "clips[emb]"
        before = copy.deepcopy(client.describe_collection(name))
        indexes = set(client.list_indexes(name))
        result, ok = self.drop_collection_field(client, name, field_name=target, check_task=CheckTasks.check_nothing)
        assert not ok and getattr(result, "code", None) == 1100, result
        word = {
            "last_child": "last sub-field",
            "last_original": "every remaining sub-field was added after the struct was created",
            "last_vector": "last vector",
        }[guard]
        assert word in str(result), result
        assert client.describe_collection(name) == before
        assert set(client.list_indexes(name)) == indexes

    @pytest.mark.tags(CaseLabel.L2)
    def test_stale_schema_omits_added_child_and_rejects_dropped_child(self):
        """
        target: retain struct shape when an old client omits an added child
        method: prepare a pre-DDL request, add a child, replay it, then drop an original child
        expected: old client receives null backfill; dropped wire columns are rejected atomically
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(24)]
        )
        row = make_nullable_row(102, make_nullable_elements(102))
        handler = client._get_connection()
        stale = handler._prepare_row_insert_request(name, [row])
        # Exercise the documented unpinned old-client path.
        stale.schema_timestamp = 0
        self.add_collection_field(client, name, "clips[added]", DataType.INT64, nullable=True)
        for stored in model.values():
            for element in stored["clips"] or []:
                element["added"] = None
        check_status(handler._stub.Insert(stale, timeout=30).status)
        model[row["id"]] = copy.deepcopy(row)
        for element in model[row["id"]]["clips"] or []:
            element["added"] = None
        self.assert_rows(client, name, model)
        self.drop_collection_field(client, name, field_name="clips[label]")
        for stored in model.values():
            for element in stored["clips"] or []:
                element.pop("label")
        response = handler._stub.Insert(stale, timeout=30)
        assert response.status.code == 1100, response.status
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayAddSubfield(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_with_nullable_child_backfills_and_preserves_null_elements(self):
        """
        target: accept nullable scalar children when adding a whole struct through the SDK
        method: add to existing rows, verify backfill, then write null/omitted/valued children and reload
        expected: parent and child nullability round trip and every logical element survives
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(6)]
        )
        children, _ = self.create_struct_field_schema(client)
        children.add_field("score", DataType.INT64, nullable=True)
        self.add_collection_struct_field(client, name, "added", children, max_capacity=8)
        for row in model.values():
            row["added"] = None
        self.refresh_load(client, name)
        self.assert_rows(client, name, model)

        description, _ = self.describe_collection(client, name)
        parent = next(field for field in description["fields"] if field["name"] == "added")
        restored = CollectionSchema.construct_from_dict(description)
        added = next(field for field in restored.struct_fields if field.name == "added")
        assert parent["nullable"] is True
        assert added.max_capacity == 8
        assert added.fields[0].name == "score" and added.fields[0].nullable is True

        updates = [
            {"id": 0, "added": [{"score": None}, {"score": 7}, {}]},
            {"id": 1, "added": []},
            {"id": 2, "added": None},
        ]
        self.upsert(client, name, updates, partial_update=True)
        model[0]["added"] = [{"score": None}, {"score": 7}, {"score": None}]
        model[1]["added"] = []
        self.assert_rows(client, name, model)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["scalar", "vector", "array"])
    @pytest.mark.parametrize("parent_nullable", [False, True])
    @pytest.mark.parametrize("before_ddl", ["growing", "sealed"])
    def test_add_child_backfills_each_existing_element(self, kind, parent_nullable, before_ddl):
        """
        target: add a real sub-field to a populated struct without changing original offsets
        method: add scalar/vector/Array to growing or sealed rows, write and drop/re-add it
        expected: N null children per old row, preserved parent validity, fresh IDs on re-add
        """
        client = self._client()
        name = self.create_struct_collection(
            client,
            {"anchor": child_schema(DataType.INT64, False), "old": child_schema(DataType.INT64)},
            parent_nullable,
        )
        rows = [
            make_nullable_row(
                n, None if parent_nullable and n % 11 == 0 else [{"anchor": j, "old": n + j} for j in range(n % 6)]
            )
            for n in range(1200)
        ]
        model = self.insert_rows_with_model(client, name, rows, split=False)
        if before_ddl == "sealed":
            self.reload_collection(client, name)
        dtype, options = {
            "scalar": child_schema(DataType.VARCHAR),
            "vector": child_schema(DataType.FLOAT_VECTOR),
            "array": child_schema(DataType.ARRAY, element_type=DataType.INT64, max_capacity=4, element_nullable=True),
        }[kind]
        self.add_collection_field(client, name, "clips[added]", dtype, **options)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self.assert_rows(client, name, model)
        if kind == "vector":
            indexes, _ = self.prepare_index_params(client)
            indexes.add_index("clips[added]", index_type="FLAT", metric_type="L2")
            self.create_index(client, name, indexes)
        self.refresh_load(client, name)
        changes = []
        for n in list(range(0, 1200, 17)) + [1300]:
            value = {"scalar": "雪", "vector": normalized_vector(n), "array": [n, None]}[kind]
            patch = make_nullable_row(
                n, [{"anchor": 0, "old": n, "added": value}, {"anchor": 1, "old": None, "added": None}], revision=3
            )
            changes.append(patch)
        self.upsert(client, name, changes)
        model.update({r["id"]: copy.deepcopy(r) for r in changes})
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        old_id = self.get_child_id(client, name, "added")
        self.drop_collection_field(client, name, field_name="clips[added]")
        self.add_collection_field(client, name, "clips[added]", DataType.INT64, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self.assert_rows(client, name, model)
        new_id = self.get_child_id(client, name, "added")
        assert new_id > old_id
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("path", ["missing[new]", "clips[]", "[new]", "clips[new][leaf]", "clips[anchor]"])
    def test_invalid_add_child_is_non_mutating(self, path):
        """
        target: reject malformed, missing-parent and duplicate child additions atomically
        method: compare schema and full rows around failed DDL, then add a valid child
        expected: rejected DDL leaves schema/data intact and a valid addition still works
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(20)]
        )
        before, _ = self.describe_collection(client, name)
        response, succeeded = self.add_collection_field(
            client, name, path, DataType.INT64, nullable=True, check_task=CheckTasks.check_nothing
        )
        assert not succeeded and getattr(response, "code", None) in (1, 1100), response
        after, _ = self.describe_collection(client, name)
        assert after["fields"] == before["fields"]
        self.assert_rows(client, name, model)
        self.add_collection_field(client, name, "clips[valid_new]", DataType.INT64, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["valid_new"] = None
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    def test_added_children_survive_compaction_and_mmap(self):
        """
        target: materialize child backfill without changing nested validity or original fields
        method: two sealed batches, add scalar/Array children, partial writes, real compaction and mmap
        expected: completed compaction has output segments and all rows/predicates stay correct
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        rows = [make_nullable_row(n, make_nullable_elements(n)) for n in range(1500)]
        model = self.insert_rows_with_model(client, name, rows)
        self.flush(client, name)
        self.add_collection_field(client, name, "clips[new_scalar]", DataType.INT64, nullable=True)
        self.add_collection_field(
            client,
            name,
            "clips[new_array]",
            DataType.ARRAY,
            nullable=True,
            element_type=DataType.VARCHAR,
            max_length=16,
            max_capacity=3,
            element_nullable=True,
        )
        for row in model.values():
            for element in row["clips"] or []:
                element.update(new_scalar=None, new_array=None)
        changes = []
        for n in range(0, 1500, 7):
            row = copy.deepcopy(model[n])
            for element in row["clips"] or []:
                element.update(new_scalar=n, new_array=["雪", None])
            changes.append(row)
        self.upsert(client, name, changes)
        model.update({r["id"]: r for r in changes})
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        deadline = time.monotonic() + 120
        while True:
            compact_id, _ = self.compact(client, name)
            if compact_id > 0:
                break
            assert time.monotonic() < deadline, "No compaction accepted"
            time.sleep(2)
        assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
        # The shared wrapper has no get_compaction_plans method.
        plans = client.get_compaction_plans(compact_id, timeout=30)
        assert any(p.sources and p.targets for p in plans.plans), plans
        log.info(f"STRUCT_ENHANCE_COMPACTION id={compact_id} plans={plans}")
        self.release_collection(client, name)
        self.alter_collection_properties(client, name, properties={"mmap.enabled": True})
        self.load_collection(client, name)
        self.assert_rows(client, name, model)
        self.assert_nullable_predicates(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_new_vector_child_search_ready_after_refresh(self):
        """
        target: make a newly indexed vector child searchable when refresh reports completion
        method: add to sealed rows, create FLAT index, refresh, search before any new write
        expected: existing null children yield zero hits without a field-not-loaded error
        """
        client = self._client()
        # Match the minimal reproducer: no unrelated top-level vector index.
        name = cf.gen_unique_str("struct_refresh")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        struct, _ = self.create_struct_field_schema(client)
        struct.add_field("v", DataType.FLOAT_VECTOR, dim=2, nullable=True)
        struct.add_field("score", DataType.INT64, nullable=True)
        schema.add_field(
            "clips", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct, max_capacity=8, nullable=False
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("clips[v]", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong")
        rows = [{"id": n, "clips": [{"v": [float(n), 0.0], "score": n}, {"v": None, "score": None}]} for n in range(5)]
        model = self.insert_rows_with_model(client, name, rows, split=False)
        self.reload_collection(client, name)
        self.add_collection_field(client, name, "clips[new_vector]", DataType.FLOAT_VECTOR, dim=2, nullable=True)
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("clips[new_vector]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, indexes)
        self.refresh_load(client, name)
        result, _ = self.search(
            client, name, [[7.0, 0.0]], anns_field="clips[new_vector]", limit=10, search_params={"metric_type": "L2"}
        )
        assert result == [[]]
        for row in model.values():
            for element in row["clips"] or []:
                element["new_vector"] = None
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayAddedSubfieldTypes(TypedStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    @pytest.mark.parametrize("nested", [False, True], ids=["scalar", "nested"])
    def test_added_scalar_types_backfill_write_and_reload(self, dtype, nested):
        """
        target: exercise every scalar and nested leaf type through AddCollectionField
        method: populated shared schema, NULL backfill, selected replacements and full reload
        expected: old NULL/empty parents remain distinct and new values retain their type
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(pk, make_nullable_elements(pk)) for pk in range(32)]
        )
        options = {"nullable": True}
        if dtype == DataType.VARCHAR:
            options["max_length"] = 32
        if nested:
            options.update(element_type=dtype, max_capacity=4, element_nullable=True)
        self.add_collection_field(client, name, "clips[added]", DataType.ARRAY if nested else dtype, **options)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self.assert_rows(client, name, model)
        changes = []
        for pk in range(2, 32, 3):
            row = copy.deepcopy(model[pk])
            for j, element in enumerate(row["clips"] or []):
                element["added"] = (
                    [scalar_boundary_value(dtype, pk + j), None] if nested else scalar_boundary_value(dtype, pk + j)
                )
            changes.append(row)
            model[pk] = row
        self.upsert(client, name, changes, partial_update=True)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "dtype",
        [
            DataType.FLOAT_VECTOR,
            DataType.FLOAT16_VECTOR,
            DataType.BFLOAT16_VECTOR,
            DataType.INT8_VECTOR,
            DataType.BINARY_VECTOR,
        ],
        ids=lambda d: d.name,
    )
    def test_added_typed_vector_null_backfill_and_search(self, dtype):
        """
        target: distinguish typed vector creation from nullable typed vector addition
        method: add to sealed rows, index/reload, write one logical position then clear it
        expected: NULL backfill, exactly one coordinate after replacement, zero hits after NULL
        """
        client = self._client()
        kind = "BIN_FLAT" if dtype == DataType.BINARY_VECTOR else "HNSW" if dtype == DataType.INT8_VECTOR else "FLAT"
        metric = "HAMMING" if dtype == DataType.BINARY_VECTOR else "L2"
        name = self.create_typed_collection(client, dtype, kind, metric)
        model = self.insert_rows_with_model(client, name, self.make_typed_rows(dtype), split=False)
        self.reload_collection(client, name)
        self.add_collection_field(
            client, name, "clips[newvec]", dtype, nullable=True, dim=128 if dtype == DataType.BINARY_VECTOR else 32
        )
        for row in model.values():
            for element in row["clips"] or []:
                element["newvec"] = None
        self.assert_typed_rows(client, name, model, dtype)
        index, _ = self.prepare_index_params(client)
        index.add_index(
            "clips[newvec]",
            index_type=kind,
            metric_type=metric,
            params={"M": 16, "efConstruction": 200} if kind == "HNSW" else {},
        )
        self.create_index(client, name, index)
        query = make_typed_vector(dtype, 500001)
        for phase in range(3):
            self.reload_collection(client, name)
            hits, _ = self.search(
                client, name, [query], anns_field="clips[newvec]", limit=10, search_params={"metric_type": metric}
            )
            if phase == 1:
                assert len(hits[0]) == 1
                assert (hits[0][0]["id"], hits[0][0]["offset"]) == (7, 1)
                assert hits[0][0]["distance"] == pytest.approx(0, abs=1e-6)
            else:
                assert hits == [[]]
            self.assert_typed_rows(client, name, model, dtype)
            if phase < 2:
                model[7]["clips"][1]["newvec"] = query if phase == 0 else None
                self.upsert(client, name, [model[7]], partial_update=True)


class TestMilvusClientStructArrayFieldProperties(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target", ["clips", "clips[label]", "clips[tags]"])
    @pytest.mark.parametrize(
        "property_name,value",
        [("nullable", False), ("element_nullable", False), ("max_capacity", 16), ("max_length", 64)],
    )
    def test_struct_field_immutable_properties(self, target, property_name, value):
        """
        target: preserve struct nullability and layout parameters under AlterCollectionField
        method: reject one property change and compare full schema, rows and a description control
        expected: specific struct-property rejection, no partial change, valid alter still succeeds
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(client, name, make_predicate_rows())
        self.release_collection(client, name)
        before = client.describe_collection(name)
        result, ok = self.alter_collection_field(
            client, name, field_name=target, field_params={property_name: value}, check_task=CheckTasks.check_nothing
        )
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "can not be altered on struct" in str(result), result
        assert client.describe_collection(name) == before
        self.alter_collection_field(
            client, name, field_name=target, field_params={"field.description": "document control"}
        )
        self.load_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("target", ["clips", "clips[label]", "clips[tags]", "clips[emb]"])
    @pytest.mark.parametrize(
        "property_name,value",
        [
            ("mmap.enabled", "true"),
            ("mmap.enabled", "false"),
            ("warmup", "sync"),
            ("warmup", "async"),
            ("warmup", "disable"),
            ("field.description", "nullable struct description"),
        ],
    )
    def test_struct_field_allowed_properties_persist(self, target, property_name, value):
        """
        target: alter allowed parent/child properties without changing sibling metadata or data
        method: release, alter, describe, reload and check every retained value and sibling field
        expected: property persists across reload and unrelated fields remain identical
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(16)]
        )
        self.flush(client, name)
        self.release_collection(client, name)
        before = client.describe_collection(name)
        self.alter_collection_field(client, name, field_name=target, field_params={property_name: value})
        self.load_collection(client, name)
        self.assert_rows(client, name, model)
        after = client.describe_collection(name)
        old_parent = next(f for f in before["fields"] if f["name"] == "clips")
        parent = next(f for f in after["fields"] if f["name"] == "clips")
        selected = (
            parent if target == "clips" else next(f for f in parent["struct_fields"] if f["name"] == target[6:-1])
        )

        def wire_properties():
            response = client._get_connection()._stub.DescribeCollection(
                milvus_pb2.DescribeCollectionRequest(collection_name=name), timeout=30
            )
            check_status(response.status)
            struct = next(f for f in response.schema.struct_array_fields if f.name == "clips")
            field = struct if target == "clips" else next(f for f in struct.fields if f.name == target[6:-1])
            return {p.key: p.value for p in field.type_params}

        if property_name != "field.description":
            assert wire_properties()[property_name] == value
        if property_name == "field.description":
            assert selected["description"] == value, selected
        if target != "clips":
            for old, new in zip(old_parent["struct_fields"], parent["struct_fields"]):
                if old["name"] != selected["name"]:
                    assert old == new
        self.reload_collection(client, name)
        assert client.describe_collection(name) == after
        self.assert_rows(client, name, model)
        if property_name != "field.description":
            assert wire_properties()[property_name] == value
            described_key = "mmap_enabled" if property_name == "mmap.enabled" else property_name
            assert str(selected.get("params", {}).get(described_key)).lower() == value, selected

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_child_removes_index_and_readd_has_no_old_values(self):
        """
        target: remove a child and its index and prevent old values returning after name reuse
        method: drop indexed nullable vector, reject old paths, re-add as scalar and reload
        expected: exact keys/index inventory, new field ID, NULL backfill and successful new writes
        """
        client = self._client()
        name = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(16)]
        )
        old_id = self.get_child_id(client, name, "emb")
        assert "clips[emb]" in client.list_indexes(name)
        self.drop_collection_field(client, name, field_name="clips[emb]")
        assert "clips[emb]" not in client.list_indexes(name)
        for row in model.values():
            for element in row["clips"] or []:
                del element["emb"]
        self.assert_rows(client, name, model)
        result, ok = self.query(
            client, name, filter="id >= 0", output_fields=["clips[emb]"], check_task=CheckTasks.check_nothing
        )
        assert not ok, result
        result, ok = self.search(
            client,
            name,
            [normalized_vector(1)],
            anns_field="clips[emb]",
            limit=1,
            search_params={"metric_type": "L2"},
            check_task=CheckTasks.check_nothing,
        )
        assert not ok, result
        self.add_collection_field(client, name, "clips[emb]", DataType.INT64, nullable=True)
        assert self.get_child_id(client, name, "emb") > old_id
        for row in model.values():
            for element in row["clips"] or []:
                element["emb"] = None
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        model[2]["clips"][0]["emb"] = 999
        self.upsert(client, name, [model[2]])
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayMmapWarmup(TypedStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mmap", [None, "true", "false"], ids=["default", "on", "off"])
    @pytest.mark.parametrize("warmup", [None, "disable", "sync", "async"], ids=["default", "disable", "sync", "async"])
    def test_parent_mmap_and_warmup_preserve_rows_after_reload(self, mmap, warmup):
        """
        target: retain nullable values and search results across parent mmap/warmup settings
        method: sealed shared schema, configured load, first/repeated reads and second load
        expected: independent complete rows and element scores; runtime path evidence is separate
        """
        client = self._client()
        dtype = DataType.FLOAT_VECTOR
        name = self.create_typed_collection(client, dtype, "FLAT", "L2")
        model = self.insert_rows_with_model(client, name, self.make_typed_rows(dtype), split=False)
        self.flush(client, name)
        self.release_collection(client, name)
        properties = {}
        if mmap is not None:
            properties["mmap.enabled"] = mmap
        if warmup is not None:
            properties["warmup"] = warmup
        if properties:
            self.alter_collection_field(client, name, field_name="clips", field_params=properties)
        log.info(f"STRUCT_CONFIG collection={name} parent_properties={properties}")
        self.load_collection(client, name)
        for phase in range(3):
            self.assert_typed_rows(client, name, model, dtype)
            self.assert_typed_search(client, name, model, dtype, "FLAT", "L2")
            if phase == 1:
                self.release_collection(client, name)
                self.load_collection(client, name)


def _permission_denied(error):
    code = error.code() if isinstance(error, grpc.RpcError) else error.code
    return code in (7, grpc.StatusCode.PERMISSION_DENIED) and "permission deny" in str(error).lower()


class TestMilvusClientStructArraySchemaRbac(StructArrayTestBase):
    skip_global_role_cleanup = True

    @pytest.mark.tags(CaseLabel.L3, CaseLabel.RBAC)
    @pytest.mark.parametrize("operation", ["add", "drop", "alter"])
    def test_struct_schema_permissions_grant_and_revoke(self, operation):
        """
        target: enforce DDL privileges on nullable Struct children for an authenticated reader
        method: denied request and owner readback, grant/same-session success, revoke/fresh-session denial
        expected: code 7 at authorization, no denied mutation, exact rows after allowed DDL and reload
        requires: authorizationEnabled=true and an administrator connection (dedicated RBAC lane)
        """
        owner = self._client()
        name = self.create_struct_collection(owner, nullable_children())
        model = self.insert_rows_with_model(
            owner, name, [make_nullable_row(n, make_nullable_elements(n)) for n in range(12)]
        )
        user, role = cf.gen_unique_str("struct_user"), cf.gen_unique_str("struct_role")
        password = secrets.token_urlsafe(24)
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        owner.create_user(user, password)
        owner.create_role(role)
        owner.grant_role(user, role)
        owner.grant_privilege_v2(role, "CollectionReadOnly", name, db_name="default")
        privilege = "CreateCollection" if operation == "alter" else "AlterCollectionSchema"
        scope = {
            "collection_name": "*" if operation == "alter" else name,
            "db_name": "*" if operation == "alter" else "default",
        }
        actors = []

        def invoke(actor, second=False):
            if operation == "add":
                actor.add_collection_field(
                    name, "clips[second]" if second else "clips[added]", DataType.INT64, nullable=True, timeout=20
                )
            elif operation == "drop":
                actor.drop_collection_field(name, field_name="clips[label]" if second else "clips[score]", timeout=20)
            else:
                actor.alter_collection_field(
                    name, "clips[score]", {"field.description": "second" if second else "allowed"}, timeout=20
                )

        try:
            actor = MilvusClient(uri, user=user, password=password)
            actors.append(actor)
            self.assert_rows(actor, name, model)
            before = owner.describe_collection(name)
            with pytest.raises((MilvusException, grpc.RpcError)) as denied:
                invoke(actor)
            assert _permission_denied(denied.value), denied.value
            assert owner.describe_collection(name) == before
            self.assert_rows(owner, name, model)
            owner.grant_privilege_v2(role, privilege, **scope)
            deadline = time.monotonic() + 30
            while True:
                try:
                    invoke(actor)
                    break
                except (MilvusException, grpc.RpcError) as exc:
                    assert _permission_denied(exc) and time.monotonic() < deadline, exc
                    time.sleep(0.5)
            for row in model.values():
                for element in row["clips"] or []:
                    if operation == "add":
                        element["added"] = None
                    elif operation == "drop":
                        del element["score"]
            self.reload_collection(owner, name)
            self.assert_rows(owner, name, model)
            owner.revoke_privilege_v2(role, privilege, **scope)
            fresh = MilvusClient(uri, user=user, password=password)
            actors.append(fresh)
            before = owner.describe_collection(name)
            with pytest.raises((MilvusException, grpc.RpcError)) as denied:
                invoke(fresh, second=True)
            assert _permission_denied(denied.value), denied.value
            assert owner.describe_collection(name) == before
            self.assert_rows(owner, name, model)
        finally:
            for actor in actors:
                actor.close()
            owner.revoke_role(user, role)
            owner.drop_user(user)
            owner.revoke_privilege_v2(role, privilege, **scope)
            owner.revoke_privilege_v2(role, "CollectionReadOnly", name, db_name="default")
            owner.drop_role(role)
