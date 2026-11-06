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

"""Struct array schema validation regression cases."""

import copy

import pytest
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _child,
    _clips,
    _core_children,
    _record,
)
from pymilvus import DataType, FieldSchema
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status
from pymilvus.grpc_gen import schema_pb2


class TestMilvusClientStructArraySchemaValidation(StructArrayNullableBase):
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
        name = self._create(client, _core_children(), parent_nullable=parent_nullable)
        rows = [_record(n, _clips(n)) for n in range(12)]
        if not parent_nullable:
            for row in rows:
                if row["clips"] is None:
                    row["clips"] = []
        model = self._seed(client, name, rows)
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
        self._check(client, name, model)
        if not parent_nullable:
            status = handler._stub.AlterCollectionSchema(good, timeout=30).alter_status
            assert (
                status.code == 1100
                and "sub-field in non-nullable struct cannot be nullable individually" in status.reason
            ), status
            assert client.describe_collection(name) == before
            self._check(client, name, model)
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
        self._reload(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("kind", ["element_nullable", "nested"])
    def test_add_top_level_native_array_rejected(self, kind):
        """
        target: reject native-list placement at AddCollectionField as well as Create
        method: raw schema alteration with a top-level element-nullable or nested array
        expected: precise placement rejection, unchanged schema/data, ordinary Array control succeeds
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
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
        self._check(client, name, model)
        client.add_collection_field(
            name, "new_array", DataType.ARRAY, element_type=DataType.INT64, max_capacity=8, nullable=True
        )
        for row in model.values():
            row["new_array"] = None
        self._reload(client, name)
        self._check(client, name, model)

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
                self._reload(client, name)
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
            self._reload(client, name)
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
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
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
        self._check(client, name, model)
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
        self._reload(client, name)
        self._check(client, name, model)

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
            name = self._create(client, _core_children())
            model = self._seed(client, name, [_record(n, _clips(n)) for n in range(6)])
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
            self._check(client, name, model)
            field.type_params[-1].value = "false"
            check_status(client._get_connection()._stub.AlterCollectionSchema(request, timeout=30).alter_status)
            for row in model.values():
                for element in row["clips"] or []:
                    element["text"] = None
            self._reload(client, name)
            self._check(client, name, model)
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
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(6)])
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
        self._reload(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_utf8_byte_boundary_counts_bytes_not_characters(self):
        """
        target: enforce a VARCHAR child limit in UTF-8 bytes
        method: write five/six-byte values with NULL elements, reject seven bytes through raw RPC
        expected: exact byte boundary survives reload and rejected new PK stays absent
        """
        client = self._client()
        name = self._create(client, {"text": _child(DataType.VARCHAR, max_length=6)})
        rows = [_record(n, [{"text": value}, {"text": None}]) for n, value in enumerate(["雪ab", "雪雪"])]
        model = self._seed(client, name, rows)
        bad = _record(100, [{"text": "雪雪a"}])
        handler = client._get_connection()
        status = handler._stub.Insert(handler._prepare_row_insert_request(name, [bad]), timeout=30).status
        assert status.code == 1100 and "length" in status.reason, status
        self._reload(client, name)
        self._check(client, name, model)

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
            name = self._create(client, {"anchor": _child(DataType.INT64, False)})
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
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(24)])
        row = _record(102, _clips(102))
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
        self._check(client, name, model)
        self.drop_collection_field(client, name, field_name="clips[label]")
        for stored in model.values():
            for element in stored["clips"] or []:
                element.pop("label")
        response = handler._stub.Insert(stale, timeout=30)
        assert response.status.code == 1100, response.status
        self._reload(client, name)
        self._check(client, name, model)
