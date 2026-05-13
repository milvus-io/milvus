import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, FieldOp


class TestMilvusClientArrayPartialOpValid(TestMilvusClientV2Base):
    """Test field_ops: ARRAY_APPEND / ARRAY_REMOVE (Issue #49241)"""

    """
    ******************************************************************
    #  Module 1: Array operations — all element types (P0)
    ******************************************************************
    """

    # 1.1–1.7 Append all element types (parametrized)
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize(
        "element_type,field_name,init_vals,append_vals,expected",
        [
            (
                DataType.BOOL,
                "flags",
                [[True, False], [True]],
                [[True, True], [False]],
                [[True, False, True, True], [True, False]],
            ),
            (DataType.INT8, "scores", [[1, 2], [10, 20]], [[3, 4], [30]], [[1, 2, 3, 4], [10, 20, 30]]),
            (DataType.INT16, "tags16", [[100, 200], [300]], [[400], [500, 600]], [[100, 200, 400], [300, 500, 600]]),
            (DataType.INT32, "values", [[1000, 2000], [3000]], [[4000], [5000]], [[1000, 2000, 4000], [3000, 5000]]),
            (DataType.INT64, "ids", [[10, 20], [30]], [[40], [50, 60]], [[10, 20, 40], [30, 50, 60]]),
            (
                DataType.FLOAT,
                "metrics",
                [[1.0, 2.0], [10.5]],
                [[3.0], [20.5, 30.5]],
                [[1.0, 2.0, 3.0], [10.5, 20.5, 30.5]],
            ),
            (DataType.DOUBLE, "precise", [[1.0, 2.0], [10.5]], [[3.0], [20.5]], [[1.0, 2.0, 3.0], [10.5, 20.5]]),
            (DataType.VARCHAR, "labels", [["a", "b"], ["x", "y"]], [["c"], ["z"]], [["a", "b", "c"], ["x", "y", "z"]]),
        ],
    )
    def test_array_partial_op_append_all_types(self, element_type, field_name, init_vals, append_vals, expected):
        """
        target: test array append with all element types
        method: insert rows with array field, then append via field_ops
        expected: appended values are concatenated correctly
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        kw = {"max_capacity": 16}
        if element_type == DataType.VARCHAR:
            kw["max_length"] = 50
        schema.add_field(field_name, DataType.ARRAY, element_type=element_type, **kw)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        rows = [{"id": i, "vector": [0.1 * (i + 1)] * 4, field_name: init_vals[i]} for i in range(2)]
        self.upsert(client, collection_name, rows, partial_update=True)

        append_rows = [{"id": i, field_name: append_vals[i]} for i in range(2)]
        self.upsert(client, collection_name, append_rows, field_ops={field_name: FieldOp.array_append()})

        res = self.query(client, collection_name, filter="id >= 0", output_fields=[field_name])[0]
        actual = {row["id"]: row[field_name] for row in res}
        for i in range(2):
            if element_type in (DataType.FLOAT, DataType.DOUBLE):
                assert actual[i] == pytest.approx(expected[i]), f"pk={i}: expected {expected[i]}, got {actual[i]}"
            else:
                assert actual[i] == expected[i], f"pk={i}: expected {expected[i]}, got {actual[i]}"

        self.drop_collection(client, collection_name)

    # 1.8 Remove Int64
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_remove_int64(self):
        """
        target: test array remove with Int64 element type
        method: insert [10,20,10,30], remove [10]
        expected: all 10s removed, result [20,30]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [10, 20, 10, 30]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [10]},
            ],
            field_ops={"tags": FieldOp.array_remove()},
        )

        self.flush(client, collection_name)
        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == [20, 30]

        self.drop_collection(client, collection_name)

    # 1.9 Remove Bool
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_remove_bool(self):
        """
        target: test array remove with Bool element type
        method: insert [T,F,T] and [F,F], remove [T] from both
        expected: row0=[F], row1=[F,F]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("flags", DataType.ARRAY, element_type=DataType.BOOL, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "flags": [True, False, True]},
                {"id": 1, "vector": [0.2] * 4, "flags": [False, False]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "flags": [True]},
                {"id": 1, "flags": [True]},
            ],
            field_ops={"flags": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["flags"])[0]
        actual = {row["id"]: row["flags"] for row in res}
        assert actual[0] == [False]
        assert actual[1] == [False, False]

        self.drop_collection(client, collection_name)

    # 1.10 Remove Int32
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_remove_int32(self):
        """
        target: test array remove with Int32 element type
        method: insert [5,10,5,15] and [20,30], remove [5] and [40]
        expected: row0=[10,15], row1=[20,30] (40 not found, unchanged)
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("values", DataType.ARRAY, element_type=DataType.INT32, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "values": [5, 10, 5, 15]},
                {"id": 1, "vector": [0.2] * 4, "values": [20, 30]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "values": [5]},
                {"id": 1, "values": [40]},
            ],
            field_ops={"values": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["values"])[0]
        actual = {row["id"]: row["values"] for row in res}
        assert actual[0] == [10, 15]
        assert actual[1] == [20, 30]

        self.drop_collection(client, collection_name)

    # 1.11 Remove Float
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_remove_float(self):
        """
        target: test array remove with Float element type
        method: insert [1.0,2.0,1.0] and [3.0,4.0], remove [1.0] and [5.0]
        expected: row0=[2.0], row1=[3.0,4.0]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("metrics", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "metrics": [1.0, 2.0, 1.0]},
                {"id": 1, "vector": [0.2] * 4, "metrics": [3.0, 4.0]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "metrics": [1.0]},
                {"id": 1, "metrics": [5.0]},
            ],
            field_ops={"metrics": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["metrics"])[0]
        actual = {row["id"]: row["metrics"] for row in res}
        assert actual[0] == pytest.approx([2.0])
        assert actual[1] == pytest.approx([3.0, 4.0])

        self.drop_collection(client, collection_name)

    # 1.12 Remove VarChar
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_remove_varchar(self):
        """
        target: test array remove with VarChar element type
        method: insert ["red","blue","red"] and ["green"], remove ["red"] and ["yellow"]
        expected: row0=["blue"], row1=["green"]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("labels", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=16, max_length=50)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "labels": ["red", "blue", "red"]},
                {"id": 1, "vector": [0.2] * 4, "labels": ["green"]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "labels": ["red"]},
                {"id": 1, "labels": ["yellow"]},
            ],
            field_ops={"labels": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["labels"])[0]
        actual = {row["id"]: row["labels"] for row in res}
        assert actual[0] == ["blue"]
        assert actual[1] == ["green"]

        self.drop_collection(client, collection_name)

    # 1.13 Multiple fields, different ops in one request
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_multiple_fields_same_request(self):
        """
        target: test different ops on different fields in one request
        method: insert tags=[1,2] scores=[10,20], upsert tags append [3], scores remove [10]
        expected: tags=[1,2,3], scores=[20]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)
        schema.add_field("scores", DataType.ARRAY, element_type=DataType.INT32, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2], "scores": [10, 20]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [3], "scores": [10]},
            ],
            field_ops={"tags": FieldOp.array_append(), "scores": FieldOp.array_remove()},
        )

        self.flush(client, collection_name)
        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags", "scores"])[0]
        assert res[0]["tags"] == [1, 2, 3]
        assert res[0]["scores"] == [20]

        self.drop_collection(client, collection_name)

    """
    ******************************************************************
    #  Module 2: Error / validation scenarios (P0)
    ******************************************************************
    """

    # 2.1 field_ops on non-Array field
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_on_non_array_field(self):
        """
        target: test field_ops on a non-Array field raises error
        method: create schema with VarChar field, apply array_append on it
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("name", DataType.VARCHAR, max_length=64)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "name": "alice"},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 1100, ct.err_msg: 'op ARRAY_APPEND requires Array field, but field "name" is VarChar'}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "name": "bob"},
            ],
            field_ops={"name": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 2.2 field_ops on PK field
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_on_pk_field(self):
        """
        target: test field_ops on primary key field raises error
        method: apply array_append on the pk field
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 1100, ct.err_msg: "is the primary key and cannot carry a partial-update op"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0},
            ],
            field_ops={"id": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 2.3 field_ops on nonexistent field
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_unknown_field_name(self):
        """
        target: test field_ops on a nonexistent field raises error
        method: apply array_append on a field not in schema
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1]},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 1100, ct.err_msg: "nonexistent_field"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "nonexistent_field": [1]},
            ],
            field_ops={"nonexistent_field": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 2.4 Element type mismatch
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_element_type_mismatch(self):
        """
        target: test appending wrong element type raises error
        method: append string values to Array<Int64> field
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2]},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 1, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": ["string_value"]},
            ],
            field_ops={"tags": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 2.5 Missing field data for declared field_ops
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_missing_field_data(self):
        """
        target: test field_ops declared but data missing the field raises error
        method: field_ops has "tags" op but data rows have no "tags" key
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2]},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 1100, ct.err_msg: 'partial-update op targets field "tags" not present in fields_data'}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0},
            ],
            field_ops={"tags": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 2.6 field_ops auto-enables partial_update
    @pytest.mark.tags(CaseLabel.L0)
    def test_field_ops_auto_enables_partial_update(self):
        """
        target: test that field_ops implies partial_update=True
        method: set field_ops but NOT partial_update=True, verify name field is preserved;
                also verify partial_update=False is overridden when field_ops is present
        expected: tags appended, name unchanged in both cases
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)
        schema.add_field("name", DataType.VARCHAR, max_length=64)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1], "name": "alice"},
            ],
            partial_update=True,
        )

        # field_ops without explicit partial_update
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [2]},
            ],
            field_ops={"tags": FieldOp.array_append()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags", "name"])[0]
        assert res[0]["tags"] == [1, 2]
        assert res[0]["name"] == "alice"

        # field_ops with explicit partial_update=False — should still auto-enable
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [3]},
            ],
            field_ops={"tags": FieldOp.array_append()},
            partial_update=False,
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags", "name"])[0]
        assert res[0]["tags"] == [1, 2, 3]
        assert res[0]["name"] == "alice"

        self.drop_collection(client, collection_name)

    # 2.7 Mixed valid/invalid field_ops in one request — atomic rollback
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_mixed_valid_invalid_atomic(self):
        """
        target: test that a request with both valid and invalid field_ops fails atomically
        method: one field has valid array_append, another field (VarChar) has invalid array_append
        expected: entire request fails; neither field is modified
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)
        schema.add_field("name", DataType.VARCHAR, max_length=64)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2], "name": "alice"},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 1100, ct.err_msg: 'op ARRAY_APPEND requires Array field, but field "name" is VarChar'}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [3], "name": "bob"},
            ],
            field_ops={"tags": FieldOp.array_append(), "name": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        # verify atomicity — neither field should have been modified
        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags", "name"])[0]
        assert res[0]["tags"] == [1, 2]  # append did NOT apply
        assert res[0]["name"] == "alice"  # scalar field unchanged

        self.drop_collection(client, collection_name)

    # 2.8 Mixed valid/invalid field_ops — execution-stage error (capacity exceeded)
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_mixed_valid_invalid_execution_stage(self):
        """
        target: test atomic rollback when error is found at execution stage
        method: both fields are Array type, one append is valid, another exceeds capacity
        expected: entire request fails; neither field is modified
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4)
        schema.add_field("labels", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1], "labels": [1]},
            ],
            partial_update=True,
        )

        # tags append is valid: [1] + [2] = [1,2] (length 2 <= 4)
        # labels append exceeds capacity: [1] + [2,3,4,5] = [1,2,3,4,5] (length 5 > 4)
        error = {ct.err_code: 65535, ct.err_msg: "array length 5 exceeds max_capacity 4"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [2], "labels": [2, 3, 4, 5]},
            ],
            field_ops={"tags": FieldOp.array_append(), "labels": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        # verify atomicity — neither field should have been modified
        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags", "labels"])[0]
        assert res[0]["tags"] == [1]  # valid append did NOT apply
        assert res[0]["labels"] == [1]  # invalid append did NOT apply

        self.drop_collection(client, collection_name)

    # 2.9 Mixed valid/invalid field_ops — execution-stage error (max_length exceeded)
    @pytest.mark.tags(CaseLabel.L0)
    def test_array_partial_op_mixed_valid_invalid_max_length(self):
        """
        target: test atomic rollback when VarChar element exceeds max_length
        method: tags append is valid, labels append has element exceeding max_length
        expected: entire request fails; neither field is modified
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)
        schema.add_field("labels", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=16, max_length=10)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1], "labels": ["short"]},
            ],
            partial_update=True,
        )

        # tags append is valid: [1] + [2] = [1,2]
        # labels append exceeds max_length: "this_string_is_way_too_long" > 10 chars
        error = {ct.err_code: 1100, ct.err_msg: "exceeds max length"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [2], "labels": ["this_string_is_way_too_long"]},
            ],
            field_ops={"tags": FieldOp.array_append(), "labels": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        # verify atomicity — neither field should have been modified
        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags", "labels"])[0]
        assert res[0]["tags"] == [1]  # valid append did NOT apply
        assert res[0]["labels"] == ["short"]  # invalid append did NOT apply

        self.drop_collection(client, collection_name)

    """
    ******************************************************************
    #  Module 3: Array boundary scenarios (P1)
    ******************************************************************
    """

    # 3.1 Append to empty array
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_append_to_empty_array(self):
        """
        target: test appending to an empty array
        method: insert tags=[], append [1,2,3]
        expected: tags=[1,2,3]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": []},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [1, 2, 3]},
            ],
            field_ops={"tags": FieldOp.array_append()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == [1, 2, 3]

        self.drop_collection(client, collection_name)

    # 3.2 Remove all elements
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_remove_all_elements(self):
        """
        target: test removing all elements from array
        method: insert [1,2,3], remove [1,2,3]
        expected: tags=[]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2, 3]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [1, 2, 3]},
            ],
            field_ops={"tags": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == []

        self.drop_collection(client, collection_name)

    # 3.3 Remove non-existent element — no error
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_remove_non_existent_element(self):
        """
        target: test removing a non-existent element does not error
        method: insert [1,2,3], remove [99]
        expected: data unchanged, no error
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2, 3]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [99]},
            ],
            field_ops={"tags": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == [1, 2, 3]

        self.drop_collection(client, collection_name)

    # 3.4 Remove duplicate elements
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_remove_duplicate_elements(self):
        """
        target: test removing a value that appears multiple times
        method: insert [1,1,2,1,3], remove [1]
        expected: all 1s removed, tags=[2,3]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 1, 2, 1, 3]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [1]},
            ],
            field_ops={"tags": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == [2, 3]

        self.drop_collection(client, collection_name)

    # 3.6 Remove from single element
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_remove_from_single_element(self):
        """
        target: test removing the only element
        method: insert [42], remove [42]
        expected: tags=[]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [42]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [42]},
            ],
            field_ops={"tags": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == []

        self.drop_collection(client, collection_name)

    # 3.7 Nullable field — null payload row preserved
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_with_null_payload_rows(self):
        """
        target: test that rows with tags=None in append data preserve original value
        method: insert 3 rows, upsert row0 append [3], row1 tags=None
        expected: row0=[1,2,3], row1=[10,20] preserved
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16, nullable=True)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2]},
                {"id": 1, "vector": [0.2] * 4, "tags": [10, 20]},
                {"id": 2, "vector": [0.3] * 4, "tags": [100]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [3]},
                {"id": 1, "tags": None},
            ],
            field_ops={"tags": FieldOp.array_append()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        actual = {row["id"]: row["tags"] for row in res}
        assert actual[0] == [1, 2, 3]
        assert actual[1] == [10, 20]
        assert actual[2] == [100]

        self.drop_collection(client, collection_name)

    # 3.8 Nullable field — append to null value
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_append_to_null_field(self):
        """
        target: test appending to a field whose current value is null
        method: insert tags=None, append [1,2]
        expected: tags=[1,2] (treated as appending to empty array)
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16, nullable=True)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": None},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [1, 2]},
            ],
            field_ops={"tags": FieldOp.array_append()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == [1, 2]

        self.drop_collection(client, collection_name)

    # 3.9 Append exceeds capacity (Int64)
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_append_exceeds_capacity(self):
        """
        target: test append exceeding max_capacity raises error
        method: max_capacity=4, insert [1,2], append [3,4,5] (total 5)
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2]},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 65535, ct.err_msg: "array length 5 exceeds max_capacity 4"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [3, 4, 5]},
            ],
            field_ops={"tags": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 3.10 Append exceeds max_length (VarChar element length)
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_append_exceeds_max_length(self):
        """
        target: test appending a VarChar element that exceeds max_length
        method: max_length=10, insert ["short"], append ["this_string_is_way_too_long"]
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("labels", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=16, max_length=10)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "labels": ["short"]},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 1100, ct.err_msg: "exceeds max length"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "labels": ["this_string_is_way_too_long"]},
            ],
            field_ops={"labels": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 3.11 Append exceeds capacity (Bool)
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_append_exceeds_capacity_bool(self):
        """
        target: test append exceeding max_capacity with Bool array
        method: max_capacity=4, insert [T,F,T], append [T,F] (total 5)
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("flags", DataType.ARRAY, element_type=DataType.BOOL, max_capacity=4)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "flags": [True, False, True]},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 65535, ct.err_msg: "array length 5 exceeds max_capacity 4"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "flags": [True, False]},
            ],
            field_ops={"flags": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 3.12 Append exceeds capacity (Float)
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_append_exceeds_capacity_float(self):
        """
        target: test append exceeding max_capacity with Float array
        method: max_capacity=4, insert [1.0,2.0,3.0], append [4.0,5.0]
        expected: error raised
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("metrics", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=4)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "metrics": [1.0, 2.0, 3.0]},
            ],
            partial_update=True,
        )

        error = {ct.err_code: 65535, ct.err_msg: "array length 5 exceeds max_capacity 4"}
        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "metrics": [4.0, 5.0]},
            ],
            field_ops={"metrics": FieldOp.array_append()},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    # 3.13 Append exactly fills capacity
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_append_to_full_capacity_exact(self):
        """
        target: test append that exactly fills capacity
        method: max_capacity=4, insert [1,2], append [3,4] (total exactly 4)
        expected: success, tags=[1,2,3,4]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [3, 4]},
            ],
            field_ops={"tags": FieldOp.array_append()},
        )

        self.flush(client, collection_name)
        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == [1, 2, 3, 4]

        self.drop_collection(client, collection_name)

    # 3.14 Remove not limited by capacity
    @pytest.mark.tags(CaseLabel.L1)
    def test_array_partial_op_remove_not_limited_by_capacity(self):
        """
        target: test that remove works on a full-capacity array
        method: max_capacity=4, insert [1,2,3,4] (full), remove [1]
        expected: success, tags=[2,3,4]
        """
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4)

        index_params = self.prepare_index_params(client)[0]
        for f in schema.fields:
            index_params.add_index(f.name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client, collection_name, 4, schema=schema, consistency_level="Strong", index_params=index_params
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "vector": [0.1] * 4, "tags": [1, 2, 3, 4]},
            ],
            partial_update=True,
        )

        self.upsert(
            client,
            collection_name,
            [
                {"id": 0, "tags": [1]},
            ],
            field_ops={"tags": FieldOp.array_remove()},
        )

        res = self.query(client, collection_name, filter="id >= 0", output_fields=["tags"])[0]
        assert res[0]["tags"] == [2, 3, 4]

        self.drop_collection(client, collection_name)
