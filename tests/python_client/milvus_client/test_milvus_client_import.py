import csv
import json
import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from minio import Minio
from minio.commonconfig import CopySource
from pymilvus import DataType, Function, FunctionType
from pymilvus.bulk_writer import abort_import, bulk_import, commit_import, get_import_progress
from utils.util_log import test_log as log


class TestMilvusClientImportIndependent(TestMilvusClientV2Base):
    @staticmethod
    def _import_url():
        return cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

    def _create_import_collection(self, client, collection_name):
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

    @staticmethod
    def _upload_parquet(storage_client, bucket_name, tmp_path, rows, file_name):
        schema = pa.schema(
            [
                pa.field("tag", pa.string(), nullable=False),
                pa.field("vector", pa.list_(pa.float32()), nullable=False),
            ]
        )
        file_path = tmp_path / file_name
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), file_path)
        object_name = f"bulkinsert_data/{uuid4()}/{file_name}"
        storage_client.fput_object(bucket_name, object_name, str(file_path))
        return object_name

    @staticmethod
    def _wait_for_state(url, job_id, expected_states, timeout=300):
        deadline = time.monotonic() + timeout
        last_data = None
        while time.monotonic() < deadline:
            response = get_import_progress(url=url, job_id=job_id, api_key=cf.param_info.param_token)
            payload = response.json()
            assert payload["code"] == 0, payload
            last_data = payload["data"]
            if last_data["state"] in expected_states:
                return last_data
            if last_data["state"] in {"Failed", "Completed"}:
                raise AssertionError(f"Import job {job_id} unexpectedly terminated: {last_data}")
            time.sleep(2)
        raise AssertionError(f"Import job {job_id} did not reach {expected_states}: {last_data}")

    @pytest.fixture
    def upload_import_file(self, minio_host, minio_bucket, tmp_path):
        storage_client = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        prefix = f"bulkinsert_data/{uuid4()}"
        objects = []

        def upload(file_path):
            object_name = f"{prefix}/{file_path.relative_to(tmp_path).as_posix()}"
            objects.append(object_name)
            storage_client.fput_object(minio_bucket, object_name, str(file_path))
            return object_name

        yield upload
        for object_name in objects:
            storage_client.remove_object(minio_bucket, object_name)

    def _import_and_wait(self, collection_name, files, options=None, expected_state="Completed", partition_name=""):
        response = bulk_import(
            url=self._import_url(),
            collection_name=collection_name,
            partition_name=partition_name,
            files=files,
            options=options or {},
            api_key=cf.param_info.param_token,
        ).json()
        assert response["code"] == 0, response
        return self._wait_for_state(self._import_url(), response["data"]["jobId"], {expected_state})

    @staticmethod
    def _write_rows(file_path, rows, arrow_schema):
        # Write raw files to exercise reader validation without BulkWriter normalizing the input.
        if file_path.suffix in {".json", ".jsonl"}:
            with file_path.open("w", encoding="utf-8") as stream:
                if file_path.suffix == ".jsonl":
                    stream.write("\n".join(json.dumps(row, ensure_ascii=False) for row in rows))
                else:
                    json.dump(rows, stream, ensure_ascii=False)
        elif file_path.suffix == ".parquet":
            encoded_rows = [
                {
                    key: json.dumps(value, ensure_ascii=False) if isinstance(value, dict) else value
                    for key, value in row.items()
                }
                for row in rows
            ]
            pq.write_table(pa.Table.from_pylist(encoded_rows, schema=arrow_schema), file_path, row_group_size=5)
        elif file_path.suffix == ".csv":
            # Reordered headers and quoted delimiters/newlines must preserve field identity.
            with file_path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream, delimiter="|")
                columns = list(reversed(arrow_schema.names))
                writer.writerow(columns)
                for row in rows:
                    values = []
                    for column in columns:
                        value = row.get(column)
                        if value is None:
                            value = "__NULL__"
                        elif isinstance(value, (dict, list, bool)):
                            value = json.dumps(value, ensure_ascii=False)
                        values.append(value)
                    writer.writerow(values)
        else:
            raise ValueError(f"Unsupported import format: {file_path.suffix}")

    def _assert_import_rows(self, client, collection_name, expected_rows):
        results = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=list(expected_rows[0]),
            limit=len(expected_rows) + 1,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": len(expected_rows)},
        )[0]
        expected_by_id = {row["id"]: row for row in expected_rows}
        assert len(expected_by_id) == len(expected_rows)
        assert {row["id"] for row in results} == set(expected_by_id)
        for row in results:
            # These fixtures use exactly representable floats, so no rounding tolerance is needed.
            assert row == expected_by_id[row["id"]], {"actual": row, "expected": expected_by_id[row["id"]]}

    def _create_data_collection(self, client, collection_name, schema):
        index_params = self.prepare_index_params(client)[0]
        for field in schema.fields:
            if field.dtype == DataType.FLOAT_VECTOR:
                index_params.add_index(field_name=field.name, index_type="FLAT", metric_type="L2")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", ["json", "jsonl", "csv", "parquet"])
    def test_import_nullable_fields_preserve_values_across_files(self, file_format, upload_import_file, tmp_path):
        """
        target: nullable scalar, array, JSON and vector value alignment
        method: import distinct per-row values and staggered nulls across files and Parquet row groups
        expected: every cell and null-filter PK set is preserved, including after release/load
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_nullable_values")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        scalar_fields = [
            ("bool_value", DataType.BOOL, pa.bool_()),
            ("int8_value", DataType.INT8, pa.int8()),
            ("int16_value", DataType.INT16, pa.int16()),
            ("int32_value", DataType.INT32, pa.int32()),
            ("int64_value", DataType.INT64, pa.int64()),
            ("float_value", DataType.FLOAT, pa.float32()),
            ("double_value", DataType.DOUBLE, pa.float64()),
            ("string_value", DataType.VARCHAR, pa.string()),
            ("json_value", DataType.JSON, pa.string()),
        ]
        arrow_fields = [pa.field("id", pa.int64()), pa.field("vector", pa.list_(pa.float32()))]
        for field_name, datatype, arrow_type in scalar_fields:
            schema.add_field(
                field_name, datatype, nullable=True, **({"max_length": 256} if datatype == DataType.VARCHAR else {})
            )
            arrow_fields.append(pa.field(field_name, arrow_type))
        for name, datatype, arrow_type in [
            ("int_array", DataType.INT64, pa.int64()),
            ("float_array", DataType.FLOAT, pa.float32()),
            ("bool_array", DataType.BOOL, pa.bool_()),
            ("string_array", DataType.VARCHAR, pa.string()),
        ]:
            schema.add_field(
                name,
                DataType.ARRAY,
                element_type=datatype,
                max_capacity=8,
                nullable=True,
                **({"max_length": 256} if datatype == DataType.VARCHAR else {}),
            )
            arrow_fields.append(pa.field(name, pa.list_(arrow_type)))
        schema.add_field("nullable_vector", DataType.FLOAT_VECTOR, dim=8, nullable=True)
        arrow_fields.append(pa.field("nullable_vector", pa.list_(pa.float32())))
        self._create_data_collection(client, collection_name, schema)
        rows = []
        for i in range(33):
            row = {
                "id": i,
                "vector": [float(i + j) / 8 for j in range(8)],
                "bool_value": bool(i % 2),
                "int8_value": i - 16,
                "int16_value": i * 31 - 500,
                "int32_value": i * 100003,
                "int64_value": 2**53 + i,
                "float_value": i / 8,
                "double_value": -i / 16,
                "string_value": "" if i % 3 == 0 else f'row_{i}|中文,"quoted"\nnext',
                "json_value": [None, {}, {"row": i, "nested": None}][i % 3],
                "int_array": [None, [], [i, -i]][i % 3],
                "float_array": [None, [], [i / 8, -i / 4]][(i + 1) % 3],
                "bool_array": [None, [], [bool(i % 2), False]][(i + 2) % 3],
                "string_array": [None, [], [f"row_{i}", "", "中文"]][i % 3],
                "nullable_vector": None if i % 4 in (0, 1) else [float(i - j) / 8 for j in range(8)],
            }
            for offset, (field_name, _, _) in enumerate(scalar_fields[:-1]):
                if (i + offset) % 5 == 0:
                    row[field_name] = None
            rows.append(row)
        files = []
        for part, group in enumerate((rows[:13], rows[13:])):
            path = tmp_path / f"part_{part}.{file_format}"
            self._write_rows(path, group, pa.schema(arrow_fields))
            if file_format == "parquet":
                assert pq.read_metadata(path).num_row_groups > 1
            files.append([upload_import_file(path)])
        options = {"sep": "|", "nullkey": "__NULL__"} if file_format == "csv" else {}
        progress = self._import_and_wait(collection_name, files, options)
        assert progress["importedRows"] == progress["totalRows"] == len(rows), progress
        self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, rows)
        # Vector NULL predicates are unsupported; validate their null mask through query and search instead.
        for field_name in [
            field.name for field in schema.fields if field.nullable and field.dtype != DataType.FLOAT_VECTOR
        ]:
            for is_null in (True, False):
                results = self.query(
                    client,
                    collection_name,
                    filter=f"{field_name} is {'null' if is_null else 'not null'}",
                    output_fields=["id"],
                    limit=len(rows),
                )[0]
                assert {row["id"] for row in results} == {
                    row["id"] for row in rows if (row[field_name] is None) == is_null
                }, field_name
        vector_hits = self.search(
            client,
            collection_name,
            data=[rows[18]["nullable_vector"]],
            anns_field="nullable_vector",
            search_params={"metric_type": "L2", "params": {}},
            limit=len(rows),
        )[0][0]
        assert {hit["id"] for hit in vector_hits} == {row["id"] for row in rows if row["nullable_vector"] is not None}
        assert vector_hits[0]["id"] == 18 and vector_hits[0]["distance"] == pytest.approx(0), vector_hits
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self._assert_import_rows(client, collection_name, rows)
        hits = self.search(
            client,
            collection_name,
            data=[rows[17]["vector"]],
            anns_field="vector",
            search_params={"metric_type": "L2", "params": {}},
            limit=1,
            output_fields=["int64_value", "string_value", "json_value", "int_array"],
        )[0][0]
        assert hits[0]["id"] == 17 and hits[0]["distance"] == pytest.approx(0), hits
        for field_name, value in hits[0]["entity"].items():
            assert value == rows[17][field_name]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
    def test_import_defaults_preserve_explicit_zero_false_and_empty_string(
        self, file_format, upload_import_file, tmp_path
    ):
        """
        target: missing and null default fields versus explicit falsy values
        method: import a file with omitted columns and a file with null, zero and non-default values
        expected: only missing/null values use defaults; explicit zero, false and empty strings survive
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_defaults")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        base_fields = [pa.field("id", pa.int64()), pa.field("vector", pa.list_(pa.float32()))]
        default_fields = [
            ("int_value", DataType.INT64, pa.int64(), 19),
            ("float_value", DataType.FLOAT, pa.float32(), 2.5),
            ("double_value", DataType.DOUBLE, pa.float64(), 3.25),
            ("bool_value", DataType.BOOL, pa.bool_(), True),
            ("string_value", DataType.VARCHAR, pa.string(), "default"),
        ]
        for field_name, datatype, _, default in default_fields:
            schema.add_field(
                field_name,
                datatype,
                nullable=True,
                default_value=default,
                **({"max_length": 64} if datatype == DataType.VARCHAR else {}),
            )
        schema.add_field("required_default", DataType.INT64, default_value=-7)
        schema.add_field("missing_nullable", DataType.INT64, nullable=True)
        self._create_data_collection(client, collection_name, schema)
        rows = [{"id": i, "vector": [float(i)] * 8} for i in range(12)]
        expected = []
        for row in rows:
            i = row["id"]
            values = {
                "int_value": i,
                "float_value": i / 8,
                "double_value": -i / 8,
                "bool_value": False,
                "string_value": f"row_{i}",
            }
            if i >= 4:
                if i % 3 == 0:
                    row.update(dict.fromkeys(values))
                elif i % 3 == 1:
                    row.update(
                        {
                            "int_value": 0,
                            "float_value": 0.0,
                            "double_value": 0.0,
                            "bool_value": False,
                            "string_value": "",
                        }
                    )
                else:
                    row.update(values)
                row["required_default"] = None if i % 3 == 0 else i
            expected.append(
                {
                    **row,
                    **{name: default if row.get(name) is None else row[name] for name, _, _, default in default_fields},
                    "required_default": -7 if row.get("required_default") is None else row["required_default"],
                    "missing_nullable": None,
                }
            )
        files = []
        full_fields = base_fields + [pa.field(name, arrow_type) for name, _, arrow_type, _ in default_fields]
        full_fields.append(pa.field("required_default", pa.int64()))
        for part, (group, fields) in enumerate(((rows[:4], base_fields), (rows[4:], full_fields))):
            path = tmp_path / f"defaults_{part}.{file_format}"
            self._write_rows(path, group, pa.schema(fields))
            files.append([upload_import_file(path)])
        self._import_and_wait(
            collection_name, files, {"sep": "|", "nullkey": "__NULL__"} if file_format == "csv" else {}
        )
        self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, expected)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("column_mode", ["present", "omitted"])
    @pytest.mark.parametrize("byte_order", ["<", ">"], ids=["little_endian", "big_endian"])
    def test_import_numpy_nullable_columns_preserve_values(self, column_mode, byte_order, upload_import_file, tmp_path):
        """
        target: nullable Numpy columns and byte order
        method: provide all-valid nullable columns or omit them, using both endian encodings
        expected: exact scalar/vector values or null/default values are returned for every PK
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_numpy_nullable")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("value", DataType.INT64, nullable=True)
        schema.add_field("label", DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field("default_value", DataType.INT64, nullable=True, default_value=23)
        self._create_data_collection(client, collection_name, schema)
        rows = [
            {
                "id": i,
                "vector": [float(i + j) / 8 for j in range(8)],
                "value": 2**53 + i if column_mode == "present" else None,
                "label": ("" if i == 0 else f"中文_{i}") if column_mode == "present" else None,
                "default_value": i if column_mode == "present" else 23,
            }
            for i in range(9)
        ]
        columns = {"id": "i8", "vector": "f8"}
        if column_mode == "present":
            columns.update({"value": "i8", "label": "U16", "default_value": "i8"})
        files = []
        for name, dtype in columns.items():
            path = tmp_path / f"{name}.npy"
            np.save(path, np.asarray([row[name] for row in rows], dtype=f"{byte_order}{dtype}"), allow_pickle=False)
            files.append(upload_import_file(path))
        self._import_and_wait(collection_name, [files])
        self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("arrow_null_type", [False, True], ids=["typed_nulls", "arrow_nulls"])
    def test_import_parquet_all_null_columns_preserve_nulls(self, arrow_null_type, upload_import_file, tmp_path):
        """
        target: Arrow Null columns and typed columns with all validity bits unset
        method: import scalar, array and vector columns that are entirely null
        expected: query preserves NULL rather than substituting zero, empty arrays or vectors
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_all_null")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("value", DataType.INT64, nullable=True)
        schema.add_field("array_value", DataType.ARRAY, element_type=DataType.INT64, max_capacity=8, nullable=True)
        schema.add_field("json_value", DataType.JSON, nullable=True)
        schema.add_field("nullable_vector", DataType.FLOAT_VECTOR, dim=8, nullable=True)
        self._create_data_collection(client, collection_name, schema)
        rows = [
            {
                "id": i,
                "vector": [float(i)] * 8,
                "value": None,
                "array_value": None,
                "json_value": None,
                "nullable_vector": None,
            }
            for i in range(11)
        ]
        columns = [pa.field("id", pa.int64()), pa.field("vector", pa.list_(pa.float32()))]
        for name, dtype in [
            ("value", pa.int64()),
            ("array_value", pa.list_(pa.int64())),
            ("json_value", pa.string()),
            ("nullable_vector", pa.list_(pa.float32())),
        ]:
            columns.append(pa.field(name, pa.null() if arrow_null_type else dtype))
        path = tmp_path / "all_null.parquet"
        self._write_rows(path, rows, pa.schema(columns))
        self._import_and_wait(collection_name, [[upload_import_file(path)]])
        self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, rows)

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_multi_file_auto_id_preserves_rows_and_file_progress(self, minio_host, minio_bucket, tmp_path):
        """
        target: multi-file autoID import and per-file progress
        method: import two populated Parquet files and one empty Parquet file in one job
        expected: every row has one unique ID and each file reports its own row count
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_multi_file_auto_id")
        self._create_import_collection(client, collection_name)
        storage_client = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        groups = [
            [{"tag": f"first_{i}", "vector": [float(i)] * 8} for i in range(3)],
            [],
            [{"tag": f"last_{i}", "vector": [float(i + 3)] * 8} for i in range(5)],
        ]
        files = []
        try:
            for index, rows in enumerate(groups):
                files.append(
                    self._upload_parquet(storage_client, minio_bucket, tmp_path, rows, f"part_{index}.parquet")
                )

            response = bulk_import(
                url=self._import_url(),
                collection_name=collection_name,
                files=[[file_name] for file_name in files],
                api_key=cf.param_info.param_token,
            )
            job_id = response.json()["data"]["jobId"]
            progress = self._wait_for_state(self._import_url(), job_id, {"Completed"})
            assert progress["importedRows"] == progress["totalRows"] == 8, progress

            details = progress["details"]
            assert len(details) == len(files), progress
            for file_name, rows in zip(files, groups):
                matching = [detail for detail in details if file_name in detail["fileName"]]
                assert len(matching) == 1, progress
                assert matching[0]["totalRows"] == len(rows), matching[0]
                assert matching[0]["importedRows"] == len(rows), matching[0]

            client.refresh_load(collection_name=collection_name)
            results = self.query(
                client,
                collection_name,
                filter='tag != ""',
                output_fields=["id", "tag"],
                limit=100,
            )[0]
            expected_tags = {row["tag"] for rows in groups for row in rows}
            assert {row["tag"] for row in results} == expected_tags, results
            assert len({row["id"] for row in results}) == len(expected_tags), results
        finally:
            for file_name in files:
                storage_client.remove_object(minio_bucket, file_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "damage", ["numpy_row_count", "numpy_truncated", "parquet_footer", "jsonl_tail", "csv_short_row"]
    )
    def test_import_malformed_file_preserves_existing_rows(self, damage, upload_import_file, tmp_path):
        """
        target: malformed source files and unequal Numpy column lengths
        method: import a valid prefix followed by malformed data into a populated collection
        expected: the job fails with a reason, no imported row becomes visible and existing data is unchanged
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_malformed")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        self._create_data_collection(client, collection_name, schema)
        existing = [{"id": 1000, "vector": [125.0] * 8}]
        self.insert(client, collection_name, existing)
        rows = [{"id": i, "vector": [float(i)] * 8} for i in range(32)]
        files = []
        if damage.startswith("numpy"):
            for field, dtype in (("id", np.int64), ("vector", np.float32)):
                path = tmp_path / f"{field}.npy"
                values = [row[field] for row in rows]
                if field == "vector" and damage == "numpy_row_count":
                    values = values[:-1]
                np.save(path, np.asarray(values, dtype=dtype), allow_pickle=False)
                if field == "vector" and damage == "numpy_truncated":
                    path.write_bytes(path.read_bytes()[:-7])
                files.append(upload_import_file(path))
        else:
            suffix = {"parquet_footer": "parquet", "jsonl_tail": "jsonl", "csv_short_row": "csv"}[damage]
            path = tmp_path / f"invalid.{suffix}"
            self._write_rows(path, rows, pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32()))]))
            if damage == "parquet_footer":
                path.write_bytes(path.read_bytes()[:-8])
            else:
                with path.open("a", encoding="utf-8") as stream:
                    stream.write('\n{"id": 32, "vector": [' if damage == "jsonl_tail" else "32\n")
            files.append(upload_import_file(path))
        progress = self._import_and_wait(
            collection_name, [files], {"sep": "|"} if damage == "csv_short_row" else {}, expected_state="Failed"
        )
        expected_reason = {
            "numpy_row_count": "not aligned",
            "numpy_truncated": "eof",
            "parquet_footer": "parquet",
            "jsonl_tail": "eof",
            "csv_short_row": "number of fields",
        }
        assert expected_reason[damage] in progress.get("reason", "").lower(), progress
        self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, existing)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self._assert_import_rows(client, collection_name, existing)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["json", "numpy", "parquet"])
    @pytest.mark.parametrize("partition_type", [DataType.INT64, DataType.VARCHAR], ids=["int_key", "varchar_key"])
    @pytest.mark.parametrize("auto_id", [False, True])
    def test_import_partition_key_preserves_row_identity(
        self, file_format, partition_type, auto_id, upload_import_file, tmp_path
    ):
        """
        target: partition-key routing with explicit PKs and autoID across multiple files
        method: import deterministic rows with four key values and query each key separately
        expected: each key returns exactly its source rows, including scalar and vector values
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_partition_values")
        schema = self.create_schema(client, auto_id=auto_id, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)
        schema.add_field(
            "group_key",
            partition_type,
            is_partition_key=True,
            **({"max_length": 64} if partition_type == DataType.VARCHAR else {}),
        )
        schema.add_field("value", DataType.INT64, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params, num_partitions=4, shards_num=2
        )
        rows = [
            {
                "id": i,
                "vector": [float(i + j) / 8 for j in range(8)],
                "tag": f"row_{i}",
                "group_key": f"group_{i % 4}" if partition_type == DataType.VARCHAR else i % 4,
                "value": i * 37,
            }
            for i in range(24)
        ]
        arrow_fields = [
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.float32())),
            pa.field("tag", pa.string()),
            pa.field("group_key", pa.string() if partition_type == DataType.VARCHAR else pa.int64()),
            pa.field("value", pa.int64()),
        ]
        if auto_id:
            arrow_fields = arrow_fields[1:]
        files = []
        for part, group in enumerate((rows[:11], rows[11:])):
            source_rows = [{key: value for key, value in row.items() if not (auto_id and key == "id")} for row in group]
            if file_format == "numpy":
                directory = tmp_path / f"part_{part}"
                directory.mkdir()
                paths = []
                for field in arrow_fields:
                    dtype = (
                        np.float32 if field.name == "vector" else "U64" if pa.types.is_string(field.type) else np.int64
                    )
                    path = directory / f"{field.name}.npy"
                    np.save(path, np.asarray([row[field.name] for row in source_rows], dtype=dtype), allow_pickle=False)
                    paths.append(upload_import_file(path))
                files.append(paths)
            else:
                path = tmp_path / f"part_{part}.{file_format}"
                self._write_rows(path, source_rows, pa.schema(arrow_fields))
                files.append([upload_import_file(path)])
        self._import_and_wait(collection_name, files)
        self.refresh_load(client, collection_name)
        all_ids = set()
        for group_key in {row["group_key"] for row in rows}:
            expected = {row["tag"]: row for row in rows if row["group_key"] == group_key}
            actual = self.query(
                client,
                collection_name,
                filter=f"group_key == {json.dumps(group_key)}",
                output_fields=["id", "tag", "group_key", "value", "vector"],
                limit=len(rows),
            )[0]
            assert len(actual) == len(expected)
            assert {row["tag"] for row in actual} == set(expected)
            for row in actual:
                assert row["id"] not in all_ids
                all_ids.add(row["id"])
                source = expected[row["tag"]]
                for field in source:
                    if field != "id" or not auto_id:
                        assert row[field] == source[field], {"field": field, "row": row, "expected": source}
        assert len(all_ids) == len(rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("action,terminal_state", [("commit", "Completed"), ("abort", "Failed")])
    def test_import_empty_file_manual_job_reaches_terminal_state(
        self, minio_host, minio_bucket, tmp_path, action, terminal_state
    ):
        """
        target: zero-row manual import lifecycle
        method: create a manual import from an empty Parquet file, then commit or abort
        expected: the job reaches the requested terminal state without visible rows
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_empty_manual")
        self._create_import_collection(client, collection_name)
        storage_client = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        file_name = self._upload_parquet(storage_client, minio_bucket, tmp_path, [], "empty.parquet")
        try:
            response = bulk_import(
                url=self._import_url(),
                collection_name=collection_name,
                files=[[file_name]],
                options={"auto_commit": "false"},
                api_key=cf.param_info.param_token,
            )
            job_id = response.json()["data"]["jobId"]
            progress = self._wait_for_state(self._import_url(), job_id, {"Uncommitted"})
            assert progress["importedRows"] == progress["totalRows"] == 0, progress

            operation = commit_import if action == "commit" else abort_import
            operation(url=self._import_url(), job_id=job_id, api_key=cf.param_info.param_token)
            progress = self._wait_for_state(self._import_url(), job_id, {terminal_state})
            assert progress["importedRows"] == progress["totalRows"] == 0, progress

            client.refresh_load(collection_name=collection_name)
            results = self.query(client, collection_name, filter="id >= 0", output_fields=["id"])[0]
            assert results == [], results
        finally:
            storage_client.remove_object(minio_bucket, file_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
    def test_import_scalar_boundaries_preserve_values(self, file_format, upload_import_file, tmp_path):
        """
        target: scalar numeric precision at integer bounds and float32 limits
        method: import exact min/max integers and finite floats through each row-oriented reader
        expected: values are preserved without integer rounding or non-finite conversion
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_scalar_bounds")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        arrow_fields = [pa.field("id", pa.int64()), pa.field("vector", pa.list_(pa.float32()))]
        integers = [
            ("int8_value", DataType.INT8, pa.int8(), 8),
            ("int16_value", DataType.INT16, pa.int16(), 16),
            ("int32_value", DataType.INT32, pa.int32(), 32),
            ("int64_value", DataType.INT64, pa.int64(), 64),
        ]
        for name, datatype, arrow_type, _ in integers:
            schema.add_field(name, datatype, nullable=True)
            arrow_fields.append(pa.field(name, arrow_type))
        schema.add_field("float_value", DataType.FLOAT, nullable=True)
        schema.add_field("double_value", DataType.DOUBLE)
        arrow_fields += [pa.field("float_value", pa.float32()), pa.field("double_value", pa.float64())]
        self._create_data_collection(client, collection_name, schema)
        rows = []
        for i in range(5):
            row = {
                "id": i,
                "vector": [float(i)] * 8,
                "float_value": [None, float(np.finfo(np.float32).max), -float(np.finfo(np.float32).max), 0.0, 0.125][i],
                "double_value": [0.0, 2**40 + 0.5, -(2**40 + 0.5), 0.125, -0.125][i],
            }
            for name, _, _, bits in integers:
                row[name] = [None, -(2 ** (bits - 1)), 2 ** (bits - 1) - 1, 0, 1][i]
            rows.append(row)
        path = tmp_path / f"bounds.{file_format}"
        self._write_rows(path, rows, pa.schema(arrow_fields))
        self._import_and_wait(
            collection_name,
            [[upload_import_file(path)]],
            {"sep": "|", "nullkey": "__NULL__"} if file_format == "csv" else {},
        )
        self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, rows)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
    def test_import_dynamic_fields_and_added_field_survive_mutations(self, file_format, upload_import_file, tmp_path):
        """
        target: dynamic values, missing newly added fields and DML after import
        method: add a nullable field after writing files, import, then upsert/delete distinct PKs
        expected: source values, filled nulls and final row versions survive release/load
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_dynamic_values")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=True)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        self._create_data_collection(client, collection_name, schema)
        rows = [
            {
                "id": i,
                "vector": [float(i)] * 8,
                "extra_text": f"row_{i}_中文",
                "extra_number": str(i) if file_format == "csv" else i,
            }
            for i in range(9)
        ]
        fields = [pa.field("id", pa.int64()), pa.field("vector", pa.list_(pa.float32()))]
        if file_format == "parquet":
            source_rows = [
                {
                    "id": row["id"],
                    "vector": row["vector"],
                    "$meta": {"extra_text": row["extra_text"], "extra_number": row["extra_number"]},
                }
                for row in rows
            ]
            fields.append(pa.field("$meta", pa.string()))
        else:
            source_rows = rows
            fields += [pa.field("extra_text", pa.string()), pa.field("extra_number", pa.string())]
        path = tmp_path / f"dynamic.{file_format}"
        self._write_rows(path, source_rows, pa.schema(fields))
        self.add_collection_field(client, collection_name, "added", DataType.INT64, nullable=True)
        self._import_and_wait(
            collection_name, [[upload_import_file(path)]], {"sep": "|"} if file_format == "csv" else {}
        )
        self.refresh_load(client, collection_name)
        expected = [{**row, "added": None} for row in rows]
        self._assert_import_rows(client, collection_name, expected)
        changed = {**expected[2], "vector": [99.0] * 8, "extra_text": "updated", "added": 42}
        self.upsert(client, collection_name, [changed])
        self.delete(client, collection_name, ids=[4])
        expected = [changed if row["id"] == 2 else row for row in expected if row["id"] != 4]
        self._assert_import_rows(client, collection_name, expected)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self._assert_import_rows(client, collection_name, expected)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["json", "parquet"])
    def test_import_bm25_output_matches_source_documents(self, file_format, upload_import_file, tmp_path):
        """
        target: generated BM25 output remains aligned with imported documents
        method: import distinct token sets and search every token through the generated sparse field
        expected: each token returns exactly its source PKs and document contents
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_bm25_values")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("document", DataType.VARCHAR, max_length=256, enable_analyzer=True)
        schema.add_field("bm25", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="document_bm25",
                function_type=FunctionType.BM25,
                input_field_names=["document"],
                output_field_names=["bm25"],
            )
        )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="bm25", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        rows = [
            {"id": 0, "document": "cobalt cobalt"},
            {"id": 1, "document": "amber"},
            {"id": 2, "document": "violet"},
            {"id": 3, "document": "cobalt amber"},
        ]
        path = tmp_path / f"documents.{file_format}"
        self._write_rows(path, rows, pa.schema([("id", pa.int64()), ("document", pa.string())]))
        self._import_and_wait(collection_name, [[upload_import_file(path)]])
        self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, rows)
        for token in ("cobalt", "amber", "violet"):
            expected = {row["id"]: row["document"] for row in rows if token in row["document"].split()}
            hits = self.search(
                client,
                collection_name,
                data=[token],
                anns_field="bm25",
                limit=len(rows),
                search_params={"metric_type": "BM25", "params": {}},
                output_fields=["document"],
            )[0][0]
            assert {hit["id"] for hit in hits} == set(expected), hits
            for hit in hits:
                assert hit["entity"]["document"] == expected[hit["id"]]
                assert hit["distance"] > 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
    def test_import_null_array_element_fails_without_visible_rows(self, file_format, upload_import_file, tmp_path):
        """
        target: nullable array parents do not permit null elements
        method: import valid rows followed by an array containing a null element
        expected: the job fails and no prefix of the invalid import becomes visible
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_null_element")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("array_value", DataType.ARRAY, element_type=DataType.INT64, max_capacity=8, nullable=True)
        self._create_data_collection(client, collection_name, schema)
        rows = [{"id": i, "vector": [float(i)] * 8, "array_value": [i, -i]} for i in range(12)]
        rows[-1]["array_value"] = [11, None, -11]
        path = tmp_path / f"null_element.{file_format}"
        self._write_rows(
            path,
            rows,
            pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32())), ("array_value", pa.list_(pa.int64()))]),
        )
        progress = self._import_and_wait(
            collection_name,
            [[upload_import_file(path)]],
            {"sep": "|", "nullkey": "__NULL__"} if file_format == "csv" else {},
            expected_state="Failed",
        )
        assert "array" in progress["reason"].lower(), progress
        self.refresh_load(client, collection_name)
        self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id"],
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": []},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["parquet", "numpy"])
    @pytest.mark.parametrize("vector_type", [DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR], ids=["fp16", "bf16"])
    @pytest.mark.parametrize("representation", ["floats", "bytes"])
    def test_import_half_vector_encodings_preserve_values(
        self, file_format, vector_type, representation, upload_import_file, tmp_path
    ):
        """
        target: half-precision vectors stored as numbers or bytes
        method: import both encodings and compare every component with the expected quantized value
        expected: vector contents and nullable Parquet row positions are preserved
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_half_encoding")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("half_vector", vector_type, dim=8, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="half_vector", index_type="FLAT", metric_type="L2")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        dtype = np.float16 if vector_type == DataType.FLOAT16_VECTOR else cf.bfloat16
        # Numeric BF16 import truncates FP32 mantissas (Float32ToBFloat16Bytes);
        # ml_dtypes rounds instead. Keep explicit expected values for tenths 0.0 through 0.8.
        truncated_bf16 = [0.0, 0.099609375, 0.19921875, 0.298828125, 0.3984375, 0.5, 0.59765625, 0.69921875, 0.796875]
        expected = {}
        rows = []
        for i in range(9):
            values = np.asarray([(i - j) / 10 for j in range(8)], dtype=np.float32)
            quantized = values.astype(dtype)
            if file_format == "parquet" and i % 3 == 0:
                expected[i] = None
                value = None
            else:
                expected[i] = quantized.astype(np.float32)
                if vector_type == DataType.BFLOAT16_VECTOR and representation == "floats":
                    expected[i] = np.asarray(
                        [truncated_bf16[abs(i - j)] * (1 if i >= j else -1) for j in range(8)], dtype=np.float32
                    )
                value = quantized.view(np.uint8).tolist() if representation == "bytes" else values.tolist()
            rows.append({"id": i, "half_vector": value})
        if file_format == "parquet":
            path = tmp_path / "half.parquet"
            arrow_type = pa.uint8() if representation == "bytes" else pa.float32()
            self._write_rows(path, rows, pa.schema([("id", pa.int64()), ("half_vector", pa.list_(arrow_type))]))
            files = [upload_import_file(path)]
        else:
            files = []
            for field, storage_type in (
                ("id", np.int64),
                ("half_vector", np.uint8 if representation == "bytes" else np.float32),
            ):
                path = tmp_path / f"{field}.npy"
                np.save(path, np.asarray([row[field] for row in rows], dtype=storage_type), allow_pickle=False)
                files.append(upload_import_file(path))
        self._import_and_wait(collection_name, [files])
        self.refresh_load(client, collection_name)
        actual = self.query(client, collection_name, filter="id >= 0", output_fields=["id", "half_vector"], limit=10)[0]
        assert len(actual) == len(expected) and {row["id"] for row in actual} == set(expected)
        for row in actual:
            value = expected[row["id"]]
            if value is None:
                assert row["half_vector"] is None
            else:
                decoded = np.frombuffer(row["half_vector"][0], dtype=dtype).astype(np.float32)
                np.testing.assert_array_equal(decoded, value)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
    def test_import_geometry_and_timestamps_preserve_values(self, file_format, upload_import_file, tmp_path):
        """
        target: nullable Geometry and Timestamptz values across file formats
        method: import points and equivalent timestamps with different offsets and microsecond fractions
        expected: spatial predicates match the exact source PKs and timestamps preserve the UTC instant
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_geo_time")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("geo", DataType.GEOMETRY, nullable=True)
        schema.add_field("event_time", DataType.TIMESTAMPTZ, nullable=True)
        self._create_data_collection(client, collection_name, schema)
        timestamps = [
            "2025-06-01T00:00:00.123456Z",
            "2025-06-01T08:00:00.123456+08:00",
            "2025-05-31T20:00:00.123456-04:00",
            "2025-06-01T00:00:00.123457Z",
            None,
        ]
        rows = [
            {
                "id": i,
                "vector": [float(i)] * 8,
                "geo": None if i == 1 else f"POINT ({i} {i + 1})",
                "event_time": timestamp,
            }
            for i, timestamp in enumerate(timestamps)
        ]
        path = tmp_path / f"geo_time.{file_format}"
        self._write_rows(
            path,
            rows,
            pa.schema(
                [
                    ("id", pa.int64()),
                    ("vector", pa.list_(pa.float32())),
                    ("geo", pa.string()),
                    ("event_time", pa.string()),
                ]
            ),
        )
        self._import_and_wait(
            collection_name,
            [[upload_import_file(path)]],
            {"sep": "|", "nullkey": "__NULL__"} if file_format == "csv" else {},
        )
        self.refresh_load(client, collection_name)
        results = self.query(
            client, collection_name, filter="id >= 0", output_fields=["id", "geo", "event_time"], limit=6
        )[0]
        assert len(results) == len(rows) and {row["id"] for row in results} == set(range(len(rows)))
        for actual in results:
            expected = rows[actual["id"]]
            if expected["event_time"] is None:
                assert actual["event_time"] is None
            else:
                assert datetime.fromisoformat(actual["event_time"]).astimezone(UTC) == datetime.fromisoformat(
                    expected["event_time"]
                ).astimezone(UTC)
            if expected["geo"] is None:
                assert actual["geo"] is None
            else:
                matches = self.query(
                    client, collection_name, filter=f"ST_EQUALS(geo, '{expected['geo']}')", output_fields=["id"]
                )[0]
                assert matches == [{"id": expected["id"]}], matches
        matches = self.query(
            client, collection_name, filter="event_time == ISO '2025-06-01T00:00:00.123456Z'", output_fields=["id"]
        )[0]
        assert {row["id"] for row in matches} == {0, 1, 2}, matches

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
    def test_import_nullable_timestamps_apply_defaults(self, file_format, upload_import_file, tmp_path):
        """
        target: nullable Timestamptz defaults for explicit nulls and missing columns
        method: import mixed timestamps/nulls and a second file without the timestamp column
        expected: nulls and missing values receive the default while explicit timestamps remain unchanged
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_timestamp_default")
        default_timestamp = "2023-11-14T22:13:20Z"
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("event_time", DataType.TIMESTAMPTZ, nullable=True, default_value=default_timestamp)
        self._create_data_collection(client, collection_name, schema)
        timestamps = [
            "2024-01-01T00:00:00Z",
            None,
            "2024-01-02T08:00:00.123456+08:00",
            None,
            "1970-01-01T00:00:00Z",
            None,
            "2024-01-03T00:00:00Z",
        ]
        rows = [{"id": i, "vector": [float(i)] * 8, "event_time": value} for i, value in enumerate(timestamps)]
        missing_rows = [{"id": i, "vector": [float(i)] * 8} for i in range(len(rows), len(rows) + 2)]
        arrow_fields = [("id", pa.int64()), ("vector", pa.list_(pa.float32()))]
        mixed_path = tmp_path / f"mixed.{file_format}"
        missing_path = tmp_path / f"missing.{file_format}"
        self._write_rows(mixed_path, rows, pa.schema(arrow_fields + [("event_time", pa.string())]))
        self._write_rows(missing_path, missing_rows, pa.schema(arrow_fields))
        self._import_and_wait(
            collection_name,
            [[upload_import_file(mixed_path)], [upload_import_file(missing_path)]],
            {"sep": "|", "nullkey": "__NULL__"} if file_format == "csv" else {},
        )
        self.refresh_load(client, collection_name)
        expected = {
            row["id"]: datetime.fromisoformat(row.get("event_time") or default_timestamp).astimezone(UTC)
            for row in rows + missing_rows
        }
        results = self.query(
            client, collection_name, filter="id >= 0", output_fields=["id", "event_time"], limit=len(expected) + 1
        )[0]
        assert len(results) == len(expected) and {row["id"] for row in results} == set(expected)
        for row in results:
            assert row["event_time"] is not None, row
            assert datetime.fromisoformat(row["event_time"]).astimezone(UTC) == expected[row["id"]], row
        null_rows = self.query(client, collection_name, filter="event_time is null", output_fields=["id"])[0]
        assert null_rows == [], null_rows

    @pytest.fixture
    def copy_import_binlogs(self, minio_host, minio_bucket):
        storage_client = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        root_path = os.getenv("MILVUS_MINIO_ROOT_PATH", "files").strip("/")
        backup_prefix = f"bulkinsert_data/{uuid4()}/binlog_backup"
        copied_objects = []

        def copy_segments(collection_id, segments):
            # Use the same metadata API and column-group layout as milvus-backup.
            response = requests.post(
                f"{self._import_url()}/v2/vectordb/segments/describe",
                headers={"Authorization": f"Bearer {cf.param_info.param_token}"},
                json={
                    "dbName": "default",
                    "collectionID": collection_id,
                    "segmentIDs": [segment.segment_id for segment in segments],
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            assert payload["code"] == 0, payload
            infos = payload["data"]["segmentInfos"]
            assert {info["segmentID"] for info in infos} == {segment.segment_id for segment in segments}, payload
            files = []
            for info in infos:
                assert info["insertLogs"] and not info["deltaLogs"], info
                source_prefix = f"{root_path}/insert_log/{collection_id}/{info['partitionID']}/{info['segmentID']}"
                # The public import API lists segments below a partition/group prefix.
                group_prefix = f"{backup_prefix}/{info['segmentID']}"
                destination_prefix = f"{group_prefix}/{info['segmentID']}"
                for field in info["insertLogs"]:
                    for log_id in field["logIDs"]:
                        suffix = f"{field['fieldID']}/{log_id}"
                        destination = f"{destination_prefix}/{suffix}"
                        copied_objects.append(destination)
                        storage_client.copy_object(
                            minio_bucket,
                            destination,
                            CopySource(minio_bucket, f"{source_prefix}/{suffix}"),
                        )
                files.append([f"{group_prefix}/"])
            return files, infos

        yield copy_segments
        for object_name in copied_objects:
            storage_client.remove_object(minio_bucket, object_name)

    def _wait_for_sorted_binlog_segments(self, client, collection_name, row_count, timeout=120):
        deadline = time.monotonic() + timeout
        segments = []
        while time.monotonic() < deadline:
            segments = self.list_persistent_segments(client, collection_name)[0]
            if (
                segments
                and sum(segment.num_rows for segment in segments) == row_count
                and all(segment.state_name == "Flushed" and segment.is_sorted for segment in segments)
            ):
                return segments
            time.sleep(2)
        raise AssertionError(f"Sorted binlogs for {row_count} rows are not ready: {segments}")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "vector_type", [DataType.FLOAT_VECTOR, DataType.SPARSE_FLOAT_VECTOR], ids=["dense", "sparse"]
    )
    def test_import_binlog_restores_added_nullable_vector(self, vector_type, copy_import_binlogs):
        """
        target: restore packed StorageV2 segments sealed before and after adding a nullable vector field
        method: copy real binlogs and import them with backup=true into an equivalent schema
        expected: old rows retain NULL, new mixed values remain aligned, and search excludes NULL rows
        """
        client = self._client()
        source = cf.gen_unique_str("binlog_source")
        target = cf.gen_unique_str("binlog_restored")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("label", DataType.VARCHAR, max_length=64, nullable=True)
        self.create_collection(client, source, schema=schema, properties={"collection.autocompaction.enabled": "false"})
        old_rows = [{"id": i, "vector": [i / 8] * 8, "label": None if i % 3 == 0 else str(i)} for i in range(8)]
        self.insert(client, source, old_rows)
        self.flush(client, source)
        old_segments = self._wait_for_sorted_binlog_segments(client, source, len(old_rows))
        versions = {segment.storage_version for segment in old_segments}
        if versions == {3}:
            pytest.skip("Native StorageV3 backup requires snapshot export/restore coverage")
        assert versions == {2}, old_segments
        storage_version = 2
        collection_id = self.describe_collection(client, source)[0]["collection_id"]
        old_files, old_infos = copy_import_binlogs(collection_id, old_segments)

        vector_params = {"dim": 8} if vector_type == DataType.FLOAT_VECTOR else {}
        self.add_collection_field(client, source, "added_vector", vector_type, nullable=True, **vector_params)
        source_fields = self.describe_collection(client, source)[0]["fields"]
        added_field_id = next(field["field_id"] for field in source_fields if field["name"] == "added_vector")
        # Prove the triggering condition instead of assuming that flush preceded schema propagation.
        assert all(added_field_id not in {field["fieldID"] for field in info["insertLogs"]} for info in old_infos)
        new_rows = [
            {
                "id": i,
                "vector": [i / 8] * 8,
                "label": "" if i % 3 == 0 else str(i),
                "added_vector": None
                if i % 2 == 0
                else ([i / 8] * 8 if vector_type == DataType.FLOAT_VECTOR else {0: i / 8, i: 0.5}),
            }
            for i in range(8, 16)
        ]
        self.insert(client, source, new_rows)
        self.flush(client, source)
        old_ids = {segment.segment_id for segment in old_segments}
        all_segments = self._wait_for_sorted_binlog_segments(client, source, len(old_rows) + len(new_rows))
        assert old_ids.issubset({segment.segment_id for segment in all_segments}), all_segments
        new_segments = [segment for segment in all_segments if segment.segment_id not in old_ids]
        assert new_segments and sum(segment.num_rows for segment in new_segments) == len(new_rows), new_segments
        assert all(segment.storage_version == storage_version for segment in new_segments), new_segments
        new_files, new_infos = copy_import_binlogs(collection_id, new_segments)
        assert all(added_field_id in {field["fieldID"] for field in info["insertLogs"]} for info in new_infos)
        log.info(
            f"Binlog restore storage_version={storage_version}, old_segments={old_infos}, new_segments={new_infos}"
        )

        schema.add_field("added_vector", vector_type, nullable=True, **vector_params)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        indexes.add_index(
            "added_vector",
            index_type="FLAT" if vector_type == DataType.FLOAT_VECTOR else "SPARSE_INVERTED_INDEX",
            metric_type="L2" if vector_type == DataType.FLOAT_VECTOR else "IP",
        )
        self.create_collection(client, target, schema=schema, index_params=indexes)
        target_fields = self.describe_collection(client, target)[0]["fields"]
        assert {field["name"]: field["field_id"] for field in target_fields} == {
            field["name"]: field["field_id"] for field in source_fields
        }
        # Removing the source proves the restore reads the copied backup objects.
        self.drop_collection(client, source)
        self._import_and_wait(
            target,
            old_files + new_files,
            {"backup": "true", "storage_version": str(storage_version)},
            partition_name="_default",
        )
        self.refresh_load(client, target)
        expected_rows = [dict(row, added_vector=None) for row in old_rows] + new_rows
        self._assert_import_rows(client, target, expected_rows)
        self.release_collection(client, target)
        self.load_collection(client, target)
        self._assert_import_rows(client, target, expected_rows)
        hits = self.search(
            client,
            target,
            data=[new_rows[1]["added_vector"]],
            anns_field="added_vector",
            limit=16,
            search_params={"metric_type": "L2" if vector_type == DataType.FLOAT_VECTOR else "IP", "params": {}},
        )[0][0]
        assert {hit["id"] for hit in hits} == {row["id"] for row in new_rows if row["added_vector"] is not None}
