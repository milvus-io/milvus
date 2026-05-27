import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from minio import Minio
from minio.error import S3Error
from pymilvus import DataType
from pymilvus.bulk_writer import (
    bulk_import,
    get_import_progress,
)
from pymilvus.client.embedding_list import EmbeddingList
from utils.util_log import test_log as log
from utils.util_pymilvus import *  # noqa: F403

prefix = "struct_array"
epsilon = 0.001
default_dim = 128
INDEX_PARAMS = {"M": 16, "efConstruction": 200}

# Dim for emb list index tests (smaller for faster index building)
EMB_LIST_DIM = 32


@dataclass(frozen=True)
class StructArrayNullableSchemaSpec:
    pk_field: str = "id"
    vector_field: str = "normal_vector"
    tag_field: str = "doc_tag"
    struct_field: str = "profile"
    int_subfield: str = "p_int"
    tag_subfield: str = "p_tag"
    vector_subfield: str = "p_vec"
    vector_dim: int = default_dim
    struct_max_capacity: int = 4
    tag_max_length: int = 128

    @property
    def scalar_subfields(self) -> tuple[str, str]:
        return (self.int_subfield, self.tag_subfield)

    @property
    def vector_subfields(self) -> tuple[str, str, str]:
        return (self.int_subfield, self.tag_subfield, self.vector_subfield)


DEFAULT_SCHEMA_SPEC = StructArrayNullableSchemaSpec()
OMITTED_FIELD = object()


class StructArrayNullableTestMixin:
    @staticmethod
    def _vector(seed: int, dim: int = default_dim) -> list[float]:
        return [float(seed) + float(i) / 1000 for i in range(dim)]

    @staticmethod
    def _schema_as_dict(schema) -> dict[str, Any]:
        if isinstance(schema, dict):
            return schema
        return schema.to_dict()

    @classmethod
    def _schema_field_names(cls, schema) -> set[str]:
        schema_dict = cls._schema_as_dict(schema)
        field_names = {field.get("name") for field in schema_dict.get("fields", [])}
        field_names.update(field.get("name") for field in schema_dict.get("struct_fields", []))
        return field_names

    @classmethod
    def _schema_field_dim(cls, schema, field_name: str, default: int) -> int:
        schema_dict = cls._schema_as_dict(schema)
        for field in schema_dict.get("fields", []):
            if field.get("name") != field_name:
                continue
            return int((field.get("params") or {}).get("dim", default))
        return default

    @classmethod
    def _default_struct_array_schema_dict(
        cls,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        include_struct: bool = False,
        include_vector_subfield: bool = False,
        normal_vector_dim: int | None = None,
        struct_vector_type: DataType = DataType.FLOAT_VECTOR,
        struct_vector_dim: int | None = None,
        struct_nullable: bool = True,
    ) -> dict[str, Any]:
        normal_vector_dim = normal_vector_dim or spec.vector_dim
        struct_vector_dim = struct_vector_dim or normal_vector_dim
        fields = [
            {
                "name": spec.pk_field,
                "type": DataType.INT64,
                "is_primary": True,
                "auto_id": False,
            },
            {
                "name": spec.vector_field,
                "type": DataType.FLOAT_VECTOR,
                "params": {"dim": normal_vector_dim},
            },
            {
                "name": spec.tag_field,
                "type": DataType.VARCHAR,
                "params": {"max_length": spec.tag_max_length},
            },
        ]
        schema = {
            "auto_id": False,
            "enable_dynamic_field": False,
            "fields": fields,
            "functions": [],
        }
        if include_struct:
            struct_subfields = [
                {"name": spec.int_subfield, "type": DataType.INT64, "nullable": True},
                {
                    "name": spec.tag_subfield,
                    "type": DataType.VARCHAR,
                    "params": {"max_length": spec.tag_max_length},
                    "nullable": True,
                },
            ]
            if include_vector_subfield:
                struct_subfields.append(
                    {
                        "name": spec.vector_subfield,
                        "type": struct_vector_type,
                        "params": {"dim": struct_vector_dim},
                        "nullable": True,
                    }
                )
            fields.append(
                {
                    "name": spec.struct_field,
                    "type": DataType.ARRAY,
                    "element_type": DataType.STRUCT,
                    "params": {"max_capacity": spec.struct_max_capacity},
                    "nullable": struct_nullable,
                    "struct_fields": struct_subfields,
                }
            )
            schema["struct_fields"] = [
                {
                    "name": spec.struct_field,
                    "max_capacity": spec.struct_max_capacity,
                    "nullable": struct_nullable,
                    "fields": struct_subfields,
                }
            ]
        return schema

    @staticmethod
    def _create_struct_field_schema(
        client,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        include_vector_subfield: bool = False,
        vector_type: DataType = DataType.FLOAT_VECTOR,
        vector_dim: int = default_dim,
    ):
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(spec.int_subfield, DataType.INT64)
        struct_schema.add_field(spec.tag_subfield, DataType.VARCHAR, max_length=spec.tag_max_length)
        if include_vector_subfield:
            struct_schema.add_field(spec.vector_subfield, vector_type, dim=vector_dim)
        return struct_schema

    @classmethod
    def _create_nullable_struct_array_schema(
        cls,
        client,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        include_struct: bool = True,
        include_vector_subfield: bool = False,
        normal_vector_dim: int = default_dim,
        struct_vector_type: DataType = DataType.FLOAT_VECTOR,
        struct_vector_dim: int | None = None,
        include_tag_field: bool = True,
        enable_dynamic_field: bool = False,
    ):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=enable_dynamic_field)
        schema.add_field(field_name=spec.pk_field, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=spec.vector_field, datatype=DataType.FLOAT_VECTOR, dim=normal_vector_dim)
        if include_tag_field:
            schema.add_field(field_name=spec.tag_field, datatype=DataType.VARCHAR, max_length=spec.tag_max_length)
        if include_struct:
            struct_vector_dim = struct_vector_dim or normal_vector_dim
            struct_schema = cls._create_struct_field_schema(
                client,
                spec=spec,
                include_vector_subfield=include_vector_subfield,
                vector_type=struct_vector_type,
                vector_dim=struct_vector_dim,
            )
            schema.add_field(
                spec.struct_field,
                datatype=DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=struct_schema,
                max_capacity=spec.struct_max_capacity,
                nullable=True,
            )
        return schema

    def _create_indexed_nullable_struct_array_collection(
        self,
        client,
        collection_name: str,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        include_vector_subfield: bool = False,
        normal_vector_dim: int = default_dim,
        struct_vector_type: DataType = DataType.FLOAT_VECTOR,
        struct_vector_dim: int | None = None,
    ):
        schema = self._create_nullable_struct_array_schema(
            client,
            spec=spec,
            include_vector_subfield=include_vector_subfield,
            normal_vector_dim=normal_vector_dim,
            struct_vector_type=struct_vector_type,
            struct_vector_dim=struct_vector_dim,
        )
        index_params = client.prepare_index_params()
        index_params.add_index(field_name=spec.vector_field, index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check
        return schema

    @classmethod
    def _generated_rows_by_schema(
        cls,
        schema,
        count: int,
        *,
        start_id: int = 0,
        tag_prefix: str = "row",
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        profiles: list[Any] | None = None,
        default_values: dict[str, Any] | None = None,
        vector_dim: int | None = None,
    ) -> list[dict[str, Any]]:
        field_names = cls._schema_field_names(schema)
        row_ids = list(range(start_id, start_id + count))
        vector_dim = vector_dim or cls._schema_field_dim(schema, spec.vector_field, spec.vector_dim)
        overrides = {
            spec.pk_field: row_ids,
            spec.vector_field: [cls._vector(row_id, vector_dim) for row_id in row_ids],
            spec.tag_field: [f"{tag_prefix}_{i}" for i in range(count)],
        }
        if profiles is not None:
            overrides[spec.struct_field] = profiles
        if default_values:
            overrides.update(default_values)
        overrides = {field_name: value for field_name, value in overrides.items() if field_name in field_names}
        return cf.gen_row_data_by_schema_with_defaults(
            nb=count,
            schema=schema,
            start=start_id,
            default_values=overrides,
        )

    @classmethod
    def _generated_row_by_schema(
        cls,
        schema,
        row_id: int,
        tag: str,
        *,
        profile=OMITTED_FIELD,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        vector_dim: int | None = None,
    ) -> dict[str, Any]:
        vector_dim = vector_dim or cls._schema_field_dim(schema, spec.vector_field, spec.vector_dim)
        default_values = {
            spec.pk_field: [row_id],
            spec.vector_field: [cls._vector(row_id, vector_dim)],
            spec.tag_field: [tag],
        }
        if profile is not OMITTED_FIELD:
            default_values[spec.struct_field] = [profile]
        row = cls._generated_rows_by_schema(
            schema,
            1,
            start_id=row_id,
            tag_prefix=tag,
            spec=spec,
            default_values=default_values,
            vector_dim=vector_dim,
        )[0]
        if profile is OMITTED_FIELD:
            row.pop(spec.struct_field, None)
        return row

    @classmethod
    def _default_nullable_scalar_profile(
        cls,
        row_id: int,
        offset: int,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        if offset % 3 == 0:
            return None
        if offset % 3 == 1:
            return []
        return cls._scalar_profile(row_id, spec=spec)

    @classmethod
    def _nullable_scalar_struct_rows_by_schema(
        cls,
        schema,
        count: int,
        *,
        start_id: int = 0,
        tag_prefix: str = "row",
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        profile_factory=None,
        tag_factory=None,
        vector_factory=None,
    ) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
        row_ids = list(range(start_id, start_id + count))
        profile_factory = profile_factory or cls._default_nullable_scalar_profile
        tag_factory = tag_factory or (lambda row_id, _offset: f"{tag_prefix}_{row_id}")
        vector_dim = cls._schema_field_dim(schema, spec.vector_field, spec.vector_dim)
        vector_factory = vector_factory or (lambda row_id, _offset: cls._vector(row_id, vector_dim))
        profiles = [profile_factory(row_id, offset, spec=spec) for offset, row_id in enumerate(row_ids)]
        default_values = {
            spec.vector_field: [vector_factory(row_id, offset) for offset, row_id in enumerate(row_ids)],
            spec.tag_field: [tag_factory(row_id, offset) for offset, row_id in enumerate(row_ids)],
        }
        rows = cls._generated_rows_by_schema(
            schema,
            count,
            start_id=start_id,
            tag_prefix=tag_prefix,
            spec=spec,
            profiles=profiles,
            default_values=default_values,
            vector_dim=vector_dim,
        )
        return rows, {row[spec.pk_field]: row for row in rows}

    @staticmethod
    def _scalar_struct_profile_arrow_type(
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        return pa.list_(
            pa.struct(
                [
                    pa.field(spec.int_subfield, pa.int64()),
                    pa.field(spec.tag_subfield, pa.string()),
                ]
            )
        )

    @classmethod
    def _write_scalar_struct_rows_parquet(
        cls,
        rows: list[dict[str, Any]],
        local_file_path: str,
        *,
        row_group_size: int,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        table = pa.table(
            {
                spec.pk_field: pa.array([row[spec.pk_field] for row in rows], type=pa.int64()),
                spec.vector_field: pa.array([row[spec.vector_field] for row in rows], type=pa.list_(pa.float32())),
                spec.tag_field: pa.array([row[spec.tag_field] for row in rows], type=pa.string()),
                spec.struct_field: pa.array(
                    [row[spec.struct_field] for row in rows],
                    type=cls._scalar_struct_profile_arrow_type(spec=spec),
                ),
            }
        )
        pq.write_table(table, local_file_path, row_group_size=row_group_size)

    @classmethod
    def _index_filler_rows(
        cls,
        start_id: int,
        count: int,
        tag_prefix: str,
        *,
        schema=None,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        vector_dim: int | None = None,
    ) -> list[dict[str, Any]]:
        schema = schema or cls._default_struct_array_schema_dict(spec=spec, normal_vector_dim=vector_dim)
        return cls._generated_rows_by_schema(
            schema,
            count,
            start_id=start_id,
            tag_prefix=tag_prefix,
            spec=spec,
            vector_dim=vector_dim,
        )

    @classmethod
    def _scalar_struct_index_filler_rows(
        cls,
        start_id: int,
        count: int,
        tag_prefix: str,
        *,
        schema=None,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        vector_dim: int | None = None,
    ) -> list[dict[str, Any]]:
        schema = schema or cls._default_struct_array_schema_dict(
            spec=spec,
            include_struct=True,
            normal_vector_dim=vector_dim,
        )
        profiles = [
            [{spec.int_subfield: -(start_id + i), spec.tag_subfield: f"{tag_prefix}_profile_{i}"}] for i in range(count)
        ]
        return cls._generated_rows_by_schema(
            schema,
            count,
            start_id=start_id,
            tag_prefix=tag_prefix,
            spec=spec,
            profiles=profiles,
            vector_dim=vector_dim,
        )

    @classmethod
    def _vector_struct_index_filler_rows(
        cls,
        start_id: int,
        count: int,
        tag_prefix: str,
        *,
        schema=None,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        vector_dim: int | None = None,
    ) -> list[dict[str, Any]]:
        schema = schema or cls._default_struct_array_schema_dict(
            spec=spec,
            include_struct=True,
            include_vector_subfield=True,
            normal_vector_dim=vector_dim,
        )
        vector_dim = vector_dim or cls._schema_field_dim(schema, spec.vector_field, spec.vector_dim)
        profiles = [
            [
                {
                    spec.int_subfield: -(start_id + i),
                    spec.tag_subfield: f"{tag_prefix}_profile_{i}",
                    spec.vector_subfield: cls._vector(start_id + i, vector_dim),
                }
            ]
            for i in range(count)
        ]
        return cls._generated_rows_by_schema(
            schema,
            count,
            start_id=start_id,
            tag_prefix=tag_prefix,
            spec=spec,
            profiles=profiles,
            vector_dim=vector_dim,
        )

    @staticmethod
    def _unit_vector(axis: int, dim: int = default_dim) -> list[float]:
        vector = [0.0 for _ in range(dim)]
        vector[axis % dim] = 1.0
        return vector

    def _profile(
        self,
        row_id: int,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ) -> list[dict[str, Any]]:
        return [
            {
                spec.int_subfield: row_id * 10,
                spec.tag_subfield: f"profile_{row_id}_0",
                spec.vector_subfield: self._vector(row_id * 10),
            },
            {
                spec.int_subfield: row_id * 10 + 1,
                spec.tag_subfield: f"profile_{row_id}_1",
                spec.vector_subfield: self._vector(row_id * 10 + 1),
            },
        ]

    @staticmethod
    def _typed_vector(vector_type: DataType, seed: int, dim: int = EMB_LIST_DIM):
        if vector_type == DataType.FLOAT16_VECTOR:
            return np.array([((seed + i) % 17) / 17 for i in range(dim)], dtype=np.float16)
        if vector_type == DataType.BFLOAT16_VECTOR:
            return np.array([((seed + i) % 19) / 19 for i in range(dim)], dtype=cf.bfloat16)
        if vector_type == DataType.INT8_VECTOR:
            return np.array([((seed + i) % 255) - 128 for i in range(dim)], dtype=np.int8)
        if vector_type == DataType.BINARY_VECTOR:
            bits = [((seed + i) % 2) for i in range(dim)]
            return bytes(np.packbits(bits, axis=-1).tolist())
        return [float(seed) + float(i) / 1000 for i in range(dim)]

    def _typed_profile(
        self,
        row_id: int,
        vector_type: DataType,
        dim: int = EMB_LIST_DIM,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ) -> list[dict[str, Any]]:
        return [
            {
                spec.int_subfield: row_id * 10,
                spec.tag_subfield: f"profile_{row_id}_0",
                spec.vector_subfield: self._typed_vector(vector_type, row_id * 10, dim),
            },
            {
                spec.int_subfield: row_id * 10 + 1,
                spec.tag_subfield: f"profile_{row_id}_1",
                spec.vector_subfield: self._typed_vector(vector_type, row_id * 10 + 1, dim),
            },
        ]

    def _assert_struct_array_equal(
        self,
        actual,
        expected,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
        expected_keys=None,
        vector_type: DataType = DataType.FLOAT_VECTOR,
        exact_keys: bool = False,
    ):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            keys = tuple(expected_keys or expected_item.keys())
            if exact_keys:
                assert set(actual_item) == set(keys)
            for key in keys:
                if key == spec.vector_subfield:
                    self._assert_typed_vector_equal(actual_item[key], expected_item[key], vector_type)
                else:
                    assert actual_item[key] == expected_item[key]

    def _assert_profile_equal(
        self,
        actual,
        expected,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        self._assert_struct_array_equal(actual, expected, spec=spec, expected_keys=spec.vector_subfields)

    def _assert_profile_vector_subfield_equal(
        self,
        actual,
        expected,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        self._assert_struct_array_equal(
            actual,
            expected,
            spec=spec,
            expected_keys=(spec.vector_subfield,),
            exact_keys=True,
        )

    @staticmethod
    def _binary_vector_bytes(value) -> bytes:
        if isinstance(value, bytes | bytearray):
            return bytes(value)
        if isinstance(value, np.ndarray):
            return value.tobytes()
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes | bytearray):
            return bytes(value[0])
        return bytes(value)

    def _assert_typed_vector_equal(self, actual, expected, vector_type: DataType):
        if vector_type == DataType.BINARY_VECTOR:
            assert self._binary_vector_bytes(actual) == self._binary_vector_bytes(expected)
            return

        if vector_type == DataType.BFLOAT16_VECTOR:
            actual_bits = np.asarray(actual, dtype=np.uint16).tolist()
            expected_bits = np.asarray(expected).view(np.uint16).tolist()
            assert actual_bits == expected_bits
            return

        if vector_type == DataType.INT8_VECTOR:
            assert np.asarray(actual, dtype=np.int8).tolist() == np.asarray(expected, dtype=np.int8).tolist()
            return

        assert np.asarray(actual, dtype=np.float32).tolist() == pytest.approx(
            np.asarray(expected, dtype=np.float32).tolist(),
            abs=epsilon,
        )

    def _assert_typed_profile_equal(
        self,
        actual,
        expected,
        vector_type: DataType,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        self._assert_struct_array_equal(
            actual,
            expected,
            spec=spec,
            expected_keys=spec.vector_subfields,
            vector_type=vector_type,
        )

    def _assert_typed_profile_vector_subfield_equal(
        self,
        actual,
        expected,
        vector_type: DataType,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        self._assert_struct_array_equal(
            actual,
            expected,
            spec=spec,
            expected_keys=(spec.vector_subfield,),
            vector_type=vector_type,
            exact_keys=True,
        )

    @staticmethod
    def _search_entity(hit):
        return hit.get("entity", hit)

    @staticmethod
    def _drain_iterator(iterator):
        assert iterator is not None
        rows = []
        while True:
            batch = iterator.next()
            if not batch:
                break
            rows.extend(batch)
        iterator.close()
        return rows

    @staticmethod
    def _scalar_profile(
        row_id: int,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ) -> list[dict[str, Any]]:
        return [
            {
                spec.int_subfield: row_id * 10,
                spec.tag_subfield: f"profile_{row_id}_0",
            },
            {
                spec.int_subfield: row_id * 10 + 1,
                spec.tag_subfield: f"profile_{row_id}_1",
            },
        ]

    def _assert_scalar_profile_equal(
        self,
        actual,
        expected,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        self._assert_struct_array_equal(actual, expected, spec=spec, expected_keys=spec.scalar_subfields)

    def _assert_nullable_scalar_profile_equal(
        self,
        actual,
        expected,
        expected_keys=None,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        self._assert_struct_array_equal(
            actual,
            expected,
            spec=spec,
            expected_keys=expected_keys,
            exact_keys=True,
        )

    def _setup_nullable_scalar_struct_expression_collection(
        self,
        client,
        collection_name,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        schema = self._create_nullable_struct_array_schema(client, spec=spec, include_vector_subfield=False)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        sealed_explicit_null_profile_row = self._generated_row_by_schema(
            schema, 0, "sealed_explicit_null_profile", profile=None, spec=spec
        )
        sealed_omitted_profile_row = self._generated_row_by_schema(schema, 1, "sealed_omit_profile", spec=spec)
        sealed_empty_profile_row = self._generated_row_by_schema(
            schema, 2, "sealed_empty_profile", profile=[], spec=spec
        )
        sealed_one_match_profile_row = self._generated_row_by_schema(
            schema,
            3,
            "sealed_one_match_profile",
            profile=[
                {spec.int_subfield: 9100, spec.tag_subfield: "match_9100"},
                {spec.int_subfield: 100, spec.tag_subfield: "low_100"},
            ],
            spec=spec,
        )
        sealed_two_match_profile_row = self._generated_row_by_schema(
            schema,
            4,
            "sealed_two_match_profile",
            profile=[
                {spec.int_subfield: 9200, spec.tag_subfield: "match_9200"},
                {spec.int_subfield: 9300, spec.tag_subfield: "match_9300"},
            ],
            spec=spec,
        )
        sealed_zero_match_profile_row = self._generated_row_by_schema(
            schema,
            5,
            "sealed_zero_match_profile",
            profile=[
                {spec.int_subfield: 100, spec.tag_subfield: "low_100"},
                {spec.int_subfield: 200, spec.tag_subfield: "low_200"},
            ],
            spec=spec,
        )
        sealed_control_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_one_match_profile_row,
            sealed_two_match_profile_row,
            sealed_zero_match_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_control_rows),
            "sealed_expr_index_filler",
            schema=schema,
            spec=spec,
        )
        sealed_rows = sealed_control_rows + sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name=spec.vector_field, index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, spec.vector_field, timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        growing_explicit_null_profile_row = self._generated_row_by_schema(
            schema, 7000, "growing_explicit_null_profile", profile=None, spec=spec
        )
        growing_omitted_profile_row = self._generated_row_by_schema(schema, 7001, "growing_omit_profile", spec=spec)
        growing_empty_profile_row = self._generated_row_by_schema(
            schema, 7002, "growing_empty_profile", profile=[], spec=spec
        )
        growing_one_match_profile_row = self._generated_row_by_schema(
            schema,
            7003,
            "growing_one_match_profile",
            profile=[
                {spec.int_subfield: 9600, spec.tag_subfield: "match_9600"},
                {spec.int_subfield: 600, spec.tag_subfield: "low_600"},
            ],
            spec=spec,
        )
        growing_two_match_profile_row = self._generated_row_by_schema(
            schema,
            7004,
            "growing_two_match_profile",
            profile=[
                {spec.int_subfield: 9700, spec.tag_subfield: "match_9700"},
                {spec.int_subfield: 9800, spec.tag_subfield: "match_9800"},
            ],
            spec=spec,
        )
        growing_zero_match_profile_row = self._generated_row_by_schema(
            schema,
            7005,
            "growing_zero_match_profile",
            profile=[
                {spec.int_subfield: 700, spec.tag_subfield: "low_700"},
                {spec.int_subfield: 800, spec.tag_subfield: "low_800"},
            ],
            spec=spec,
        )
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_one_match_profile_row,
            growing_two_match_profile_row,
            growing_zero_match_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {}
        source_by_id[sealed_explicit_null_profile_row[spec.pk_field]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row[spec.pk_field]] = {
            **sealed_omitted_profile_row,
            spec.struct_field: None,
        }
        source_by_id[sealed_empty_profile_row[spec.pk_field]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row[spec.pk_field]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row[spec.pk_field]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row[spec.pk_field]] = sealed_zero_match_profile_row
        source_by_id.update({row[spec.pk_field]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row[spec.pk_field]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row[spec.pk_field]] = {
            **growing_omitted_profile_row,
            spec.struct_field: None,
        }
        source_by_id[growing_empty_profile_row[spec.pk_field]] = growing_empty_profile_row
        source_by_id[growing_one_match_profile_row[spec.pk_field]] = growing_one_match_profile_row
        source_by_id[growing_two_match_profile_row[spec.pk_field]] = growing_two_match_profile_row
        source_by_id[growing_zero_match_profile_row[spec.pk_field]] = growing_zero_match_profile_row

        controlled_ids = {row[spec.pk_field] for row in sealed_control_rows + growing_rows}
        return {
            "source_by_id": source_by_id,
            "source_rows": list(source_by_id.values()),
            "controlled_ids": controlled_ids,
            "schema_spec": spec,
        }

    def _assert_expression_rows_match_source(
        self,
        rows,
        source_by_id,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ):
        for row in rows:
            expected = source_by_id[row[spec.pk_field]]
            assert row[spec.tag_field] == expected[spec.tag_field]
            self._assert_scalar_profile_equal(row[spec.struct_field], expected[spec.struct_field], spec=spec)

    @staticmethod
    def _parse_expression_value(raw_value: str):
        raw_value = raw_value.strip()
        if raw_value.startswith('"') and raw_value.endswith('"'):
            return raw_value[1:-1]
        if raw_value.lower() == "null":
            return None
        try:
            return int(raw_value)
        except ValueError:
            return float(raw_value)

    @classmethod
    def _parse_expression_list(cls, raw_value: str):
        return json.loads(raw_value)

    @staticmethod
    def _compare_expression_values(actual, op: str, expected) -> bool:
        if actual is None or expected is None:
            if op == "==":
                return actual is expected
            if op == "!=":
                return actual is not expected
            return False
        if op == ">=":
            return actual >= expected
        if op == "<=":
            return actual <= expected
        if op == "==":
            return actual == expected
        if op == "!=":
            return actual != expected
        if op == ">":
            return actual > expected
        if op == "<":
            return actual < expected
        raise AssertionError(f"unsupported operator: {op}")

    @classmethod
    def _eval_struct_element_condition(cls, element: dict[str, Any], condition: str) -> bool:
        condition = condition.strip()
        if " && " in condition:
            return all(cls._eval_struct_element_condition(element, part) for part in condition.split(" && "))
        if " || " in condition:
            return any(cls._eval_struct_element_condition(element, part) for part in condition.split(" || "))
        for op in [">=", "<=", "==", "!=", ">", "<"]:
            if op not in condition:
                continue
            left, right = [part.strip() for part in condition.split(op, 1)]
            assert left.startswith("$[") and left.endswith("]")
            field_name = left[2:-1]
            actual = element.get(field_name)
            expected = cls._parse_expression_value(right)
            return cls._compare_expression_values(actual, op, expected)
        raise AssertionError(f"unsupported element condition: {condition}")

    @staticmethod
    def _strip_id_scope(
        rows: list[dict[str, Any]],
        expression: str,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ) -> tuple[list[dict[str, Any]], str]:
        expression = expression.strip()
        match = re.match(rf"^{re.escape(spec.pk_field)}\s+in\s+\[([^\]]*)\]\s*&&\s*(.+)$", expression)
        if match is None:
            return rows, expression
        id_values = {int(value.strip()) for value in match.group(1).split(",") if value.strip()}
        return [row for row in rows if row[spec.pk_field] in id_values], match.group(2).strip()

    @classmethod
    def _expected_struct_array_expression_rows(
        cls,
        rows: list[dict[str, Any]],
        expression: str,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ) -> list[dict[str, Any]]:
        scoped_rows, expression = cls._strip_id_scope(rows, expression, spec=spec)
        struct_field_pattern = re.escape(spec.struct_field)

        def output_row(row, offset=None):
            result = {
                spec.pk_field: row[spec.pk_field],
                spec.tag_field: row[spec.tag_field],
                spec.struct_field: row.get(spec.struct_field),
            }
            if offset is not None:
                result["offset"] = offset
            return result

        if expression == f"{spec.struct_field} is null":
            return [output_row(row) for row in scoped_rows if row.get(spec.struct_field) is None]
        if expression == f"{spec.struct_field} is not null":
            return [output_row(row) for row in scoped_rows if row.get(spec.struct_field) is not None]

        length_match = re.match(
            rf"^array_length\({struct_field_pattern}(?:\[[^\]]+\])?\)\s*(==|!=|>=|<=|>|<)\s*(\d+)$",
            expression,
        )
        if length_match is not None:
            op, raw_expected = length_match.groups()
            expected = int(raw_expected)
            return [
                output_row(row)
                for row in scoped_rows
                if row.get(spec.struct_field) is not None
                and cls._compare_expression_values(len(row[spec.struct_field]), op, expected)
            ]

        array_contains_match = re.match(
            rf"^array_contains(?:_(all|any))?\({struct_field_pattern}\[([^\]]+)\],\s*(.+)\)$",
            expression,
        )
        if array_contains_match is not None:
            mode, field_name, raw_expected = array_contains_match.groups()
            expected_values = (
                cls._parse_expression_list(raw_expected)
                if mode in {"all", "any"}
                else [cls._parse_expression_value(raw_expected)]
            )
            results = []
            for row in scoped_rows:
                struct_array = row.get(spec.struct_field)
                if struct_array is None:
                    continue
                actual_values = [element.get(field_name) for element in struct_array]
                if (
                    (mode == "all" and all(value in actual_values for value in expected_values))
                    or (mode == "any" and any(value in actual_values for value in expected_values))
                    or (mode is None and expected_values[0] in actual_values)
                ):
                    results.append(output_row(row))
            return results

        index_access_match = re.match(
            rf"^{struct_field_pattern}\[(\d+)\]\[([^\]]+)\]\s*(==|!=|>=|<=|>|<)\s*(.+)$",
            expression,
        )
        if index_access_match is not None:
            raw_offset, field_name, op, raw_expected = index_access_match.groups()
            offset = int(raw_offset)
            expected = cls._parse_expression_value(raw_expected)
            results = []
            for row in scoped_rows:
                struct_array = row.get(spec.struct_field) or []
                if len(struct_array) <= offset:
                    continue
                if cls._compare_expression_values(struct_array[offset].get(field_name), op, expected):
                    results.append(output_row(row))
            return results

        element_filter_match = re.match(rf"^element_filter\({struct_field_pattern},\s*(.+)\)$", expression)
        if element_filter_match is not None:
            condition = element_filter_match.group(1)
            results = []
            for row in scoped_rows:
                struct_array = row.get(spec.struct_field) or []
                for offset, element in enumerate(struct_array):
                    if cls._eval_struct_element_condition(element, condition):
                        results.append(output_row(row, offset=offset))
            return results

        match_family_match = re.match(
            rf"^MATCH_(ALL|ANY|LEAST|MOST|EXACT)\({struct_field_pattern},\s*(.+?)(?:,\s*threshold=(\d+))?\)$",
            expression,
        )
        if match_family_match is not None:
            match_type, condition, raw_threshold = match_family_match.groups()
            threshold = int(raw_threshold) if raw_threshold is not None else None
            results = []
            for row in scoped_rows:
                struct_array = row.get(spec.struct_field) or []
                match_count = sum(
                    1 for element in struct_array if cls._eval_struct_element_condition(element, condition)
                )
                total_count = len(struct_array)
                matched = (
                    (match_type == "ALL" and match_count == total_count)
                    or (match_type == "ANY" and match_count >= 1)
                    or (match_type == "LEAST" and match_count >= threshold)
                    or (match_type == "MOST" and match_count <= threshold)
                    or (match_type == "EXACT" and match_count == threshold)
                )
                if matched:
                    results.append(output_row(row))
            return results

        raise AssertionError(f"unsupported expression: {expression}")

    @staticmethod
    def _expected_nullable_scalar_struct_expression_rows(
        rows: list[dict[str, Any]],
        expression: str,
        *,
        spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC,
    ) -> list[dict[str, Any]]:
        return StructArrayNullableTestMixin._expected_struct_array_expression_rows(rows, expression, spec=spec)

    @staticmethod
    def _expression_result_keys(rows, *, spec: StructArrayNullableSchemaSpec = DEFAULT_SCHEMA_SPEC):
        return sorted((row[spec.pk_field], row.get("offset")) for row in rows)


class TestMilvusClientStructArraySchemaEvolution(StructArrayNullableTestMixin, TestMilvusClientV2Base):
    """Test cases for struct array schema evolution"""

    min_index_sealed_rows = 3000

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_schema_nullable_propagation(self):
        """
        target: test schema nullable propagation after dynamically adding a struct array field
        method: add a nullable struct array field with scalar and vector sub-fields, then describe collection
        expected: parent struct is nullable, and raw schema shows both scalar and vector sub-fields nullable
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_schema_nullable")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert profile_field["nullable"] is True
        assert profile_field["type"] == DataType.ARRAY
        assert profile_field["element_type"] == DataType.STRUCT
        assert profile_field["params"]["max_capacity"] == 4
        user_sub_fields = {field["name"]: field for field in profile_field["struct_fields"]}
        assert set(user_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert user_sub_fields["p_int"]["type"] == DataType.INT64
        assert user_sub_fields["p_tag"]["type"] == DataType.VARCHAR
        assert user_sub_fields["p_tag"]["params"]["max_length"] == 128
        assert user_sub_fields["p_vec"]["type"] == DataType.FLOAT_VECTOR
        assert user_sub_fields["p_vec"]["params"]["dim"] == default_dim

        raw_describe = client._get_connection().describe_collection(collection_name)
        raw_profile = next(field for field in raw_describe["struct_array_fields"] if field["name"] == "profile")
        assert raw_profile["nullable"] is True
        raw_sub_fields = {field["name"]: field for field in raw_profile["fields"]}
        assert set(raw_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert raw_sub_fields["p_int"]["nullable"] is True
        assert raw_sub_fields["p_tag"]["nullable"] is True
        assert raw_sub_fields["p_vec"]["nullable"] is True
        assert raw_sub_fields["p_int"]["element_type"] == DataType.INT64
        assert raw_sub_fields["p_tag"]["element_type"] == DataType.VARCHAR
        assert raw_sub_fields["p_vec"]["element_type"] == DataType.FLOAT_VECTOR
        assert raw_sub_fields["p_vec"]["params"]["dim"] == default_dim

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_schema_nullable_propagation(self):
        """
        target: test schema nullable propagation when creating a nullable struct array field
        method: create a collection with a nullable struct array field containing scalar and vector sub-fields,
            then describe collection
        expected: parent struct is nullable, and raw schema shows both scalar and vector sub-fields nullable
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_schema_nullable")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert profile_field["nullable"] is True
        assert profile_field["type"] == DataType.ARRAY
        assert profile_field["element_type"] == DataType.STRUCT
        assert profile_field["params"]["max_capacity"] == 4
        user_sub_fields = {field["name"]: field for field in profile_field["struct_fields"]}
        assert set(user_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert user_sub_fields["p_int"]["type"] == DataType.INT64
        assert user_sub_fields["p_tag"]["type"] == DataType.VARCHAR
        assert user_sub_fields["p_tag"]["params"]["max_length"] == 128
        assert user_sub_fields["p_vec"]["type"] == DataType.FLOAT_VECTOR
        assert user_sub_fields["p_vec"]["params"]["dim"] == default_dim

        raw_describe = client._get_connection().describe_collection(collection_name)
        raw_profile = next(field for field in raw_describe["struct_array_fields"] if field["name"] == "profile")
        assert raw_profile["nullable"] is True
        raw_sub_fields = {field["name"]: field for field in raw_profile["fields"]}
        assert set(raw_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert raw_sub_fields["p_int"]["nullable"] is True
        assert raw_sub_fields["p_tag"]["nullable"] is True
        assert raw_sub_fields["p_vec"]["nullable"] is True
        assert raw_sub_fields["p_int"]["element_type"] == DataType.INT64
        assert raw_sub_fields["p_tag"]["element_type"] == DataType.VARCHAR
        assert raw_sub_fields["p_vec"]["element_type"] == DataType.FLOAT_VECTOR
        assert raw_sub_fields["p_vec"]["params"]["dim"] == default_dim

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_omit_nullable_query_search(self):
        """
        target: test query/search output for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, then insert rows with the struct field omitted,
            empty, and non-empty
        expected: omitted field is returned as null, empty array remains empty, and non-empty struct data matches
            source in query, sub-field output, element_filter query, and normal vector search
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_omit")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"},
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._scalar_profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": self._scalar_profile(3),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 20)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in filter_results} == {2}
        for row in filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(2)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_insert_explicit_null_query_search(self):
        """
        target: test explicit null insert for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, then insert rows with explicit null, omitted, empty,
            and non-empty struct values
        expected: explicit null and omitted rows return null, empty row returns [], and non-empty row matches source
            in query, sub-field query, element_filter query, and normal vector search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_explicit_null")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {
                "id": 0,
                "normal_vector": self._vector(0),
                "doc_tag": "explicit_null_profile",
                "profile": None,
            },
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "omitted_profile"},
            {"id": 2, "normal_vector": self._vector(2), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile",
                "profile": self._scalar_profile(3),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 30)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in filter_results} == {3}
        for row in filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(3)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == 3
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_upsert_null_non_null(self):
        """
        target: test upsert for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, insert null/empty/non-null rows, then upsert null to
            non-null, non-null to null, and a new row with the struct field omitted
        expected: full-row upsert updates nullable struct values correctly in query, element_filter, and search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_upsert")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "initial_null_profile"},
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "initial_non_null_profile",
                "profile": self._scalar_profile(1),
            },
            {"id": 2, "normal_vector": self._vector(2), "doc_tag": "initial_empty_profile", "profile": []},
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}
        null_to_non_null = {
            "id": 0,
            "normal_vector": self._vector(100),
            "doc_tag": "upserted_null_to_non_null",
            "profile": self._scalar_profile(100),
        }
        non_null_to_null = {
            "id": 1,
            "normal_vector": self._vector(101),
            "doc_tag": "upserted_non_null_to_null",
        }
        new_omitted_profile = {
            "id": 3,
            "normal_vector": self._vector(103),
            "doc_tag": "upserted_new_omit_profile",
        }
        res, check = self.upsert(client, collection_name, [null_to_non_null, non_null_to_null, new_omitted_profile])
        assert check
        assert res["upsert_count"] == 3

        source_by_id[null_to_non_null["id"]] = null_to_non_null
        source_by_id[non_null_to_null["id"]] = {**non_null_to_null, "profile": None}
        source_by_id[new_omitted_profile["id"]] = {**new_omitted_profile, "profile": None}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 1000)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {null_to_non_null["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], null_to_non_null["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_null_to_null["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == non_null_to_null["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_query_iterator(self):
        """
        target: test query_iterator output for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, insert omitted, empty, and non-empty rows, then drain
            query_iterator with small batch size
        expected: iterator returns every row once, and nullable struct output matches source across batch boundaries
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile_0"},
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._scalar_profile(2),
            },
            {"id": 3, "normal_vector": self._vector(3), "doc_tag": "missing_profile_3"},
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "present_profile_4",
                "profile": self._scalar_profile(4),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_search_iterator(self):
        """
        target: test search_iterator output for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, insert omitted, empty, and non-empty rows, then drain
            search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, distances are ordered, and nullable struct output matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile_0"},
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._scalar_profile(2),
            },
            {"id": 3, "normal_vector": self._vector(3), "doc_tag": "missing_profile_3"},
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "present_profile_4",
                "profile": self._scalar_profile(4),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(4)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_match_family_query_search(self):
        """
        target: test MATCH family correctness for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar Struct Array, insert sealed and growing null/empty/non-empty rows, then query
            with MATCH family and search with MATCH_ANY filter
        expected: null/empty rows do not match MATCH_ANY, MATCH family result sets match source-of-truth, and
            normal-vector search filtered by MATCH_ANY returns matching sealed and growing rows with correct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_match")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        sealed_explicit_null_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_one_match_profile_row = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "sealed_one_match_profile",
            "profile": [
                {"p_int": 9100, "p_tag": "match_9100"},
                {"p_int": 100, "p_tag": "low_100"},
            ],
        }
        sealed_two_match_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_two_match_profile",
            "profile": [
                {"p_int": 9200, "p_tag": "match_9200"},
                {"p_int": 9300, "p_tag": "match_9300"},
            ],
        }
        sealed_zero_match_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_zero_match_profile",
            "profile": [
                {"p_int": 100, "p_tag": "low_100"},
                {"p_int": 200, "p_tag": "low_200"},
            ],
        }
        sealed_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_one_match_profile_row,
            sealed_two_match_profile_row,
            sealed_zero_match_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_match_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        growing_explicit_null_profile_row = {
            "id": 7000,
            "normal_vector": self._vector(7000),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_empty_profile_row = {
            "id": 7001,
            "normal_vector": self._vector(7001),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_two_match_profile_row = {
            "id": 7002,
            "normal_vector": self._vector(7002),
            "doc_tag": "growing_two_match_profile",
            "profile": [
                {"p_int": 9600, "p_tag": "match_9600"},
                {"p_int": 9700, "p_tag": "match_9700"},
            ],
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_empty_profile_row,
            growing_two_match_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row["id"]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row["id"]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row["id"]] = sealed_zero_match_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_two_match_profile_row["id"]] = growing_two_match_profile_row

        match_any_ids = {
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_two_or_more_ids = {
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_exact_one_ids = {sealed_one_match_profile_row["id"]}
        non_nullish_scope = [
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        ]

        match_any_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in match_any_results} == match_any_ids
        for row in match_any_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_least_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_LEAST(profile, $[p_int] >= 9000, threshold=2)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in match_least_results} == match_two_or_more_ids
        for row in match_least_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_exact_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_EXACT(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in match_exact_results} == match_exact_one_ids
        for row in match_exact_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_all_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_ALL(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_nullish_scope),
        )
        assert check
        assert {row["id"] for row in match_all_results} == match_two_or_more_ids
        for row in match_all_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_most_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_MOST(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_nullish_scope),
        )
        assert check
        assert {row["id"] for row in match_most_results} == {
            sealed_one_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
        }
        for row in match_most_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_two_match_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=10,
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == match_any_ids
        assert search_results[0][0]["id"] == growing_two_match_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_element_filter_expression_query(self):
        """
        target: test element_filter query correctness for a nullable scalar Struct Array
        method: create nullable scalar Struct Array rows covering explicit null, omitted, empty, one-match,
            two-match, and zero-match profiles in both sealed and growing segments, then query with element_filter
        expected: expected result rows are computed from source data and expression; null/omitted/empty rows do not
            match, matching rows include the correct element offsets, and profile output matches source data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_element_filter_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]

        expressions = [
            "element_filter(profile, $[p_int] >= 9000)",
            'element_filter(profile, $[p_int] >= 9000 && $[p_tag] == "match_9100")',
            'element_filter(profile, $[p_tag] == "match_9700")',
        ]
        for expr in expressions:
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, expr)
            actual_rows, check = self.query(
                client,
                collection_name,
                filter=expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(expected_rows) + 5,
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_match_family_null_semantics(self):
        """
        target: test MATCH family null/empty semantics for a nullable scalar Struct Array
        method: create nullable scalar Struct Array rows covering explicit null, omitted, empty, one-match,
            two-match, and zero-match profiles in both sealed and growing segments, then query every MATCH operator
        expected: expected result sets are computed from source data and expression; MATCH_ANY/LEAST only return rows
            with enough matching elements, and MATCH_ALL/MOST/EXACT treat null/empty profiles as zero-element inputs
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_match_null_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"id in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            "MATCH_ANY(profile, $[p_int] >= 9000)",
            "MATCH_LEAST(profile, $[p_int] >= 9000, threshold=2)",
            "MATCH_EXACT(profile, $[p_int] >= 9000, threshold=1)",
            "MATCH_ALL(profile, $[p_int] >= 9000)",
            "MATCH_MOST(profile, $[p_int] >= 9000, threshold=1)",
            "MATCH_EXACT(profile, $[p_int] >= 9000, threshold=0)",
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            actual_rows, check = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(fixture["controlled_ids"]),
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

        search_expr = "MATCH_ANY(profile, $[p_int] >= 9000)"
        expected_search_ids = {
            row["id"] for row in self._expected_nullable_scalar_struct_expression_rows(source_rows, search_expr)
        }
        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7004)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter=search_expr,
            output_fields=["id", "doc_tag", "profile"],
            limit=len(expected_search_ids),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == expected_search_ids
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_subfield_projection_expression_query_search(self):
        """
        target: test PR #49178 struct sub-field projection expressions on a nullable scalar Struct Array
        method: run query and normal-vector search filters with array_length(profile[p_int]),
            array_contains(_all/_any)(profile[p_tag]), and fixed-index profile[i][sub_field] predicates
        expected: expected result sets are computed from source data and expression; null/omitted profiles do not
            match sub-field projection predicates, empty profiles only match array_length(...)=0, and search/query
            return the same filtered id sets
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_projection_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"id in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            "array_length(profile[p_int]) == 0",
            "array_length(profile[p_int]) > 0",
            'array_contains(profile[p_tag], "match_9100")',
            'array_contains_all(profile[p_tag], ["match_9200", "match_9300"])',
            'array_contains_any(profile[p_tag], ["match_9600", "missing"])',
            "profile[0][p_int] >= 9000",
            'profile[1][p_tag] == "match_9800"',
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            expected_ids = {row["id"] for row in expected_rows}

            actual_rows, check = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(fixture["controlled_ids"]),
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

            search_results, check = self.search(
                client,
                collection_name,
                data=[self._vector(7004)],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=max(len(expected_ids), 1),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == expected_ids
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="StructArray parent is not registered for direct IS NULL/IS NOT NULL or array_length expressions",
        strict=True,
    )
    def test_create_scalar_struct_array_field_nullable_parent_null_expression(self):
        """
        target: test direct null expressions on a nullable scalar Struct Array parent
        method: query `profile is null`, `profile is not null`, and `array_length(profile)` expressions on rows
            covering explicit null, omitted, empty, and non-empty profiles
        expected: expected result sets are computed from source data and expression; null expressions distinguish
            null/omitted rows from empty/non-empty rows, and array_length selects non-null Struct Array rows by length
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_parent_null_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"id in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            "profile is null",
            "profile is not null",
            "array_length(profile) == 0",
            "array_length(profile) > 0",
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            actual_rows, check = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(fixture["controlled_ids"]),
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="normal-vector search with element_filter on nullable StructArray returns rows that fail predicate",
        strict=True,
    )
    def test_create_scalar_struct_array_field_nullable_element_filter_search(self):
        """
        target: test element_filter as a normal-vector search filter for a nullable scalar Struct Array
        method: create >=3000 sealed rows plus growing rows with null/omitted/empty/non-empty profiles, then search
            normal_vector with `element_filter(profile, $[p_int] >= 9000)`
        expected: expected result ids are computed from source data and expression; search only returns rows that
            contain at least one matching struct element and excludes null/omitted/empty/zero-match rows
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_element_filter_search")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        expr = "element_filter(profile, $[p_int] >= 9000)"
        expected_ids = {row["id"] for row in self._expected_nullable_scalar_struct_expression_rows(source_rows, expr)}

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7004)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter=expr,
            output_fields=["id", "doc_tag", "profile"],
            limit=len(expected_ids),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == expected_ids
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_insert_omit_nullable_growing_row(self):
        """
        target: test query/search output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, then insert a growing row that omits
            the struct field and another growing row with non-null struct data
        expected: omitted field is returned as null, and non-null struct data matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_omit")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"},
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile",
                "profile": self._profile(1),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(1)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_sealed_null_empty_rows(self):
        """
        target: test sealed segment output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty
            rows, flush, then query/search
        expected: sealed query/search output preserves null, empty, and non-empty struct rows, and struct vector
            search skips null and empty rows
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_sealed")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        normal_search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(3)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in normal_search_results[0]} == set(source_by_id)
        for hit in normal_search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1]["profile"][0]["p_vec"])
        struct_search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        assert len(struct_search_results[0]) == len(non_empty_rows)
        hit_ids = {hit["id"] for hit in struct_search_results[0]}
        assert hit_ids == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert empty_profile_row["id"] not in hit_ids
        assert struct_search_results[0][0]["id"] == non_empty_rows[-1]["id"]
        for hit in struct_search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50068: nullable ArrayOfVector sealed output still fails after #50020 when empty row precedes single-element row",
        strict=True,
    )
    def test_create_struct_array_field_with_vector_single_element_after_empty_sealed_output(self):
        """
        target: test sealed segment output for a nullable struct array vector sub-field after an empty row
        method: create a nullable struct array field with a vector sub-field, insert one empty row followed by one
            single-element row with dim=8, flush, load, then query parent and vector sub-field output
        expected: empty row remains [], and the single-element row returns its vector payload instead of failing
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_empty_single")
        client = self._client()
        dim = 8

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        empty_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0, dim),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        present_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1, dim),
            "doc_tag": "present_single_profile",
            "profile": [
                {
                    "p_int": 10,
                    "p_tag": "profile_1_0",
                    "p_vec": self._vector(10, dim),
                }
            ],
        }
        rows = [empty_profile_row, present_profile_row]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in rows}
        query_results, check = self.query(
            client,
            collection_name,
            filter="id in [0, 1]",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(rows),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id in [0, 1]",
            output_fields=["id", "profile[p_vec]"],
            limit=len(rows),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize(
        "vector_type",
        [
            DataType.FLOAT16_VECTOR,
            DataType.BFLOAT16_VECTOR,
            DataType.INT8_VECTOR,
            DataType.BINARY_VECTOR,
        ],
    )
    def test_create_struct_array_field_with_non_float_vector_type_query_search_output(self, vector_type):
        """
        target: test nullable struct array output for non-FLOAT vector sub-field types
        method: create a nullable struct array with FLOAT16/BFLOAT16/INT8/BINARY vector sub-field, insert indexed
            sealed rows and growing rows with omitted, empty, and non-empty profile values, then query parent/sub-field
            output and search on the ordinary vector field
        expected: query/search output preserves nullable parent state and vector sub-field payload for each vector type
        """
        type_name = vector_type.name.lower()
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_{type_name}_output")
        client = self._client()
        dim = EMB_LIST_DIM

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", vector_type, dim=dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        sealed_special_rows = [
            {"id": 0, "normal_vector": self._vector(0, dim), "doc_tag": "sealed_omitted_profile"},
            {
                "id": 1,
                "normal_vector": self._vector(1, dim),
                "doc_tag": "sealed_empty_profile",
                "profile": [],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2, dim),
                "doc_tag": "sealed_present_profile_2",
                "profile": self._typed_profile(2, vector_type, dim),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3, dim),
                "doc_tag": "sealed_present_profile_3",
                "profile": self._typed_profile(3, vector_type, dim),
            },
        ]
        sealed_filler_rows = [
            {
                "id": 10000 + i,
                "normal_vector": self._vector(10000 + i, dim),
                "doc_tag": f"sealed_filler_{i}",
            }
            for i in range(self.min_index_sealed_rows - len(sealed_special_rows))
        ]
        sealed_rows = sealed_special_rows + sealed_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_metric = "MAX_SIM_HAMMING" if vector_type == DataType.BINARY_VECTOR else "MAX_SIM_COSINE"
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type=profile_metric,
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        # Mixed nullable/non-null ArrayOfVector rows in the same growing batch are tracked by #50009.
        # Keep null/empty coverage in sealed rows here, and use growing only for non-empty requery output.
        growing_rows = [
            {
                "id": 20002,
                "normal_vector": self._vector(20002, dim),
                "doc_tag": "growing_present_profile",
                "profile": self._typed_profile(20002, vector_type, dim),
            }
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in sealed_rows + growing_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_typed_profile_equal(row["profile"], expected["profile"], vector_type)

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_typed_profile_vector_subfield_equal(row["profile"], expected["profile"], vector_type)

        top_k = 16
        search_vector = growing_rows[-1]["normal_vector"]
        expected_top_ids = [
            row["id"]
            for row in sorted(
                source_by_id.values(),
                key=lambda row: (
                    sum((left - right) ** 2 for left, right in zip(row["normal_vector"], search_vector)),
                    row["id"],
                ),
            )[:top_k]
        ]
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=top_k,
        )
        assert check
        hits = search_results[0]
        assert [hit["id"] for hit in hits] == expected_top_ids
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_typed_profile_equal(entity["profile"], expected["profile"], vector_type)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_query_iterator(self):
        """
        target: test query_iterator output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty
            rows, flush, then drain query_iterator with small batch size
        expected: iterator returns every row once, and nullable struct output matches source across batch boundaries
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": self._profile(3),
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_search_iterator(self):
        """
        target: test search_iterator output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty
            rows, flush, then drain search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, distances are ordered, and nullable struct output matches
            source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": self._profile(3),
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(3)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_stress_nullable_scalar_struct_array_query_search_iterators(self):
        """
        target: stress query/search/iterator output for a nullable scalar struct array
        method: create 10k sealed rows with deterministic random nullable masks and 0/1/2/3 struct elements, then run
            full query, normal-vector search, query_iterator, and search_iterator
        expected: every output row/hit matches source data, and iterator scans have no missing or duplicated ids
        """
        collection_name = cf.gen_unique_str(f"{prefix}_stress_nullable_scalar_struct")
        client = self._client()
        dim = 8
        entities = 10000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rng = random.Random(20260526)
        rows = []
        source_by_id = {}
        mask_counts = {"omitted": 0, "null": 0, "empty": 0, "present": 0}
        element_counts = {0: 0, 1: 0, 2: 0, 3: 0}
        for row_id in range(entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id, dim=dim),
                "doc_tag": f"stress_row_{row_id}",
            }

            choice = rng.randrange(8)
            if choice == 0:
                expected_profile = None
                mask_counts["omitted"] += 1
            elif choice == 1:
                row["profile"] = None
                expected_profile = None
                mask_counts["null"] += 1
            elif choice == 2:
                row["profile"] = []
                expected_profile = []
                mask_counts["empty"] += 1
                element_counts[0] += 1
            else:
                element_count = 1 + rng.randrange(3)
                row["profile"] = [
                    {
                        "p_int": row_id * 10 + element_index,
                        "p_tag": f"profile_{row_id}_{element_index}",
                    }
                    for element_index in range(element_count)
                ]
                expected_profile = row["profile"]
                mask_counts["present"] += 1
                element_counts[element_count] += 1

            rows.append(row)
            source_by_id[row_id] = {**row, "profile": expected_profile}

        assert all(count > 0 for count in mask_counts.values())
        assert all(count > 0 for count in element_counts.values())

        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == entities

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        query_iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=997,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(query_iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)
        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            batch_size=997,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        iterator_hits = self._drain_iterator(search_iterator)
        iterator_hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(iterator_hit_ids) == len(set(iterator_hit_ids))
        assert set(iterator_hit_ids) == set(source_by_id)
        iterator_distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(iterator_distances) - 1):
            assert iterator_distances[index] <= iterator_distances[index + 1] + epsilon
        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(
        reason="known blocker: indexed sealed nullable ArrayOfVector output omits ValidData when present-row density is high",
        strict=True,
    )
    def test_stress_nullable_struct_array_vector_field_query_search_iterators(self):
        """
        target: stress query/search/iterator output for a nullable struct array with vector sub-field
        method: create 10k sealed rows with deterministic random nullable masks and 0/2/3 struct elements, then run
            full query, normal-vector search, query_iterator, and search_iterator
        expected: every output row/hit matches source data, and iterator scans have no missing or duplicated ids
        """
        collection_name = cf.gen_unique_str(f"{prefix}_stress_nullable_struct_vector")
        client = self._client()
        dim = 8
        entities = 10000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rng = random.Random(20260526)
        rows = []
        source_by_id = {}
        mask_counts = {"omitted": 0, "null": 0, "empty": 0, "present": 0}
        element_counts = {0: 0, 2: 0, 3: 0}
        for row_id in range(entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id, dim=dim),
                "doc_tag": f"stress_row_{row_id}",
            }

            choice = rng.randrange(8)
            if choice == 0:
                expected_profile = None
                mask_counts["omitted"] += 1
            elif choice == 1:
                row["profile"] = None
                expected_profile = None
                mask_counts["null"] += 1
            elif choice == 2:
                row["profile"] = []
                expected_profile = []
                mask_counts["empty"] += 1
                element_counts[0] += 1
            else:
                element_count = 2 + (rng.randrange(2))
                row["profile"] = [
                    {
                        "p_int": row_id * 10 + element_index,
                        "p_tag": f"profile_{row_id}_{element_index}",
                        "p_vec": self._vector(row_id * 10 + element_index, dim=dim),
                    }
                    for element_index in range(element_count)
                ]
                expected_profile = row["profile"]
                mask_counts["present"] += 1
                element_counts[element_count] += 1

            rows.append(row)
            source_by_id[row_id] = {**row, "profile": expected_profile}

        assert all(count > 0 for count in mask_counts.values())
        assert all(count > 0 for count in element_counts.values())

        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == entities

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        assert self.wait_for_index_ready(client, collection_name, "profile[p_vec]", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

        query_iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=997,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(query_iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)
        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            batch_size=997,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        iterator_hits = self._drain_iterator(search_iterator)
        iterator_hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(iterator_hit_ids) == len(set(iterator_hit_ids))
        assert set(iterator_hit_ids) == set(source_by_id)
        iterator_distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(iterator_distances) - 1):
            assert iterator_distances[index] <= iterator_distances[index + 1] + epsilon
        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_ann_search_iterator_rejects_embedding_list(self):
        """
        target: test search_iterator limitation on a nullable struct array vector sub-field
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty rows,
            verify regular struct-vector search succeeds, then request search_iterator on the same embedding-list query
        expected: regular search returns only non-empty struct rows, while search_iterator is rejected as unsupported for
            multi-search-multi on embedding list fields
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_ann_siter_reject")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1]["profile"][0]["p_vec"])
        results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]

        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_profile_equal(entity["profile"], expected["profile"])

        error = {
            ct.err_code: 1100,
            ct.err_msg: "search iterator is not supported for multi-search-multi on embedding list fields",
        }
        self.search_iterator(
            client,
            collection_name,
            data=[search_tensor.to_flat_array()],
            batch_size=1,
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
            is_embedding_list=True,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_iterator(self):
        """
        target: test element-level search_iterator on a nullable struct array vector sub-field
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty rows,
            index the struct vector sub-field with a non-embedding-list metric, then drain search_iterator with a plain
            query vector
        expected: iterator returns only rows containing vector elements, skips null/empty rows, and output matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in non_empty_rows}
        null_or_empty_ids = {missing_profile_row["id"], empty_profile_row["id"]}

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            batch_size=1,
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)
        assert not set(hit_ids).intersection(null_or_empty_ids)
        assert iterator_hits[0]["id"] == non_empty_rows[-1]["id"]
        assert iterator_hits[0]["offset"] == 0

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] >= distances[index + 1] - epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows",
        strict=True,
    )
    def test_create_struct_array_field_with_vector_element_search_flat(self):
        """
        target: test element-level search on a nullable struct array vector sub-field created with collection schema
        method: create a nullable struct array field with a vector sub-field, insert non-empty rows, then search the
            struct vector sub-field with a plain query vector and a FLAT index
        expected: query output matches source, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_flat")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {
                "id": 0,
                "normal_vector": self._vector(0),
                "doc_tag": "present_profile_0",
                "profile": [
                    {"p_int": 0, "p_tag": "profile_0_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 1, "p_tag": "profile_0_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in rows}
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert search_results[0][0]["id"] == rows[-1]["id"]
        assert search_results[0][0]["offset"] == 0
        entity = self._search_entity(search_results[0][0])
        assert entity["doc_tag"] == rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows",
        strict=True,
    )
    def test_create_struct_array_field_with_vector_element_search_missing_prefix(self):
        """
        target: test element-level search on a nullable struct array vector sub-field with a missing struct prefix row
        method: create a nullable struct array field with a vector sub-field, insert one row omitting the struct field
            followed by non-empty rows, then search the struct vector sub-field with a plain query vector
        expected: missing row is skipped, and the exact matching struct vector is returned as top1 with the correct row
            id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_missing_prefix")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_missing_empty_prefix(self):
        """
        target: test element-level search on a nullable struct array vector sub-field with missing and empty prefix rows
        method: create a nullable struct array field with a vector sub-field, insert one row omitting the struct field,
            one empty struct array row, and non-empty rows, then search the struct vector sub-field with a plain query
            vector
        expected: missing/empty rows are skipped, and the exact matching struct vector is returned as top1 with the
            correct row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_missing_empty_prefix")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_empty_prefix(self):
        """
        target: test element-level search on a nullable struct array vector sub-field with only an empty prefix row
        method: create a nullable struct array field with a vector sub-field, insert one empty struct array row before
            non-empty rows, then search the struct vector sub-field with a plain query vector
        expected: the empty row is skipped, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_empty_prefix")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        empty_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {empty_profile_row["id"]: empty_profile_row}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_empty_prefix_flat(self):
        """
        target: test FLAT element-level search on a nullable struct array vector sub-field with only an empty prefix row
        method: create a nullable struct array field with a vector sub-field, insert one empty struct array row before
            non-empty rows, then search the struct vector sub-field with a plain query vector and a FLAT index
        expected: the empty row is skipped, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_empty_prefix_flat")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        empty_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {empty_profile_row["id"]: empty_profile_row}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows",
        strict=True,
    )
    def test_create_struct_array_field_with_vector_element_search_missing_prefix_flat(self):
        """
        target: test FLAT element-level search on a nullable struct array vector sub-field with a missing prefix row
        method: create a nullable struct array field with a vector sub-field, insert one row omitting the struct field
            followed by non-empty rows, then search the struct vector sub-field with a plain query vector and a FLAT
            index
        expected: missing row is skipped, and the exact matching struct vector is returned as top1 with the correct row
            id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_missing_prefix_flat")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_by_alias_query_by_name_and_alias(self):
        """
        target: test dynamically adding a struct array field through collection alias
        method: create an alias for a collection, add a scalar struct array field by alias, then insert/query through
            both collection name and alias
        expected: alias resolves to the real collection for schema evolution, and both describe/query paths see the
            same nullable struct array field and data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_alias")
        alias = cf.gen_unique_str(f"{prefix}_alias")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_alias(client, collection_name, alias)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(client, alias, "profile", profile_schema, max_capacity=4)
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        for target in (collection_name, alias):
            describe_info, check = self.describe_collection(client, target)
            assert check
            profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
            assert profile_field["nullable"] is True
            assert profile_field["type"] == DataType.ARRAY
            assert profile_field["element_type"] == DataType.STRUCT
            assert {field["name"] for field in profile_field["struct_fields"]} == {"p_int", "p_tag"}

        row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "inserted_by_alias",
            "profile": self._scalar_profile(1),
        }
        res, check = self.insert(client, alias, [row])
        assert check
        assert res["insert_count"] == 1

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        for target in (collection_name, alias):
            query_results, check = self.query(
                client,
                target,
                filter="id == 1",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert len(query_results) == 1
            assert query_results[0]["id"] == row["id"]
            assert query_results[0]["doc_tag"] == row["doc_tag"]
            self._assert_scalar_profile_equal(query_results[0]["profile"], row["profile"])

        res, check = self.drop_alias(client, alias)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_non_nullable_rejected(self):
        """
        target: test non-nullable struct array validation when dynamically adding a struct array field
        method: try to add a struct array field with nullable=False through MilvusClient
        expected: PyMilvus rejects the request before RPC, and collection schema remains unchanged
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_non_nullable")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        error = {
            ct.err_code: 1,
            ct.err_msg: "Adding struct field to existing collection requires nullable=True",
        }
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
            nullable=False,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert "profile" not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_duplicate_name_rejected(self):
        """
        target: test duplicate field name validation when dynamically adding a struct array field
        method: add a struct array field, then add another struct array field with the same name and with an
            existing regular field name
        expected: duplicate names are rejected, and collection schema remains unchanged
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_dup_name")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        duplicate_profile_schema = client.create_struct_field_schema()
        duplicate_profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name profile"}
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            duplicate_profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        conflict_regular_field_schema = client.create_struct_field_schema()
        conflict_regular_field_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name normal_vector"}
        self.add_collection_struct_field(
            client,
            collection_name,
            "normal_vector",
            conflict_regular_field_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert [field["name"] for field in describe_info["fields"]].count("profile") == 1
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert {field["name"] for field in profile_field["struct_fields"]} == {"p_int"}

    @pytest.mark.tags(CaseLabel.L1)
    def test_append_subfield_to_existing_struct_array_rejected(self):
        """
        target: test appending a sub-field to an existing struct array field is not supported
        method: add a struct array field with one sub-field, then call AddCollectionStructField again with the same
            parent name and a different sub-field schema
        expected: request is rejected as a duplicate parent field, and the original struct schema is unchanged
        """
        collection_name = cf.gen_unique_str(f"{prefix}_append_struct_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        res, check = self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
        )
        assert check

        append_subfield_schema = client.create_struct_field_schema()
        append_subfield_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name profile"}
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            append_subfield_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert {field["name"] for field in profile_field["struct_fields"]} == {"p_int"}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_duplicate_subfield_name_rejected(self):
        """
        target: test duplicate sub-field name validation when dynamically adding a struct array field
        method: add a struct array field whose struct schema contains duplicate sub-field names
        expected: duplicate sub-field names are rejected, and no struct field is added to the collection
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_dup_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_int", DataType.VARCHAR, max_length=128)
        error = {
            ct.err_code: 1,
            ct.err_msg: "Duplicate field names in struct 'profile'",
        }
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert "profile" not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "field_kwargs, expected_msg",
        [
            ({"is_primary": True}, "cannot be primary key"),
            ({"is_partition_key": True}, "cannot be partition key"),
            ({"is_clustering_key": True}, "cannot be clustering key"),
            ({"is_dynamic": True}, "cannot be dynamic field"),
            ({"nullable": True}, "cannot be nullable individually"),
            ({"default_value": 7}, "cannot have default value"),
        ],
    )
    def test_add_struct_array_field_invalid_subfield_properties_rejected(self, field_kwargs, expected_msg):
        """
        target: test invalid sub-field property validation when dynamically adding a struct array field
        method: add a struct array field whose sub-field is primary key, partition key, clustering key, dynamic,
            individually nullable, or has default value
        expected: invalid sub-field properties are rejected, and no struct field is added to the collection
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_bad_subfield_attr")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64, **field_kwargs)
        error = {ct.err_code: 1, ct.err_msg: expected_msg}
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert "profile" not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "parent_name, subfield_name, expected_msg",
        [
            ("RowID", "p_int", "not support to add system field, field name = RowID"),
            ("Timestamp", "p_int", "not support to add system field, field name = Timestamp"),
            ("profile", "RowID", "not support to add system field, field name = RowID"),
            (
                "profile",
                "__virtual_pk__",
                "not support to add system field, field name = __virtual_pk__",
            ),
        ],
    )
    def test_add_struct_array_field_reserved_names_rejected(self, parent_name, subfield_name, expected_msg):
        """
        target: test reserved parent and sub-field name validation when dynamically adding a struct array field
        method: add a struct array field whose parent or sub-field name is a system field name
        expected: reserved names are rejected, and no struct field is added to the collection
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_reserved_name")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field(subfield_name, DataType.INT64)
        error = {ct.err_code: 1100, ct.err_msg: expected_msg}
        self.add_collection_struct_field(
            client,
            collection_name,
            parent_name,
            profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert parent_name not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_scalar_struct_array_field_concurrent_dml_query_search(self):
        """
        target: test insert/delete/upsert traffic while dynamically adding a nullable scalar struct array field
        method: load a collection with indexed sealed rows and growing rows, run AddCollectionStructField concurrently
            with old-schema insert, delete, and upsert requests, then insert post-add struct rows
        expected: concurrent DML requests succeed without partial schema visibility, deleted rows are absent, and old
            schema rows expose the added struct field as null while post-add rows preserve their struct values
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_concurrent_dml")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(4)]
        old_sealed_filler_rows = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(4, 8)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        concurrent_insert_rows = [
            {
                "id": 1000 + batch * 10 + i,
                "normal_vector": self._vector(1000 + batch * 10 + i),
                "doc_tag": f"concurrent_insert_{batch}_{i}",
            }
            for batch in range(3)
            for i in range(3)
        ]
        concurrent_upsert_rows = [
            {
                "id": old_sealed_rows[0]["id"],
                "normal_vector": self._vector(5000),
                "doc_tag": "concurrent_upsert_old_sealed",
            },
            {
                "id": old_sealed_rows[2]["id"],
                "normal_vector": self._vector(5002),
                "doc_tag": "concurrent_upsert_old_sealed_second",
            },
            {
                "id": old_growing_rows[0]["id"],
                "normal_vector": self._vector(5004),
                "doc_tag": "concurrent_upsert_old_growing",
            },
            {
                "id": old_growing_rows[2]["id"],
                "normal_vector": self._vector(5006),
                "doc_tag": "concurrent_upsert_old_growing_second",
            },
            {
                "id": 1100,
                "normal_vector": self._vector(5100),
                "doc_tag": "concurrent_upsert_new_row",
            },
        ]
        delete_ids = [old_sealed_rows[1]["id"], old_growing_rows[1]["id"]]

        start_barrier = threading.Barrier(4)

        def add_struct_field_task():
            task_client = self._client()
            profile_schema = task_client.create_struct_field_schema()
            profile_schema.add_field("p_int", DataType.INT64)
            profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
            start_barrier.wait()
            result, task_check = self.add_collection_struct_field(
                task_client,
                collection_name,
                "profile",
                profile_schema,
                max_capacity=4,
            )
            assert task_check
            return "add", result

        def insert_task():
            task_client = self._client()
            start_barrier.wait()
            inserted = 0
            for batch in range(3):
                batch_rows = concurrent_insert_rows[batch * 3 : (batch + 1) * 3]
                result, task_check = self.insert(task_client, collection_name, batch_rows)
                assert task_check
                inserted += result["insert_count"]
                time.sleep(0.01)
            return "insert", inserted

        def upsert_task():
            task_client = self._client()
            start_barrier.wait()
            result, task_check = self.upsert(task_client, collection_name, concurrent_upsert_rows)
            assert task_check
            return "upsert", result["upsert_count"]

        def delete_task():
            task_client = self._client()
            start_barrier.wait()
            time.sleep(0.01)
            result, task_check = self.delete(task_client, collection_name, filter=f"id in {delete_ids}")
            assert task_check
            return "delete", result["delete_count"]

        task_results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(add_struct_field_task),
                executor.submit(insert_task),
                executor.submit(upsert_task),
                executor.submit(delete_task),
            ]
            for future in as_completed(futures):
                task_name, task_result = future.result()
                task_results[task_name] = task_result

        assert task_results["insert"] == len(concurrent_insert_rows)
        assert task_results["upsert"] == len(concurrent_upsert_rows)
        assert task_results["delete"] == len(delete_ids)

        self.wait_schema_version_consistent(client, collection_name)

        post_add_rows = [
            {
                "id": 1200,
                "normal_vector": self._vector(1200),
                "doc_tag": "post_add_explicit_null_profile",
                "profile": None,
            },
            {
                "id": 1201,
                "normal_vector": self._vector(1201),
                "doc_tag": "post_add_empty_profile",
                "profile": [],
            },
            {
                "id": 1202,
                "normal_vector": self._vector(1202),
                "doc_tag": "post_add_non_empty_profile",
                "profile": self._scalar_profile(1202),
            },
        ]
        res, check = self.insert(client, collection_name, post_add_rows)
        assert check
        assert res["insert_count"] == len(post_add_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        for deleted_id in delete_ids:
            source_by_id.pop(deleted_id)
        source_by_id.update({row["id"]: {**row, "profile": None} for row in concurrent_insert_rows})
        source_by_id.update({row["id"]: {**row, "profile": None} for row in concurrent_upsert_rows})
        source_by_id[post_add_rows[0]["id"]] = post_add_rows[0]
        source_by_id[post_add_rows[1]["id"]] = post_add_rows[1]
        source_by_id[post_add_rows[2]["id"]] = post_add_rows[2]

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 12020)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {post_add_rows[2]["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], post_add_rows[2]["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[post_add_rows[2]["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0]["id"] == post_add_rows[2]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_subfield_output_query_search(self):
        """
        target: test sub-field output after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then query/search with only
            struct sub-fields in output_fields
        expected: old rows expose the added struct as null, and new rows return sub-field data matching source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert set(entity) == {"id", "profile"}
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_query_search_after_reload(self):
        """
        target: test nullable output after reload for a dynamically added scalar-only struct array field
        method: add a scalar struct array field to a loaded collection with old rows, insert new rows, flush, then
            release and load the collection before query/search
        expected: old rows still expose the added struct as null, and new rows return inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_reload")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        res, check = self.flush(client, collection_name)
        assert check
        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 50)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in filter_results} == {5}
        for row in filter_results:
            self._assert_scalar_profile_equal(row["profile"], source_by_id[row["id"]]["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_query_iterator(self):
        """
        target: test query_iterator output after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with sealed and growing old rows, insert
            new rows, then drain query_iterator with small batch size
        expected: iterator returns every row once, old rows expose the added struct as null, and new rows match source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 8)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_search_iterator(self):
        """
        target: test search_iterator output after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with sealed and growing old rows, insert
            new rows, then drain search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, old rows expose the added struct as null, and new rows match source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 8)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(7)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_upsert_null_non_null(self):
        """
        target: test upsert after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection, upsert an old null row to non-null, and
            upsert a new non-null row while omitting the nullable struct field
        expected: full-row upsert updates null/non-null struct values correctly in query and search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_upsert")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        old_null_to_non_null = {
            "id": 0,
            "normal_vector": self._vector(100),
            "doc_tag": "upserted_old_null_to_non_null",
            "profile": self._scalar_profile(100),
        }
        new_non_null_to_null = {
            "id": 5,
            "normal_vector": self._vector(500),
            "doc_tag": "upserted_new_non_null_to_null",
        }
        res, check = self.upsert(client, collection_name, [old_null_to_non_null, new_non_null_to_null])
        assert check
        assert res["upsert_count"] == 2

        source_by_id[old_null_to_non_null["id"]] = old_null_to_non_null
        source_by_id[new_non_null_to_null["id"]] = {**new_non_null_to_null, "profile": None}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 1000)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {old_null_to_non_null["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], old_null_to_non_null["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(500)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == new_non_null_to_null["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_empty_array_query_search(self):
        """
        target: test null and empty array are distinguishable after dynamically adding a nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then insert one empty profile
            row and one non-empty profile row
        expected: old rows return null, empty row returns [], non-empty row returns inserted data, and element_filter
            only matches non-empty rows with matching elements
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_empty")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        empty_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "new_non_empty_profile",
            "profile": self._scalar_profile(6),
        }
        res, check = self.insert(client, collection_name, [empty_profile_row, non_empty_profile_row])
        assert check
        assert res["insert_count"] == 2

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id[non_empty_profile_row["id"]] = non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 60)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {non_empty_profile_row["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], non_empty_profile_row["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        filtered_search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="element_filter(profile, $[p_int] == 60)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {hit["id"] for hit in filtered_search_results[0]} == {non_empty_profile_row["id"]}
        entity = self._search_entity(filtered_search_results[0][0])
        self._assert_scalar_profile_equal(entity["profile"], non_empty_profile_row["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_on_added_subfield(self):
        """
        target: test struct array vector search after dynamically adding a nullable struct array field
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, insert
            new rows, index the added vector sub-field, then search on the added vector sub-field
        expected: old null rows are not retrieved, and new rows return inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_search")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows}
        new_ids = {row["id"] for row in new_rows}

        search_tensor = EmbeddingList()
        search_tensor.add(new_rows[-1]["profile"][0]["p_vec"])
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(new_rows),
        )
        assert check
        assert len(search_results[0]) == len(new_rows)
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        assert search_results[0][0]["id"] == new_rows[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_on_added_subfield_old_growing_rows(self):
        """
        target: test struct array vector search with old growing rows after dynamically adding a nullable struct
            array field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, index the added vector sub-field, then search on the added vector sub-field
        expected: old null rows are not retrieved, and new rows return inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_search_growing")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 5,
                "normal_vector": self._vector(5),
                "doc_tag": "new_5",
                "profile": [
                    {"p_int": 50, "p_tag": "profile_5_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 51, "p_tag": "profile_5_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row["id"] for row in new_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_tensor = EmbeddingList()
        search_tensor.add(new_rows[-1]["profile"][0]["p_vec"])
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(new_rows),
        )
        assert check
        assert len(search_results[0]) == len(new_rows)
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        assert search_results[0][0]["id"] == new_rows[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows",
        strict=True,
    )
    def test_add_struct_array_field_with_vector_element_search_no_old_rows(self):
        """
        target: test element-level search after dynamically adding a nullable struct array vector sub-field with no
            historical rows
        method: create an empty collection, add a nullable struct array field with a vector sub-field, insert only
            non-empty rows, then search the added sub-field with a plain query vector and a FLAT index
        expected: query output matches source, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_elem_no_old")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        rows = [
            {
                "id": 0,
                "normal_vector": self._vector(0),
                "doc_tag": "new_0",
                "profile": [
                    {"p_int": 0, "p_tag": "profile_0_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 1, "p_tag": "profile_0_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "new_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in rows}
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert search_results[0][0]["id"] == rows[-1]["id"]
        assert search_results[0][0]["offset"] == 0
        entity = self._search_entity(search_results[0][0])
        assert entity["doc_tag"] == rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows",
        strict=True,
    )
    def test_add_struct_array_field_with_vector_element_search_filter_new_rows(self):
        """
        target: test element-level search after dynamically adding a nullable struct array vector sub-field while
            filtering out historical null rows
        method: create a loaded collection with old sealed and growing rows, add a nullable struct array field with a
            vector sub-field, insert new non-empty rows, then search the added sub-field with a plain query vector and
            an ordinary filter that keeps only new rows
        expected: old null rows are excluded by the ordinary filter, and the exact matching struct vector is returned as
            top1 with the correct row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_elem_filter_new")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        old_growing_rows = [{"id": 2, "normal_vector": self._vector(2), "doc_tag": "growing_2"}]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_results, check = self.search(
            client,
            collection_name,
            data=[new_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            filter="id >= 3",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert search_results[0][0]["id"] == new_rows[-1]["id"]
        assert search_results[0][0]["offset"] == 0
        assert search_results[0][0]["id"] not in old_ids
        entity = self._search_entity(search_results[0][0])
        assert entity["doc_tag"] == new_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], new_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_element_search_old_null_rows(self):
        """
        target: test element-level search after dynamically adding a nullable struct array vector sub-field
        method: create a loaded collection with old sealed and growing rows, add a nullable struct array field with a
            vector sub-field, insert new non-empty rows, then search on the added sub-field with a plain query vector
        expected: old rows expose the added struct as null, search returns only new rows with vector elements, and output
            matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_elem_search")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        old_growing_rows = [{"id": 2, "normal_vector": self._vector(2), "doc_tag": "growing_2"}]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row["id"] for row in new_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_results, check = self.search(
            client,
            collection_name,
            data=[new_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(new_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == new_ids
        assert not set(hit_ids).intersection(old_ids)
        assert hits[0]["id"] == new_rows[-1]["id"]
        assert hits[0]["offset"] == 0

        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] >= distances[index + 1] - epsilon

        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_skip_null_empty_rows(self):
        """
        target: test null and empty struct rows are skipped by struct array vector search after dynamic add
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, then
            insert one empty struct row and two non-empty struct rows before searching on the added vector sub-field
        expected: old null rows and the empty struct row are not retrieved by profile[p_vec] search
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_empty_search")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        empty_profile_row = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 5,
                "normal_vector": self._vector(5),
                "doc_tag": "new_5",
                "profile": [
                    {"p_int": 50, "p_tag": "profile_5_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 51, "p_tag": "profile_5_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, [empty_profile_row, *non_empty_rows])
        assert check
        assert res["insert_count"] == 1 + len(non_empty_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1]["profile"][0]["p_vec"])
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        assert len(search_results[0]) == len(non_empty_rows)
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == {row["id"] for row in non_empty_rows}
        assert empty_profile_row["id"] not in hit_ids
        assert not hit_ids.intersection({row["id"] for row in old_sealed_rows})
        assert search_results[0][0]["id"] == non_empty_rows[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_subfield_output_query_search(self):
        """
        target: test vector sub-field output after dynamically adding a nullable struct array field
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, then
            query/search with only the vector sub-field in output_fields
        expected: old rows expose the added struct as null, and new rows return vector sub-field data matching source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(3, 5)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert set(entity) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_subfield_output_old_growing_rows(self):
        """
        target: test vector sub-field only output after dynamically adding a nullable struct array field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, then query/search with only the vector sub-field in output_fields
        expected: old rows expose the added struct as null, and new rows return only vector sub-field data matching
            source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_subfield_growing")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert set(entity) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_query_iterator(self):
        """
        target: test query_iterator output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, then drain query_iterator with small batch size
        expected: old rows expose the added struct as null, new rows return vector sub-field data matching source
            across iterator batch boundaries
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(4, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_iterator(self):
        """
        target: test search_iterator output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, then drain search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, old rows expose the added struct as null, and new rows return
            vector sub-field data matching source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(4, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(6)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_element_filter(self):
        """
        target: test struct array vector search with element_filter after dynamically adding a nullable struct array field
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, insert
            new rows, index the added vector sub-field, then search with element_filter on the added struct field
        expected: old null rows are not retrieved, and returned rows satisfy the element_filter
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows}

        search_results, check = self.search(
            client,
            collection_name,
            data=[new_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            filter="element_filter(profile, $[p_int] == 30)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == {3}
        assert not {hit["id"] for hit in search_results[0]}.intersection(old_ids)
        entity = self._search_entity(search_results[0][0])
        assert "profile" in entity
        self._assert_profile_equal(entity["profile"], source_by_id[3]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_query_search_old_new_rows(self):
        """
        target: test query/search output after dynamically adding a scalar-only nullable struct array field
        method: create loaded collection with sealed and growing rows, add a scalar struct array field,
            then insert new rows
        expected: old rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_element_filter_query_search(self):
        """
        target: test element filter correctness after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then query/search with
            element_filter on the added field
        expected: old null rows do not match element_filter, and output fields match inserted data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row["id"] for row in new_rows}
        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        int_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 40)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in int_filter_results} == {4}
        for row in int_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        string_filter_results, check = self.query(
            client,
            collection_name,
            filter='element_filter(profile, $[p_tag] == "profile_5_1")',
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in string_filter_results} == {5}
        for row in string_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="element_filter(profile, $[p_int] >= 40)",
            output_fields=["id", "profile"],
            limit=len(new_ids),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="known blocker: MATCH_ANY on dynamically added StructArray with old rows fails because MatchExpr expects ColumnVector",
        strict=True,
    )
    def test_add_scalar_struct_array_field_match_family_query_search(self):
        """
        target: test MATCH family correctness after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old sealed/growing rows, insert post-add
            sealed and growing null/empty/non-empty rows, then query with MATCH family and search with MATCH_ANY filter
        expected: old/null/empty rows do not match MATCH_ANY, MATCH family result sets match source-of-truth, and
            normal-vector search filtered by MATCH_ANY returns the expected sealed and growing rows with correct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_match")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [
            {"id": row_id, "normal_vector": self._vector(row_id), "doc_tag": f"old_sealed_{row_id}"}
            for row_id in range(self.min_index_sealed_rows)
        ]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": 3000, "normal_vector": self._vector(3000), "doc_tag": "old_growing_3000"},
            {"id": 3001, "normal_vector": self._vector(3001), "doc_tag": "old_growing_3001"},
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_explicit_null_profile_row = {
            "id": 4000,
            "normal_vector": self._vector(4000),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 4001,
            "normal_vector": self._vector(4001),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 4002,
            "normal_vector": self._vector(4002),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_one_match_profile_row = {
            "id": 4003,
            "normal_vector": self._vector(4003),
            "doc_tag": "sealed_one_match_profile",
            "profile": [
                {"p_int": 9100, "p_tag": "match_9100"},
                {"p_int": 100, "p_tag": "low_100"},
            ],
        }
        sealed_two_match_profile_row = {
            "id": 4004,
            "normal_vector": self._vector(4004),
            "doc_tag": "sealed_two_match_profile",
            "profile": [
                {"p_int": 9200, "p_tag": "match_9200"},
                {"p_int": 9300, "p_tag": "match_9300"},
            ],
        }
        sealed_zero_match_profile_row = {
            "id": 4005,
            "normal_vector": self._vector(4005),
            "doc_tag": "sealed_zero_match_profile",
            "profile": [
                {"p_int": 100, "p_tag": "low_100"},
                {"p_int": 200, "p_tag": "low_200"},
            ],
        }
        sealed_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_one_match_profile_row,
            sealed_two_match_profile_row,
            sealed_zero_match_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_match_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        growing_explicit_null_profile_row = {
            "id": 7000,
            "normal_vector": self._vector(7000),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_empty_profile_row = {
            "id": 7001,
            "normal_vector": self._vector(7001),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_two_match_profile_row = {
            "id": 7002,
            "normal_vector": self._vector(7002),
            "doc_tag": "growing_two_match_profile",
            "profile": [
                {"p_int": 9600, "p_tag": "match_9600"},
                {"p_int": 9700, "p_tag": "match_9700"},
            ],
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_empty_profile_row,
            growing_two_match_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row["id"]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row["id"]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row["id"]] = sealed_zero_match_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_two_match_profile_row["id"]] = growing_two_match_profile_row

        match_any_ids = {
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_two_or_more_ids = {
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_exact_one_ids = {sealed_one_match_profile_row["id"]}
        non_nullish_scope = [
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        ]

        match_any_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_any_ids),
        )
        assert check
        assert {row["id"] for row in match_any_results} == match_any_ids
        assert not {row["id"] for row in match_any_results}.intersection(
            {
                *[row["id"] for row in old_sealed_rows + old_growing_rows],
                sealed_explicit_null_profile_row["id"],
                sealed_omitted_profile_row["id"],
                sealed_empty_profile_row["id"],
                sealed_zero_match_profile_row["id"],
                growing_explicit_null_profile_row["id"],
                growing_empty_profile_row["id"],
            }
        )
        for row in match_any_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_least_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_LEAST(profile, $[p_int] >= 9000, threshold=2)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_two_or_more_ids),
        )
        assert check
        assert {row["id"] for row in match_least_results} == match_two_or_more_ids
        for row in match_least_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_exact_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_EXACT(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_exact_one_ids),
        )
        assert check
        assert {row["id"] for row in match_exact_results} == match_exact_one_ids
        for row in match_exact_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_all_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_ALL(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_two_or_more_ids),
        )
        assert check
        assert {row["id"] for row in match_all_results} == match_two_or_more_ids
        for row in match_all_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_most_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_MOST(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=2,
        )
        assert check
        assert {row["id"] for row in match_most_results} == {
            sealed_one_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
        }
        for row in match_most_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_two_match_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_any_ids),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == match_any_ids
        assert search_results[0][0]["id"] == growing_two_match_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_insert_omit_empty_non_empty_rows(self):
        """
        target: test insert omitted, empty, and non-empty rows after dynamically adding a scalar-only struct array
            field
        method: add a scalar struct array field to a loaded collection with old rows, then insert one row omitting
            the field, one empty row, and one non-empty row
        expected: old and omitted rows return null, empty row returns [], and non-empty row matches source in
            query, sub-field query, element_filter query, and normal vector search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_omit_empty")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        omitted_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "new_omit_profile",
        }
        empty_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "new_non_empty_profile",
            "profile": self._scalar_profile(6),
        }
        res, check = self.insert(
            client, collection_name, [omitted_profile_row, empty_profile_row, non_empty_profile_row]
        )
        assert check
        assert res["insert_count"] == 3

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[omitted_profile_row["id"]] = {**omitted_profile_row, "profile": None}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id[non_empty_profile_row["id"]] = non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 60)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {non_empty_profile_row["id"]}
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_insert_explicit_null_rows(self):
        """
        target: test explicit null insert after dynamically adding a scalar-only struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then insert rows with
            explicit null, omitted, empty, and non-empty struct values
        expected: old, explicit null, and omitted rows return null, empty row returns [], and non-empty row matches
            source in query, sub-field query, element_filter query, and normal vector search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_explicit_null")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "new_explicit_null_profile",
            "profile": None,
        }
        omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "new_omit_profile",
        }
        empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "new_non_empty_profile",
            "profile": self._scalar_profile(7),
        }
        res, check = self.insert(
            client,
            collection_name,
            [explicit_null_profile_row, omitted_profile_row, empty_profile_row, non_empty_profile_row],
        )
        assert check
        assert res["insert_count"] == 4

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[explicit_null_profile_row["id"]] = explicit_null_profile_row
        source_by_id[omitted_profile_row["id"]] = {**omitted_profile_row, "profile": None}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id[non_empty_profile_row["id"]] = non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 70)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {non_empty_profile_row["id"]}
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == non_empty_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_delete_null_empty_non_empty_rows(self):
        """
        target: test delete after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, insert explicit null, omitted,
            empty, and non-empty rows, then delete rows across those states
        expected: deleted rows are absent from query/search, and remaining rows keep correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_delete")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_non_empty_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_non_empty_profile",
            "profile": self._scalar_profile(7),
        }
        sealed_deleted_non_empty_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_deleted_non_empty_profile",
            "profile": self._scalar_profile(8),
        }
        sealed_inserted_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_non_empty_profile_row,
            sealed_deleted_non_empty_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_inserted_rows),
            "sealed_index_filler",
        )
        sealed_inserted_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_inserted_rows)
        assert check
        assert res["insert_count"] == len(sealed_inserted_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_non_empty_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_non_empty_profile",
            "profile": self._scalar_profile(12),
        }
        growing_deleted_non_empty_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_deleted_non_empty_profile",
            "profile": self._scalar_profile(13),
        }
        growing_inserted_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_non_empty_profile_row,
            growing_deleted_non_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_inserted_rows)
        assert check
        assert res["insert_count"] == len(growing_inserted_rows)

        delete_ids = [
            old_sealed_rows[1]["id"],
            old_growing_rows[1]["id"],
            sealed_explicit_null_profile_row["id"],
            sealed_empty_profile_row["id"],
            sealed_deleted_non_empty_profile_row["id"],
            growing_explicit_null_profile_row["id"],
            growing_empty_profile_row["id"],
            growing_deleted_non_empty_profile_row["id"],
        ]
        res, check = self.delete(client, collection_name, filter=f"id in {delete_ids}")
        assert check
        assert res["delete_count"] == len(delete_ids)

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.pop(old_sealed_rows[1]["id"])
        source_by_id.pop(old_growing_rows[1]["id"])
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_non_empty_profile_row["id"]] = sealed_non_empty_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_non_empty_profile_row["id"]] = growing_non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] >= 70)",
            output_fields=["id", "doc_tag", "profile"],
            limit=4,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {
            sealed_non_empty_profile_row["id"],
            growing_non_empty_profile_row["id"],
        }
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_non_empty_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0]["id"] == growing_non_empty_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_delete_null_empty_non_empty_rows(self):
        """
        target: test delete after dynamically adding a nullable struct array field with a vector sub-field
        method: add a struct array field with scalar and vector sub-fields to a loaded collection with old rows, insert
            explicit null, omitted, empty, and non-empty rows, then delete rows across those states
        expected: deleted rows are absent from query/search, and remaining rows keep correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_delete")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_non_empty_profile",
            "profile": self._profile(7),
        }
        sealed_deleted_non_empty_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_deleted_non_empty_profile",
            "profile": self._profile(8),
        }
        sealed_non_empty_rows = [sealed_non_empty_profile_row, sealed_deleted_non_empty_profile_row]
        sealed_index_filler_rows = self._vector_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_rows),
            "sealed_index_filler",
        )
        sealed_non_empty_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_non_empty_rows)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_rows)

        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_nullish_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, sealed_nullish_rows)
        assert check
        assert res["insert_count"] == len(sealed_nullish_rows)

        res, check = self.flush(client, collection_name)
        assert check

        growing_non_empty_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_non_empty_profile",
            "profile": self._profile(12),
        }
        growing_deleted_non_empty_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_deleted_non_empty_profile",
            "profile": self._profile(13),
        }
        growing_non_empty_rows = [growing_non_empty_profile_row, growing_deleted_non_empty_profile_row]
        res, check = self.insert(client, collection_name, growing_non_empty_rows)
        assert check
        assert res["insert_count"] == len(growing_non_empty_rows)

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_nullish_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_nullish_rows)
        assert check
        assert res["insert_count"] == len(growing_nullish_rows)

        delete_ids = [
            old_sealed_rows[1]["id"],
            old_growing_rows[1]["id"],
            sealed_explicit_null_profile_row["id"],
            sealed_empty_profile_row["id"],
            sealed_deleted_non_empty_profile_row["id"],
            growing_explicit_null_profile_row["id"],
            growing_empty_profile_row["id"],
            growing_deleted_non_empty_profile_row["id"],
        ]
        res, check = self.delete(client, collection_name, filter=f"id in {delete_ids}")
        assert check
        assert res["delete_count"] == len(delete_ids)

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.pop(old_sealed_rows[1]["id"])
        source_by_id.pop(old_growing_rows[1]["id"])
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_non_empty_profile_row["id"]] = sealed_non_empty_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_non_empty_profile_row["id"]] = growing_non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        assert not {row["id"] for row in subfield_results}.intersection(delete_ids)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] >= 70)",
            output_fields=["id", "doc_tag", "profile"],
            limit=4,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {
            sealed_non_empty_profile_row["id"],
            growing_non_empty_profile_row["id"],
        }
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_non_empty_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0]["id"] == growing_non_empty_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_upsert_null_empty_non_empty_rows(self):
        """
        target: test upsert after dynamically adding a nullable struct array field with a vector sub-field
        method: add a struct array field with scalar and vector sub-fields to a loaded collection with old rows, then
            upsert old null rows and post-add non-empty rows across sealed and growing segments
        expected: upserted rows expose correct null, empty, and non-empty struct output in query/search
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_upsert_states")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        post_add_sealed_rows = [
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "post_add_sealed_non_empty_to_null",
                "profile": self._profile(4),
            },
            {
                "id": 5,
                "normal_vector": self._vector(5),
                "doc_tag": "post_add_sealed_non_empty_to_empty",
                "profile": self._profile(5),
            },
        ]
        res, check = self.insert(client, collection_name, post_add_sealed_rows)
        assert check
        assert res["insert_count"] == len(post_add_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        post_add_growing_rows = [
            {
                "id": 6,
                "normal_vector": self._vector(6),
                "doc_tag": "post_add_growing_non_empty_to_null",
                "profile": self._profile(6),
            },
            {
                "id": 7,
                "normal_vector": self._vector(7),
                "doc_tag": "post_add_growing_non_empty_to_empty",
                "profile": self._profile(7),
            },
        ]
        res, check = self.insert(client, collection_name, post_add_growing_rows)
        assert check
        assert res["insert_count"] == len(post_add_growing_rows)

        non_empty_upserts = [
            {
                "id": old_sealed_rows[0]["id"],
                "normal_vector": self._vector(100),
                "doc_tag": "upsert_old_sealed_null_to_non_empty",
                "profile": self._profile(100),
            },
            {
                "id": old_growing_rows[0]["id"],
                "normal_vector": self._vector(200),
                "doc_tag": "upsert_old_growing_null_to_non_empty",
                "profile": self._profile(200),
            },
            {
                "id": 8,
                "normal_vector": self._vector(800),
                "doc_tag": "upsert_new_non_empty",
                "profile": self._profile(800),
            },
        ]
        res, check = self.upsert(client, collection_name, non_empty_upserts)
        assert check
        assert res["upsert_count"] == len(non_empty_upserts)

        nullish_upserts = [
            {
                "id": post_add_sealed_rows[0]["id"],
                "normal_vector": self._vector(400),
                "doc_tag": "upsert_post_add_sealed_non_empty_to_null",
            },
            {
                "id": post_add_sealed_rows[1]["id"],
                "normal_vector": self._vector(500),
                "doc_tag": "upsert_post_add_sealed_non_empty_to_empty",
                "profile": [],
            },
            {
                "id": post_add_growing_rows[0]["id"],
                "normal_vector": self._vector(600),
                "doc_tag": "upsert_post_add_growing_non_empty_to_null",
                "profile": None,
            },
            {
                "id": post_add_growing_rows[1]["id"],
                "normal_vector": self._vector(700),
                "doc_tag": "upsert_post_add_growing_non_empty_to_empty",
                "profile": [],
            },
            {
                "id": 9,
                "normal_vector": self._vector(900),
                "doc_tag": "upsert_new_omitted_profile",
            },
        ]
        res, check = self.upsert(client, collection_name, nullish_upserts)
        assert check
        assert res["upsert_count"] == len(nullish_upserts)

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in non_empty_upserts})
        source_by_id[nullish_upserts[0]["id"]] = {**nullish_upserts[0], "profile": None}
        source_by_id[nullish_upserts[1]["id"]] = nullish_upserts[1]
        source_by_id[nullish_upserts[2]["id"]] = nullish_upserts[2]
        source_by_id[nullish_upserts[3]["id"]] = nullish_upserts[3]
        source_by_id[nullish_upserts[4]["id"]] = {**nullish_upserts[4], "profile": None}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        for expected_row in non_empty_upserts:
            element_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {expected_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert {row["id"] for row in element_filter_results} == {expected_row["id"]}
            row = element_filter_results[0]
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_upserts[-1]["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == non_empty_upserts[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_delete_by_element_filter_sealed_growing_rows(self):
        """
        target: test deleting dynamically added scalar struct array rows by element_filter
        method: add a scalar struct array field to a loaded collection with old rows, insert post-add sealed and
            growing null/empty/non-empty rows, then delete one sealed and one growing row with one element_filter
        expected: only rows whose struct elements match the filter are deleted, and null/empty rows are not matched
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_delete_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        deleted_profile = [
            {"p_int": 7000, "p_tag": "deleted_shared_0"},
            {"p_int": 7001, "p_tag": "deleted_shared_1"},
        ]
        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_deleted_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        sealed_kept_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_kept_after_element_filter",
            "profile": self._scalar_profile(8),
        }
        sealed_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_deleted_profile_row,
            sealed_kept_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_deleted_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        growing_kept_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_kept_after_element_filter",
            "profile": self._scalar_profile(13),
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_deleted_profile_row,
            growing_kept_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        delete_filter = f"element_filter(profile, $[p_int] == {deleted_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_kept_profile_row["id"]] = sealed_kept_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_kept_profile_row["id"]] = growing_kept_profile_row
        deleted_ids = {sealed_deleted_profile_row["id"], growing_deleted_profile_row["id"]}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(deleted_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        deleted_filter_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert deleted_filter_results == []

        for kept_row in (sealed_kept_profile_row, growing_kept_profile_row):
            kept_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {kept_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert {row["id"] for row in kept_filter_results} == {kept_row["id"]}
            row = kept_filter_results[0]
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_kept_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
        assert search_results[0][0]["id"] == growing_kept_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_delete_by_element_filter_sealed_growing_rows(self):
        """
        target: test deleting dynamically added struct array rows with vector sub-field by scalar element_filter
        method: add a struct array field with scalar and vector sub-fields to a loaded collection with old rows, insert
            post-add sealed and growing null/empty/non-empty rows, then delete one sealed and one growing row with one
            scalar element_filter
        expected: only rows whose struct elements match the scalar filter are deleted, and remaining vector payloads
            are still returned correctly
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_delete_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        deleted_profile = [
            {"p_int": 7000, "p_tag": "deleted_shared_0", "p_vec": self._vector(7000)},
            {"p_int": 7001, "p_tag": "deleted_shared_1", "p_vec": self._vector(7001)},
        ]
        sealed_deleted_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        sealed_kept_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_kept_after_element_filter",
            "profile": self._profile(8),
        }
        sealed_non_empty_rows = [sealed_deleted_profile_row, sealed_kept_profile_row]
        sealed_index_filler_rows = self._vector_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_rows),
            "sealed_index_filler",
        )
        sealed_non_empty_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_non_empty_rows)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_rows)

        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_nullish_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, sealed_nullish_rows)
        assert check
        assert res["insert_count"] == len(sealed_nullish_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_deleted_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        growing_kept_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_kept_after_element_filter",
            "profile": self._profile(13),
        }
        growing_non_empty_rows = [growing_deleted_profile_row, growing_kept_profile_row]
        res, check = self.insert(client, collection_name, growing_non_empty_rows)
        assert check
        assert res["insert_count"] == len(growing_non_empty_rows)

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_nullish_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_nullish_rows)
        assert check
        assert res["insert_count"] == len(growing_nullish_rows)

        delete_filter = f"element_filter(profile, $[p_int] == {deleted_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_kept_profile_row["id"]] = sealed_kept_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_kept_profile_row["id"]] = growing_kept_profile_row
        deleted_ids = {sealed_deleted_profile_row["id"], growing_deleted_profile_row["id"]}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(deleted_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        assert not {row["id"] for row in subfield_results}.intersection(deleted_ids)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        deleted_filter_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert deleted_filter_results == []

        for kept_row in (sealed_kept_profile_row, growing_kept_profile_row):
            kept_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {kept_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert {row["id"] for row in kept_filter_results} == {kept_row["id"]}
            row = kept_filter_results[0]
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_kept_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
        assert search_results[0][0]["id"] == growing_kept_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_partition_query_search_sealed_growing_rows(self):
        """
        target: test partition-scoped query/search after dynamically adding a scalar nullable struct array field
        method: create two partitions with old sealed and growing rows, add a scalar struct array field, insert
            post-add sealed and growing rows in both partitions, then query/search with partition_names
        expected: partition-scoped query/search only returns rows from selected partitions, with correct nullable output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_non_empty",
            "profile": self._scalar_profile(2),
        }
        sealed_null_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_non_empty_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_non_empty",
            "profile": self._scalar_profile(102),
        }
        sealed_omit_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_omit",
        }
        res, check = self.insert(
            client, collection_name, [sealed_non_empty_a, sealed_null_a], partition_name=partition_a
        )
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(
            client, collection_name, [sealed_non_empty_b, sealed_omit_b], partition_name=partition_b
        )
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        growing_empty_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_non_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_growing_non_empty",
            "profile": self._scalar_profile(5),
        }
        growing_null_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_growing_null",
            "profile": None,
        }
        growing_non_empty_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_non_empty",
            "profile": self._scalar_profile(105),
        }
        res, check = self.insert(
            client, collection_name, [growing_empty_a, growing_non_empty_a], partition_name=partition_a
        )
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(
            client, collection_name, [growing_null_b, growing_non_empty_b], partition_name=partition_b
        )
        assert check
        assert res["insert_count"] == 2

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a["id"]: sealed_non_empty_a,
                sealed_null_a["id"]: sealed_null_a,
                growing_empty_a["id"]: growing_empty_a,
                growing_non_empty_a["id"]: growing_non_empty_a,
            }
        )
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b["id"]: sealed_non_empty_b,
                sealed_omit_b["id"]: {**sealed_omit_b, "profile": None},
                growing_null_b["id"]: growing_null_b,
                growing_non_empty_b["id"]: growing_non_empty_b,
            }
        )

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert {row["id"] for row in query_results} == {sealed_non_empty_a["id"]}

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=1,
        )
        assert check
        assert query_results == []

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id"],
            partition_names=[partition_a, partition_b],
            limit=len(source_a) + len(source_b),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_a) | set(source_b)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_partition_query_search_sealed_growing_rows(self):
        """
        target: test partition-scoped query/search after dynamically adding a nullable struct array field with vector
            sub-field
        method: create two partitions with old sealed and growing rows, add a struct array field with scalar and vector
            sub-fields, insert post-add sealed and growing rows in both partitions, then query/search with
            partition_names
        expected: partition-scoped query/search only returns rows from selected partitions, with correct nullable and
            vector sub-field output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_non_empty",
            "profile": self._profile(2),
        }
        sealed_non_empty_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_non_empty",
            "profile": self._profile(102),
        }
        res, check = self.insert(client, collection_name, [sealed_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [sealed_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        sealed_null_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_empty_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        sealed_null_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        res, check = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [sealed_empty_b, sealed_null_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        growing_non_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_growing_non_empty",
            "profile": self._profile(5),
        }
        growing_non_empty_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_non_empty",
            "profile": self._profile(105),
        }
        res, check = self.insert(client, collection_name, [growing_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        growing_empty_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a["id"]: sealed_non_empty_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_non_empty_a["id"]: growing_non_empty_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b["id"]: sealed_non_empty_b,
                sealed_empty_b["id"]: sealed_empty_b,
                sealed_null_b["id"]: sealed_null_b,
                growing_non_empty_b["id"]: growing_non_empty_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            subfield_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "profile[p_vec]"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in subfield_results} == set(source_by_id)
            for row in subfield_results:
                expected = source_by_id[row["id"]]
                assert set(row) == {"id", "profile"}
                self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert {row["id"] for row in query_results} == {sealed_non_empty_a["id"]}
        self._assert_profile_equal(query_results[0]["profile"], sealed_non_empty_a["profile"])

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=1,
        )
        assert check
        assert query_results == []

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id"],
            partition_names=[partition_a, partition_b],
            limit=len(source_a) + len(source_b),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_a) | set(source_b)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_load_partitions_after_release(self):
        """
        target: test load_partitions after dynamically adding a scalar nullable struct array field
        method: create two partitions with old sealed and growing rows, add a scalar struct array field, insert
            post-add rows, release the collection, then load and verify one partition at a time
        expected: partition load returns only rows from the loaded partition, with correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_load_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        rows_a = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "a_post_add_non_empty",
                "profile": self._scalar_profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "a_post_add_null",
                "profile": None,
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "a_post_add_empty",
                "profile": [],
            },
        ]
        rows_b = [
            {
                "id": 102,
                "normal_vector": self._vector(102),
                "doc_tag": "b_post_add_non_empty",
                "profile": self._scalar_profile(102),
            },
            {
                "id": 103,
                "normal_vector": self._vector(103),
                "doc_tag": "b_post_add_null",
                "profile": None,
            },
            {
                "id": 104,
                "normal_vector": self._vector(104),
                "doc_tag": "b_post_add_empty",
                "profile": [],
            },
        ]
        res, check = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(rows_a)
        res, check = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(rows_b)

        res, check = self.flush(client, collection_name)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row["id"]: row for row in rows_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row["id"]: row for row in rows_b})

        res, check = self.release_collection(client, collection_name)
        assert check

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            res, check = self.load_partitions(client, collection_name, [partition_name])
            assert check

            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

            element_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {search_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=1,
            )
            assert check
            assert {row["id"] for row in element_filter_results} == {search_row["id"]}
            self._assert_scalar_profile_equal(element_filter_results[0]["profile"], search_row["profile"])

            res, check = self.release_partitions(client, collection_name, [partition_name])
            assert check

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_load_partitions_after_release(self):
        """
        target: test load_partitions after dynamically adding a nullable struct array field with vector sub-field
        method: create two partitions with old sealed and growing rows, add a struct array field with scalar and vector
            sub-fields, insert post-add rows, release the collection, then load and verify one partition at a time
        expected: partition load returns only rows from the loaded partition, with correct nullable and vector output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_load_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_non_empty",
            "profile": self._profile(2),
        }
        sealed_non_empty_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_non_empty",
            "profile": self._profile(102),
        }
        res, check = self.insert(client, collection_name, [sealed_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [sealed_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        sealed_null_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_null_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        sealed_empty_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [sealed_null_b, sealed_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        growing_non_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_growing_non_empty",
            "profile": self._profile(5),
        }
        growing_non_empty_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_non_empty",
            "profile": self._profile(105),
        }
        res, check = self.insert(client, collection_name, [growing_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        growing_empty_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a["id"]: sealed_non_empty_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_non_empty_a["id"]: growing_non_empty_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b["id"]: sealed_non_empty_b,
                sealed_null_b["id"]: sealed_null_b,
                sealed_empty_b["id"]: sealed_empty_b,
                growing_non_empty_b["id"]: growing_non_empty_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )

        res, check = self.release_collection(client, collection_name)
        assert check

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            res, check = self.load_partitions(client, collection_name, [partition_name])
            assert check

            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            subfield_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "profile[p_vec]"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in subfield_results} == set(source_by_id)
            for row in subfield_results:
                expected = source_by_id[row["id"]]
                assert set(row) == {"id", "profile"}
                self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

            element_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {search_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=1,
            )
            assert check
            assert {row["id"] for row in element_filter_results} == {search_row["id"]}
            self._assert_profile_equal(element_filter_results[0]["profile"], search_row["profile"])

            res, check = self.release_partitions(client, collection_name, [partition_name])
            assert check

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_delete_by_element_filter_with_partition_name(self):
        """
        target: test partition-scoped delete by element_filter after dynamically adding a scalar struct array field
        method: create two partitions with old sealed and growing rows, add a scalar struct array field, insert
            post-add sealed and growing rows in both partitions, then delete matching rows only from one partition
        expected: delete removes matching sealed and growing rows from the target partition, without deleting rows in
            other partitions that have the same struct element values
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_delete_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        delete_profile = [
            {"p_int": 9000, "p_tag": "delete_shared_0"},
            {"p_int": 9001, "p_tag": "delete_shared_1"},
        ]
        sealed_kept_profile = [
            {"p_int": 9100, "p_tag": "kept_sealed_0"},
            {"p_int": 9101, "p_tag": "kept_sealed_1"},
        ]
        growing_kept_profile = [
            {"p_int": 9300, "p_tag": "kept_growing_0"},
            {"p_int": 9301, "p_tag": "kept_growing_1"},
        ]

        sealed_deleted_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        sealed_kept_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_kept",
            "profile": sealed_kept_profile,
        }
        sealed_null_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_same_filter_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_same_filter_kept",
            "profile": delete_profile,
        }
        sealed_null_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        sealed_empty_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        sealed_rows_a = [sealed_deleted_a, sealed_kept_a, sealed_null_a, sealed_empty_a]
        sealed_index_filler_a = self._scalar_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_rows_a),
            "a_sealed_index_filler",
        )
        sealed_rows_a += sealed_index_filler_a
        res, check = self.insert(client, collection_name, sealed_rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(sealed_rows_a)
        sealed_rows_b = [sealed_same_filter_b, sealed_null_b, sealed_empty_b]
        sealed_index_filler_b = self._scalar_struct_index_filler_rows(
            40000,
            self.min_index_sealed_rows - len(sealed_rows_b),
            "b_sealed_index_filler",
        )
        sealed_rows_b += sealed_index_filler_b
        res, check = self.insert(client, collection_name, sealed_rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(sealed_rows_b)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_deleted_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        growing_kept_a = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "a_growing_kept",
            "profile": growing_kept_profile,
        }
        growing_empty_a = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_same_filter_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_same_filter_kept",
            "profile": delete_profile,
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(
            client,
            collection_name,
            [growing_deleted_a, growing_kept_a, growing_empty_a],
            partition_name=partition_a,
        )
        assert check
        assert res["insert_count"] == 3
        res, check = self.insert(
            client,
            collection_name,
            [growing_same_filter_b, growing_empty_b],
            partition_name=partition_b,
        )
        assert check
        assert res["insert_count"] == 2

        delete_filter = f"element_filter(profile, $[p_int] == {delete_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter, partition_name=partition_a)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_kept_a["id"]: sealed_kept_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_kept_a["id"]: growing_kept_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_a.update({row["id"]: row for row in sealed_index_filler_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_same_filter_b["id"]: sealed_same_filter_b,
                sealed_null_b["id"]: sealed_null_b,
                sealed_empty_b["id"]: sealed_empty_b,
                growing_same_filter_b["id"]: growing_same_filter_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )
        source_b.update({row["id"]: row for row in sealed_index_filler_b})
        deleted_ids = {sealed_deleted_a["id"], growing_deleted_a["id"]}

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_kept_a),
            (partition_b, source_b, growing_same_filter_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            assert not {row["id"] for row in query_results}.intersection(deleted_ids)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        partition_a_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert partition_a_results == []

        partition_b_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=2,
        )
        assert check
        partition_b_source = {
            sealed_same_filter_b["id"]: sealed_same_filter_b,
            growing_same_filter_b["id"]: growing_same_filter_b,
        }
        assert {row["id"] for row in partition_b_results} == set(partition_b_source)
        for row in partition_b_results:
            self._assert_scalar_profile_equal(row["profile"], partition_b_source[row["id"]]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_delete_by_element_filter_with_partition_name(self):
        """
        target: test partition-scoped delete by scalar element_filter after dynamically adding a struct array field with
            vector sub-field
        method: create two partitions with old sealed and growing rows, add a struct array field with scalar and vector
            sub-fields, insert post-add sealed and growing rows in both partitions, then delete matching rows only from
            one partition by scalar element_filter
        expected: delete removes matching sealed and growing rows from the target partition, without deleting rows in
            other partitions that have the same scalar struct element values
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_delete_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        delete_profile = [
            {"p_int": 9000, "p_tag": "delete_shared_0", "p_vec": self._vector(9000)},
            {"p_int": 9001, "p_tag": "delete_shared_1", "p_vec": self._vector(9001)},
        ]
        sealed_kept_profile = [
            {"p_int": 9100, "p_tag": "kept_sealed_0", "p_vec": self._vector(9100)},
            {"p_int": 9101, "p_tag": "kept_sealed_1", "p_vec": self._vector(9101)},
        ]
        growing_kept_profile = [
            {"p_int": 9300, "p_tag": "kept_growing_0", "p_vec": self._vector(9300)},
            {"p_int": 9301, "p_tag": "kept_growing_1", "p_vec": self._vector(9301)},
        ]

        sealed_deleted_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        sealed_kept_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_kept",
            "profile": sealed_kept_profile,
        }
        sealed_same_filter_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_same_filter_kept",
            "profile": delete_profile,
        }
        sealed_non_empty_a = [sealed_deleted_a, sealed_kept_a]
        sealed_index_filler_a = self._vector_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_a),
            "a_sealed_index_filler",
        )
        sealed_non_empty_a += sealed_index_filler_a
        res, check = self.insert(client, collection_name, sealed_non_empty_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_a)
        sealed_non_empty_b = [sealed_same_filter_b]
        sealed_index_filler_b = self._vector_struct_index_filler_rows(
            40000,
            self.min_index_sealed_rows - len(sealed_non_empty_b),
            "b_sealed_index_filler",
        )
        sealed_non_empty_b += sealed_index_filler_b
        res, check = self.insert(client, collection_name, sealed_non_empty_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_b)

        sealed_null_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_null_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        sealed_empty_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [sealed_null_b, sealed_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_deleted_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        growing_kept_a = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "a_growing_kept",
            "profile": growing_kept_profile,
        }
        growing_same_filter_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_same_filter_kept",
            "profile": delete_profile,
        }
        res, check = self.insert(
            client,
            collection_name,
            [growing_deleted_a, growing_kept_a],
            partition_name=partition_a,
        )
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [growing_same_filter_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        growing_empty_a = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        delete_filter = f"element_filter(profile, $[p_int] == {delete_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter, partition_name=partition_a)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_kept_a["id"]: sealed_kept_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_kept_a["id"]: growing_kept_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_a.update({row["id"]: row for row in sealed_index_filler_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_same_filter_b["id"]: sealed_same_filter_b,
                sealed_null_b["id"]: sealed_null_b,
                sealed_empty_b["id"]: sealed_empty_b,
                growing_same_filter_b["id"]: growing_same_filter_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )
        source_b.update({row["id"]: row for row in sealed_index_filler_b})
        deleted_ids = {sealed_deleted_a["id"], growing_deleted_a["id"]}

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_kept_a),
            (partition_b, source_b, growing_same_filter_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            assert not {row["id"] for row in query_results}.intersection(deleted_ids)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            subfield_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "profile[p_vec]"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in subfield_results} == set(source_by_id)
            assert not {row["id"] for row in subfield_results}.intersection(deleted_ids)
            for row in subfield_results:
                expected = source_by_id[row["id"]]
                assert set(row) == {"id", "profile"}
                self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

        partition_a_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert partition_a_results == []

        partition_b_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=2,
        )
        assert check
        partition_b_source = {
            sealed_same_filter_b["id"]: sealed_same_filter_b,
            growing_same_filter_b["id"]: growing_same_filter_b,
        }
        assert {row["id"] for row in partition_b_results} == set(partition_b_source)
        for row in partition_b_results:
            self._assert_profile_equal(row["profile"], partition_b_source[row["id"]]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_partition_query_search_iterator(self):
        """
        target: test partition-scoped iterators after dynamically adding a scalar nullable struct array field
        method: create two partitions with indexed old sealed rows and old growing rows, add a scalar struct array
            field, insert post-add growing rows in both partitions, then drain query_iterator and search_iterator with
            partition_names
        expected: iterators only return rows from the requested partition, with correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_partition_iter")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        rows_a = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "a_non_empty",
                "profile": self._scalar_profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "a_null",
                "profile": None,
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "a_empty",
                "profile": [],
            },
        ]
        rows_b = [
            {
                "id": 102,
                "normal_vector": self._vector(102),
                "doc_tag": "b_non_empty",
                "profile": self._scalar_profile(102),
            },
            {
                "id": 103,
                "normal_vector": self._vector(103),
                "doc_tag": "b_null",
                "profile": None,
            },
            {
                "id": 104,
                "normal_vector": self._vector(104),
                "doc_tag": "b_empty",
                "profile": [],
            },
        ]
        res, check = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(rows_a)
        res, check = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(rows_b)

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row["id"]: row for row in rows_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row["id"]: row for row in rows_b})

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            iterator, check = self.query_iterator(
                client,
                collection_name,
                batch_size=257,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                consistency_level="Strong",
            )
            assert check
            iterator_rows = self._drain_iterator(iterator)
            iterator_ids = [row["id"] for row in iterator_rows]
            assert len(iterator_ids) == len(set(iterator_ids))
            assert set(iterator_ids) == set(source_by_id)
            for row in iterator_rows:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            iterator, check = self.search_iterator(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                batch_size=257,
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            iterator_hits = self._drain_iterator(iterator)
            hit_ids = [hit["id"] for hit in iterator_hits]
            assert len(hit_ids) == len(set(hit_ids))
            assert set(hit_ids) == set(source_by_id)

            distances = [hit["distance"] for hit in iterator_hits]
            for index in range(len(distances) - 1):
                assert distances[index] <= distances[index + 1] + epsilon

            for hit in iterator_hits:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_partition_query_search_iterator(self):
        """
        target: test partition-scoped iterators after dynamically adding a nullable struct array field with vector
            sub-field
        method: create two partitions with indexed old sealed rows and old growing rows, add a struct array field with
            scalar and vector sub-fields, insert post-add growing rows in both partitions, then drain query_iterator
            and search_iterator with partition_names
        expected: iterators only return rows from the requested partition, with correct nullable struct and vector
            sub-field output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_partition_iter")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        rows_a = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "a_non_empty",
                "profile": self._profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "a_empty",
                "profile": [],
            },
        ]
        rows_b = [
            {
                "id": 102,
                "normal_vector": self._vector(102),
                "doc_tag": "b_non_empty",
                "profile": self._profile(102),
            },
            {
                "id": 103,
                "normal_vector": self._vector(103),
                "doc_tag": "b_empty",
                "profile": [],
            },
        ]
        res, check = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(rows_a)
        res, check = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(rows_b)

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row["id"]: row for row in rows_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row["id"]: row for row in rows_b})

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            iterator, check = self.query_iterator(
                client,
                collection_name,
                batch_size=257,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                consistency_level="Strong",
            )
            assert check
            iterator_rows = self._drain_iterator(iterator)
            iterator_ids = [row["id"] for row in iterator_rows]
            assert len(iterator_ids) == len(set(iterator_ids))
            assert set(iterator_ids) == set(source_by_id)
            for row in iterator_rows:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            iterator, check = self.search_iterator(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                batch_size=257,
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            iterator_hits = self._drain_iterator(iterator)
            hit_ids = [hit["id"] for hit in iterator_hits]
            assert len(hit_ids) == len(set(hit_ids))
            assert set(hit_ids) == set(source_by_id)

            distances = [hit["distance"] for hit in iterator_hits]
            for index in range(len(distances) - 1):
                assert distances[index] <= distances[index + 1] + epsilon

            for hit in iterator_hits:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_query_search_old_sealed_rows(self):
        """
        target: test query/search output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with sealed rows, add a struct array field with vector sub-field,
            then insert new rows
        expected: old rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(3, 5)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_insert_omit_nullable_field(self):
        """
        target: test omitted nullable struct array field after dynamically adding a vector sub-field struct
        method: add a struct array field with vector sub-field, then insert one row without the new field and
            one row with the new field
        expected: old rows and omitted-field new row expose the added struct as null, while present-field new row
            returns inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_omit")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        omitted_profile_row = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "new_omitted_profile",
        }
        present_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "new_present_profile",
            "profile": self._profile(4),
        }
        res, check = self.insert(client, collection_name, [omitted_profile_row, present_profile_row])
        assert check
        assert res["insert_count"] == 2

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id[omitted_profile_row["id"]] = {**omitted_profile_row, "profile": None}
        source_by_id[present_profile_row["id"]] = present_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 40)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {present_profile_row["id"]}
        self._assert_profile_equal(element_filter_results[0]["profile"], present_profile_row["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

        filtered_search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="element_filter(profile, $[p_int] == 40)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {hit["id"] for hit in filtered_search_results[0]} == {present_profile_row["id"]}
        entity = self._search_entity(filtered_search_results[0][0])
        self._assert_profile_equal(entity["profile"], present_profile_row["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_query_search_old_growing_rows(self):
        """
        target: test query/search output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with growing rows, add a struct array field with vector sub-field,
            then insert new rows
        expected: old growing rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_growing")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(3, 5)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_query_search_old_new_rows(self):
        """
        target: test query/search output after dynamically adding a nullable struct array field
        method: create loaded collection with sealed and growing rows, add a struct array field, then insert new rows
        expected: old rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(4)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(4, 6)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(6, 8)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])


class TestMilvusClientStructArrayNullableImport(StructArrayNullableTestMixin, TestMilvusClientV2Base):
    """Nullable struct array bulk import coverage."""

    # MinIO configuration constants
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    REMOTE_DATA_PATH = "bulkinsert_data"
    LOCAL_FILES_PATH = "/tmp/milvus_bulkinsert/"

    @pytest.fixture(scope="function", autouse=True)
    def setup_minio(self, minio_host, minio_bucket):
        """Setup MinIO configuration from fixtures"""
        Path(self.LOCAL_FILES_PATH).mkdir(parents=True, exist_ok=True)
        self.minio_host = minio_host
        self.bucket_name = minio_bucket
        self.minio_endpoint = f"{minio_host}:9000"

    def upload_to_minio(self, local_file_path: str) -> list[list[str]]:
        """
        Upload parquet file to MinIO

        Args:
            local_file_path: Local path of the file to upload

        Returns:
            List of remote file paths in MinIO
        """
        if not os.path.exists(local_file_path):
            raise Exception(f"Local file '{local_file_path}' doesn't exist")

        try:
            minio_client = Minio(
                endpoint=self.minio_endpoint,
                access_key=self.MINIO_ACCESS_KEY,
                secret_key=self.MINIO_SECRET_KEY,
                secure=False,
            )

            # Check if bucket exists
            if not minio_client.bucket_exists(self.bucket_name):
                raise Exception(f"MinIO bucket '{self.bucket_name}' doesn't exist")

            # Upload file
            filename = os.path.basename(local_file_path)
            minio_file_path = os.path.join(self.REMOTE_DATA_PATH, filename)
            minio_client.fput_object(self.bucket_name, minio_file_path, local_file_path)

            log.info(f"Uploaded file to MinIO: {minio_file_path}")
            return [[minio_file_path]]

        except S3Error as e:
            raise Exception(f"Failed to connect MinIO server {self.minio_endpoint}, error: {e}")

    def call_bulkinsert(
        self,
        collection_name: str,
        batch_files: list[list[str]],
        expect_fail: bool = False,
        partition_name: str = "",
    ) -> dict[str, Any]:
        """
        Call bulk import API and wait for completion

        Args:
            collection_name: Target collection name
            batch_files: List of file paths in MinIO
            expect_fail: Whether the import job is expected to fail
            partition_name: Optional target partition name

        Returns:
            Import result dict with state and reason
        """
        url = f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

        log.info(f"Starting bulk import to collection '{collection_name}'")
        resp = bulk_import(
            url=url,
            collection_name=collection_name,
            files=batch_files,
            api_key=cf.param_info.param_token,
            partition_name=partition_name,
        )

        job_id = resp.json()["data"]["jobId"]
        log.info(f"Bulk import job created, job_id: {job_id}")

        # Wait for import to complete
        timeout = 300
        start_time = time.time()
        while time.time() - start_time < timeout:
            time.sleep(5)

            resp = get_import_progress(url=url, job_id=job_id, api_key=cf.param_info.param_token)
            state = resp.json()["data"]["state"]
            progress = resp.json()["data"].get("progress", 0)

            log.info(f"Import job {job_id} - state: {state}, progress: {progress}%")

            if state == "Importing":
                continue
            elif state == "Failed":
                reason = resp.json()["data"].get("reason", "Unknown reason")
                if expect_fail:
                    log.info(f"Bulk import job {job_id} failed as expected: {reason}")
                    return {"state": "Failed", "reason": reason}
                raise Exception(f"Bulk import job {job_id} failed: {reason}")
            elif state == "Completed" and progress == 100:
                if expect_fail:
                    raise AssertionError(f"Bulk import job {job_id} unexpectedly completed")
                log.info(f"Bulk import job {job_id} completed successfully")
                return {"state": "Completed", "reason": None}
        else:
            raise Exception(f"Bulk import job {job_id} timeout after {timeout}s")

        log.info("Bulk import finished")

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_json(self):
        """
        target: test JSON bulk import for a nullable scalar-only struct array field
        method: create a collection with nullable scalar Struct Array, import 3000 JSON rows with null, empty, and
            non-empty profile values, then query and search the imported data
        expected: imported row count matches source data, and query/search output preserves nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_json")
        entities = 3000

        schema = self._create_indexed_nullable_struct_array_collection(client, collection_name)
        rows, source_by_id = self._nullable_scalar_struct_rows_by_schema(
            schema,
            entities,
            tag_prefix="import_row",
        )

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_json_partition_query_search(self):
        """
        target: test JSON bulk import into a specified partition for a nullable scalar-only struct array field
        method: create two partitions, import 3000 JSON rows into one partition, insert same-vector interference rows
            into the other partition, then query and search with partition_names
        expected: imported row count is scoped to the target partition, and partition-scoped query/search never returns
            rows from the other partition while preserving nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_json_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        entities = 3000
        other_entities = 3000

        schema = self._create_indexed_nullable_struct_array_collection(client, collection_name)
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        other_partition_rows = []
        other_source_by_id = {}
        for offset in range(other_entities):
            row_id = 100000 + offset
            if offset == 0:
                profile = self._scalar_profile(row_id)
                vector = self._vector(entities - 1)
                doc_tag = "other_partition_same_vector"
            elif offset % 3 == 1:
                profile = None
                vector = self._vector(row_id)
                doc_tag = f"other_partition_null_profile_{offset}"
            elif offset % 3 == 2:
                profile = []
                vector = self._vector(row_id)
                doc_tag = f"other_partition_empty_profile_{offset}"
            else:
                profile = self._scalar_profile(row_id)
                vector = self._vector(row_id)
                doc_tag = f"other_partition_profile_{offset}"
            row = {
                "id": row_id,
                "normal_vector": vector,
                "doc_tag": doc_tag,
                "profile": profile,
            }
            other_partition_rows.append(row)
            other_source_by_id[row_id] = row
        res, check = self.insert(client, collection_name, other_partition_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_partition_rows)
        res, check = self.flush(client, collection_name)
        assert check

        rows, source_by_id = self._nullable_scalar_struct_rows_by_schema(
            schema,
            entities,
            tag_prefix="target_partition_row",
        )

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities + len(other_partition_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_partition_rows)

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_partition_rows),
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection({row["id"] for row in other_partition_rows})
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_parquet_partition_query_search(self):
        """
        target: test Parquet bulk import into a specified partition for a nullable scalar-only struct array field
        method: create two partitions, import 3000 Parquet rows into one partition, insert same-vector interference
            rows into the other partition, then query and search with partition_names
        expected: imported row count is scoped to the target partition, and partition-scoped query/search never returns
            rows from the other partition while preserving nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_parquet_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        entities = 3000
        other_entities = 3000

        schema = self._create_indexed_nullable_struct_array_collection(client, collection_name)
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        other_partition_rows = []
        other_source_by_id = {}
        for offset in range(other_entities):
            row_id = 100000 + offset
            if offset == 0:
                profile = self._scalar_profile(row_id)
                vector = self._vector(entities - 1)
                doc_tag = "other_partition_same_vector"
            elif offset % 3 == 1:
                profile = None
                vector = self._vector(row_id)
                doc_tag = f"other_partition_null_profile_{offset}"
            elif offset % 3 == 2:
                profile = []
                vector = self._vector(row_id)
                doc_tag = f"other_partition_empty_profile_{offset}"
            else:
                profile = self._scalar_profile(row_id)
                vector = self._vector(row_id)
                doc_tag = f"other_partition_profile_{offset}"
            row = {
                "id": row_id,
                "normal_vector": vector,
                "doc_tag": doc_tag,
                "profile": profile,
            }
            other_partition_rows.append(row)
            other_source_by_id[row_id] = row
        res, check = self.insert(client, collection_name, other_partition_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_partition_rows)
        res, check = self.flush(client, collection_name)
        assert check

        rows, source_by_id = self._nullable_scalar_struct_rows_by_schema(
            schema,
            entities,
            tag_prefix="target_partition_row",
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        self._write_scalar_struct_rows_parquet(rows, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities + len(other_partition_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_partition_rows)

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_partition_rows),
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection({row["id"] for row in other_partition_rows})
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(
        reason="known blocker: nullable scalar sub-field null values inside StructArray are rejected by JSON bulk import and PyMilvus row insert",
        strict=True,
    )
    def test_import_nullable_scalar_struct_array_subfield_null_json(self):
        """
        target: test JSON bulk import for nullable scalar sub-fields inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 JSON rows where non-empty profile
            elements contain explicit null p_int or p_tag values, then query and search the imported data
        expected: parent null/empty semantics are preserved, and scalar sub-field null values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_null_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 4 == 0:
                profile = None
            elif row_id % 4 == 1:
                profile = []
            elif row_id % 4 == 2:
                profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_null_int"},
                    {"p_int": row_id * 10 + 1, "p_tag": None},
                ]
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(
        reason="known blocker: nullable scalar sub-field null values inside StructArray are rejected by Parquet bulk import",
        strict=True,
    )
    def test_import_nullable_scalar_struct_array_subfield_null_parquet(self):
        """
        target: test Parquet bulk import for nullable scalar sub-fields inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 Parquet rows where non-empty profile
            elements contain explicit null p_int or p_tag values, then query and search the imported data
        expected: parent null/empty semantics are preserved, and scalar sub-field null values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_null_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 4 == 0:
                profile = None
            elif row_id % 4 == 1:
                profile = []
            elif row_id % 4 == 2:
                profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_null_int"},
                    {"p_int": row_id * 10 + 1, "p_tag": None},
                ]
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(
        reason="known blocker: nullable scalar sub-field omission inside StructArray element is rejected by JSON bulk import and row insert",
        strict=True,
    )
    def test_import_nullable_scalar_struct_array_subfield_omit_json(self):
        """
        target: test JSON bulk import for omitted nullable scalar sub-fields inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 JSON rows where non-empty profile
            elements omit p_int or p_tag, then query and search the imported data
        expected: parent null/empty semantics are preserved, and omitted scalar sub-field values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_omit_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 4 == 0:
                input_profile = None
                expected_profile = None
            elif row_id % 4 == 1:
                input_profile = []
                expected_profile = []
            elif row_id % 4 == 2:
                input_profile = [
                    {"p_tag": f"profile_{row_id}_missing_int"},
                    {"p_int": row_id * 10 + 1},
                ]
                expected_profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_missing_int"},
                    {"p_int": row_id * 10 + 1, "p_tag": None},
                ]
            else:
                input_profile = self._scalar_profile(row_id)
                expected_profile = input_profile
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": input_profile,
            }
            rows.append(row)
            source_by_id[row_id] = {**row, "profile": expected_profile}

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(
        reason="known blocker: nullable scalar sub-field omitted from Parquet StructArray schema is rejected",
        strict=True,
    )
    def test_import_nullable_scalar_struct_array_subfield_omit_parquet(self):
        """
        target: test Parquet bulk import with an omitted nullable scalar sub-field inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 Parquet rows whose profile struct
            file schema only contains p_tag and omits p_int, then query and search the imported data
        expected: parent null/empty semantics are preserved, and omitted p_int values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_omit_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
                expected_profile = None
            elif row_id % 3 == 1:
                profile = []
                expected_profile = []
            else:
                profile = [
                    {"p_tag": f"profile_{row_id}_missing_int_0"},
                    {"p_tag": f"profile_{row_id}_missing_int_1"},
                ]
                expected_profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_missing_int_0"},
                    {"p_int": None, "p_tag": f"profile_{row_id}_missing_int_1"},
                ]
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = {**row, "profile": expected_profile}

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_null_json(self):
        """
        target: test JSON bulk import rejects null for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 JSON rows where one row uses
            profile=null
        expected: import job fails with a nullable-related error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        rows = []
        for row_id in range(entities):
            if row_id == 0:
                profile = None
            elif row_id % 2 == 0:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            rows.append(
                {
                    "id": row_id,
                    "normal_vector": self._vector(row_id),
                    "doc_tag": f"import_row_{row_id}",
                    "profile": profile,
                }
            )

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        assert any(keyword in import_result["reason"].lower() for keyword in ["null", "nullable"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_omit_field_json(self):
        """
        target: test JSON bulk import rejects omitted data for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 JSON rows that omit profile
            entirely
        expected: import job fails with a schema/data mismatch error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_omit_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        rows = []
        for row_id in range(entities):
            rows.append(
                {
                    "id": row_id,
                    "normal_vector": self._vector(row_id),
                    "doc_tag": f"import_row_{row_id}",
                }
            )

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        reason = import_result["reason"].lower()
        assert any(keyword in reason for keyword in ["profile", "null", "nullable", "missing", "required"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_omit_field_parquet(self):
        """
        target: test Parquet bulk import rejects omitted data for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 Parquet rows whose file schema
            omits profile entirely
        expected: import job fails with a schema/data mismatch error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_omit_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        ids = []
        vectors = []
        doc_tags = []
        for row_id in range(entities):
            ids.append(row_id)
            vectors.append(self._vector(row_id))
            doc_tags.append(f"import_row_{row_id}")

        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        reason = import_result["reason"].lower()
        assert any(keyword in reason for keyword in ["profile", "null", "nullable", "missing", "required"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_json(self):
        """
        target: test JSON bulk import after dynamically adding a nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 JSON rows with null,
            empty, and non-empty profile values, then query and search all rows
        expected: old sealed rows expose the added struct field as null, imported rows preserve nullable struct
            semantics, and normal vector search requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_json")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            import_rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_json_partition_query_search(self):
        """
        target: test JSON bulk import into a specified partition after dynamically adding a nullable scalar-only Struct
            Array field
        method: insert and flush 3000 old rows in the target partition, add nullable scalar Struct Array, import 3000
            JSON rows into the same partition, and keep same-vector interference rows in another partition
        expected: old target-partition rows expose the added struct field as null, imported rows preserve nullable
            struct semantics, and partition-scoped query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_json_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_target_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_row_{row_id}",
                "profile": profile,
            }
            import_rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_parquet_partition_query_search(self):
        """
        target: test Parquet bulk import into a specified partition after dynamically adding a nullable scalar-only
            Struct Array field
        method: insert and flush 3000 old rows in both target and non-target partitions, add nullable scalar Struct
            Array, import 3000 Parquet rows into the target partition, and keep a growing same-vector interference row
            in another partition
        expected: old target-partition rows expose the added struct field as null, imported rows preserve nullable
            struct semantics, and partition-scoped query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_parquet_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        for row_id in range(old_entities, total_target_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_json_partition_query_search(self):
        """
        target: test JSON bulk import omitting a dynamically added nullable scalar-only Struct Array field into a
            specified partition
        method: insert and flush 3000 old rows in both target and non-target partitions, add nullable scalar Struct
            Array, import 3000 JSON rows that omit profile into the target partition, and keep a growing same-vector
            interference row in another partition
        expected: old and imported target-partition rows expose the added struct field as null, and partition-scoped
            query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_json_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_target_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_omit_profile_row_{row_id}",
            }
            import_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_parquet_partition_query_search(self):
        """
        target: test Parquet bulk import omitting a dynamically added nullable scalar-only Struct Array field into a
            specified partition
        method: insert and flush 3000 old rows in both target and non-target partitions, add nullable scalar Struct
            Array, import 3000 Parquet rows whose file schema omits profile into the target partition, and keep a
            growing same-vector interference row in another partition
        expected: old and imported target-partition rows expose the added struct field as null, and partition-scoped
            query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_parquet_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        for row_id in range(old_entities, total_target_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_omit_profile_row_{row_id}",
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            source_by_id[row_id] = {**row, "profile": None}

        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_parquet(self):
        """
        target: test Parquet bulk import after dynamically adding a nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 Parquet rows with null,
            empty, and non-empty profile values, then query and search all rows
        expected: old sealed rows expose the added struct field as null, imported rows preserve nullable struct
            semantics, and normal vector search requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_parquet")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        for row_id in range(old_entities, total_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_json(self):
        """
        target: test JSON bulk import omitting a dynamically added nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 JSON rows that omit
            profile entirely, then query and search all rows
        expected: both old rows and imported rows expose the added struct field as null, and normal vector search
            requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_json")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
            }
            import_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_parquet(self):
        """
        target: test Parquet bulk import omitting a dynamically added nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 Parquet rows whose file
            schema omits profile entirely, then query and search all rows
        expected: both old rows and imported rows expose the added struct field as null, and normal vector search
            requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_parquet")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        for row_id in range(old_entities, total_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            source_by_id[row_id] = {**row, "profile": None}

        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(
        reason="known blocker: nullable StructArray vector sub-field imported by JSON loses ArrayOfVector ValidData on output",
        strict=True,
    )
    def test_import_nullable_struct_array_with_vector_json(self):
        """
        target: test JSON bulk import for a nullable struct array field with scalar and vector sub-fields
        method: create a collection with nullable Struct Array, import 3000 JSON rows with null, empty, and non-empty
            profile values, build indexes on normal vector and struct vector sub-field, then query and normal-vector
            search the imported data
        expected: imported row count matches source data, indexes are ready, and query/search output preserves nullable
            struct semantics including vector sub-field values
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_struct_vector_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        assert self.wait_for_index_ready(client, collection_name, "profile[p_vec]", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_parquet(self):
        """
        target: test Parquet bulk import for a nullable scalar-only struct array field
        method: create a collection with nullable scalar Struct Array, import 3000 Parquet rows with null, empty, and
            non-empty profile values, then query and search the imported data
        expected: imported row count matches source data, and query/search output preserves nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_null_parquet(self):
        """
        target: test Parquet bulk import rejects null for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 Parquet rows where one row uses
            profile=null
        expected: import job fails with a nullable-related error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        for row_id in range(entities):
            if row_id == 0:
                profile = None
            elif row_id % 2 == 0:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            ids.append(row_id)
            vectors.append(self._vector(row_id))
            doc_tags.append(f"import_row_{row_id}")
            profiles.append(profile)

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        assert any(keyword in import_result["reason"].lower() for keyword in ["null", "nullable"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0
