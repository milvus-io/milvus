import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from pymilvus import AnnSearchRequest, DataType, WeightedRanker
from pymilvus.bulk_writer import (
    bulk_import,
    get_import_progress,
)
from pymilvus.client.embedding_list import EmbeddingList
from utils.util_log import test_log as log
from utils.util_pymilvus import *  # noqa: F403

prefix = "struct_array"
epsilon = 0.001

# Dim for emb list index tests (smaller for faster index building)
EMB_LIST_DIM = 32

default_dim = 128

PK_FIELD = "id"
VECTOR_FIELD = "normal_vector"
TAG_FIELD = "doc_tag"
STRUCT_FIELD = "profile"
INT_SUBFIELD = "p_int"
TAG_SUBFIELD = "p_tag"
VECTOR_SUBFIELD = "p_vec"

PK_TYPE = DataType.INT64
VECTOR_TYPE = DataType.FLOAT_VECTOR
TAG_TYPE = DataType.VARCHAR
STRUCT_TYPE = DataType.ARRAY
STRUCT_ELEMENT_TYPE = DataType.STRUCT
INT_SUBFIELD_TYPE = DataType.INT64
TAG_SUBFIELD_TYPE = DataType.VARCHAR
VECTOR_SUBFIELD_TYPE = DataType.FLOAT_VECTOR

VECTOR_DIM = 128
VECTOR_SUBFIELD_DIM = 128
TAG_MAX_LENGTH = 128
STRUCT_MAX_CAPACITY = 4

SCALAR_SUBFIELDS = (INT_SUBFIELD, TAG_SUBFIELD)
VECTOR_SUBFIELDS = (INT_SUBFIELD, TAG_SUBFIELD, VECTOR_SUBFIELD)
STRUCT_INT_FIELD = f"{STRUCT_FIELD}[{INT_SUBFIELD}]"
STRUCT_TAG_FIELD = f"{STRUCT_FIELD}[{TAG_SUBFIELD}]"
STRUCT_VECTOR_FIELD = f"{STRUCT_FIELD}[{VECTOR_SUBFIELD}]"
ALL_ROWS_FILTER = f"{PK_FIELD} >= 0"

NORMAL_VECTOR_INDEX_TYPE = "FLAT"
NORMAL_VECTOR_METRIC_TYPE = "L2"
NORMAL_VECTOR_DISKANN_INDEX_TYPE = "DISKANN"
NORMAL_VECTOR_DISKANN_METRIC_TYPE = "L2"
STRUCT_VECTOR_HNSW_INDEX_TYPE = "HNSW"
STRUCT_VECTOR_HNSW_METRIC_TYPE = "MAX_SIM_COSINE"
STRUCT_VECTOR_DISKANN_INDEX_TYPE = "DISKANN"
STRUCT_VECTOR_DISKANN_METRIC_TYPE = "MAX_SIM_COSINE"
STRUCT_VECTOR_INDEX_TYPE = STRUCT_VECTOR_HNSW_INDEX_TYPE
STRUCT_VECTOR_METRIC_TYPE = STRUCT_VECTOR_HNSW_METRIC_TYPE
HNSW_INDEX_PARAMS = {"M": 16, "efConstruction": 200}
INDEX_PARAMS = HNSW_INDEX_PARAMS
NORMAL_VECTOR_DISKANN_INDEX_PARAMS = {}
STRUCT_VECTOR_DISKANN_INDEX_PARAMS = {}

NORMAL_VECTOR_SEARCH_PARAMS = {"metric_type": NORMAL_VECTOR_METRIC_TYPE, "params": {}}
NORMAL_VECTOR_DISKANN_SEARCH_PARAMS = {
    "metric_type": NORMAL_VECTOR_DISKANN_METRIC_TYPE,
    "params": {"search_list": 30},
}
STRUCT_VECTOR_SEARCH_PARAMS = {"metric_type": STRUCT_VECTOR_METRIC_TYPE}
STRUCT_VECTOR_DISKANN_SEARCH_PARAMS = {
    "metric_type": STRUCT_VECTOR_DISKANN_METRIC_TYPE,
    "params": {"search_list": 30},
}
OMITTED_FIELD = object()


# Schema generation


def gen_schema(case, client, **kwargs):
    schema, _ = case.create_schema(client, **kwargs)
    return schema


def gen_struct_schema(case, client, **kwargs):
    struct_schema, _ = case.create_struct_field_schema(client, **kwargs)
    return struct_schema


def gen_index_params(case, client, **kwargs):
    index_params, _ = case.prepare_index_params(client, **kwargs)
    return index_params


def _schema_as_dict(schema) -> dict[str, Any]:
    if isinstance(schema, dict):
        return schema
    return schema.to_dict()


def _schema_field_names(schema) -> set[str]:
    schema_dict = _schema_as_dict(schema)
    field_names = {field.get("name") for field in schema_dict.get("fields", [])}
    field_names.update(field.get("name") for field in schema_dict.get("struct_fields", []))
    return field_names


def _schema_field_dim(schema, field_name: str, default: int) -> int:
    schema_dict = _schema_as_dict(schema)
    for field in schema_dict.get("fields", []):
        if field.get("name") != field_name:
            continue
        return int((field.get("params") or {}).get("dim", default))
    return default


def gen_struct_array_schema_dict(
    *,
    include_struct: bool = False,
    include_vector_subfield: bool = False,
    normal_vector_dim: int | None = None,
    struct_vector_type: DataType | None = None,
    struct_vector_dim: int | None = None,
    struct_nullable: bool = True,
) -> dict[str, Any]:
    normal_vector_dim = normal_vector_dim or VECTOR_DIM
    struct_vector_type = struct_vector_type or VECTOR_SUBFIELD_TYPE
    struct_vector_dim = struct_vector_dim or VECTOR_SUBFIELD_DIM
    fields = [
        {
            "name": PK_FIELD,
            "type": PK_TYPE,
            "is_primary": True,
            "auto_id": False,
        },
        {
            "name": VECTOR_FIELD,
            "type": VECTOR_TYPE,
            "params": {"dim": normal_vector_dim},
        },
        {
            "name": TAG_FIELD,
            "type": TAG_TYPE,
            "params": {"max_length": TAG_MAX_LENGTH},
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
            {"name": INT_SUBFIELD, "type": INT_SUBFIELD_TYPE, "nullable": True},
            {
                "name": TAG_SUBFIELD,
                "type": TAG_SUBFIELD_TYPE,
                "params": {"max_length": TAG_MAX_LENGTH},
                "nullable": True,
            },
        ]
        if include_vector_subfield:
            struct_subfields.append(
                {
                    "name": VECTOR_SUBFIELD,
                    "type": struct_vector_type,
                    "params": {"dim": struct_vector_dim},
                    "nullable": True,
                }
            )
        fields.append(
            {
                "name": STRUCT_FIELD,
                "type": STRUCT_TYPE,
                "element_type": STRUCT_ELEMENT_TYPE,
                "params": {"max_capacity": STRUCT_MAX_CAPACITY},
                "nullable": struct_nullable,
                "struct_fields": struct_subfields,
            }
        )
        schema["struct_fields"] = [
            {
                "name": STRUCT_FIELD,
                "max_capacity": STRUCT_MAX_CAPACITY,
                "nullable": struct_nullable,
                "fields": struct_subfields,
            }
        ]
    return schema


def gen_struct_field_schema(
    case,
    client,
    *,
    include_vector_subfield: bool = False,
    vector_type: DataType | None = None,
    vector_dim: int | None = None,
):
    vector_type = vector_type or VECTOR_SUBFIELD_TYPE
    vector_dim = vector_dim or VECTOR_SUBFIELD_DIM
    struct_schema = gen_struct_schema(case, client)
    struct_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
    struct_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
    if include_vector_subfield:
        struct_schema.add_field(VECTOR_SUBFIELD, vector_type, dim=vector_dim)
    return struct_schema


def gen_struct_array_schema(
    case,
    client,
    *,
    include_struct: bool = True,
    include_vector_subfield: bool = False,
    normal_vector_dim: int | None = None,
    struct_vector_type: DataType | None = None,
    struct_vector_dim: int | None = None,
    include_tag_field: bool = True,
    enable_dynamic_field: bool = False,
):
    normal_vector_dim = normal_vector_dim or VECTOR_DIM
    struct_vector_type = struct_vector_type or VECTOR_SUBFIELD_TYPE
    schema = gen_schema(case, client, auto_id=False, enable_dynamic_field=enable_dynamic_field)
    schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
    schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=normal_vector_dim)
    if include_tag_field:
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)
    if include_struct:
        struct_vector_dim = struct_vector_dim or VECTOR_SUBFIELD_DIM
        struct_schema = gen_struct_field_schema(
            case,
            client,
            include_vector_subfield=include_vector_subfield,
            vector_type=struct_vector_type,
            vector_dim=struct_vector_dim,
        )
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=struct_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )
    return schema


# Data generation


def gen_vector(seed: int, dim: int | None = None) -> list[float]:
    dim = dim or VECTOR_DIM
    return [float(seed) + float(i) / 1000 for i in range(dim)]


def gen_rows_by_schema(
    schema,
    count: int,
    *,
    start_id: int = 0,
    tag_prefix: str = "row",
    profiles: list[Any] | None = None,
    default_values: dict[str, Any] | None = None,
    vector_dim: int | None = None,
) -> list[dict[str, Any]]:
    field_names = _schema_field_names(schema)
    row_ids = list(range(start_id, start_id + count))
    vector_dim = vector_dim or _schema_field_dim(schema, VECTOR_FIELD, VECTOR_DIM)
    overrides = {
        PK_FIELD: row_ids,
        VECTOR_FIELD: [gen_vector(row_id, vector_dim) for row_id in row_ids],
        TAG_FIELD: [f"{tag_prefix}_{i}" for i in range(count)],
    }
    if profiles is not None:
        overrides[STRUCT_FIELD] = profiles
    if default_values:
        overrides.update(default_values)
    overrides = {field_name: value for field_name, value in overrides.items() if field_name in field_names}
    return cf.gen_row_data_by_schema_with_defaults(
        nb=count,
        schema=schema,
        start=start_id,
        default_values=overrides,
    )


def gen_row_by_schema(
    schema,
    row_id: int,
    tag: str,
    *,
    profile=OMITTED_FIELD,
    vector_dim: int | None = None,
) -> dict[str, Any]:
    vector_dim = vector_dim or _schema_field_dim(schema, VECTOR_FIELD, VECTOR_DIM)
    default_values = {
        PK_FIELD: [row_id],
        VECTOR_FIELD: [gen_vector(row_id, vector_dim)],
        TAG_FIELD: [tag],
    }
    if profile is not OMITTED_FIELD:
        default_values[STRUCT_FIELD] = [profile]
    row = gen_rows_by_schema(
        schema,
        1,
        start_id=row_id,
        tag_prefix=tag,
        default_values=default_values,
        vector_dim=vector_dim,
    )[0]
    if profile is OMITTED_FIELD:
        row.pop(STRUCT_FIELD, None)
    return row


def _scalar_struct_profile_arrow_type():
    return pa.list_(
        pa.struct(
            [
                pa.field(INT_SUBFIELD, pa.int64()),
                pa.field(TAG_SUBFIELD, pa.string()),
            ]
        )
    )


def write_scalar_struct_rows_parquet(
    rows: list[dict[str, Any]],
    local_file_path: str,
    *,
    row_group_size: int,
):
    table = pa.table(
        {
            PK_FIELD: pa.array([row[PK_FIELD] for row in rows], type=pa.int64()),
            VECTOR_FIELD: pa.array([row[VECTOR_FIELD] for row in rows], type=pa.list_(pa.float32())),
            TAG_FIELD: pa.array([row[TAG_FIELD] for row in rows], type=pa.string()),
            STRUCT_FIELD: pa.array(
                [row[STRUCT_FIELD] for row in rows],
                type=_scalar_struct_profile_arrow_type(),
            ),
        }
    )
    pq.write_table(table, local_file_path, row_group_size=row_group_size)


def write_struct_vector_rows_parquet(
    rows: list[dict[str, Any]],
    local_file_path: str,
    *,
    row_group_size: int,
):
    profile_type = pa.list_(
        pa.struct(
            [
                pa.field(INT_SUBFIELD, pa.int64()),
                pa.field(TAG_SUBFIELD, pa.string()),
                pa.field(VECTOR_SUBFIELD, pa.list_(pa.float32())),
            ]
        )
    )
    table = pa.table(
        {
            PK_FIELD: pa.array([row[PK_FIELD] for row in rows], type=pa.int64()),
            VECTOR_FIELD: pa.array([row[VECTOR_FIELD] for row in rows], type=pa.list_(pa.float32())),
            TAG_FIELD: pa.array([row[TAG_FIELD] for row in rows], type=pa.string()),
            STRUCT_FIELD: pa.array([row[STRUCT_FIELD] for row in rows], type=profile_type),
        }
    )
    pq.write_table(table, local_file_path, row_group_size=row_group_size)


def gen_index_filler_rows(
    start_id: int,
    count: int,
    tag_prefix: str,
    *,
    schema=None,
    vector_dim: int | None = None,
) -> list[dict[str, Any]]:
    schema = schema or gen_struct_array_schema_dict(normal_vector_dim=vector_dim)
    return gen_rows_by_schema(
        schema,
        count,
        start_id=start_id,
        tag_prefix=tag_prefix,
        vector_dim=vector_dim,
    )


def gen_scalar_index_filler_rows(
    start_id: int,
    count: int,
    tag_prefix: str,
    *,
    schema=None,
    vector_dim: int | None = None,
) -> list[dict[str, Any]]:
    schema = schema or gen_struct_array_schema_dict(
        include_struct=True,
        normal_vector_dim=vector_dim,
    )
    profiles = [[{INT_SUBFIELD: -(start_id + i), TAG_SUBFIELD: f"{tag_prefix}_profile_{i}"}] for i in range(count)]
    return gen_rows_by_schema(
        schema,
        count,
        start_id=start_id,
        tag_prefix=tag_prefix,
        profiles=profiles,
        vector_dim=vector_dim,
    )


def gen_vector_index_filler_rows(
    start_id: int,
    count: int,
    tag_prefix: str,
    *,
    schema=None,
    vector_dim: int | None = None,
) -> list[dict[str, Any]]:
    schema = schema or gen_struct_array_schema_dict(
        include_struct=True,
        include_vector_subfield=True,
        normal_vector_dim=vector_dim,
    )
    vector_dim = vector_dim or _schema_field_dim(schema, VECTOR_FIELD, VECTOR_DIM)
    profiles = [
        [
            {
                INT_SUBFIELD: -(start_id + i),
                TAG_SUBFIELD: f"{tag_prefix}_profile_{i}",
                VECTOR_SUBFIELD: gen_vector(start_id + i, vector_dim),
            }
        ]
        for i in range(count)
    ]
    return gen_rows_by_schema(
        schema,
        count,
        start_id=start_id,
        tag_prefix=tag_prefix,
        profiles=profiles,
        vector_dim=vector_dim,
    )


def gen_unit_vector(axis: int, dim: int | None = None) -> list[float]:
    dim = dim or VECTOR_SUBFIELD_DIM
    vector = [0.0 for _ in range(dim)]
    vector[axis % dim] = 1.0
    return vector


def gen_profile(
    row_id: int,
) -> list[dict[str, Any]]:
    return [
        {
            INT_SUBFIELD: row_id * 10,
            TAG_SUBFIELD: f"profile_{row_id}_0",
            VECTOR_SUBFIELD: gen_vector(row_id * 10, VECTOR_SUBFIELD_DIM),
        },
        {
            INT_SUBFIELD: row_id * 10 + 1,
            TAG_SUBFIELD: f"profile_{row_id}_1",
            VECTOR_SUBFIELD: gen_vector(row_id * 10 + 1, VECTOR_SUBFIELD_DIM),
        },
    ]


def gen_typed_vector(vector_type: DataType, seed: int, dim: int = EMB_LIST_DIM):
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


def gen_typed_profile(
    row_id: int,
    vector_type: DataType,
    dim: int = EMB_LIST_DIM,
) -> list[dict[str, Any]]:
    return [
        {
            INT_SUBFIELD: row_id * 10,
            TAG_SUBFIELD: f"profile_{row_id}_0",
            VECTOR_SUBFIELD: gen_typed_vector(vector_type, row_id * 10, dim),
        },
        {
            INT_SUBFIELD: row_id * 10 + 1,
            TAG_SUBFIELD: f"profile_{row_id}_1",
            VECTOR_SUBFIELD: gen_typed_vector(vector_type, row_id * 10 + 1, dim),
        },
    ]


def gen_scalar_profile(
    row_id: int,
) -> list[dict[str, Any]]:
    return [
        {
            INT_SUBFIELD: row_id * 10,
            TAG_SUBFIELD: f"profile_{row_id}_0",
        },
        {
            INT_SUBFIELD: row_id * 10 + 1,
            TAG_SUBFIELD: f"profile_{row_id}_1",
        },
    ]


def gen_expression_fixture(
    case,
    client,
    collection_name,
):
    schema = gen_struct_array_schema(case, client, include_vector_subfield=False)

    res, _ = case.create_collection(client, collection_name, schema=schema)

    sealed_explicit_null_profile_row = gen_row_by_schema(schema, 0, "sealed_explicit_null_profile", profile=None)
    sealed_omitted_profile_row = gen_row_by_schema(schema, 1, "sealed_omit_profile")
    sealed_empty_profile_row = gen_row_by_schema(schema, 2, "sealed_empty_profile", profile=[])
    sealed_one_match_profile_row = gen_row_by_schema(
        schema,
        3,
        "sealed_one_match_profile",
        profile=[
            {INT_SUBFIELD: 9100, TAG_SUBFIELD: "match_9100"},
            {INT_SUBFIELD: 100, TAG_SUBFIELD: "low_100"},
        ],
    )
    sealed_two_match_profile_row = gen_row_by_schema(
        schema,
        4,
        "sealed_two_match_profile",
        profile=[
            {INT_SUBFIELD: 9200, TAG_SUBFIELD: "match_9200"},
            {INT_SUBFIELD: 9300, TAG_SUBFIELD: "match_9300"},
        ],
    )
    sealed_zero_match_profile_row = gen_row_by_schema(
        schema,
        5,
        "sealed_zero_match_profile",
        profile=[
            {INT_SUBFIELD: 100, TAG_SUBFIELD: "low_100"},
            {INT_SUBFIELD: 200, TAG_SUBFIELD: "low_200"},
        ],
    )
    sealed_control_rows = [
        sealed_explicit_null_profile_row,
        sealed_omitted_profile_row,
        sealed_empty_profile_row,
        sealed_one_match_profile_row,
        sealed_two_match_profile_row,
        sealed_zero_match_profile_row,
    ]
    sealed_index_filler_rows = gen_scalar_index_filler_rows(
        50000,
        case.min_index_sealed_rows - len(sealed_control_rows),
        "sealed_expr_index_filler",
        schema=schema,
    )
    sealed_rows = sealed_control_rows + sealed_index_filler_rows
    res, _ = case.insert(client, collection_name, sealed_rows)
    assert res["insert_count"] == len(sealed_rows)

    res, _ = case.flush(client, collection_name)

    index_params = gen_index_params(case, client)
    index_params.add_index(
        field_name=VECTOR_FIELD,
        index_type=NORMAL_VECTOR_INDEX_TYPE,
        metric_type=NORMAL_VECTOR_METRIC_TYPE,
    )
    res, _ = case.create_index(client, collection_name, index_params)
    assert case.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

    res, _ = case.load_collection(client, collection_name)

    growing_explicit_null_profile_row = gen_row_by_schema(schema, 7000, "growing_explicit_null_profile", profile=None)
    growing_omitted_profile_row = gen_row_by_schema(schema, 7001, "growing_omit_profile")
    growing_empty_profile_row = gen_row_by_schema(schema, 7002, "growing_empty_profile", profile=[])
    growing_one_match_profile_row = gen_row_by_schema(
        schema,
        7003,
        "growing_one_match_profile",
        profile=[
            {INT_SUBFIELD: 9600, TAG_SUBFIELD: "match_9600"},
            {INT_SUBFIELD: 600, TAG_SUBFIELD: "low_600"},
        ],
    )
    growing_two_match_profile_row = gen_row_by_schema(
        schema,
        7004,
        "growing_two_match_profile",
        profile=[
            {INT_SUBFIELD: 9700, TAG_SUBFIELD: "match_9700"},
            {INT_SUBFIELD: 9800, TAG_SUBFIELD: "match_9800"},
        ],
    )
    growing_zero_match_profile_row = gen_row_by_schema(
        schema,
        7005,
        "growing_zero_match_profile",
        profile=[
            {INT_SUBFIELD: 700, TAG_SUBFIELD: "low_700"},
            {INT_SUBFIELD: 800, TAG_SUBFIELD: "low_800"},
        ],
    )
    growing_rows = [
        growing_explicit_null_profile_row,
        growing_omitted_profile_row,
        growing_empty_profile_row,
        growing_one_match_profile_row,
        growing_two_match_profile_row,
        growing_zero_match_profile_row,
    ]
    res, _ = case.insert(client, collection_name, growing_rows)
    assert res["insert_count"] == len(growing_rows)

    source_by_id = {}
    source_by_id[sealed_explicit_null_profile_row[PK_FIELD]] = sealed_explicit_null_profile_row
    source_by_id[sealed_omitted_profile_row[PK_FIELD]] = {
        **sealed_omitted_profile_row,
        STRUCT_FIELD: None,
    }
    source_by_id[sealed_empty_profile_row[PK_FIELD]] = sealed_empty_profile_row
    source_by_id[sealed_one_match_profile_row[PK_FIELD]] = sealed_one_match_profile_row
    source_by_id[sealed_two_match_profile_row[PK_FIELD]] = sealed_two_match_profile_row
    source_by_id[sealed_zero_match_profile_row[PK_FIELD]] = sealed_zero_match_profile_row
    source_by_id.update({row[PK_FIELD]: row for row in sealed_index_filler_rows})
    source_by_id[growing_explicit_null_profile_row[PK_FIELD]] = growing_explicit_null_profile_row
    source_by_id[growing_omitted_profile_row[PK_FIELD]] = {
        **growing_omitted_profile_row,
        STRUCT_FIELD: None,
    }
    source_by_id[growing_empty_profile_row[PK_FIELD]] = growing_empty_profile_row
    source_by_id[growing_one_match_profile_row[PK_FIELD]] = growing_one_match_profile_row
    source_by_id[growing_two_match_profile_row[PK_FIELD]] = growing_two_match_profile_row
    source_by_id[growing_zero_match_profile_row[PK_FIELD]] = growing_zero_match_profile_row

    controlled_ids = {row[PK_FIELD] for row in sealed_control_rows + growing_rows}
    return {
        "source_by_id": source_by_id,
        "source_rows": list(source_by_id.values()),
        "controlled_ids": controlled_ids,
    }


# Ground truth generation and result assertions


def assert_struct_array_equal(
    actual,
    expected,
    *,
    expected_keys=None,
    vector_type: DataType | None = None,
    exact_keys: bool = False,
):
    vector_type = vector_type or VECTOR_SUBFIELD_TYPE
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
            if key == VECTOR_SUBFIELD:
                assert_typed_vector_equal(actual_item[key], expected_item[key], vector_type)
            else:
                assert actual_item[key] == expected_item[key]


def assert_profile_equal(
    actual,
    expected,
):
    assert_struct_array_equal(actual, expected, expected_keys=VECTOR_SUBFIELDS)


def assert_profile_vector_subfield_equal(
    actual,
    expected,
):
    assert_struct_array_equal(
        actual,
        expected,
        expected_keys=(VECTOR_SUBFIELD,),
        exact_keys=True,
    )


def assert_ann_recall(actual_keys, expected_keys, minimum_recall=0.8):
    """Assert ANN Top-K recall against exact ground truth without requiring a specific hit."""
    expected_set = set(expected_keys)
    actual_set = set(actual_keys)
    assert expected_set
    recall = len(actual_set.intersection(expected_set)) / len(expected_set)
    assert recall >= minimum_recall, (
        f"ANN recall {recall:.2f} is below {minimum_recall:.2f}; "
        f"missing={sorted(expected_set - actual_set)}, extra={sorted(actual_set - expected_set)}"
    )


def _binary_vector_bytes(value) -> bytes:
    if isinstance(value, bytes | bytearray):
        return bytes(value)
    if isinstance(value, np.ndarray):
        return value.tobytes()
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes | bytearray):
        return bytes(value[0])
    return bytes(value)


def assert_typed_vector_equal(actual, expected, vector_type: DataType):
    if vector_type == DataType.BINARY_VECTOR:
        assert _binary_vector_bytes(actual) == _binary_vector_bytes(expected)
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


def assert_typed_profile_equal(
    actual,
    expected,
    vector_type: DataType,
):
    assert_struct_array_equal(
        actual,
        expected,
        expected_keys=VECTOR_SUBFIELDS,
        vector_type=vector_type,
    )


def assert_typed_profile_vector_subfield_equal(
    actual,
    expected,
    vector_type: DataType,
):
    assert_struct_array_equal(
        actual,
        expected,
        expected_keys=(VECTOR_SUBFIELD,),
        vector_type=vector_type,
        exact_keys=True,
    )


def search_entity(hit):
    return hit.get("entity", hit)


def drain_iterator(iterator):
    assert iterator is not None
    rows = []
    while True:
        batch = iterator.next()
        if not batch:
            break
        rows.extend(batch)
    iterator.close()
    return rows


def assert_scalar_profile_equal(
    actual,
    expected,
):
    assert_struct_array_equal(actual, expected, expected_keys=SCALAR_SUBFIELDS)


def assert_nullable_scalar_profile_equal(
    actual,
    expected,
    expected_keys=None,
):
    assert_struct_array_equal(
        actual,
        expected,
        expected_keys=expected_keys,
        exact_keys=True,
    )


def assert_expression_rows_match_source(
    rows,
    source_by_id,
):
    for row in rows:
        expected = source_by_id[row[PK_FIELD]]
        assert row[TAG_FIELD] == expected[TAG_FIELD]
        assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])


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


def _parse_expression_list(raw_value: str):
    return json.loads(raw_value)


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


def _eval_struct_element_condition(element: dict[str, Any], condition: str) -> bool:
    condition = condition.strip()
    if " && " in condition:
        return all(_eval_struct_element_condition(element, part) for part in condition.split(" && "))
    if " || " in condition:
        return any(_eval_struct_element_condition(element, part) for part in condition.split(" || "))
    for op in [">=", "<=", "==", "!=", ">", "<"]:
        if op not in condition:
            continue
        left, right = [part.strip() for part in condition.split(op, 1)]
        assert left.startswith("$[") and left.endswith("]")
        field_name = left[2:-1]
        actual = element.get(field_name)
        expected = _parse_expression_value(right)
        return _compare_expression_values(actual, op, expected)
    raise AssertionError(f"unsupported element condition: {condition}")


def _strip_id_scope(
    rows: list[dict[str, Any]],
    expression: str,
) -> tuple[list[dict[str, Any]], str]:
    expression = expression.strip()
    match = re.match(rf"^{re.escape(PK_FIELD)}\s+in\s+\[([^\]]*)\]\s*&&\s*(.+)$", expression)
    if match is None:
        return rows, expression
    id_values = {int(value.strip()) for value in match.group(1).split(",") if value.strip()}
    return [row for row in rows if row[PK_FIELD] in id_values], match.group(2).strip()


def gt_struct_array_expression_rows(
    rows: list[dict[str, Any]],
    expression: str,
) -> list[dict[str, Any]]:
    scoped_rows, expression = _strip_id_scope(rows, expression)
    struct_field_pattern = re.escape(STRUCT_FIELD)

    def output_row(row, offset=None):
        result = {
            PK_FIELD: row[PK_FIELD],
            TAG_FIELD: row[TAG_FIELD],
            STRUCT_FIELD: row.get(STRUCT_FIELD),
        }
        if offset is not None:
            result["offset"] = offset
        return result

    if expression == f"{STRUCT_FIELD} is null":
        return [output_row(row) for row in scoped_rows if row.get(STRUCT_FIELD) is None]
    if expression == f"{STRUCT_FIELD} is not null":
        return [output_row(row) for row in scoped_rows if row.get(STRUCT_FIELD) is not None]

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
            if row.get(STRUCT_FIELD) is not None and _compare_expression_values(len(row[STRUCT_FIELD]), op, expected)
        ]

    array_contains_match = re.match(
        rf"^array_contains(?:_(all|any))?\({struct_field_pattern}\[([^\]]+)\],\s*(.+)\)$",
        expression,
    )
    if array_contains_match is not None:
        mode, field_name, raw_expected = array_contains_match.groups()
        expected_values = (
            _parse_expression_list(raw_expected) if mode in {"all", "any"} else [_parse_expression_value(raw_expected)]
        )
        results = []
        for row in scoped_rows:
            struct_array = row.get(STRUCT_FIELD)
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
        expected = _parse_expression_value(raw_expected)
        results = []
        for row in scoped_rows:
            struct_array = row.get(STRUCT_FIELD) or []
            if len(struct_array) <= offset:
                continue
            if _compare_expression_values(struct_array[offset].get(field_name), op, expected):
                results.append(output_row(row))
        return results

    element_filter_match = re.match(rf"^element_filter\({struct_field_pattern},\s*(.+)\)$", expression)
    if element_filter_match is not None:
        condition = element_filter_match.group(1)
        results = []
        for row in scoped_rows:
            struct_array = row.get(STRUCT_FIELD) or []
            for offset, element in enumerate(struct_array):
                if _eval_struct_element_condition(element, condition):
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
            struct_array = row.get(STRUCT_FIELD)
            if struct_array is None:
                continue
            match_count = sum(1 for element in struct_array if _eval_struct_element_condition(element, condition))
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


def gt_nullable_scalar_struct_expression_rows(
    rows: list[dict[str, Any]],
    expression: str,
) -> list[dict[str, Any]]:
    return gt_struct_array_expression_rows(rows, expression)


def gt_expression_result_keys(rows):
    return sorted((row[PK_FIELD], row.get("offset")) for row in rows)


class TestMilvusClientStructArraySchemaEvolution(TestMilvusClientV2Base):
    """Test cases for struct array schema evolution"""

    min_index_sealed_rows = 3000

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/51414", strict=True)
    def test_struct_array_compaction_preserves_null_empty_offsets_and_indexes(self):
        """
        target: verify Struct Array data and element identities survive segment compaction
        method: create multiple sealed segments containing null, empty, and variable-length Struct Arrays,
            apply delete and upsert deltas, then compare projection, predicates, element search, and
            embedding-list search before compaction, after compaction, and after release/load
        expected: every phase returns identical Struct values, PK sets, and element offsets
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_compact_offsets")
        element_vector_subfield = "p_vec_element"
        struct_element_vector_field = f"{STRUCT_FIELD}[{element_vector_subfield}]"
        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_SUBFIELD_DIM)
        profile_schema.add_field(element_vector_subfield, VECTOR_SUBFIELD_TYPE, dim=VECTOR_SUBFIELD_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD,
            index_type="HNSW",
            metric_type=NORMAL_VECTOR_METRIC_TYPE,
            params=HNSW_INDEX_PARAMS,
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_HNSW_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_HNSW_METRIC_TYPE,
            params=HNSW_INDEX_PARAMS,
        )
        index_params.add_index(
            field_name=struct_element_vector_field,
            index_type="HNSW",
            metric_type="COSINE",
            params=HNSW_INDEX_PARAMS,
        )
        index_params.add_index(field_name=STRUCT_INT_FIELD, index_type="STL_SORT")
        index_params.add_index(field_name=STRUCT_TAG_FIELD, index_type="BITMAP")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        def profile(row_id, length, tag_prefix="keep"):
            result = []
            for offset in range(length):
                rng = np.random.RandomState(row_id * STRUCT_MAX_CAPACITY + offset)
                vector = rng.uniform(-1.0, 1.0, VECTOR_SUBFIELD_DIM).astype(np.float32)
                vector = (vector / np.linalg.norm(vector)).tolist()
                result.append(
                    {
                        INT_SUBFIELD: row_id * 10 + offset,
                        TAG_SUBFIELD: f"{tag_prefix}_{row_id}_{offset}",
                        VECTOR_SUBFIELD: vector,
                        element_vector_subfield: vector,
                    }
                )
            return result

        source_by_id = {
            0: gen_row_by_schema(schema, 0, "null", profile=None),
            1: gen_row_by_schema(schema, 1, "empty", profile=[]),
            2: gen_row_by_schema(schema, 2, "two", profile=profile(2, 2)),
            3: gen_row_by_schema(schema, 3, "one", profile=profile(3, 1)),
            4: gen_row_by_schema(schema, 4, "initial", profile=profile(4, 2, "old")),
            5: gen_row_by_schema(schema, 5, "deleted", profile=profile(5, 3)),
            10: gen_row_by_schema(schema, 10, "three", profile=profile(10, 3)),
            11: gen_row_by_schema(schema, 11, "null_second", profile=None),
            12: gen_row_by_schema(schema, 12, "empty_second", profile=[]),
            20: gen_row_by_schema(schema, 20, "one_third", profile=profile(20, 1)),
            21: gen_row_by_schema(schema, 21, "four_third", profile=profile(21, 4)),
        }
        for batch_ids in ([0, 1, 2, 3, 4, 5], [10, 11, 12], [20, 21]):
            result, _ = self.insert(client, collection_name, [source_by_id[row_id] for row_id in batch_ids])
            assert result["insert_count"] == len(batch_ids)
            self.flush(client, collection_name)

        self.delete(client, collection_name, ids=[5])
        del source_by_id[5]
        replacement = gen_row_by_schema(schema, 4, "replaced", profile=profile(4, 3))
        self.upsert(client, collection_name, [replacement])
        source_by_id[4] = replacement
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        non_empty_rows = [row for row in source_by_id.values() if row[STRUCT_FIELD]]
        valid_element_keys = {
            (row[PK_FIELD], offset) for row in non_empty_rows for offset in range(len(row[STRUCT_FIELD]))
        }
        element_query = np.asarray(non_empty_rows[0][STRUCT_FIELD][0][element_vector_subfield])
        element_limit = min(10, len(valid_element_keys))
        exact_element_keys = [
            (row_id, offset)
            for row_id, offset, _ in sorted(
                (
                    (
                        row[PK_FIELD],
                        offset,
                        float(np.dot(element_query, np.asarray(element[element_vector_subfield]))),
                    )
                    for row in non_empty_rows
                    for offset, element in enumerate(row[STRUCT_FIELD])
                ),
                key=lambda item: item[2],
                reverse=True,
            )[:element_limit]
        ]
        embedding_query_vectors = [np.asarray(element[VECTOR_SUBFIELD]) for element in non_empty_rows[0][STRUCT_FIELD]]
        embedding_limit = min(5, len(non_empty_rows))
        exact_embedding_ids = [
            row[PK_FIELD]
            for row in sorted(
                non_empty_rows,
                key=lambda row: sum(
                    max(
                        float(np.dot(query_vector, np.asarray(element[VECTOR_SUBFIELD])))
                        for element in row[STRUCT_FIELD]
                    )
                    for query_vector in embedding_query_vectors
                ),
                reverse=True,
            )[:embedding_limit]
        ]

        def collect_observations():
            rows, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=len(source_by_id),
            )
            rows_by_id = {row[PK_FIELD]: row for row in rows}
            assert set(rows_by_id) == set(source_by_id)
            for row_id, expected in source_by_id.items():
                assert rows_by_id[row_id][TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(rows_by_id[row_id][STRUCT_FIELD], expected[STRUCT_FIELD])

            match_rows, _ = self.query(
                client,
                collection_name,
                filter=f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 0)",
                output_fields=[PK_FIELD],
                limit=len(source_by_id),
            )
            contains_rows, _ = self.query(
                client,
                collection_name,
                filter=f'array_contains({STRUCT_TAG_FIELD}, "keep_4_1")',
                output_fields=[PK_FIELD],
                limit=len(source_by_id),
            )
            element_results, _ = self.search(
                client,
                collection_name,
                data=[element_query.tolist()],
                anns_field=struct_element_vector_field,
                search_params={"metric_type": "COSINE", "params": {"ef": 128}},
                filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 0)",
                output_fields=[PK_FIELD],
                limit=element_limit,
            )
            query_tensor = EmbeddingList()
            for query_vector in embedding_query_vectors:
                query_tensor.add(query_vector.tolist())
            embedding_results, _ = self.search(
                client,
                collection_name,
                data=[query_tensor],
                anns_field=STRUCT_VECTOR_FIELD,
                search_params={"metric_type": STRUCT_VECTOR_HNSW_METRIC_TYPE, "params": {"ef": 128}},
                output_fields=[PK_FIELD],
                limit=embedding_limit,
            )
            element_keys = [(hit[PK_FIELD], hit["offset"]) for hit in element_results[0]]
            assert set(element_keys).issubset(valid_element_keys)
            assert_ann_recall(element_keys, exact_element_keys)
            embedding_ids = [hit[PK_FIELD] for hit in embedding_results[0]]
            assert set(embedding_ids).issubset({row[PK_FIELD] for row in non_empty_rows}), (
                f"embedding-list search returned a null or empty Struct row: {embedding_ids}"
            )
            assert_ann_recall(embedding_ids, exact_embedding_ids)
            return {
                "projection": sorted(
                    (row_id, rows_by_id[row_id][TAG_FIELD], rows_by_id[row_id][STRUCT_FIELD]) for row_id in rows_by_id
                ),
                "match_ids": sorted(row[PK_FIELD] for row in match_rows),
                "contains_ids": sorted(row[PK_FIELD] for row in contains_rows),
            }

        baseline = collect_observations()
        assert baseline["match_ids"] == sorted(row_id for row_id, row in source_by_id.items() if row[STRUCT_FIELD])
        assert baseline["contains_ids"] == [4]
        compact_id, _ = self.compact(client, collection_name)
        assert compact_id > 0
        assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
        assert collect_observations() == baseline

        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        assert collect_observations() == baseline

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "template, params, inline, include_offset, check_hybrid",
        [
            pytest.param(
                f"MATCH_LEAST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= {{min_score}}, threshold=1)",
                {"min_score": 10},
                f"MATCH_LEAST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 10, threshold=1)",
                False,
                True,
                marks=pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/51416", strict=True),
                id="match_int",
            ),
            pytest.param(
                f"array_contains_any({STRUCT_TAG_FIELD}, {{tags}})",
                {"tags": ["blue"]},
                f'array_contains_any({STRUCT_TAG_FIELD}, ["blue"])',
                False,
                False,
                id="contains_any_list",
            ),
            pytest.param(
                f"array_contains_all({STRUCT_TAG_FIELD}, {{tags}})",
                {"tags": []},
                f"array_contains_all({STRUCT_TAG_FIELD}, [])",
                False,
                False,
                marks=pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/51416", strict=True),
                id="contains_all_empty_list",
            ),
        ],
    )
    def test_struct_array_filter_template_matches_inline_expression(
        self,
        template,
        params,
        inline,
        include_offset,
        check_hybrid,
    ):
        """
        target: verify filter templates compose with Struct element syntax and nested scalar indexes
        method: compare inline and parameterized MATCH and contains expressions through query,
            normal-vector search, and hybrid search, including scalar, string, list, and empty-list parameters
        expected: template and inline forms return identical PK/offset results; incompatible parameter types fail
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_filter_template")
        schema = gen_struct_array_schema(self, client, include_vector_subfield=False)
        index_params = gen_index_params(self, client)
        index_params.add_index(VECTOR_FIELD, index_type="HNSW", metric_type="L2", params=INDEX_PARAMS)
        index_params.add_index(STRUCT_INT_FIELD, index_type="STL_SORT")
        index_params.add_index(STRUCT_TAG_FIELD, index_type="BITMAP")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        rows = [
            gen_row_by_schema(schema, 0, "null", profile=None),
            gen_row_by_schema(schema, 1, "empty", profile=[]),
            gen_row_by_schema(
                schema,
                2,
                "red",
                profile=[{INT_SUBFIELD: 5, TAG_SUBFIELD: "red"}],
            ),
            gen_row_by_schema(
                schema,
                3,
                "mixed",
                profile=[
                    {INT_SUBFIELD: 10, TAG_SUBFIELD: "blue"},
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "red"},
                ],
            ),
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        failures = []
        inline_query = client.query(
            collection_name,
            filter=inline,
            output_fields=[PK_FIELD],
            limit=len(rows) * STRUCT_MAX_CAPACITY,
        )
        try:
            template_query = client.query(
                collection_name,
                filter=template,
                filter_params=params,
                output_fields=[PK_FIELD],
                limit=len(rows) * STRUCT_MAX_CAPACITY,
            )
            key = (lambda row: (row[PK_FIELD], row["offset"])) if include_offset else (lambda row: row[PK_FIELD])
            assert sorted(map(key, template_query)) == sorted(map(key, inline_query))
        except Exception as exc:
            failures.append(f"query failed: {exc}")

        inline_search = client.search(
            collection_name,
            data=[rows[-1][VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            filter=inline,
            output_fields=[PK_FIELD],
            limit=len(rows),
        )[0]
        try:
            template_search = client.search(
                collection_name,
                data=[rows[-1][VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                filter=template,
                filter_params=params,
                output_fields=[PK_FIELD],
                limit=len(rows),
            )[0]
            assert [hit[PK_FIELD] for hit in template_search] == [hit[PK_FIELD] for hit in inline_search]
        except Exception as exc:
            failures.append(f"search failed: {exc}")

        if check_hybrid:
            template_request = AnnSearchRequest(
                data=[rows[-1][VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                param=NORMAL_VECTOR_SEARCH_PARAMS,
                limit=len(rows),
                expr=template,
                expr_params=params,
            )
            inline_request = AnnSearchRequest(
                data=[rows[-1][VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                param=NORMAL_VECTOR_SEARCH_PARAMS,
                limit=len(rows),
                expr=inline,
            )
            inline_hybrid = client.hybrid_search(
                collection_name,
                [inline_request],
                ranker=WeightedRanker(1.0),
                limit=len(rows),
                output_fields=[PK_FIELD],
            )[0]
            try:
                template_hybrid = client.hybrid_search(
                    collection_name,
                    [template_request],
                    ranker=WeightedRanker(1.0),
                    limit=len(rows),
                    output_fields=[PK_FIELD],
                )[0]
                assert [hit[PK_FIELD] for hit in template_hybrid] == [hit[PK_FIELD] for hit in inline_hybrid]
            except Exception as exc:
                failures.append(f"hybrid search failed: {exc}")

            with pytest.raises(Exception):
                client.query(
                    collection_name,
                    filter=f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= {{min_score}})",
                    filter_params={"min_score": "not-an-integer"},
                    output_fields=[PK_FIELD],
                )

        assert not failures, "\n".join(failures)

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_dynamic_field_name_isolation(self):
        """
        target: verify dynamic JSON keys remain isolated from same-name Struct child fields
        method: insert top-level dynamic keys named like a Struct child, query both namespaces, build a nested index,
            and submit a Struct element containing an unknown child key
        expected: dynamic projection/filter and Struct projection/filter do not overwrite each other; unknown child fails
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_dynamic_isolation")
        schema = gen_struct_array_schema(
            self,
            client,
            include_vector_subfield=False,
            enable_dynamic_field=True,
        )
        index_params = gen_index_params(self, client)
        index_params.add_index(VECTOR_FIELD, index_type="HNSW", metric_type="L2", params=INDEX_PARAMS)
        index_params.add_index(STRUCT_TAG_FIELD, index_type="BITMAP")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        rows = [
            {
                PK_FIELD: 0,
                VECTOR_FIELD: gen_vector(0),
                TAG_FIELD: "row_0",
                TAG_SUBFIELD: "dynamic_top",
                "shadow": "dynamic_shadow",
                STRUCT_FIELD: [{INT_SUBFIELD: 10, TAG_SUBFIELD: "struct_value"}],
            },
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "row_1",
                TAG_SUBFIELD: "dynamic_other",
                "shadow": "other_shadow",
                STRUCT_FIELD: [],
            },
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        projected = self.query(
            client,
            collection_name,
            filter=f'{TAG_SUBFIELD} == "dynamic_top"',
            output_fields=[PK_FIELD, TAG_SUBFIELD, "shadow", STRUCT_FIELD],
        )[0]
        assert len(projected) == 1
        assert projected[0][PK_FIELD] == 0
        assert projected[0][TAG_SUBFIELD] == "dynamic_top"
        assert projected[0]["shadow"] == "dynamic_shadow"
        assert projected[0][STRUCT_FIELD][0][TAG_SUBFIELD] == "struct_value"

        struct_rows = self.query(
            client,
            collection_name,
            filter=f'array_contains({STRUCT_TAG_FIELD}, "struct_value")',
            output_fields=[PK_FIELD, STRUCT_FIELD],
        )[0]
        assert [row[PK_FIELD] for row in struct_rows] == [0]
        assert struct_rows[0][STRUCT_FIELD][0][TAG_SUBFIELD] == "struct_value"

        with pytest.raises(Exception) as exc_info:
            client.insert(
                collection_name,
                [
                    {
                        PK_FIELD: 2,
                        VECTOR_FIELD: gen_vector(2),
                        TAG_FIELD: "row_2",
                        STRUCT_FIELD: [
                            {
                                INT_SUBFIELD: 20,
                                TAG_SUBFIELD: "known",
                                "unknown_child": "must_not_become_dynamic",
                            }
                        ],
                    }
                ],
            )
        assert "unexpected fields" in str(exc_info.value)
        count = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert count[0]["count(*)"] == len(rows)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_schema_nullable_propagation(self):
        """
        target: test schema nullable propagation after dynamically adding a struct array field
        method: add a nullable struct array field with scalar and vector sub-fields, then describe collection
        expected: public describe_collection output shows the nullable Struct Array field and its sub-fields
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_schema_nullable")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        describe_info, _ = self.describe_collection(client, collection_name)
        profile_field = next(field for field in describe_info["fields"] if field["name"] == STRUCT_FIELD)
        assert profile_field["nullable"] is True
        assert profile_field["type"] == DataType.ARRAY
        assert profile_field["element_type"] == DataType.STRUCT
        assert profile_field["params"]["max_capacity"] == STRUCT_MAX_CAPACITY
        user_sub_fields = {field["name"]: field for field in profile_field["struct_fields"]}
        assert set(user_sub_fields) == {INT_SUBFIELD, TAG_SUBFIELD, VECTOR_SUBFIELD}
        assert user_sub_fields[INT_SUBFIELD]["type"] == DataType.INT64
        assert user_sub_fields[TAG_SUBFIELD]["type"] == DataType.VARCHAR
        assert user_sub_fields[TAG_SUBFIELD]["params"]["max_length"] == TAG_MAX_LENGTH
        assert user_sub_fields[VECTOR_SUBFIELD]["type"] == DataType.FLOAT_VECTOR
        assert user_sub_fields[VECTOR_SUBFIELD]["params"]["dim"] == VECTOR_DIM

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_schema_nullable_propagation(self):
        """
        target: test schema nullable propagation when creating a nullable struct array field
        method: create a collection with a nullable struct array field containing scalar and vector sub-fields,
            then describe collection
        expected: public describe_collection output shows the nullable Struct Array field and its sub-fields
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_schema_nullable")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        describe_info, _ = self.describe_collection(client, collection_name)
        profile_field = next(field for field in describe_info["fields"] if field["name"] == STRUCT_FIELD)
        assert profile_field["nullable"] is True
        assert profile_field["type"] == DataType.ARRAY
        assert profile_field["element_type"] == DataType.STRUCT
        assert profile_field["params"]["max_capacity"] == STRUCT_MAX_CAPACITY
        user_sub_fields = {field["name"]: field for field in profile_field["struct_fields"]}
        assert set(user_sub_fields) == {INT_SUBFIELD, TAG_SUBFIELD, VECTOR_SUBFIELD}
        assert user_sub_fields[INT_SUBFIELD]["type"] == DataType.INT64
        assert user_sub_fields[TAG_SUBFIELD]["type"] == DataType.VARCHAR
        assert user_sub_fields[TAG_SUBFIELD]["params"]["max_length"] == TAG_MAX_LENGTH
        assert user_sub_fields[VECTOR_SUBFIELD]["type"] == DataType.FLOAT_VECTOR
        assert user_sub_fields[VECTOR_SUBFIELD]["params"]["dim"] == VECTOR_DIM

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rows = [
            {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"},
            {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "empty_profile", STRUCT_FIELD: []},
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: gen_scalar_profile(2),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: gen_scalar_profile(3),
            },
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD,
            index_type="HNSW",
            metric_type=NORMAL_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: row.get(STRUCT_FIELD)} for row in rows}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 20)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in filter_results} == {2}
        for row in filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(2)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rows = [
            {
                PK_FIELD: 0,
                VECTOR_FIELD: gen_vector(0),
                TAG_FIELD: "explicit_null_profile",
                STRUCT_FIELD: None,
            },
            {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "omitted_profile"},
            {PK_FIELD: 2, VECTOR_FIELD: gen_vector(2), TAG_FIELD: "empty_profile", STRUCT_FIELD: []},
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile",
                STRUCT_FIELD: gen_scalar_profile(3),
            },
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: row.get(STRUCT_FIELD)} for row in rows}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 30)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in filter_results} == {3}
        for row in filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(3)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0][PK_FIELD] == 3
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rows = [
            {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "initial_null_profile"},
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "initial_non_null_profile",
                STRUCT_FIELD: gen_scalar_profile(1),
            },
            {PK_FIELD: 2, VECTOR_FIELD: gen_vector(2), TAG_FIELD: "initial_empty_profile", STRUCT_FIELD: []},
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: row.get(STRUCT_FIELD)} for row in rows}
        null_to_non_null = {
            PK_FIELD: 0,
            VECTOR_FIELD: gen_vector(100),
            TAG_FIELD: "upserted_null_to_non_null",
            STRUCT_FIELD: gen_scalar_profile(100),
        }
        non_null_to_null = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(101),
            TAG_FIELD: "upserted_non_null_to_null",
        }
        new_omitted_profile = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(103),
            TAG_FIELD: "upserted_new_omit_profile",
        }
        res, _ = self.upsert(client, collection_name, [null_to_non_null, non_null_to_null, new_omitted_profile])
        assert res["upsert_count"] == 3

        source_by_id[null_to_non_null[PK_FIELD]] = null_to_non_null
        source_by_id[non_null_to_null[PK_FIELD]] = {**non_null_to_null, STRUCT_FIELD: None}
        source_by_id[new_omitted_profile[PK_FIELD]] = {**new_omitted_profile, STRUCT_FIELD: None}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 1000)",
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {null_to_non_null[PK_FIELD]}
        assert_scalar_profile_equal(element_filter_results[0][STRUCT_FIELD], null_to_non_null[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[non_null_to_null[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0][PK_FIELD] == non_null_to_null[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rows = [
            {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile_0"},
            {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "empty_profile", STRUCT_FIELD: []},
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: gen_scalar_profile(2),
            },
            {PK_FIELD: 3, VECTOR_FIELD: gen_vector(3), TAG_FIELD: "missing_profile_3"},
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "present_profile_4",
                STRUCT_FIELD: gen_scalar_profile(4),
            },
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: row.get(STRUCT_FIELD)} for row in rows}

        iterator, _ = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            consistency_level="Strong",
        )
        iterator_rows = drain_iterator(iterator)
        iterator_ids = [row[PK_FIELD] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rows = [
            {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile_0"},
            {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "empty_profile", STRUCT_FIELD: []},
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: gen_scalar_profile(2),
            },
            {PK_FIELD: 3, VECTOR_FIELD: gen_vector(3), TAG_FIELD: "missing_profile_3"},
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "present_profile_4",
                STRUCT_FIELD: gen_scalar_profile(4),
            },
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: row.get(STRUCT_FIELD)} for row in rows}

        iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[gen_vector(4)],
            batch_size=2,
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        iterator_hits = drain_iterator(iterator)
        hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        sealed_explicit_null_profile_row = {
            PK_FIELD: 0,
            VECTOR_FIELD: gen_vector(0),
            TAG_FIELD: "sealed_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        sealed_omitted_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            PK_FIELD: 2,
            VECTOR_FIELD: gen_vector(2),
            TAG_FIELD: "sealed_empty_profile",
            STRUCT_FIELD: [],
        }
        sealed_one_match_profile_row = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "sealed_one_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 9100, TAG_SUBFIELD: "match_9100"},
                {INT_SUBFIELD: 100, TAG_SUBFIELD: "low_100"},
            ],
        }
        sealed_two_match_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "sealed_two_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 9200, TAG_SUBFIELD: "match_9200"},
                {INT_SUBFIELD: 9300, TAG_SUBFIELD: "match_9300"},
            ],
        }
        sealed_zero_match_profile_row = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "sealed_zero_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 100, TAG_SUBFIELD: "low_100"},
                {INT_SUBFIELD: 200, TAG_SUBFIELD: "low_200"},
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
        sealed_index_filler_rows = gen_scalar_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_match_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, _ = self.insert(client, collection_name, sealed_rows)
        assert res["insert_count"] == len(sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        res, _ = self.load_collection(client, collection_name)

        growing_explicit_null_profile_row = {
            PK_FIELD: 7000,
            VECTOR_FIELD: gen_vector(7000),
            TAG_FIELD: "growing_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        growing_empty_profile_row = {
            PK_FIELD: 7001,
            VECTOR_FIELD: gen_vector(7001),
            TAG_FIELD: "growing_empty_profile",
            STRUCT_FIELD: [],
        }
        growing_two_match_profile_row = {
            PK_FIELD: 7002,
            VECTOR_FIELD: gen_vector(7002),
            TAG_FIELD: "growing_two_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 9600, TAG_SUBFIELD: "match_9600"},
                {INT_SUBFIELD: 9700, TAG_SUBFIELD: "match_9700"},
            ],
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_empty_profile_row,
            growing_two_match_profile_row,
        ]
        res, _ = self.insert(client, collection_name, growing_rows)
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {}
        source_by_id[sealed_explicit_null_profile_row[PK_FIELD]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row[PK_FIELD]] = {**sealed_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[sealed_empty_profile_row[PK_FIELD]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row[PK_FIELD]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row[PK_FIELD]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row[PK_FIELD]] = sealed_zero_match_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row[PK_FIELD]] = growing_explicit_null_profile_row
        source_by_id[growing_empty_profile_row[PK_FIELD]] = growing_empty_profile_row
        source_by_id[growing_two_match_profile_row[PK_FIELD]] = growing_two_match_profile_row

        match_any_ids = {
            sealed_one_match_profile_row[PK_FIELD],
            sealed_two_match_profile_row[PK_FIELD],
            growing_two_match_profile_row[PK_FIELD],
        }
        match_two_or_more_ids = {
            sealed_two_match_profile_row[PK_FIELD],
            growing_two_match_profile_row[PK_FIELD],
        }
        match_exact_one_ids = {sealed_one_match_profile_row[PK_FIELD]}
        non_nullish_scope = [
            sealed_one_match_profile_row[PK_FIELD],
            sealed_two_match_profile_row[PK_FIELD],
            sealed_zero_match_profile_row[PK_FIELD],
            growing_two_match_profile_row[PK_FIELD],
        ]

        match_any_results, _ = self.query(
            client,
            collection_name,
            filter=f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in match_any_results} == match_any_ids
        for row in match_any_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_least_results, _ = self.query(
            client,
            collection_name,
            filter=f"MATCH_LEAST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=2)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in match_least_results} == match_two_or_more_ids
        for row in match_least_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_exact_results, _ = self.query(
            client,
            collection_name,
            filter=f"MATCH_EXACT({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=1)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in match_exact_results} == match_exact_one_ids
        for row in match_exact_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_all_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} in {non_nullish_scope} && MATCH_ALL({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(non_nullish_scope),
        )
        assert {row[PK_FIELD] for row in match_all_results} == match_two_or_more_ids
        for row in match_all_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_most_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} in {non_nullish_scope} && MATCH_MOST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=1)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(non_nullish_scope),
        )
        assert {row[PK_FIELD] for row in match_most_results} == {
            sealed_one_match_profile_row[PK_FIELD],
            sealed_zero_match_profile_row[PK_FIELD],
        }
        for row in match_most_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[growing_two_match_profile_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            filter=f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=10,
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == match_any_ids
        assert search_results[0][0][PK_FIELD] == growing_two_match_profile_row[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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
        fixture = gen_expression_fixture(self, client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]

        expressions = [
            f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            f'element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000 && $[{TAG_SUBFIELD}] == "match_9100")',
            f'element_filter({STRUCT_FIELD}, $[{TAG_SUBFIELD}] == "match_9700")',
        ]
        for expr in expressions:
            expected_rows = gt_nullable_scalar_struct_expression_rows(source_rows, expr)
            actual_rows, _ = self.query(
                client,
                collection_name,
                filter=expr,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=len(expected_rows) + 5,
            )
            assert gt_expression_result_keys(actual_rows) == gt_expression_result_keys(expected_rows)
            assert_expression_rows_match_source(actual_rows, source_by_id)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_match_family_null_semantics(self):
        """
        target: test MATCH family null/empty semantics for a nullable scalar Struct Array
        method: create nullable scalar Struct Array rows covering explicit null, omitted, empty, one-match,
            two-match, and zero-match profiles in both sealed and growing segments, then query every MATCH operator
        expected: expected result sets are computed from source data and expression; MATCH_ANY/LEAST only return rows
            with enough matching elements, null/omitted profiles evaluate to unknown, and MATCH_ALL/MOST/EXACT treat
            empty profiles as zero-element inputs
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_match_null_expr")
        client = self._client()
        fixture = gen_expression_fixture(self, client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"{PK_FIELD} in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            f"MATCH_LEAST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=2)",
            f"MATCH_EXACT({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=1)",
            f"MATCH_ALL({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            f"MATCH_MOST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=1)",
            f"MATCH_EXACT({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=0)",
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = gt_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            actual_rows, _ = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=len(fixture["controlled_ids"]),
            )
            assert gt_expression_result_keys(actual_rows) == gt_expression_result_keys(expected_rows)
            assert_expression_rows_match_source(actual_rows, source_by_id)

        search_expr = f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)"
        expected_search_ids = {
            row[PK_FIELD] for row in gt_nullable_scalar_struct_expression_rows(source_rows, search_expr)
        }
        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(7004)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            filter=search_expr,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(expected_search_ids),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == expected_search_ids
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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
        fixture = gen_expression_fixture(self, client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"{PK_FIELD} in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            f"array_length({STRUCT_INT_FIELD}) == 0",
            f"array_length({STRUCT_INT_FIELD}) > 0",
            f'array_contains({STRUCT_TAG_FIELD}, "match_9100")',
            f'array_contains_all({STRUCT_TAG_FIELD}, ["match_9200", "match_9300"])',
            f'array_contains_any({STRUCT_TAG_FIELD}, ["match_9600", "missing"])',
            f"{STRUCT_FIELD}[0][{INT_SUBFIELD}] >= 9000",
            f'{STRUCT_FIELD}[1][{TAG_SUBFIELD}] == "match_9800"',
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = gt_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            expected_ids = {row[PK_FIELD] for row in expected_rows}

            actual_rows, _ = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=len(fixture["controlled_ids"]),
            )
            assert gt_expression_result_keys(actual_rows) == gt_expression_result_keys(expected_rows)
            assert_expression_rows_match_source(actual_rows, source_by_id)

            search_results, _ = self.search(
                client,
                collection_name,
                data=[gen_vector(7004)],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                filter=scoped_expr,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=max(len(expected_ids), 1),
            )
            assert {hit[PK_FIELD] for hit in search_results[0]} == expected_ids
            for hit in search_results[0]:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/51381", strict=True)
    def test_nullable_struct_array_index_preserves_null_validity(self):
        """
        target: test nullable struct-array predicates before and after indexing every scalar struct subfield
        method: query NOT array_contains, empty array_contains_all, and MATCH_ANY on controlled null, empty,
            and non-empty rows; then release, create nested BITMAP indexes, reload, and repeat query/search
        expected: NULL and omitted rows stay excluded under NOT and empty contains-all, raw/indexed results match,
            and MATCH remains executable after the index-only reload path
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_bitmap_validity")
        client = self._client()
        fixture = gen_expression_fixture(self, client, collection_name)
        source_by_id = fixture["source_by_id"]
        controlled_ids = sorted(fixture["controlled_ids"])
        scoped_prefix = f"{PK_FIELD} in {controlled_ids} && "

        real_array_ids = {row_id for row_id in controlled_ids if source_by_id[row_id][STRUCT_FIELD] is not None}
        not_contains_ids = {
            row_id
            for row_id in real_array_ids
            if all(element[TAG_SUBFIELD] != "match_9100" for element in source_by_id[row_id][STRUCT_FIELD])
        }
        match_any_ids = {
            row_id
            for row_id in real_array_ids
            if any(element[TAG_SUBFIELD] == "match_9100" for element in source_by_id[row_id][STRUCT_FIELD])
        }

        expressions = {
            "not_contains": f'not array_contains({STRUCT_TAG_FIELD}, "match_9100")',
            "empty_contains_all": f"array_contains_all({STRUCT_TAG_FIELD}, [])",
            "match_any": f'MATCH_ANY({STRUCT_FIELD}, $[{TAG_SUBFIELD}] == "match_9100")',
        }

        def collect_evidence():
            evidence = {}
            for name, expression in expressions.items():
                rows, _ = self.query(
                    client,
                    collection_name,
                    filter=scoped_prefix + expression,
                    output_fields=[PK_FIELD],
                    limit=len(controlled_ids),
                )
                evidence[f"query_{name}"] = {row[PK_FIELD] for row in rows}

            search_results, _ = self.search(
                client,
                collection_name,
                data=[gen_vector(7004)],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                filter=scoped_prefix + expressions["empty_contains_all"],
                output_fields=[PK_FIELD],
                limit=len(controlled_ids),
            )
            evidence["search_empty_contains_all"] = {hit[PK_FIELD] for hit in search_results[0]}
            return evidence

        raw_evidence = collect_evidence()

        res, _ = self.flush(client, collection_name)
        res, _ = self.release_collection(client, collection_name)
        nested_index_params = gen_index_params(self, client)
        nested_index_params.add_index(field_name=STRUCT_INT_FIELD, index_type="BITMAP")
        nested_index_params.add_index(field_name=STRUCT_TAG_FIELD, index_type="BITMAP")
        res, _ = self.create_index(client, collection_name, nested_index_params)
        assert self.wait_for_index_ready(client, collection_name, STRUCT_INT_FIELD, timeout=300)
        assert self.wait_for_index_ready(client, collection_name, STRUCT_TAG_FIELD, timeout=300)
        res, _ = self.load_collection(client, collection_name)

        indexed_evidence = collect_evidence()
        expected_evidence = {
            "query_not_contains": not_contains_ids,
            "query_empty_contains_all": real_array_ids,
            "query_match_any": match_any_ids,
            "search_empty_contains_all": real_array_ids,
        }
        assert {
            "raw": raw_evidence,
            "indexed": indexed_evidence,
        } == {
            "raw": expected_evidence,
            "indexed": expected_evidence,
        }

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_parent_null_expression(self):
        """
        target: test direct null expressions on a nullable scalar Struct Array parent
        method: query `profile is null` and `profile is not null` expressions on rows
            covering explicit null, omitted, empty, and non-empty profiles
        expected: expected result sets are computed from source data and expression; null expressions distinguish
            null/omitted rows from empty/non-empty rows
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_parent_null_expr")
        client = self._client()
        fixture = gen_expression_fixture(self, client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"{PK_FIELD} in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            f"{STRUCT_FIELD} is null",
            f"{STRUCT_FIELD} is not null",
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = gt_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            actual_rows, _ = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=len(fixture["controlled_ids"]),
            )
            assert gt_expression_result_keys(actual_rows) == gt_expression_result_keys(expected_rows)
            assert_expression_rows_match_source(actual_rows, source_by_id)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_element_filter_search(self):
        """
        target: test normal-vector search rejects element_filter on a nullable scalar Struct Array
        method: create >=3000 sealed rows plus growing rows with null/omitted/empty/non-empty profiles, then search
            normal_vector with `element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)`
        expected: normal-vector search returns a clear error because row-level vector search is incompatible with
            element-level filtering
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_element_filter_search")
        client = self._client()
        fixture = gen_expression_fixture(self, client, collection_name)
        expr = f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)"
        error = {ct.err_code: 1100, ct.err_msg: "element_filter"}

        self.search(
            client,
            collection_name,
            data=[gen_vector(7004)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            filter=expr,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(fixture["controlled_ids"]),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rows = [
            {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"},
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "present_profile",
                STRUCT_FIELD: gen_profile(1),
            },
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: row.get(STRUCT_FIELD)} for row in rows}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(1)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        empty_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {missing_profile_row[PK_FIELD]: {**missing_profile_row, STRUCT_FIELD: None}}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        normal_search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(3)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in normal_search_results[0]} == set(source_by_id)
        for hit in normal_search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD])
        expected_struct_search_rows = [non_empty_rows[-1]]
        struct_search_results, _ = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params=STRUCT_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(expected_struct_search_rows),
        )
        assert len(struct_search_results[0]) == len(expected_struct_search_rows)
        hit_ids = {hit[PK_FIELD] for hit in struct_search_results[0]}
        assert hit_ids == {row[PK_FIELD] for row in expected_struct_search_rows}
        assert missing_profile_row[PK_FIELD] not in hit_ids
        assert empty_profile_row[PK_FIELD] not in hit_ids
        assert struct_search_results[0][0][PK_FIELD] == expected_struct_search_rows[0][PK_FIELD]
        for hit in struct_search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_diskann_parent_output(self):
        """
        target: test sealed nullable struct array parent output when vector sub-field uses DISKANN
        method: create a nullable struct array with a vector sub-field, flush enough rows for DISKANN, then query
            id-only and id + parent struct array with the same MATCH_ANY filter
        expected: both queries succeed and parent struct array output matches source rows
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_diskann_output")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        empty_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: gen_profile(2),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: gen_profile(3),
            },
        ]
        control_rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        filler_rows = gen_vector_index_filler_rows(
            10000,
            self.min_index_sealed_rows - len(control_rows),
            "sealed_diskann_output_filler",
            vector_dim=VECTOR_DIM,
        )
        sealed_rows = control_rows + filler_rows
        res, _ = self.insert(client, collection_name, sealed_rows)
        assert res["insert_count"] == len(sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD,
            index_type=NORMAL_VECTOR_INDEX_TYPE,
            metric_type=NORMAL_VECTOR_METRIC_TYPE,
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_DISKANN_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_DISKANN_METRIC_TYPE,
            params=STRUCT_VECTOR_DISKANN_INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, STRUCT_VECTOR_FIELD, timeout=300)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: row for row in non_empty_rows}
        filter_expr = f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 0)"
        id_only_results, _ = self.query(
            client,
            collection_name,
            filter=filter_expr,
            output_fields=[PK_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in id_only_results} == set(source_by_id)
        assert all(set(row) == {PK_FIELD} for row in id_only_results)

        parent_results, _ = self.query(
            client,
            collection_name,
            filter=filter_expr,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in parent_results} == set(source_by_id)
        for row in parent_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=dim)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=dim)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        empty_profile_row = {
            PK_FIELD: 0,
            VECTOR_FIELD: gen_vector(0, dim),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        present_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1, dim),
            TAG_FIELD: "present_single_profile",
            STRUCT_FIELD: [
                {
                    INT_SUBFIELD: 10,
                    TAG_SUBFIELD: "profile_1_0",
                    VECTOR_SUBFIELD: gen_vector(10, dim),
                }
            ],
        }
        rows = [empty_profile_row, present_profile_row]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: row for row in rows}
        query_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} in [0, 1]",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(rows),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} in [0, 1]",
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(rows),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=dim)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, vector_type, dim=dim)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        sealed_special_rows = [
            {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0, dim), TAG_FIELD: "sealed_omitted_profile"},
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1, dim),
                TAG_FIELD: "sealed_empty_profile",
                STRUCT_FIELD: [],
            },
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2, dim),
                TAG_FIELD: "sealed_present_profile_2",
                STRUCT_FIELD: gen_typed_profile(2, vector_type, dim),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3, dim),
                TAG_FIELD: "sealed_present_profile_3",
                STRUCT_FIELD: gen_typed_profile(3, vector_type, dim),
            },
        ]
        sealed_filler_rows = [
            {
                PK_FIELD: 10000 + i,
                VECTOR_FIELD: gen_vector(10000 + i, dim),
                TAG_FIELD: f"sealed_filler_{i}",
            }
            for i in range(self.min_index_sealed_rows - len(sealed_special_rows))
        ]
        sealed_rows = sealed_special_rows + sealed_filler_rows
        res, _ = self.insert(client, collection_name, sealed_rows)
        assert res["insert_count"] == len(sealed_rows)

        res, _ = self.flush(client, collection_name)

        profile_metric = "MAX_SIM_HAMMING" if vector_type == DataType.BINARY_VECTOR else "MAX_SIM_COSINE"
        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=profile_metric,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        growing_rows = [
            {PK_FIELD: 20000, VECTOR_FIELD: gen_vector(20000, dim), TAG_FIELD: "growing_omitted_profile"},
            {
                PK_FIELD: 20001,
                VECTOR_FIELD: gen_vector(20001, dim),
                TAG_FIELD: "growing_empty_profile",
                STRUCT_FIELD: [],
            },
            {
                PK_FIELD: 20002,
                VECTOR_FIELD: gen_vector(20002, dim),
                TAG_FIELD: "growing_present_profile",
                STRUCT_FIELD: gen_typed_profile(20002, vector_type, dim),
            },
        ]
        res, _ = self.insert(client, collection_name, growing_rows)
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {
            row[PK_FIELD]: {**row, STRUCT_FIELD: row.get(STRUCT_FIELD)} for row in sealed_rows + growing_rows
        }

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_typed_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD], vector_type)

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_typed_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD], vector_type)

        top_k = 16
        search_vector = growing_rows[-1][VECTOR_FIELD]
        expected_top_ids = [
            row[PK_FIELD]
            for row in sorted(
                source_by_id.values(),
                key=lambda row: (
                    sum((left - right) ** 2 for left, right in zip(row[VECTOR_FIELD], search_vector)),
                    row[PK_FIELD],
                ),
            )[:top_k]
        ]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_vector],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=top_k,
        )
        hits = search_results[0]
        assert [hit[PK_FIELD] for hit in hits] == expected_top_ids
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_typed_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD], vector_type)

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        empty_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: gen_profile(2),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: gen_profile(3),
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {missing_profile_row[PK_FIELD]: {**missing_profile_row, STRUCT_FIELD: None}}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})

        iterator, _ = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            consistency_level="Strong",
        )
        iterator_rows = drain_iterator(iterator)
        iterator_ids = [row[PK_FIELD] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        empty_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: gen_profile(2),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: gen_profile(3),
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {missing_profile_row[PK_FIELD]: {**missing_profile_row, STRUCT_FIELD: None}}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})

        iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[gen_vector(3)],
            batch_size=2,
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        iterator_hits = drain_iterator(iterator)
        hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=dim)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rng = random.Random(20260526)
        rows = []
        source_by_id = {}
        mask_counts = {"omitted": 0, "null": 0, "empty": 0, "present": 0}
        element_counts = {0: 0, 1: 0, 2: 0, 3: 0}
        for row_id in range(entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id, dim=dim),
                TAG_FIELD: f"stress_row_{row_id}",
            }

            choice = rng.randrange(8)
            if choice == 0:
                expected_profile = None
                mask_counts["omitted"] += 1
            elif choice == 1:
                row[STRUCT_FIELD] = None
                expected_profile = None
                mask_counts["null"] += 1
            elif choice == 2:
                row[STRUCT_FIELD] = []
                expected_profile = []
                mask_counts["empty"] += 1
                element_counts[0] += 1
            else:
                element_count = 1 + rng.randrange(3)
                row[STRUCT_FIELD] = [
                    {
                        INT_SUBFIELD: row_id * 10 + element_index,
                        TAG_SUBFIELD: f"profile_{row_id}_{element_index}",
                    }
                    for element_index in range(element_count)
                ]
                expected_profile = row[STRUCT_FIELD]
                mask_counts["present"] += 1
                element_counts[element_count] += 1

            rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: expected_profile}

        assert all(count > 0 for count in mask_counts.values())
        assert all(count > 0 for count in element_counts.values())

        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == entities

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        res, _ = self.load_collection(client, collection_name)

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_row = source_by_id[entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit[PK_FIELD] for hit in hits} == set(source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        query_iterator, _ = self.query_iterator(
            client,
            collection_name,
            batch_size=997,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            consistency_level="Strong",
        )
        iterator_rows = drain_iterator(query_iterator)
        iterator_ids = [row[PK_FIELD] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)
        for row in iterator_rows:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            batch_size=997,
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        iterator_hits = drain_iterator(search_iterator)
        iterator_hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
        assert len(iterator_hit_ids) == len(set(iterator_hit_ids))
        assert set(iterator_hit_ids) == set(source_by_id)
        iterator_distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(iterator_distances) - 1):
            assert iterator_distances[index] <= iterator_distances[index + 1] + epsilon
        for hit in iterator_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=dim)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=dim)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rng = random.Random(20260526)
        rows = []
        source_by_id = {}
        mask_counts = {"omitted": 0, "null": 0, "empty": 0, "present": 0}
        element_counts = {0: 0, 2: 0, 3: 0}
        for row_id in range(entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id, dim=dim),
                TAG_FIELD: f"stress_row_{row_id}",
            }

            choice = rng.randrange(8)
            if choice == 0:
                expected_profile = None
                mask_counts["omitted"] += 1
            elif choice == 1:
                row[STRUCT_FIELD] = None
                expected_profile = None
                mask_counts["null"] += 1
            elif choice == 2:
                row[STRUCT_FIELD] = []
                expected_profile = []
                mask_counts["empty"] += 1
                element_counts[0] += 1
            else:
                element_count = 2 + (rng.randrange(2))
                row[STRUCT_FIELD] = [
                    {
                        INT_SUBFIELD: row_id * 10 + element_index,
                        TAG_SUBFIELD: f"profile_{row_id}_{element_index}",
                        VECTOR_SUBFIELD: gen_vector(row_id * 10 + element_index, dim=dim),
                    }
                    for element_index in range(element_count)
                ]
                expected_profile = row[STRUCT_FIELD]
                mask_counts["present"] += 1
                element_counts[element_count] += 1

            rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: expected_profile}

        assert all(count > 0 for count in mask_counts.values())
        assert all(count > 0 for count in element_counts.values())

        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == entities

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        assert self.wait_for_index_ready(client, collection_name, STRUCT_VECTOR_FIELD, timeout=300)
        res, _ = self.load_collection(client, collection_name)

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_row = source_by_id[entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit[PK_FIELD] for hit in hits} == set(source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        query_iterator, _ = self.query_iterator(
            client,
            collection_name,
            batch_size=997,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            consistency_level="Strong",
        )
        iterator_rows = drain_iterator(query_iterator)
        iterator_ids = [row[PK_FIELD] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)
        for row in iterator_rows:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            batch_size=997,
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        iterator_hits = drain_iterator(search_iterator)
        iterator_hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
        assert len(iterator_hit_ids) == len(set(iterator_hit_ids))
        assert set(iterator_hit_ids) == set(source_by_id)
        iterator_distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(iterator_distances) - 1):
            assert iterator_distances[index] <= iterator_distances[index + 1] + epsilon
        for hit in iterator_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        empty_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {missing_profile_row[PK_FIELD]: {**missing_profile_row, STRUCT_FIELD: None}}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD])
        expected_regular_search_rows = [non_empty_rows[-1]]
        results, _ = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params=STRUCT_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(expected_regular_search_rows),
        )
        hits = results[0]
        hit_ids = [hit[PK_FIELD] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row[PK_FIELD] for row in expected_regular_search_rows}
        assert missing_profile_row[PK_FIELD] not in hit_ids
        assert empty_profile_row[PK_FIELD] not in hit_ids
        assert hits[0][PK_FIELD] == expected_regular_search_rows[0][PK_FIELD]

        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        error = {
            ct.err_code: 1100,
            ct.err_msg: "search iterator is not supported for multi-search-multi on embedding list fields",
        }
        self.search_iterator(
            client,
            collection_name,
            data=[search_tensor.to_flat_array()],
            batch_size=1,
            anns_field=STRUCT_VECTOR_FIELD,
            search_params=STRUCT_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        empty_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type="FLAT",
            metric_type="COSINE",
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: row for row in non_empty_rows}
        iterator_limit = 1
        null_or_empty_ids = {missing_profile_row[PK_FIELD], empty_profile_row[PK_FIELD]}

        iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            batch_size=1,
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=iterator_limit,
        )
        iterator_hits = drain_iterator(iterator)
        assert len(iterator_hits) == iterator_limit
        hit_pairs = [(hit[PK_FIELD], hit["offset"]) for hit in iterator_hits]
        expected_hit_pairs = {
            (row[PK_FIELD], offset) for row in non_empty_rows for offset, _ in enumerate(row[STRUCT_FIELD])
        }
        assert len(hit_pairs) == len(set(hit_pairs))
        assert set(hit_pairs).issubset(expected_hit_pairs)
        hit_ids = [hit_id for hit_id, _ in hit_pairs]
        assert set(hit_ids).issubset(source_by_id)
        assert not set(hit_ids).intersection(null_or_empty_ids)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] >= distances[index + 1] - epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        rows = [
            {
                PK_FIELD: 0,
                VECTOR_FIELD: gen_vector(0),
                TAG_FIELD: "present_profile_0",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 0, TAG_SUBFIELD: "profile_0_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 1, TAG_SUBFIELD: "profile_0_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "present_profile_1",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 10, TAG_SUBFIELD: "profile_1_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 11, TAG_SUBFIELD: "profile_1_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(field_name=STRUCT_VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: row for row in rows}
        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE"},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert search_results[0][0][PK_FIELD] == rows[-1][PK_FIELD]
        assert search_results[0][0]["offset"] == 0
        entity = search_entity(search_results[0][0])
        assert entity[TAG_FIELD] == rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], rows[-1][STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        non_empty_rows = [
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "present_profile_1",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 10, TAG_SUBFIELD: "profile_1_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 11, TAG_SUBFIELD: "profile_1_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {missing_profile_row[PK_FIELD]: {**missing_profile_row, STRUCT_FIELD: None}}
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})
        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(non_empty_rows),
        )
        hits = search_results[0]
        hit_ids = [hit[PK_FIELD] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row[PK_FIELD] for row in non_empty_rows}
        assert missing_profile_row[PK_FIELD] not in hit_ids
        assert hits[0][PK_FIELD] == non_empty_rows[-1][PK_FIELD]
        assert hits[0]["offset"] == 0
        entity = search_entity(hits[0])
        assert entity[TAG_FIELD] == non_empty_rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], non_empty_rows[-1][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        empty_profile_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "present_profile_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {missing_profile_row[PK_FIELD]: {**missing_profile_row, STRUCT_FIELD: None}}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})
        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(non_empty_rows),
        )
        hits = search_results[0]
        hit_ids = [hit[PK_FIELD] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row[PK_FIELD] for row in non_empty_rows}
        assert missing_profile_row[PK_FIELD] not in hit_ids
        assert empty_profile_row[PK_FIELD] not in hit_ids
        assert hits[0][PK_FIELD] == non_empty_rows[-1][PK_FIELD]
        assert hits[0]["offset"] == 0
        entity = search_entity(hits[0])
        assert entity[TAG_FIELD] == non_empty_rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], non_empty_rows[-1][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        empty_profile_row = {
            PK_FIELD: 0,
            VECTOR_FIELD: gen_vector(0),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "present_profile_1",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 10, TAG_SUBFIELD: "profile_1_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 11, TAG_SUBFIELD: "profile_1_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {empty_profile_row[PK_FIELD]: empty_profile_row}
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})
        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(non_empty_rows),
        )
        hits = search_results[0]
        hit_ids = [hit[PK_FIELD] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row[PK_FIELD] for row in non_empty_rows}
        assert empty_profile_row[PK_FIELD] not in hit_ids
        assert hits[0][PK_FIELD] == non_empty_rows[-1][PK_FIELD]
        assert hits[0]["offset"] == 0
        entity = search_entity(hits[0])
        assert entity[TAG_FIELD] == non_empty_rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], non_empty_rows[-1][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        empty_profile_row = {
            PK_FIELD: 0,
            VECTOR_FIELD: gen_vector(0),
            TAG_FIELD: "empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "present_profile_1",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 10, TAG_SUBFIELD: "profile_1_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 11, TAG_SUBFIELD: "profile_1_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [empty_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(field_name=STRUCT_VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {empty_profile_row[PK_FIELD]: empty_profile_row}
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})
        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE"},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(non_empty_rows),
        )
        hits = search_results[0]
        hit_ids = [hit[PK_FIELD] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row[PK_FIELD] for row in non_empty_rows}
        assert empty_profile_row[PK_FIELD] not in hit_ids
        assert hits[0][PK_FIELD] == non_empty_rows[-1][PK_FIELD]
        assert hits[0]["offset"] == 0
        entity = search_entity(hits[0])
        assert entity[TAG_FIELD] == non_empty_rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], non_empty_rows[-1][STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        res, _ = self.create_collection(client, collection_name, schema=schema)

        missing_profile_row = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "missing_profile"}
        non_empty_rows = [
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "present_profile_1",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 10, TAG_SUBFIELD: "profile_1_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 11, TAG_SUBFIELD: "profile_1_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "present_profile_2",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 20, TAG_SUBFIELD: "profile_2_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "profile_2_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, *non_empty_rows]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(field_name=STRUCT_VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {missing_profile_row[PK_FIELD]: {**missing_profile_row, STRUCT_FIELD: None}}
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})
        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE"},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(non_empty_rows),
        )
        hits = search_results[0]
        hit_ids = [hit[PK_FIELD] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row[PK_FIELD] for row in non_empty_rows}
        assert missing_profile_row[PK_FIELD] not in hit_ids
        assert hits[0][PK_FIELD] == non_empty_rows[-1][PK_FIELD]
        assert hits[0]["offset"] == 0
        entity = search_entity(hits[0])
        assert entity[TAG_FIELD] == non_empty_rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], non_empty_rows[-1][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)
        res, _ = self.create_alias(client, collection_name, alias)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, alias, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        for target in (collection_name, alias):
            describe_info, _ = self.describe_collection(client, target)
            profile_field = next(field for field in describe_info["fields"] if field["name"] == STRUCT_FIELD)
            assert profile_field["nullable"] is True
            assert profile_field["type"] == DataType.ARRAY
            assert profile_field["element_type"] == DataType.STRUCT
            assert {field["name"] for field in profile_field["struct_fields"]} == {INT_SUBFIELD, TAG_SUBFIELD}

        row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "inserted_by_alias",
            STRUCT_FIELD: gen_scalar_profile(1),
        }
        res, _ = self.insert(client, alias, [row])
        assert res["insert_count"] == 1

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        for target in (collection_name, alias):
            query_results, _ = self.query(
                client,
                target,
                filter=f"{PK_FIELD} == 1",
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=1,
            )
            assert len(query_results) == 1
            assert query_results[0][PK_FIELD] == row[PK_FIELD]
            assert query_results[0][TAG_FIELD] == row[TAG_FIELD]
            assert_scalar_profile_equal(query_results[0][STRUCT_FIELD], row[STRUCT_FIELD])

        res, _ = self.drop_alias(client, alias)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_non_nullable_rejected(self):
        """
        target: test non-nullable struct array validation when dynamically adding a struct array field
        method: try to add a struct array field with nullable=False through MilvusClient
        expected: PyMilvus rejects the request before RPC, and collection schema remains unchanged
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_non_nullable")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        error = {
            ct.err_code: 1,
            ct.err_msg: "Adding struct field to existing collection requires nullable=True",
        }
        self.add_collection_struct_field(
            client,
            collection_name,
            STRUCT_FIELD,
            profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=False,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, _ = self.describe_collection(client, collection_name)
        assert STRUCT_FIELD not in {field["name"] for field in describe_info["fields"]}

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        duplicate_profile_schema = gen_struct_schema(self, client)
        duplicate_profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name profile"}
        self.add_collection_struct_field(
            client,
            collection_name,
            STRUCT_FIELD,
            duplicate_profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        conflict_regular_field_schema = gen_struct_schema(self, client)
        conflict_regular_field_schema.add_field(TAG_SUBFIELD, DataType.VARCHAR, max_length=TAG_MAX_LENGTH)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name normal_vector"}
        self.add_collection_struct_field(
            client,
            collection_name,
            VECTOR_FIELD,
            conflict_regular_field_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, _ = self.describe_collection(client, collection_name)
        assert [field["name"] for field in describe_info["fields"]].count(STRUCT_FIELD) == 1
        profile_field = next(field for field in describe_info["fields"] if field["name"] == STRUCT_FIELD)
        assert {field["name"] for field in profile_field["struct_fields"]} == {INT_SUBFIELD}

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        res, _ = self.add_collection_struct_field(
            client,
            collection_name,
            STRUCT_FIELD,
            profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
        )

        append_subfield_schema = gen_struct_schema(self, client)
        append_subfield_schema.add_field(TAG_SUBFIELD, DataType.VARCHAR, max_length=TAG_MAX_LENGTH)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name profile"}
        self.add_collection_struct_field(
            client,
            collection_name,
            STRUCT_FIELD,
            append_subfield_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, _ = self.describe_collection(client, collection_name)
        profile_field = next(field for field in describe_info["fields"] if field["name"] == STRUCT_FIELD)
        assert {field["name"] for field in profile_field["struct_fields"]} == {INT_SUBFIELD}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_duplicate_subfield_name_rejected(self):
        """
        target: test duplicate sub-field name validation when dynamically adding a struct array field
        method: add a struct array field whose struct schema contains duplicate sub-field names
        expected: duplicate sub-field names are rejected, and no struct field is added to the collection
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_dup_subfield")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(INT_SUBFIELD, DataType.VARCHAR, max_length=TAG_MAX_LENGTH)
        error = {
            ct.err_code: 1,
            ct.err_msg: "Duplicate field names in struct 'profile'",
        }
        self.add_collection_struct_field(
            client,
            collection_name,
            STRUCT_FIELD,
            profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, _ = self.describe_collection(client, collection_name)
        assert STRUCT_FIELD not in {field["name"] for field in describe_info["fields"]}

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, DataType.INT64, **field_kwargs)
        error = {ct.err_code: 1, ct.err_msg: expected_msg}
        self.add_collection_struct_field(
            client,
            collection_name,
            STRUCT_FIELD,
            profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, _ = self.describe_collection(client, collection_name)
        assert STRUCT_FIELD not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "parent_name, subfield_name, expected_msg",
        [
            ("RowID", INT_SUBFIELD, "not support to add system field, field name = RowID"),
            ("Timestamp", INT_SUBFIELD, "not support to add system field, field name = Timestamp"),
            (STRUCT_FIELD, "RowID", "not support to add system field, field name = RowID"),
            (
                STRUCT_FIELD,
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(subfield_name, DataType.INT64)
        error = {ct.err_code: 1100, ct.err_msg: expected_msg}
        self.add_collection_struct_field(
            client,
            collection_name,
            parent_name,
            profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, _ = self.describe_collection(client, collection_name)
        assert parent_name not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_scalar_struct_array_field_concurrent_dml_query_search(self):
        """
        target: test read/write traffic while dynamically adding a nullable scalar struct array field
        method: load indexed sealed and growing rows, run AddCollectionStructField concurrently with query/search,
            insert, delete, and upsert requests, then query MATCH on the newly added field and insert post-add rows
        expected: concurrent reads and DML complete without crash or partial map state, MATCH excludes backfilled NULL
            rows, deleted rows are absent, and old/new rows expose the expected struct values
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_concurrent_dml")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_sealed_{i}"} for i in range(4)]
        old_sealed_filler_rows = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [
            {PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_growing_{i}"} for i in range(4, 8)
        ]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        concurrent_insert_rows = [
            {
                PK_FIELD: 1000 + batch * 10 + i,
                VECTOR_FIELD: gen_vector(1000 + batch * 10 + i),
                TAG_FIELD: f"concurrent_insert_{batch}_{i}",
            }
            for batch in range(3)
            for i in range(3)
        ]
        concurrent_upsert_rows = [
            {
                PK_FIELD: old_sealed_rows[0][PK_FIELD],
                VECTOR_FIELD: gen_vector(5000),
                TAG_FIELD: "concurrent_upsert_old_sealed",
            },
            {
                PK_FIELD: old_sealed_rows[2][PK_FIELD],
                VECTOR_FIELD: gen_vector(5002),
                TAG_FIELD: "concurrent_upsert_old_sealed_second",
            },
            {
                PK_FIELD: old_growing_rows[0][PK_FIELD],
                VECTOR_FIELD: gen_vector(5004),
                TAG_FIELD: "concurrent_upsert_old_growing",
            },
            {
                PK_FIELD: old_growing_rows[2][PK_FIELD],
                VECTOR_FIELD: gen_vector(5006),
                TAG_FIELD: "concurrent_upsert_old_growing_second",
            },
            {
                PK_FIELD: 1100,
                VECTOR_FIELD: gen_vector(5100),
                TAG_FIELD: "concurrent_upsert_new_row",
            },
        ]
        delete_ids = [old_sealed_rows[1][PK_FIELD], old_growing_rows[1][PK_FIELD]]

        start_barrier = threading.Barrier(5)
        reader_ready = threading.Event()
        schema_added = threading.Event()

        def run_dml_with_schema_retry(action, count_key):
            last_error = None
            for _ in range(30):
                result, task_check = action()
                if task_check:
                    return result[count_key]
                last_error = result
                if "collection schema mismatch" not in str(result):
                    assert task_check, result
                time.sleep(0.2)
            raise AssertionError(f"schema mismatch retry did not recover: {last_error}")

        def add_struct_field_task():
            task_client = self._client()
            profile_schema = gen_struct_schema(self, task_client)
            profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
            profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
            start_barrier.wait()
            assert reader_ready.wait(timeout=30)
            result, task_check = self.add_collection_struct_field(
                task_client,
                collection_name,
                STRUCT_FIELD,
                profile_schema,
                max_capacity=STRUCT_MAX_CAPACITY,
            )
            assert task_check
            schema_added.set()
            return "add", result

        def query_search_task():
            task_client = self._client()
            start_barrier.wait()

            rows, task_check = self.query(
                task_client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD],
                limit=8,
            )
            assert task_check
            assert rows
            reader_ready.set()

            for _ in range(20):
                rows, task_check = self.query(
                    task_client,
                    collection_name,
                    filter=ALL_ROWS_FILTER,
                    output_fields=[PK_FIELD],
                    limit=8,
                )
                assert task_check
                assert rows
                search_results, task_check = self.search(
                    task_client,
                    collection_name,
                    data=[gen_vector(0)],
                    anns_field=VECTOR_FIELD,
                    search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                    filter=ALL_ROWS_FILTER,
                    output_fields=[PK_FIELD],
                    limit=1,
                )
                assert task_check
                assert search_results[0]

            assert schema_added.wait(timeout=30)
            match_expression = f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 0)"
            last_error = None
            for _ in range(30):
                rows, query_check = self.query(
                    task_client,
                    collection_name,
                    filter=match_expression,
                    output_fields=[PK_FIELD],
                    limit=8,
                    check_task=CheckTasks.check_nothing,
                )
                if not query_check:
                    last_error = rows
                    time.sleep(0.2)
                    continue
                assert rows == []

                search_results, search_check = self.search(
                    task_client,
                    collection_name,
                    data=[gen_vector(0)],
                    anns_field=VECTOR_FIELD,
                    search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                    filter=match_expression,
                    output_fields=[PK_FIELD],
                    limit=1,
                    check_task=CheckTasks.check_nothing,
                )
                if not search_check:
                    last_error = search_results
                    time.sleep(0.2)
                    continue
                assert search_results == [[]]
                return "read", 1
            raise AssertionError(f"MATCH query/search did not recover after schema propagation: {last_error}")

        def insert_task():
            task_client = self._client()
            start_barrier.wait()
            inserted = 0
            for batch in range(3):
                batch_rows = concurrent_insert_rows[batch * 3 : (batch + 1) * 3]
                inserted += run_dml_with_schema_retry(
                    lambda batch_rows=batch_rows: self.insert(
                        task_client,
                        collection_name,
                        batch_rows,
                        check_task=CheckTasks.check_nothing,
                    ),
                    "insert_count",
                )
                time.sleep(0.01)
            return "insert", inserted

        def upsert_task():
            task_client = self._client()
            start_barrier.wait()
            upserted = run_dml_with_schema_retry(
                lambda: self.upsert(
                    task_client,
                    collection_name,
                    concurrent_upsert_rows,
                    check_task=CheckTasks.check_nothing,
                ),
                "upsert_count",
            )
            return "upsert", upserted

        def delete_task():
            task_client = self._client()
            start_barrier.wait()
            time.sleep(0.01)
            result, task_check = self.delete(task_client, collection_name, filter=f"{PK_FIELD} in {delete_ids}")
            assert task_check
            return "delete", result["delete_count"]

        task_results = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(add_struct_field_task),
                executor.submit(query_search_task),
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
        assert task_results["read"] == 1

        post_add_rows = [
            {
                PK_FIELD: 1200,
                VECTOR_FIELD: gen_vector(1200),
                TAG_FIELD: "post_add_explicit_null_profile",
                STRUCT_FIELD: None,
            },
            {
                PK_FIELD: 1201,
                VECTOR_FIELD: gen_vector(1201),
                TAG_FIELD: "post_add_empty_profile",
                STRUCT_FIELD: [],
            },
            {
                PK_FIELD: 1202,
                VECTOR_FIELD: gen_vector(1202),
                TAG_FIELD: "post_add_non_empty_profile",
                STRUCT_FIELD: gen_scalar_profile(1202),
            },
        ]
        res, _ = self.insert(client, collection_name, post_add_rows)
        assert res["insert_count"] == len(post_add_rows)

        res, _ = self.flush(client, collection_name)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_rows + old_growing_rows}
        for deleted_id in delete_ids:
            source_by_id.pop(deleted_id)
        source_by_id.update({row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in concurrent_insert_rows})
        source_by_id.update({row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in concurrent_upsert_rows})
        source_by_id[post_add_rows[0][PK_FIELD]] = post_add_rows[0]
        source_by_id[post_add_rows[1][PK_FIELD]] = post_add_rows[1]
        source_by_id[post_add_rows[2][PK_FIELD]] = post_add_rows[2]

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 12020)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {post_add_rows[2][PK_FIELD]}
        assert_scalar_profile_equal(element_filter_results[0][STRUCT_FIELD], post_add_rows[2][STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[post_add_rows[2][VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0][PK_FIELD] == post_add_rows[2][PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = gen_index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(5)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert set(entity) == {PK_FIELD, STRUCT_FIELD}
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = gen_index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        res, _ = self.flush(client, collection_name)
        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 50)",
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in filter_results} == {5}
        for row in filter_results:
            assert_scalar_profile_equal(row[STRUCT_FIELD], source_by_id[row[PK_FIELD]][STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(5)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(3, 5)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_scalar_profile(i),
            }
            for i in range(5, 8)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        iterator, _ = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            consistency_level="Strong",
        )
        iterator_rows = drain_iterator(iterator)
        iterator_ids = [row[PK_FIELD] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(3, 5)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_scalar_profile(i),
            }
            for i in range(5, 8)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[gen_vector(7)],
            batch_size=2,
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        iterator_hits = drain_iterator(iterator)
        hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in entity
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(3, 5)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_scalar_profile(i),
            }
            for i in range(5, 7)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        old_null_to_non_null = {
            PK_FIELD: 0,
            VECTOR_FIELD: gen_vector(100),
            TAG_FIELD: "upserted_old_null_to_non_null",
            STRUCT_FIELD: gen_scalar_profile(100),
        }
        new_non_null_to_null = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(500),
            TAG_FIELD: "upserted_new_non_null_to_null",
        }
        res, _ = self.upsert(client, collection_name, [old_null_to_non_null, new_non_null_to_null])
        assert res["upsert_count"] == 2

        source_by_id[old_null_to_non_null[PK_FIELD]] = old_null_to_non_null
        source_by_id[new_non_null_to_null[PK_FIELD]] = {**new_non_null_to_null, STRUCT_FIELD: None}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 1000)",
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {old_null_to_non_null[PK_FIELD]}
        assert_scalar_profile_equal(element_filter_results[0][STRUCT_FIELD], old_null_to_non_null[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(500)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0][PK_FIELD] == new_non_null_to_null[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in entity
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "new_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "new_4",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 40, TAG_SUBFIELD: "profile_4_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 41, TAG_SUBFIELD: "profile_4_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})
        old_ids = {row[PK_FIELD] for row in old_sealed_rows}
        new_ids = {row[PK_FIELD] for row in new_rows}

        search_tensor = EmbeddingList()
        search_tensor.add(new_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD])
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params=STRUCT_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(new_rows),
        )
        assert len(search_results[0]) == len(new_rows)
        hit_ids = {hit[PK_FIELD] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        assert search_results[0][0][PK_FIELD] == new_rows[-1][PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "new_4",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 40, TAG_SUBFIELD: "profile_4_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 41, TAG_SUBFIELD: "profile_4_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 5,
                VECTOR_FIELD: gen_vector(5),
                TAG_FIELD: "new_5",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 50, TAG_SUBFIELD: "profile_5_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 51, TAG_SUBFIELD: "profile_5_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})
        old_ids = {row[PK_FIELD] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row[PK_FIELD] for row in new_rows}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        search_tensor = EmbeddingList()
        search_tensor.add(new_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD])
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params=STRUCT_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(new_rows),
        )
        assert len(search_results[0]) == len(new_rows)
        hit_ids = {hit[PK_FIELD] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        assert search_results[0][0][PK_FIELD] == new_rows[-1][PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        rows = [
            {
                PK_FIELD: 0,
                VECTOR_FIELD: gen_vector(0),
                TAG_FIELD: "new_0",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 0, TAG_SUBFIELD: "profile_0_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 1, TAG_SUBFIELD: "profile_0_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 1,
                VECTOR_FIELD: gen_vector(1),
                TAG_FIELD: "new_1",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 10, TAG_SUBFIELD: "profile_1_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 11, TAG_SUBFIELD: "profile_1_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, rows)
        assert res["insert_count"] == len(rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        index_params.add_index(field_name=STRUCT_VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: row for row in rows}
        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE"},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert search_results[0][0][PK_FIELD] == rows[-1][PK_FIELD]
        assert search_results[0][0]["offset"] == 0
        entity = search_entity(search_results[0][0])
        assert entity[TAG_FIELD] == rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], rows[-1][STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        old_growing_rows = [{PK_FIELD: 2, VECTOR_FIELD: gen_vector(2), TAG_FIELD: "growing_2"}]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        new_rows = [
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "new_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "new_4",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 40, TAG_SUBFIELD: "profile_4_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 41, TAG_SUBFIELD: "profile_4_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})
        old_ids = {row[PK_FIELD] for row in old_sealed_rows + old_growing_rows}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(field_name=STRUCT_VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        search_results, _ = self.search(
            client,
            collection_name,
            data=[new_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE"},
            filter=f"{PK_FIELD} >= 3",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert search_results[0][0][PK_FIELD] == new_rows[-1][PK_FIELD]
        assert search_results[0][0]["offset"] == 0
        assert search_results[0][0][PK_FIELD] not in old_ids
        entity = search_entity(search_results[0][0])
        assert entity[TAG_FIELD] == new_rows[-1][TAG_FIELD]
        assert_profile_equal(entity[STRUCT_FIELD], new_rows[-1][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        old_growing_rows = [{PK_FIELD: 2, VECTOR_FIELD: gen_vector(2), TAG_FIELD: "growing_2"}]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        new_rows = [
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "new_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "new_4",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 40, TAG_SUBFIELD: "profile_4_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 41, TAG_SUBFIELD: "profile_4_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})
        old_ids = {row[PK_FIELD] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row[PK_FIELD] for row in new_rows}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        search_results, _ = self.search(
            client,
            collection_name,
            data=[new_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(new_rows),
        )
        hits = search_results[0]
        hit_ids = [hit[PK_FIELD] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == new_ids
        assert not set(hit_ids).intersection(old_ids)
        assert hits[0][PK_FIELD] == new_rows[-1][PK_FIELD]
        assert hits[0]["offset"] == 0

        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] >= distances[index + 1] - epsilon

        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        empty_profile_row = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "new_empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_rows = [
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "new_4",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 40, TAG_SUBFIELD: "profile_4_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 41, TAG_SUBFIELD: "profile_4_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 5,
                VECTOR_FIELD: gen_vector(5),
                TAG_FIELD: "new_5",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 50, TAG_SUBFIELD: "profile_5_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 51, TAG_SUBFIELD: "profile_5_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, [empty_profile_row, *non_empty_rows])
        assert res["insert_count"] == 1 + len(non_empty_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD])
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params=STRUCT_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(non_empty_rows),
        )
        assert len(search_results[0]) == len(non_empty_rows)
        hit_ids = {hit[PK_FIELD] for hit in search_results[0]}
        assert hit_ids == {row[PK_FIELD] for row in non_empty_rows}
        assert empty_profile_row[PK_FIELD] not in hit_ids
        assert not hit_ids.intersection({row[PK_FIELD] for row in old_sealed_rows})
        assert search_results[0][0][PK_FIELD] == non_empty_rows[-1][PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_profile(i),
            }
            for i in range(3, 5)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(4)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert set(entity) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_profile(i),
            }
            for i in range(4, 6)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(5)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert set(entity) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_profile(i),
            }
            for i in range(4, 7)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        iterator, _ = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            consistency_level="Strong",
        )
        iterator_rows = drain_iterator(iterator)
        iterator_ids = [row[PK_FIELD] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_profile(i),
            }
            for i in range(4, 7)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[gen_vector(6)],
            batch_size=2,
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        iterator_hits = drain_iterator(iterator)
        hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        new_rows = [
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "new_3",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 30, TAG_SUBFIELD: "profile_3_0", VECTOR_SUBFIELD: gen_unit_vector(0)},
                    {INT_SUBFIELD: 31, TAG_SUBFIELD: "profile_3_1", VECTOR_SUBFIELD: gen_unit_vector(1)},
                ],
            },
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "new_4",
                STRUCT_FIELD: [
                    {INT_SUBFIELD: 40, TAG_SUBFIELD: "profile_4_0", VECTOR_SUBFIELD: gen_unit_vector(2)},
                    {INT_SUBFIELD: 41, TAG_SUBFIELD: "profile_4_1", VECTOR_SUBFIELD: gen_unit_vector(3)},
                ],
            },
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})
        old_ids = {row[PK_FIELD] for row in old_sealed_rows}

        search_results, _ = self.search(
            client,
            collection_name,
            data=[new_rows[-1][STRUCT_FIELD][0][VECTOR_SUBFIELD]],
            anns_field=STRUCT_VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 30)",
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == {3}
        assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(old_ids)
        entity = search_entity(search_results[0][0])
        assert STRUCT_FIELD in entity
        assert_profile_equal(entity[STRUCT_FIELD], source_by_id[3][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(3, 5)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_scalar_profile(i),
            }
            for i in range(5, 7)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(6)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        hit_ids = {hit[PK_FIELD] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_element_filter_query_search(self):
        """
        target: test element filter correctness after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then query/search with
            element_filter on the added field
        expected: query supports element_filter, and row-level vector search rejects element_filter with a clear error
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_filter")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        int_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 40)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in int_filter_results} == {4}
        for row in int_filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        string_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f'element_filter({STRUCT_FIELD}, $[{TAG_SUBFIELD}] == "profile_5_1")',
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in string_filter_results} == {5}
        for row in string_filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        error = {
            ct.err_code: 1100,
            ct.err_msg: "element_filter is only supported for element-level search",
        }
        self.search(
            client,
            collection_name,
            data=[gen_vector(5)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 40)",
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(new_rows),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/51381", strict=True)
    def test_add_scalar_struct_array_field_match_null_vs_empty_after_reload(self):
        """
        target: test MATCH three-valued semantics for schema-evolution backfill rows
        method: add a nullable struct array field after sealed and growing rows exist, insert explicit null,
            omitted, empty, matching, and non-matching rows, then query before and after release/load
        expected: backfilled/null/omitted rows are UNKNOWN and excluded, while real empty arrays retain
            vacuous MATCH_ALL, MATCH_EXACT(0), and NOT MATCH_ANY behavior across reload
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_match_null_empty")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_row = {
            PK_FIELD: 0,
            VECTOR_FIELD: gen_vector(0),
            TAG_FIELD: "old_sealed_backfill_null",
        }
        res, _ = self.insert(client, collection_name, [old_sealed_row])
        assert res["insert_count"] == 1
        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD,
            index_type=NORMAL_VECTOR_INDEX_TYPE,
            metric_type=NORMAL_VECTOR_METRIC_TYPE,
        )
        res, _ = self.create_index(client, collection_name, index_params)
        res, _ = self.load_collection(client, collection_name)

        old_growing_row = {
            PK_FIELD: 1,
            VECTOR_FIELD: gen_vector(1),
            TAG_FIELD: "old_growing_backfill_null",
        }
        res, _ = self.insert(client, collection_name, [old_growing_row])
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client,
            collection_name,
            STRUCT_FIELD,
            profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
        )

        post_add_rows = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "explicit_null",
                STRUCT_FIELD: None,
            },
            {PK_FIELD: 3, VECTOR_FIELD: gen_vector(3), TAG_FIELD: "omitted"},
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "empty",
                STRUCT_FIELD: [],
            },
            {
                PK_FIELD: 5,
                VECTOR_FIELD: gen_vector(5),
                TAG_FIELD: "positive",
                STRUCT_FIELD: [{INT_SUBFIELD: 10, TAG_SUBFIELD: "positive"}],
            },
            {
                PK_FIELD: 6,
                VECTOR_FIELD: gen_vector(6),
                TAG_FIELD: "negative",
                STRUCT_FIELD: [{INT_SUBFIELD: -1, TAG_SUBFIELD: "negative"}],
            },
        ]
        res, _ = self.insert(client, collection_name, post_add_rows)
        assert res["insert_count"] == len(post_add_rows)

        controlled_ids = list(range(7))
        scoped_prefix = f"{PK_FIELD} in {controlled_ids} && "
        expected_ids_by_expression = {
            f"MATCH_ALL({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 0)": {4, 5},
            f"MATCH_EXACT({STRUCT_FIELD}, $[{INT_SUBFIELD}] > 1000, threshold=0)": {4, 5, 6},
            f"not MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 0)": {4, 6},
        }

        def query_ids_by_expression():
            actual = {}
            for expression in expected_ids_by_expression:
                rows, _ = self.query(
                    client,
                    collection_name,
                    filter=scoped_prefix + expression,
                    output_fields=[PK_FIELD],
                    limit=len(controlled_ids),
                )
                actual[expression] = {row[PK_FIELD] for row in rows}
            return actual

        live_evidence = query_ids_by_expression()

        res, _ = self.flush(client, collection_name)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        res, _ = self.release_collection(client, collection_name)
        res, _ = self.load_collection(client, collection_name)

        reloaded_evidence = query_ids_by_expression()
        assert {
            "live": live_evidence,
            "reloaded": reloaded_evidence,
        } == {
            "live": expected_ids_by_expression,
            "reloaded": expected_ids_by_expression,
        }

    @pytest.mark.tags(CaseLabel.L0)
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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [
            {PK_FIELD: row_id, VECTOR_FIELD: gen_vector(row_id), TAG_FIELD: f"old_sealed_{row_id}"}
            for row_id in range(self.min_index_sealed_rows)
        ]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [
            {PK_FIELD: 3000, VECTOR_FIELD: gen_vector(3000), TAG_FIELD: "old_growing_3000"},
            {PK_FIELD: 3001, VECTOR_FIELD: gen_vector(3001), TAG_FIELD: "old_growing_3001"},
        ]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        sealed_explicit_null_profile_row = {
            PK_FIELD: 4000,
            VECTOR_FIELD: gen_vector(4000),
            TAG_FIELD: "sealed_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        sealed_omitted_profile_row = {
            PK_FIELD: 4001,
            VECTOR_FIELD: gen_vector(4001),
            TAG_FIELD: "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            PK_FIELD: 4002,
            VECTOR_FIELD: gen_vector(4002),
            TAG_FIELD: "sealed_empty_profile",
            STRUCT_FIELD: [],
        }
        sealed_one_match_profile_row = {
            PK_FIELD: 4003,
            VECTOR_FIELD: gen_vector(4003),
            TAG_FIELD: "sealed_one_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 9100, TAG_SUBFIELD: "match_9100"},
                {INT_SUBFIELD: 100, TAG_SUBFIELD: "low_100"},
            ],
        }
        sealed_two_match_profile_row = {
            PK_FIELD: 4004,
            VECTOR_FIELD: gen_vector(4004),
            TAG_FIELD: "sealed_two_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 9200, TAG_SUBFIELD: "match_9200"},
                {INT_SUBFIELD: 9300, TAG_SUBFIELD: "match_9300"},
            ],
        }
        sealed_zero_match_profile_row = {
            PK_FIELD: 4005,
            VECTOR_FIELD: gen_vector(4005),
            TAG_FIELD: "sealed_zero_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 100, TAG_SUBFIELD: "low_100"},
                {INT_SUBFIELD: 200, TAG_SUBFIELD: "low_200"},
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
        sealed_index_filler_rows = gen_scalar_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_match_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, _ = self.insert(client, collection_name, sealed_rows)
        assert res["insert_count"] == len(sealed_rows)

        res, _ = self.flush(client, collection_name)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        res, _ = self.refresh_load(client, collection_name)

        growing_explicit_null_profile_row = {
            PK_FIELD: 7000,
            VECTOR_FIELD: gen_vector(7000),
            TAG_FIELD: "growing_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        growing_empty_profile_row = {
            PK_FIELD: 7001,
            VECTOR_FIELD: gen_vector(7001),
            TAG_FIELD: "growing_empty_profile",
            STRUCT_FIELD: [],
        }
        growing_two_match_profile_row = {
            PK_FIELD: 7002,
            VECTOR_FIELD: gen_vector(7002),
            TAG_FIELD: "growing_two_match_profile",
            STRUCT_FIELD: [
                {INT_SUBFIELD: 9600, TAG_SUBFIELD: "match_9600"},
                {INT_SUBFIELD: 9700, TAG_SUBFIELD: "match_9700"},
            ],
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_empty_profile_row,
            growing_two_match_profile_row,
        ]
        res, _ = self.insert(client, collection_name, growing_rows)
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row[PK_FIELD]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row[PK_FIELD]] = {**sealed_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[sealed_empty_profile_row[PK_FIELD]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row[PK_FIELD]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row[PK_FIELD]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row[PK_FIELD]] = sealed_zero_match_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row[PK_FIELD]] = growing_explicit_null_profile_row
        source_by_id[growing_empty_profile_row[PK_FIELD]] = growing_empty_profile_row
        source_by_id[growing_two_match_profile_row[PK_FIELD]] = growing_two_match_profile_row

        match_any_ids = {
            sealed_one_match_profile_row[PK_FIELD],
            sealed_two_match_profile_row[PK_FIELD],
            growing_two_match_profile_row[PK_FIELD],
        }
        match_two_or_more_ids = {
            sealed_two_match_profile_row[PK_FIELD],
            growing_two_match_profile_row[PK_FIELD],
        }
        match_exact_one_ids = {sealed_one_match_profile_row[PK_FIELD]}
        non_nullish_scope = [
            sealed_one_match_profile_row[PK_FIELD],
            sealed_two_match_profile_row[PK_FIELD],
            sealed_zero_match_profile_row[PK_FIELD],
            growing_two_match_profile_row[PK_FIELD],
        ]

        match_any_results, _ = self.query(
            client,
            collection_name,
            filter=f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(match_any_ids),
        )
        assert {row[PK_FIELD] for row in match_any_results} == match_any_ids
        assert not {row[PK_FIELD] for row in match_any_results}.intersection(
            {
                *[row[PK_FIELD] for row in old_sealed_rows + old_growing_rows],
                sealed_explicit_null_profile_row[PK_FIELD],
                sealed_omitted_profile_row[PK_FIELD],
                sealed_empty_profile_row[PK_FIELD],
                sealed_zero_match_profile_row[PK_FIELD],
                growing_explicit_null_profile_row[PK_FIELD],
                growing_empty_profile_row[PK_FIELD],
            }
        )
        for row in match_any_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_least_results, _ = self.query(
            client,
            collection_name,
            filter=f"MATCH_LEAST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=2)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(match_two_or_more_ids),
        )
        assert {row[PK_FIELD] for row in match_least_results} == match_two_or_more_ids
        for row in match_least_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_exact_results, _ = self.query(
            client,
            collection_name,
            filter=f"MATCH_EXACT({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=1)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(match_exact_one_ids),
        )
        assert {row[PK_FIELD] for row in match_exact_results} == match_exact_one_ids
        for row in match_exact_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_all_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} in {non_nullish_scope} && MATCH_ALL({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(match_two_or_more_ids),
        )
        assert {row[PK_FIELD] for row in match_all_results} == match_two_or_more_ids
        for row in match_all_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        match_most_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} in {non_nullish_scope} && MATCH_MOST({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000, threshold=1)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=2,
        )
        assert {row[PK_FIELD] for row in match_most_results} == {
            sealed_one_match_profile_row[PK_FIELD],
            sealed_zero_match_profile_row[PK_FIELD],
        }
        for row in match_most_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[growing_two_match_profile_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            filter=f"MATCH_ANY({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 9000)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(match_any_ids),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == match_any_ids
        assert search_results[0][0][PK_FIELD] == growing_two_match_profile_row[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        omitted_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "new_omit_profile",
        }
        empty_profile_row = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "new_empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_profile_row = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "new_non_empty_profile",
            STRUCT_FIELD: gen_scalar_profile(6),
        }
        res, _ = self.insert(client, collection_name, [omitted_profile_row, empty_profile_row, non_empty_profile_row])
        assert res["insert_count"] == 3

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[omitted_profile_row[PK_FIELD]] = {**omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id[non_empty_profile_row[PK_FIELD]] = non_empty_profile_row

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 60)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {non_empty_profile_row[PK_FIELD]}
        for row in element_filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(6)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        explicit_null_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "new_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        omitted_profile_row = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "new_omit_profile",
        }
        empty_profile_row = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "new_empty_profile",
            STRUCT_FIELD: [],
        }
        non_empty_profile_row = {
            PK_FIELD: 7,
            VECTOR_FIELD: gen_vector(7),
            TAG_FIELD: "new_non_empty_profile",
            STRUCT_FIELD: gen_scalar_profile(7),
        }
        res, _ = self.insert(
            client,
            collection_name,
            [explicit_null_profile_row, omitted_profile_row, empty_profile_row, non_empty_profile_row],
        )
        assert res["insert_count"] == 4

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[explicit_null_profile_row[PK_FIELD]] = explicit_null_profile_row
        source_by_id[omitted_profile_row[PK_FIELD]] = {**omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[empty_profile_row[PK_FIELD]] = empty_profile_row
        source_by_id[non_empty_profile_row[PK_FIELD]] = non_empty_profile_row

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 70)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {non_empty_profile_row[PK_FIELD]}
        for row in element_filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(7)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0][PK_FIELD] == non_empty_profile_row[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = gen_index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        sealed_explicit_null_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "sealed_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        sealed_omitted_profile_row = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "sealed_empty_profile",
            STRUCT_FIELD: [],
        }
        sealed_non_empty_profile_row = {
            PK_FIELD: 7,
            VECTOR_FIELD: gen_vector(7),
            TAG_FIELD: "sealed_non_empty_profile",
            STRUCT_FIELD: gen_scalar_profile(7),
        }
        sealed_deleted_non_empty_profile_row = {
            PK_FIELD: 8,
            VECTOR_FIELD: gen_vector(8),
            TAG_FIELD: "sealed_deleted_non_empty_profile",
            STRUCT_FIELD: gen_scalar_profile(8),
        }
        sealed_inserted_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_non_empty_profile_row,
            sealed_deleted_non_empty_profile_row,
        ]
        sealed_index_filler_rows = gen_scalar_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_inserted_rows),
            "sealed_index_filler",
        )
        sealed_inserted_rows += sealed_index_filler_rows
        res, _ = self.insert(client, collection_name, sealed_inserted_rows)
        assert res["insert_count"] == len(sealed_inserted_rows)

        res, _ = self.flush(client, collection_name)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        growing_explicit_null_profile_row = {
            PK_FIELD: 9,
            VECTOR_FIELD: gen_vector(9),
            TAG_FIELD: "growing_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        growing_omitted_profile_row = {
            PK_FIELD: 10,
            VECTOR_FIELD: gen_vector(10),
            TAG_FIELD: "growing_omit_profile",
        }
        growing_empty_profile_row = {
            PK_FIELD: 11,
            VECTOR_FIELD: gen_vector(11),
            TAG_FIELD: "growing_empty_profile",
            STRUCT_FIELD: [],
        }
        growing_non_empty_profile_row = {
            PK_FIELD: 12,
            VECTOR_FIELD: gen_vector(12),
            TAG_FIELD: "growing_non_empty_profile",
            STRUCT_FIELD: gen_scalar_profile(12),
        }
        growing_deleted_non_empty_profile_row = {
            PK_FIELD: 13,
            VECTOR_FIELD: gen_vector(13),
            TAG_FIELD: "growing_deleted_non_empty_profile",
            STRUCT_FIELD: gen_scalar_profile(13),
        }
        growing_inserted_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_non_empty_profile_row,
            growing_deleted_non_empty_profile_row,
        ]
        res, _ = self.insert(client, collection_name, growing_inserted_rows)
        assert res["insert_count"] == len(growing_inserted_rows)

        delete_ids = [
            old_sealed_rows[1][PK_FIELD],
            old_growing_rows[1][PK_FIELD],
            sealed_explicit_null_profile_row[PK_FIELD],
            sealed_empty_profile_row[PK_FIELD],
            sealed_deleted_non_empty_profile_row[PK_FIELD],
            growing_explicit_null_profile_row[PK_FIELD],
            growing_empty_profile_row[PK_FIELD],
            growing_deleted_non_empty_profile_row[PK_FIELD],
        ]
        res, _ = self.delete(client, collection_name, filter=f"{PK_FIELD} in {delete_ids}")
        assert res["delete_count"] == len(delete_ids)

        res, _ = self.flush(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.pop(old_sealed_rows[1][PK_FIELD])
        source_by_id.pop(old_growing_rows[1][PK_FIELD])
        source_by_id[sealed_omitted_profile_row[PK_FIELD]] = {**sealed_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[sealed_non_empty_profile_row[PK_FIELD]] = sealed_non_empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in sealed_index_filler_rows})
        source_by_id[growing_omitted_profile_row[PK_FIELD]] = {**growing_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[growing_non_empty_profile_row[PK_FIELD]] = growing_non_empty_profile_row

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 70)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=4,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {
            sealed_non_empty_profile_row[PK_FIELD],
            growing_non_empty_profile_row[PK_FIELD],
        }
        for row in element_filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[growing_non_empty_profile_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0][PK_FIELD] == growing_non_empty_profile_row[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = gen_index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(2, 4)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        sealed_non_empty_profile_row = {
            PK_FIELD: 7,
            VECTOR_FIELD: gen_vector(7),
            TAG_FIELD: "sealed_non_empty_profile",
            STRUCT_FIELD: gen_profile(7),
        }
        sealed_deleted_non_empty_profile_row = {
            PK_FIELD: 8,
            VECTOR_FIELD: gen_vector(8),
            TAG_FIELD: "sealed_deleted_non_empty_profile",
            STRUCT_FIELD: gen_profile(8),
        }
        sealed_non_empty_rows = [sealed_non_empty_profile_row, sealed_deleted_non_empty_profile_row]
        sealed_index_filler_rows = gen_vector_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_rows),
            "sealed_index_filler",
        )
        sealed_non_empty_rows += sealed_index_filler_rows
        res, _ = self.insert(client, collection_name, sealed_non_empty_rows)
        assert res["insert_count"] == len(sealed_non_empty_rows)

        sealed_explicit_null_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "sealed_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        sealed_omitted_profile_row = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "sealed_empty_profile",
            STRUCT_FIELD: [],
        }
        sealed_nullish_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
        ]
        res, _ = self.insert(client, collection_name, sealed_nullish_rows)
        assert res["insert_count"] == len(sealed_nullish_rows)

        res, _ = self.flush(client, collection_name)

        growing_non_empty_profile_row = {
            PK_FIELD: 12,
            VECTOR_FIELD: gen_vector(12),
            TAG_FIELD: "growing_non_empty_profile",
            STRUCT_FIELD: gen_profile(12),
        }
        growing_deleted_non_empty_profile_row = {
            PK_FIELD: 13,
            VECTOR_FIELD: gen_vector(13),
            TAG_FIELD: "growing_deleted_non_empty_profile",
            STRUCT_FIELD: gen_profile(13),
        }
        growing_non_empty_rows = [growing_non_empty_profile_row, growing_deleted_non_empty_profile_row]
        res, _ = self.insert(client, collection_name, growing_non_empty_rows)
        assert res["insert_count"] == len(growing_non_empty_rows)

        growing_explicit_null_profile_row = {
            PK_FIELD: 9,
            VECTOR_FIELD: gen_vector(9),
            TAG_FIELD: "growing_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        growing_omitted_profile_row = {
            PK_FIELD: 10,
            VECTOR_FIELD: gen_vector(10),
            TAG_FIELD: "growing_omit_profile",
        }
        growing_empty_profile_row = {
            PK_FIELD: 11,
            VECTOR_FIELD: gen_vector(11),
            TAG_FIELD: "growing_empty_profile",
            STRUCT_FIELD: [],
        }
        growing_nullish_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
        ]
        res, _ = self.insert(client, collection_name, growing_nullish_rows)
        assert res["insert_count"] == len(growing_nullish_rows)

        delete_ids = [
            old_sealed_rows[1][PK_FIELD],
            old_growing_rows[1][PK_FIELD],
            sealed_explicit_null_profile_row[PK_FIELD],
            sealed_empty_profile_row[PK_FIELD],
            sealed_deleted_non_empty_profile_row[PK_FIELD],
            growing_explicit_null_profile_row[PK_FIELD],
            growing_empty_profile_row[PK_FIELD],
            growing_deleted_non_empty_profile_row[PK_FIELD],
        ]
        res, _ = self.delete(client, collection_name, filter=f"{PK_FIELD} in {delete_ids}")
        assert res["delete_count"] == len(delete_ids)

        res, _ = self.flush(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.pop(old_sealed_rows[1][PK_FIELD])
        source_by_id.pop(old_growing_rows[1][PK_FIELD])
        source_by_id[sealed_omitted_profile_row[PK_FIELD]] = {**sealed_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[sealed_non_empty_profile_row[PK_FIELD]] = sealed_non_empty_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in sealed_index_filler_rows})
        source_by_id[growing_omitted_profile_row[PK_FIELD]] = {**growing_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[growing_non_empty_profile_row[PK_FIELD]] = growing_non_empty_profile_row

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in subfield_results}.intersection(delete_ids)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] >= 70)",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=4,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {
            sealed_non_empty_profile_row[PK_FIELD],
            growing_non_empty_profile_row[PK_FIELD],
        }
        for row in element_filter_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[growing_non_empty_profile_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0][PK_FIELD] == growing_non_empty_profile_row[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = gen_index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [
            {PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        post_add_sealed_rows = [
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "post_add_sealed_non_empty_to_null",
                STRUCT_FIELD: gen_profile(4),
            },
            {
                PK_FIELD: 5,
                VECTOR_FIELD: gen_vector(5),
                TAG_FIELD: "post_add_sealed_non_empty_to_empty",
                STRUCT_FIELD: gen_profile(5),
            },
        ]
        res, _ = self.insert(client, collection_name, post_add_sealed_rows)
        assert res["insert_count"] == len(post_add_sealed_rows)

        res, _ = self.flush(client, collection_name)

        post_add_growing_rows = [
            {
                PK_FIELD: 6,
                VECTOR_FIELD: gen_vector(6),
                TAG_FIELD: "post_add_growing_non_empty_to_null",
                STRUCT_FIELD: gen_profile(6),
            },
            {
                PK_FIELD: 7,
                VECTOR_FIELD: gen_vector(7),
                TAG_FIELD: "post_add_growing_non_empty_to_empty",
                STRUCT_FIELD: gen_profile(7),
            },
        ]
        res, _ = self.insert(client, collection_name, post_add_growing_rows)
        assert res["insert_count"] == len(post_add_growing_rows)

        non_empty_upserts = [
            {
                PK_FIELD: old_sealed_rows[0][PK_FIELD],
                VECTOR_FIELD: gen_vector(100),
                TAG_FIELD: "upsert_old_sealed_null_to_non_empty",
                STRUCT_FIELD: gen_profile(100),
            },
            {
                PK_FIELD: old_growing_rows[0][PK_FIELD],
                VECTOR_FIELD: gen_vector(200),
                TAG_FIELD: "upsert_old_growing_null_to_non_empty",
                STRUCT_FIELD: gen_profile(200),
            },
            {
                PK_FIELD: 8,
                VECTOR_FIELD: gen_vector(800),
                TAG_FIELD: "upsert_new_non_empty",
                STRUCT_FIELD: gen_profile(800),
            },
        ]
        res, _ = self.upsert(client, collection_name, non_empty_upserts)
        assert res["upsert_count"] == len(non_empty_upserts)

        nullish_upserts = [
            {
                PK_FIELD: post_add_sealed_rows[0][PK_FIELD],
                VECTOR_FIELD: gen_vector(400),
                TAG_FIELD: "upsert_post_add_sealed_non_empty_to_null",
            },
            {
                PK_FIELD: post_add_sealed_rows[1][PK_FIELD],
                VECTOR_FIELD: gen_vector(500),
                TAG_FIELD: "upsert_post_add_sealed_non_empty_to_empty",
                STRUCT_FIELD: [],
            },
            {
                PK_FIELD: post_add_growing_rows[0][PK_FIELD],
                VECTOR_FIELD: gen_vector(600),
                TAG_FIELD: "upsert_post_add_growing_non_empty_to_null",
                STRUCT_FIELD: None,
            },
            {
                PK_FIELD: post_add_growing_rows[1][PK_FIELD],
                VECTOR_FIELD: gen_vector(700),
                TAG_FIELD: "upsert_post_add_growing_non_empty_to_empty",
                STRUCT_FIELD: [],
            },
            {
                PK_FIELD: 9,
                VECTOR_FIELD: gen_vector(900),
                TAG_FIELD: "upsert_new_omitted_profile",
            },
        ]
        res, _ = self.upsert(client, collection_name, nullish_upserts)
        assert res["upsert_count"] == len(nullish_upserts)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in non_empty_upserts})
        source_by_id[nullish_upserts[0][PK_FIELD]] = {**nullish_upserts[0], STRUCT_FIELD: None}
        source_by_id[nullish_upserts[1][PK_FIELD]] = nullish_upserts[1]
        source_by_id[nullish_upserts[2][PK_FIELD]] = nullish_upserts[2]
        source_by_id[nullish_upserts[3][PK_FIELD]] = nullish_upserts[3]
        source_by_id[nullish_upserts[4][PK_FIELD]] = {**nullish_upserts[4], STRUCT_FIELD: None}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        for expected_row in non_empty_upserts:
            element_filter_results, _ = self.query(
                client,
                collection_name,
                filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {expected_row[STRUCT_FIELD][0][INT_SUBFIELD]})",
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=1,
            )
            assert {row[PK_FIELD] for row in element_filter_results} == {expected_row[PK_FIELD]}
            row = element_filter_results[0]
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[non_empty_upserts[-1][VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0][PK_FIELD] == non_empty_upserts[-1][PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = gen_index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [
            {PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        deleted_profile = [
            {INT_SUBFIELD: 7000, TAG_SUBFIELD: "deleted_shared_0"},
            {INT_SUBFIELD: 7001, TAG_SUBFIELD: "deleted_shared_1"},
        ]
        sealed_explicit_null_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "sealed_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        sealed_omitted_profile_row = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "sealed_empty_profile",
            STRUCT_FIELD: [],
        }
        sealed_deleted_profile_row = {
            PK_FIELD: 7,
            VECTOR_FIELD: gen_vector(7),
            TAG_FIELD: "sealed_deleted_by_element_filter",
            STRUCT_FIELD: deleted_profile,
        }
        sealed_kept_profile_row = {
            PK_FIELD: 8,
            VECTOR_FIELD: gen_vector(8),
            TAG_FIELD: "sealed_kept_after_element_filter",
            STRUCT_FIELD: gen_scalar_profile(8),
        }
        sealed_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_deleted_profile_row,
            sealed_kept_profile_row,
        ]
        sealed_index_filler_rows = gen_scalar_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, _ = self.insert(client, collection_name, sealed_rows)
        assert res["insert_count"] == len(sealed_rows)

        res, _ = self.flush(client, collection_name)

        growing_explicit_null_profile_row = {
            PK_FIELD: 9,
            VECTOR_FIELD: gen_vector(9),
            TAG_FIELD: "growing_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        growing_omitted_profile_row = {
            PK_FIELD: 10,
            VECTOR_FIELD: gen_vector(10),
            TAG_FIELD: "growing_omit_profile",
        }
        growing_empty_profile_row = {
            PK_FIELD: 11,
            VECTOR_FIELD: gen_vector(11),
            TAG_FIELD: "growing_empty_profile",
            STRUCT_FIELD: [],
        }
        growing_deleted_profile_row = {
            PK_FIELD: 12,
            VECTOR_FIELD: gen_vector(12),
            TAG_FIELD: "growing_deleted_by_element_filter",
            STRUCT_FIELD: deleted_profile,
        }
        growing_kept_profile_row = {
            PK_FIELD: 13,
            VECTOR_FIELD: gen_vector(13),
            TAG_FIELD: "growing_kept_after_element_filter",
            STRUCT_FIELD: gen_scalar_profile(13),
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_deleted_profile_row,
            growing_kept_profile_row,
        ]
        res, _ = self.insert(client, collection_name, growing_rows)
        assert res["insert_count"] == len(growing_rows)

        delete_filter = f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {deleted_profile[0][INT_SUBFIELD]})"
        res, _ = self.delete(client, collection_name, filter=delete_filter)
        assert res["delete_count"] == 2

        res, _ = self.flush(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row[PK_FIELD]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row[PK_FIELD]] = {**sealed_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[sealed_empty_profile_row[PK_FIELD]] = sealed_empty_profile_row
        source_by_id[sealed_kept_profile_row[PK_FIELD]] = sealed_kept_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row[PK_FIELD]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row[PK_FIELD]] = {**growing_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[growing_empty_profile_row[PK_FIELD]] = growing_empty_profile_row
        source_by_id[growing_kept_profile_row[PK_FIELD]] = growing_kept_profile_row
        deleted_ids = {sealed_deleted_profile_row[PK_FIELD], growing_deleted_profile_row[PK_FIELD]}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(deleted_ids)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        deleted_filter_results, _ = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert deleted_filter_results == []

        for kept_row in (sealed_kept_profile_row, growing_kept_profile_row):
            kept_filter_results, _ = self.query(
                client,
                collection_name,
                filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {kept_row[STRUCT_FIELD][0][INT_SUBFIELD]})",
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=1,
            )
            assert {row[PK_FIELD] for row in kept_filter_results} == {kept_row[PK_FIELD]}
            row = kept_filter_results[0]
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[growing_kept_profile_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(deleted_ids)
        assert search_results[0][0][PK_FIELD] == growing_kept_profile_row[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = gen_index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, _ = self.insert(client, collection_name, all_old_sealed_rows)
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [
            {PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        deleted_profile = [
            {INT_SUBFIELD: 7000, TAG_SUBFIELD: "deleted_shared_0", VECTOR_SUBFIELD: gen_vector(7000)},
            {INT_SUBFIELD: 7001, TAG_SUBFIELD: "deleted_shared_1", VECTOR_SUBFIELD: gen_vector(7001)},
        ]
        sealed_deleted_profile_row = {
            PK_FIELD: 7,
            VECTOR_FIELD: gen_vector(7),
            TAG_FIELD: "sealed_deleted_by_element_filter",
            STRUCT_FIELD: deleted_profile,
        }
        sealed_kept_profile_row = {
            PK_FIELD: 8,
            VECTOR_FIELD: gen_vector(8),
            TAG_FIELD: "sealed_kept_after_element_filter",
            STRUCT_FIELD: gen_profile(8),
        }
        sealed_non_empty_rows = [sealed_deleted_profile_row, sealed_kept_profile_row]
        sealed_index_filler_rows = gen_vector_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_rows),
            "sealed_index_filler",
        )
        sealed_non_empty_rows += sealed_index_filler_rows
        res, _ = self.insert(client, collection_name, sealed_non_empty_rows)
        assert res["insert_count"] == len(sealed_non_empty_rows)

        sealed_explicit_null_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "sealed_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        sealed_omitted_profile_row = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "sealed_empty_profile",
            STRUCT_FIELD: [],
        }
        sealed_nullish_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
        ]
        res, _ = self.insert(client, collection_name, sealed_nullish_rows)
        assert res["insert_count"] == len(sealed_nullish_rows)

        res, _ = self.flush(client, collection_name)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        growing_deleted_profile_row = {
            PK_FIELD: 12,
            VECTOR_FIELD: gen_vector(12),
            TAG_FIELD: "growing_deleted_by_element_filter",
            STRUCT_FIELD: deleted_profile,
        }
        growing_kept_profile_row = {
            PK_FIELD: 13,
            VECTOR_FIELD: gen_vector(13),
            TAG_FIELD: "growing_kept_after_element_filter",
            STRUCT_FIELD: gen_profile(13),
        }
        growing_non_empty_rows = [growing_deleted_profile_row, growing_kept_profile_row]
        res, _ = self.insert(client, collection_name, growing_non_empty_rows)
        assert res["insert_count"] == len(growing_non_empty_rows)

        growing_explicit_null_profile_row = {
            PK_FIELD: 9,
            VECTOR_FIELD: gen_vector(9),
            TAG_FIELD: "growing_explicit_null_profile",
            STRUCT_FIELD: None,
        }
        growing_omitted_profile_row = {
            PK_FIELD: 10,
            VECTOR_FIELD: gen_vector(10),
            TAG_FIELD: "growing_omit_profile",
        }
        growing_empty_profile_row = {
            PK_FIELD: 11,
            VECTOR_FIELD: gen_vector(11),
            TAG_FIELD: "growing_empty_profile",
            STRUCT_FIELD: [],
        }
        growing_nullish_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
        ]
        res, _ = self.insert(client, collection_name, growing_nullish_rows)
        assert res["insert_count"] == len(growing_nullish_rows)

        delete_filter = f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {deleted_profile[0][INT_SUBFIELD]})"
        res, _ = self.delete(client, collection_name, filter=delete_filter)
        assert res["delete_count"] == 2

        res, _ = self.flush(client, collection_name)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row[PK_FIELD]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row[PK_FIELD]] = {**sealed_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[sealed_empty_profile_row[PK_FIELD]] = sealed_empty_profile_row
        source_by_id[sealed_kept_profile_row[PK_FIELD]] = sealed_kept_profile_row
        source_by_id.update({row[PK_FIELD]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row[PK_FIELD]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row[PK_FIELD]] = {**growing_omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[growing_empty_profile_row[PK_FIELD]] = growing_empty_profile_row
        source_by_id[growing_kept_profile_row[PK_FIELD]] = growing_kept_profile_row
        deleted_ids = {sealed_deleted_profile_row[PK_FIELD], growing_deleted_profile_row[PK_FIELD]}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(deleted_ids)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in subfield_results}.intersection(deleted_ids)
        for row in subfield_results:
            expected = source_by_id[row[PK_FIELD]]
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        deleted_filter_results, _ = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert deleted_filter_results == []

        for kept_row in (sealed_kept_profile_row, growing_kept_profile_row):
            kept_filter_results, _ = self.query(
                client,
                collection_name,
                filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {kept_row[STRUCT_FIELD][0][INT_SUBFIELD]})",
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                limit=1,
            )
            assert {row[PK_FIELD] for row in kept_filter_results} == {kept_row[PK_FIELD]}
            row = kept_filter_results[0]
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[growing_kept_profile_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(deleted_ids)
        assert search_results[0][0][PK_FIELD] == growing_kept_profile_row[PK_FIELD]
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        sealed_non_empty_a = {
            PK_FIELD: 2,
            VECTOR_FIELD: gen_vector(2),
            TAG_FIELD: "a_sealed_non_empty",
            STRUCT_FIELD: gen_scalar_profile(2),
        }
        sealed_null_a = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "a_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_non_empty_b = {
            PK_FIELD: 102,
            VECTOR_FIELD: gen_vector(102),
            TAG_FIELD: "b_sealed_non_empty",
            STRUCT_FIELD: gen_scalar_profile(102),
        }
        sealed_omit_b = {
            PK_FIELD: 103,
            VECTOR_FIELD: gen_vector(103),
            TAG_FIELD: "b_sealed_omit",
        }
        res, _ = self.insert(client, collection_name, [sealed_non_empty_a, sealed_null_a], partition_name=partition_a)
        assert res["insert_count"] == 2
        res, _ = self.insert(client, collection_name, [sealed_non_empty_b, sealed_omit_b], partition_name=partition_b)
        assert res["insert_count"] == 2

        res, _ = self.flush(client, collection_name)

        growing_empty_a = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "a_growing_empty",
            STRUCT_FIELD: [],
        }
        growing_non_empty_a = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "a_growing_non_empty",
            STRUCT_FIELD: gen_scalar_profile(5),
        }
        growing_null_b = {
            PK_FIELD: 104,
            VECTOR_FIELD: gen_vector(104),
            TAG_FIELD: "b_growing_null",
            STRUCT_FIELD: None,
        }
        growing_non_empty_b = {
            PK_FIELD: 105,
            VECTOR_FIELD: gen_vector(105),
            TAG_FIELD: "b_growing_non_empty",
            STRUCT_FIELD: gen_scalar_profile(105),
        }
        res, _ = self.insert(
            client, collection_name, [growing_empty_a, growing_non_empty_a], partition_name=partition_a
        )
        assert res["insert_count"] == 2
        res, _ = self.insert(client, collection_name, [growing_null_b, growing_non_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 2

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a[PK_FIELD]: sealed_non_empty_a,
                sealed_null_a[PK_FIELD]: sealed_null_a,
                growing_empty_a[PK_FIELD]: growing_empty_a,
                growing_non_empty_a[PK_FIELD]: growing_non_empty_a,
            }
        )
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b[PK_FIELD]: sealed_non_empty_b,
                sealed_omit_b[PK_FIELD]: {**sealed_omit_b, STRUCT_FIELD: None},
                growing_null_b[PK_FIELD]: growing_null_b,
                growing_non_empty_b[PK_FIELD]: growing_non_empty_b,
            }
        )

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            query_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            search_results, _ = self.search(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0][PK_FIELD] == search_row[PK_FIELD]
            for hit in search_results[0]:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        query_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {sealed_non_empty_a[STRUCT_FIELD][0][INT_SUBFIELD]})",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_a],
            limit=1,
        )
        assert {row[PK_FIELD] for row in query_results} == {sealed_non_empty_a[PK_FIELD]}

        query_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {sealed_non_empty_a[STRUCT_FIELD][0][INT_SUBFIELD]})",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_b],
            limit=1,
        )
        assert query_results == []

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD],
            partition_names=[partition_a, partition_b],
            limit=len(source_a) + len(source_b),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_a) | set(source_b)

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        sealed_non_empty_a = {
            PK_FIELD: 2,
            VECTOR_FIELD: gen_vector(2),
            TAG_FIELD: "a_sealed_non_empty",
            STRUCT_FIELD: gen_profile(2),
        }
        sealed_non_empty_b = {
            PK_FIELD: 102,
            VECTOR_FIELD: gen_vector(102),
            TAG_FIELD: "b_sealed_non_empty",
            STRUCT_FIELD: gen_profile(102),
        }
        res, _ = self.insert(client, collection_name, [sealed_non_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [sealed_non_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        sealed_null_a = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "a_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_empty_a = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "a_sealed_empty",
            STRUCT_FIELD: [],
        }
        sealed_empty_b = {
            PK_FIELD: 103,
            VECTOR_FIELD: gen_vector(103),
            TAG_FIELD: "b_sealed_empty",
            STRUCT_FIELD: [],
        }
        sealed_null_b = {
            PK_FIELD: 104,
            VECTOR_FIELD: gen_vector(104),
            TAG_FIELD: "b_sealed_null",
            STRUCT_FIELD: None,
        }
        res, _ = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 2
        res, _ = self.insert(client, collection_name, [sealed_empty_b, sealed_null_b], partition_name=partition_b)
        assert res["insert_count"] == 2

        res, _ = self.flush(client, collection_name)

        growing_non_empty_a = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "a_growing_non_empty",
            STRUCT_FIELD: gen_profile(5),
        }
        growing_non_empty_b = {
            PK_FIELD: 105,
            VECTOR_FIELD: gen_vector(105),
            TAG_FIELD: "b_growing_non_empty",
            STRUCT_FIELD: gen_profile(105),
        }
        res, _ = self.insert(client, collection_name, [growing_non_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [growing_non_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        growing_empty_a = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "a_growing_empty",
            STRUCT_FIELD: [],
        }
        growing_empty_b = {
            PK_FIELD: 106,
            VECTOR_FIELD: gen_vector(106),
            TAG_FIELD: "b_growing_empty",
            STRUCT_FIELD: [],
        }
        res, _ = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a[PK_FIELD]: sealed_non_empty_a,
                sealed_null_a[PK_FIELD]: sealed_null_a,
                sealed_empty_a[PK_FIELD]: sealed_empty_a,
                growing_non_empty_a[PK_FIELD]: growing_non_empty_a,
                growing_empty_a[PK_FIELD]: growing_empty_a,
            }
        )
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b[PK_FIELD]: sealed_non_empty_b,
                sealed_empty_b[PK_FIELD]: sealed_empty_b,
                sealed_null_b[PK_FIELD]: sealed_null_b,
                growing_non_empty_b[PK_FIELD]: growing_non_empty_b,
                growing_empty_b[PK_FIELD]: growing_empty_b,
            }
        )

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            query_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            subfield_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
            for row in subfield_results:
                expected = source_by_id[row[PK_FIELD]]
                assert set(row) == {PK_FIELD, STRUCT_FIELD}
                assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            search_results, _ = self.search(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0][PK_FIELD] == search_row[PK_FIELD]
            for hit in search_results[0]:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        query_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {sealed_non_empty_a[STRUCT_FIELD][0][INT_SUBFIELD]})",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_a],
            limit=1,
        )
        assert {row[PK_FIELD] for row in query_results} == {sealed_non_empty_a[PK_FIELD]}
        assert_profile_equal(query_results[0][STRUCT_FIELD], sealed_non_empty_a[STRUCT_FIELD])

        query_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {sealed_non_empty_a[STRUCT_FIELD][0][INT_SUBFIELD]})",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_b],
            limit=1,
        )
        assert query_results == []

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD],
            partition_names=[partition_a, partition_b],
            limit=len(source_a) + len(source_b),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_a) | set(source_b)

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        rows_a = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "a_post_add_non_empty",
                STRUCT_FIELD: gen_scalar_profile(2),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "a_post_add_null",
                STRUCT_FIELD: None,
            },
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "a_post_add_empty",
                STRUCT_FIELD: [],
            },
        ]
        rows_b = [
            {
                PK_FIELD: 102,
                VECTOR_FIELD: gen_vector(102),
                TAG_FIELD: "b_post_add_non_empty",
                STRUCT_FIELD: gen_scalar_profile(102),
            },
            {
                PK_FIELD: 103,
                VECTOR_FIELD: gen_vector(103),
                TAG_FIELD: "b_post_add_null",
                STRUCT_FIELD: None,
            },
            {
                PK_FIELD: 104,
                VECTOR_FIELD: gen_vector(104),
                TAG_FIELD: "b_post_add_empty",
                STRUCT_FIELD: [],
            },
        ]
        res, _ = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert res["insert_count"] == len(rows_a)
        res, _ = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert res["insert_count"] == len(rows_b)

        res, _ = self.flush(client, collection_name)

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row[PK_FIELD]: row for row in rows_a})
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row[PK_FIELD]: row for row in rows_b})

        res, _ = self.release_collection(client, collection_name)

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            res, _ = self.load_partitions(client, collection_name, [partition_name])

            query_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            search_results, _ = self.search(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0][PK_FIELD] == search_row[PK_FIELD]
            for hit in search_results[0]:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

            element_filter_results, _ = self.query(
                client,
                collection_name,
                filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {search_row[STRUCT_FIELD][0][INT_SUBFIELD]})",
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=1,
            )
            assert {row[PK_FIELD] for row in element_filter_results} == {search_row[PK_FIELD]}
            assert_scalar_profile_equal(element_filter_results[0][STRUCT_FIELD], search_row[STRUCT_FIELD])

            res, _ = self.release_partitions(client, collection_name, [partition_name])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        sealed_non_empty_a = {
            PK_FIELD: 2,
            VECTOR_FIELD: gen_vector(2),
            TAG_FIELD: "a_sealed_non_empty",
            STRUCT_FIELD: gen_profile(2),
        }
        sealed_non_empty_b = {
            PK_FIELD: 102,
            VECTOR_FIELD: gen_vector(102),
            TAG_FIELD: "b_sealed_non_empty",
            STRUCT_FIELD: gen_profile(102),
        }
        res, _ = self.insert(client, collection_name, [sealed_non_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [sealed_non_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        sealed_null_a = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "a_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_empty_a = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "a_sealed_empty",
            STRUCT_FIELD: [],
        }
        sealed_null_b = {
            PK_FIELD: 103,
            VECTOR_FIELD: gen_vector(103),
            TAG_FIELD: "b_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_empty_b = {
            PK_FIELD: 104,
            VECTOR_FIELD: gen_vector(104),
            TAG_FIELD: "b_sealed_empty",
            STRUCT_FIELD: [],
        }
        res, _ = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 2
        res, _ = self.insert(client, collection_name, [sealed_null_b, sealed_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 2

        res, _ = self.flush(client, collection_name)

        growing_non_empty_a = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "a_growing_non_empty",
            STRUCT_FIELD: gen_profile(5),
        }
        growing_non_empty_b = {
            PK_FIELD: 105,
            VECTOR_FIELD: gen_vector(105),
            TAG_FIELD: "b_growing_non_empty",
            STRUCT_FIELD: gen_profile(105),
        }
        res, _ = self.insert(client, collection_name, [growing_non_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [growing_non_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        growing_empty_a = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "a_growing_empty",
            STRUCT_FIELD: [],
        }
        growing_empty_b = {
            PK_FIELD: 106,
            VECTOR_FIELD: gen_vector(106),
            TAG_FIELD: "b_growing_empty",
            STRUCT_FIELD: [],
        }
        res, _ = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=STRUCT_VECTOR_FIELD,
            index_type=STRUCT_VECTOR_INDEX_TYPE,
            metric_type=STRUCT_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        res, _ = self.create_index(client, collection_name, index_params)

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a[PK_FIELD]: sealed_non_empty_a,
                sealed_null_a[PK_FIELD]: sealed_null_a,
                sealed_empty_a[PK_FIELD]: sealed_empty_a,
                growing_non_empty_a[PK_FIELD]: growing_non_empty_a,
                growing_empty_a[PK_FIELD]: growing_empty_a,
            }
        )
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b[PK_FIELD]: sealed_non_empty_b,
                sealed_null_b[PK_FIELD]: sealed_null_b,
                sealed_empty_b[PK_FIELD]: sealed_empty_b,
                growing_non_empty_b[PK_FIELD]: growing_non_empty_b,
                growing_empty_b[PK_FIELD]: growing_empty_b,
            }
        )

        res, _ = self.release_collection(client, collection_name)

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            res, _ = self.load_partitions(client, collection_name, [partition_name])

            query_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            subfield_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
            for row in subfield_results:
                expected = source_by_id[row[PK_FIELD]]
                assert set(row) == {PK_FIELD, STRUCT_FIELD}
                assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            search_results, _ = self.search(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0][PK_FIELD] == search_row[PK_FIELD]
            for hit in search_results[0]:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

            element_filter_results, _ = self.query(
                client,
                collection_name,
                filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {search_row[STRUCT_FIELD][0][INT_SUBFIELD]})",
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=1,
            )
            assert {row[PK_FIELD] for row in element_filter_results} == {search_row[PK_FIELD]}
            assert_profile_equal(element_filter_results[0][STRUCT_FIELD], search_row[STRUCT_FIELD])

            res, _ = self.release_partitions(client, collection_name, [partition_name])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        delete_profile = [
            {INT_SUBFIELD: 9000, TAG_SUBFIELD: "delete_shared_0"},
            {INT_SUBFIELD: 9001, TAG_SUBFIELD: "delete_shared_1"},
        ]
        sealed_kept_profile = [
            {INT_SUBFIELD: 9100, TAG_SUBFIELD: "kept_sealed_0"},
            {INT_SUBFIELD: 9101, TAG_SUBFIELD: "kept_sealed_1"},
        ]
        growing_kept_profile = [
            {INT_SUBFIELD: 9300, TAG_SUBFIELD: "kept_growing_0"},
            {INT_SUBFIELD: 9301, TAG_SUBFIELD: "kept_growing_1"},
        ]

        sealed_deleted_a = {
            PK_FIELD: 2,
            VECTOR_FIELD: gen_vector(2),
            TAG_FIELD: "a_sealed_deleted_by_partition_filter",
            STRUCT_FIELD: delete_profile,
        }
        sealed_kept_a = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "a_sealed_kept",
            STRUCT_FIELD: sealed_kept_profile,
        }
        sealed_null_a = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "a_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_empty_a = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "a_sealed_empty",
            STRUCT_FIELD: [],
        }
        sealed_same_filter_b = {
            PK_FIELD: 102,
            VECTOR_FIELD: gen_vector(102),
            TAG_FIELD: "b_sealed_same_filter_kept",
            STRUCT_FIELD: delete_profile,
        }
        sealed_null_b = {
            PK_FIELD: 103,
            VECTOR_FIELD: gen_vector(103),
            TAG_FIELD: "b_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_empty_b = {
            PK_FIELD: 104,
            VECTOR_FIELD: gen_vector(104),
            TAG_FIELD: "b_sealed_empty",
            STRUCT_FIELD: [],
        }
        sealed_rows_a = [sealed_deleted_a, sealed_kept_a, sealed_null_a, sealed_empty_a]
        sealed_index_filler_a = gen_scalar_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_rows_a),
            "a_sealed_index_filler",
        )
        sealed_rows_a += sealed_index_filler_a
        res, _ = self.insert(client, collection_name, sealed_rows_a, partition_name=partition_a)
        assert res["insert_count"] == len(sealed_rows_a)
        sealed_rows_b = [sealed_same_filter_b, sealed_null_b, sealed_empty_b]
        sealed_index_filler_b = gen_scalar_index_filler_rows(
            40000,
            self.min_index_sealed_rows - len(sealed_rows_b),
            "b_sealed_index_filler",
        )
        sealed_rows_b += sealed_index_filler_b
        res, _ = self.insert(client, collection_name, sealed_rows_b, partition_name=partition_b)
        assert res["insert_count"] == len(sealed_rows_b)

        res, _ = self.flush(client, collection_name)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        growing_deleted_a = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "a_growing_deleted_by_partition_filter",
            STRUCT_FIELD: delete_profile,
        }
        growing_kept_a = {
            PK_FIELD: 7,
            VECTOR_FIELD: gen_vector(7),
            TAG_FIELD: "a_growing_kept",
            STRUCT_FIELD: growing_kept_profile,
        }
        growing_empty_a = {
            PK_FIELD: 8,
            VECTOR_FIELD: gen_vector(8),
            TAG_FIELD: "a_growing_empty",
            STRUCT_FIELD: [],
        }
        growing_same_filter_b = {
            PK_FIELD: 105,
            VECTOR_FIELD: gen_vector(105),
            TAG_FIELD: "b_growing_same_filter_kept",
            STRUCT_FIELD: delete_profile,
        }
        growing_empty_b = {
            PK_FIELD: 106,
            VECTOR_FIELD: gen_vector(106),
            TAG_FIELD: "b_growing_empty",
            STRUCT_FIELD: [],
        }
        res, _ = self.insert(
            client,
            collection_name,
            [growing_deleted_a, growing_kept_a, growing_empty_a],
            partition_name=partition_a,
        )
        assert res["insert_count"] == 3
        res, _ = self.insert(
            client,
            collection_name,
            [growing_same_filter_b, growing_empty_b],
            partition_name=partition_b,
        )
        assert res["insert_count"] == 2

        delete_filter = f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {delete_profile[0][INT_SUBFIELD]})"
        res, _ = self.delete(client, collection_name, filter=delete_filter, partition_name=partition_a)
        assert res["delete_count"] == 2

        res, _ = self.flush(client, collection_name)

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_kept_a[PK_FIELD]: sealed_kept_a,
                sealed_null_a[PK_FIELD]: sealed_null_a,
                sealed_empty_a[PK_FIELD]: sealed_empty_a,
                growing_kept_a[PK_FIELD]: growing_kept_a,
                growing_empty_a[PK_FIELD]: growing_empty_a,
            }
        )
        source_a.update({row[PK_FIELD]: row for row in sealed_index_filler_a})
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_same_filter_b[PK_FIELD]: sealed_same_filter_b,
                sealed_null_b[PK_FIELD]: sealed_null_b,
                sealed_empty_b[PK_FIELD]: sealed_empty_b,
                growing_same_filter_b[PK_FIELD]: growing_same_filter_b,
                growing_empty_b[PK_FIELD]: growing_empty_b,
            }
        )
        source_b.update({row[PK_FIELD]: row for row in sealed_index_filler_b})
        deleted_ids = {sealed_deleted_a[PK_FIELD], growing_deleted_a[PK_FIELD]}

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_kept_a),
            (partition_b, source_b, growing_same_filter_b),
        ):
            query_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
            assert not {row[PK_FIELD] for row in query_results}.intersection(deleted_ids)
            for row in query_results:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            search_results, _ = self.search(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
            assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(deleted_ids)
            assert search_results[0][0][PK_FIELD] == search_row[PK_FIELD]
            for hit in search_results[0]:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        partition_a_results, _ = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_a],
            limit=1,
        )
        assert partition_a_results == []

        partition_b_results, _ = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_b],
            limit=2,
        )
        partition_b_source = {
            sealed_same_filter_b[PK_FIELD]: sealed_same_filter_b,
            growing_same_filter_b[PK_FIELD]: growing_same_filter_b,
        }
        assert {row[PK_FIELD] for row in partition_b_results} == set(partition_b_source)
        for row in partition_b_results:
            assert_scalar_profile_equal(row[STRUCT_FIELD], partition_b_source[row[PK_FIELD]][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        delete_profile = [
            {INT_SUBFIELD: 9000, TAG_SUBFIELD: "delete_shared_0", VECTOR_SUBFIELD: gen_vector(9000)},
            {INT_SUBFIELD: 9001, TAG_SUBFIELD: "delete_shared_1", VECTOR_SUBFIELD: gen_vector(9001)},
        ]
        sealed_kept_profile = [
            {INT_SUBFIELD: 9100, TAG_SUBFIELD: "kept_sealed_0", VECTOR_SUBFIELD: gen_vector(9100)},
            {INT_SUBFIELD: 9101, TAG_SUBFIELD: "kept_sealed_1", VECTOR_SUBFIELD: gen_vector(9101)},
        ]
        growing_kept_profile = [
            {INT_SUBFIELD: 9300, TAG_SUBFIELD: "kept_growing_0", VECTOR_SUBFIELD: gen_vector(9300)},
            {INT_SUBFIELD: 9301, TAG_SUBFIELD: "kept_growing_1", VECTOR_SUBFIELD: gen_vector(9301)},
        ]

        sealed_deleted_a = {
            PK_FIELD: 2,
            VECTOR_FIELD: gen_vector(2),
            TAG_FIELD: "a_sealed_deleted_by_partition_filter",
            STRUCT_FIELD: delete_profile,
        }
        sealed_kept_a = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "a_sealed_kept",
            STRUCT_FIELD: sealed_kept_profile,
        }
        sealed_same_filter_b = {
            PK_FIELD: 102,
            VECTOR_FIELD: gen_vector(102),
            TAG_FIELD: "b_sealed_same_filter_kept",
            STRUCT_FIELD: delete_profile,
        }
        sealed_non_empty_a = [sealed_deleted_a, sealed_kept_a]
        sealed_index_filler_a = gen_vector_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_a),
            "a_sealed_index_filler",
        )
        sealed_non_empty_a += sealed_index_filler_a
        res, _ = self.insert(client, collection_name, sealed_non_empty_a, partition_name=partition_a)
        assert res["insert_count"] == len(sealed_non_empty_a)
        sealed_non_empty_b = [sealed_same_filter_b]
        sealed_index_filler_b = gen_vector_index_filler_rows(
            40000,
            self.min_index_sealed_rows - len(sealed_non_empty_b),
            "b_sealed_index_filler",
        )
        sealed_non_empty_b += sealed_index_filler_b
        res, _ = self.insert(client, collection_name, sealed_non_empty_b, partition_name=partition_b)
        assert res["insert_count"] == len(sealed_non_empty_b)

        sealed_null_a = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "a_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_empty_a = {
            PK_FIELD: 5,
            VECTOR_FIELD: gen_vector(5),
            TAG_FIELD: "a_sealed_empty",
            STRUCT_FIELD: [],
        }
        sealed_null_b = {
            PK_FIELD: 103,
            VECTOR_FIELD: gen_vector(103),
            TAG_FIELD: "b_sealed_null",
            STRUCT_FIELD: None,
        }
        sealed_empty_b = {
            PK_FIELD: 104,
            VECTOR_FIELD: gen_vector(104),
            TAG_FIELD: "b_sealed_empty",
            STRUCT_FIELD: [],
        }
        res, _ = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 2
        res, _ = self.insert(client, collection_name, [sealed_null_b, sealed_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 2

        res, _ = self.flush(client, collection_name)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        growing_deleted_a = {
            PK_FIELD: 6,
            VECTOR_FIELD: gen_vector(6),
            TAG_FIELD: "a_growing_deleted_by_partition_filter",
            STRUCT_FIELD: delete_profile,
        }
        growing_kept_a = {
            PK_FIELD: 7,
            VECTOR_FIELD: gen_vector(7),
            TAG_FIELD: "a_growing_kept",
            STRUCT_FIELD: growing_kept_profile,
        }
        growing_same_filter_b = {
            PK_FIELD: 105,
            VECTOR_FIELD: gen_vector(105),
            TAG_FIELD: "b_growing_same_filter_kept",
            STRUCT_FIELD: delete_profile,
        }
        res, _ = self.insert(
            client,
            collection_name,
            [growing_deleted_a, growing_kept_a],
            partition_name=partition_a,
        )
        assert res["insert_count"] == 2
        res, _ = self.insert(client, collection_name, [growing_same_filter_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        growing_empty_a = {
            PK_FIELD: 8,
            VECTOR_FIELD: gen_vector(8),
            TAG_FIELD: "a_growing_empty",
            STRUCT_FIELD: [],
        }
        growing_empty_b = {
            PK_FIELD: 106,
            VECTOR_FIELD: gen_vector(106),
            TAG_FIELD: "b_growing_empty",
            STRUCT_FIELD: [],
        }
        res, _ = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        delete_filter = f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == {delete_profile[0][INT_SUBFIELD]})"
        res, _ = self.delete(client, collection_name, filter=delete_filter, partition_name=partition_a)
        assert res["delete_count"] == 2

        res, _ = self.flush(client, collection_name)

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_kept_a[PK_FIELD]: sealed_kept_a,
                sealed_null_a[PK_FIELD]: sealed_null_a,
                sealed_empty_a[PK_FIELD]: sealed_empty_a,
                growing_kept_a[PK_FIELD]: growing_kept_a,
                growing_empty_a[PK_FIELD]: growing_empty_a,
            }
        )
        source_a.update({row[PK_FIELD]: row for row in sealed_index_filler_a})
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_same_filter_b[PK_FIELD]: sealed_same_filter_b,
                sealed_null_b[PK_FIELD]: sealed_null_b,
                sealed_empty_b[PK_FIELD]: sealed_empty_b,
                growing_same_filter_b[PK_FIELD]: growing_same_filter_b,
                growing_empty_b[PK_FIELD]: growing_empty_b,
            }
        )
        source_b.update({row[PK_FIELD]: row for row in sealed_index_filler_b})
        deleted_ids = {sealed_deleted_a[PK_FIELD], growing_deleted_a[PK_FIELD]}

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_kept_a),
            (partition_b, source_b, growing_same_filter_b),
        ):
            query_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
            assert not {row[PK_FIELD] for row in query_results}.intersection(deleted_ids)
            for row in query_results:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            subfield_results, _ = self.query(
                client,
                collection_name,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, STRUCT_VECTOR_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
            assert not {row[PK_FIELD] for row in subfield_results}.intersection(deleted_ids)
            for row in subfield_results:
                expected = source_by_id[row[PK_FIELD]]
                assert set(row) == {PK_FIELD, STRUCT_FIELD}
                assert_profile_vector_subfield_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            search_results, _ = self.search(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
            assert not {hit[PK_FIELD] for hit in search_results[0]}.intersection(deleted_ids)
            assert search_results[0][0][PK_FIELD] == search_row[PK_FIELD]
            for hit in search_results[0]:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        partition_a_results, _ = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_a],
            limit=1,
        )
        assert partition_a_results == []

        partition_b_results, _ = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            partition_names=[partition_b],
            limit=2,
        )
        partition_b_source = {
            sealed_same_filter_b[PK_FIELD]: sealed_same_filter_b,
            growing_same_filter_b[PK_FIELD]: growing_same_filter_b,
        }
        assert {row[PK_FIELD] for row in partition_b_results} == set(partition_b_source)
        for row in partition_b_results:
            assert_profile_equal(row[STRUCT_FIELD], partition_b_source[row[PK_FIELD]][STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        rows_a = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "a_non_empty",
                STRUCT_FIELD: gen_scalar_profile(2),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "a_null",
                STRUCT_FIELD: None,
            },
            {
                PK_FIELD: 4,
                VECTOR_FIELD: gen_vector(4),
                TAG_FIELD: "a_empty",
                STRUCT_FIELD: [],
            },
        ]
        rows_b = [
            {
                PK_FIELD: 102,
                VECTOR_FIELD: gen_vector(102),
                TAG_FIELD: "b_non_empty",
                STRUCT_FIELD: gen_scalar_profile(102),
            },
            {
                PK_FIELD: 103,
                VECTOR_FIELD: gen_vector(103),
                TAG_FIELD: "b_null",
                STRUCT_FIELD: None,
            },
            {
                PK_FIELD: 104,
                VECTOR_FIELD: gen_vector(104),
                TAG_FIELD: "b_empty",
                STRUCT_FIELD: [],
            },
        ]
        res, _ = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert res["insert_count"] == len(rows_a)
        res, _ = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert res["insert_count"] == len(rows_b)

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row[PK_FIELD]: row for row in rows_a})
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row[PK_FIELD]: row for row in rows_b})

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            iterator, _ = self.query_iterator(
                client,
                collection_name,
                batch_size=257,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                consistency_level="Strong",
            )
            iterator_rows = drain_iterator(iterator)
            iterator_ids = [row[PK_FIELD] for row in iterator_rows]
            assert len(iterator_ids) == len(set(iterator_ids))
            assert set(iterator_ids) == set(source_by_id)
            for row in iterator_rows:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            iterator, _ = self.search_iterator(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                batch_size=257,
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            iterator_hits = drain_iterator(iterator)
            hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
            assert len(hit_ids) == len(set(hit_ids))
            assert set(hit_ids) == set(source_by_id)

            distances = [hit["distance"] for hit in iterator_hits]
            for index in range(len(distances) - 1):
                assert distances[index] <= distances[index + 1] + epsilon

            for hit in iterator_hits:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        for partition_name in (partition_a, partition_b):
            res, _ = self.create_partition(client, collection_name, partition_name)

        old_sealed_a = {PK_FIELD: 0, VECTOR_FIELD: gen_vector(0), TAG_FIELD: "a_old_sealed"}
        old_sealed_b = {PK_FIELD: 100, VECTOR_FIELD: gen_vector(100), TAG_FIELD: "b_old_sealed"}
        old_sealed_filler_a = gen_index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = gen_index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, _ = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert res["insert_count"] == len(all_old_sealed_a)
        res, _ = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert res["insert_count"] == len(all_old_sealed_b)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)

        res, _ = self.load_collection(client, collection_name)

        old_growing_a = {PK_FIELD: 1, VECTOR_FIELD: gen_vector(1), TAG_FIELD: "a_old_growing"}
        old_growing_b = {PK_FIELD: 101, VECTOR_FIELD: gen_vector(101), TAG_FIELD: "b_old_growing"}
        res, _ = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert res["insert_count"] == 1
        res, _ = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert res["insert_count"] == 1

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        rows_a = [
            {
                PK_FIELD: 2,
                VECTOR_FIELD: gen_vector(2),
                TAG_FIELD: "a_non_empty",
                STRUCT_FIELD: gen_profile(2),
            },
            {
                PK_FIELD: 3,
                VECTOR_FIELD: gen_vector(3),
                TAG_FIELD: "a_empty",
                STRUCT_FIELD: [],
            },
        ]
        rows_b = [
            {
                PK_FIELD: 102,
                VECTOR_FIELD: gen_vector(102),
                TAG_FIELD: "b_non_empty",
                STRUCT_FIELD: gen_profile(102),
            },
            {
                PK_FIELD: 103,
                VECTOR_FIELD: gen_vector(103),
                TAG_FIELD: "b_empty",
                STRUCT_FIELD: [],
            },
        ]
        res, _ = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert res["insert_count"] == len(rows_a)
        res, _ = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert res["insert_count"] == len(rows_b)

        source_a = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row[PK_FIELD]: row for row in rows_a})
        source_b = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row[PK_FIELD]: row for row in rows_b})

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            iterator, _ = self.query_iterator(
                client,
                collection_name,
                batch_size=257,
                filter=ALL_ROWS_FILTER,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                consistency_level="Strong",
            )
            iterator_rows = drain_iterator(iterator)
            iterator_ids = [row[PK_FIELD] for row in iterator_rows]
            assert len(iterator_ids) == len(set(iterator_ids))
            assert set(iterator_ids) == set(source_by_id)
            for row in iterator_rows:
                expected = source_by_id[row[PK_FIELD]]
                assert row[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

            iterator, _ = self.search_iterator(
                client,
                collection_name,
                data=[search_row[VECTOR_FIELD]],
                batch_size=257,
                anns_field=VECTOR_FIELD,
                search_params=NORMAL_VECTOR_SEARCH_PARAMS,
                output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            iterator_hits = drain_iterator(iterator)
            hit_ids = [hit[PK_FIELD] for hit in iterator_hits]
            assert len(hit_ids) == len(set(hit_ids))
            assert set(hit_ids) == set(source_by_id)

            distances = [hit["distance"] for hit in iterator_hits]
            for index in range(len(distances) - 1):
                assert distances[index] <= distances[index + 1] + epsilon

            for hit in iterator_hits:
                expected = source_by_id[hit[PK_FIELD]]
                entity = search_entity(hit)
                assert entity[TAG_FIELD] == expected[TAG_FIELD]
                assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_profile(i),
            }
            for i in range(3, 5)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(4)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        hit_ids = {hit[PK_FIELD] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        omitted_profile_row = {
            PK_FIELD: 3,
            VECTOR_FIELD: gen_vector(3),
            TAG_FIELD: "new_omitted_profile",
        }
        present_profile_row = {
            PK_FIELD: 4,
            VECTOR_FIELD: gen_vector(4),
            TAG_FIELD: "new_present_profile",
            STRUCT_FIELD: gen_profile(4),
        }
        res, _ = self.insert(client, collection_name, [omitted_profile_row, present_profile_row])
        assert res["insert_count"] == 2

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows}
        source_by_id[omitted_profile_row[PK_FIELD]] = {**omitted_profile_row, STRUCT_FIELD: None}
        source_by_id[present_profile_row[PK_FIELD]] = present_profile_row

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        element_filter_results, _ = self.query(
            client,
            collection_name,
            filter=f"element_filter({STRUCT_FIELD}, $[{INT_SUBFIELD}] == 40)",
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=1,
        )
        assert {row[PK_FIELD] for row in element_filter_results} == {present_profile_row[PK_FIELD]}
        assert_profile_equal(element_filter_results[0][STRUCT_FIELD], present_profile_row[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(4)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {hit[PK_FIELD] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

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

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(3)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_profile(i),
            }
            for i in range(3, 5)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(4)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        hit_ids = {hit[PK_FIELD] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_query_search_old_new_rows(self):
        """
        target: test query/search output after dynamically adding a nullable struct array field
        method: create loaded collection with sealed and growing rows, add a struct array field, then insert new rows
        expected: old rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct")
        client = self._client()

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        res, _ = self.create_collection(client, collection_name, schema=schema)

        old_sealed_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"sealed_{i}"} for i in range(4)]
        res, _ = self.insert(client, collection_name, old_sealed_rows)
        assert res["insert_count"] == len(old_sealed_rows)

        res, _ = self.flush(client, collection_name)

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        res, _ = self.create_index(client, collection_name, index_params)

        res, _ = self.load_collection(client, collection_name)

        old_growing_rows = [{PK_FIELD: i, VECTOR_FIELD: gen_vector(i), TAG_FIELD: f"growing_{i}"} for i in range(4, 6)]
        res, _ = self.insert(client, collection_name, old_growing_rows)
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        res, _ = self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )
        new_rows = [
            {
                PK_FIELD: i,
                VECTOR_FIELD: gen_vector(i),
                TAG_FIELD: f"new_{i}",
                STRUCT_FIELD: gen_profile(i),
            }
            for i in range(6, 8)
        ]
        res, _ = self.insert(client, collection_name, new_rows)
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row[PK_FIELD]: {**row, STRUCT_FIELD: None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row[PK_FIELD]: row for row in new_rows})

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert STRUCT_FIELD in row
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_results, _ = self.search(
            client,
            collection_name,
            data=[gen_vector(7)],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, STRUCT_FIELD],
            limit=len(source_by_id),
        )
        hit_ids = {hit[PK_FIELD] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert STRUCT_FIELD in entity
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])


class TestMilvusClientStructArrayNullableImport(TestMilvusClientV2Base):
    """Nullable struct array bulk import coverage."""

    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    REMOTE_DATA_PATH = "bulkinsert_data"
    LOCAL_FILES_PATH = "/tmp/milvus_bulkinsert/"
    SCALAR_IMPORT_FORMATS = ("json", "parquet")

    @pytest.fixture(scope="function", autouse=True)
    def setup_minio(self, minio_host, minio_bucket):
        """Setup MinIO configuration from fixtures."""
        Path(self.LOCAL_FILES_PATH).mkdir(parents=True, exist_ok=True)
        self.minio_host = minio_host
        self.bucket_name = minio_bucket
        self.minio_endpoint = f"{minio_host}:9000"

    def upload_to_minio(self, local_file_path: str) -> list[list[str]]:
        """Upload a local bulk import file to MinIO."""
        if not os.path.exists(local_file_path):
            raise Exception(f"Local file '{local_file_path}' doesn't exist")

        try:
            minio_client = Minio(
                endpoint=self.minio_endpoint,
                access_key=self.MINIO_ACCESS_KEY,
                secret_key=self.MINIO_SECRET_KEY,
                secure=False,
            )

            if not minio_client.bucket_exists(self.bucket_name):
                raise Exception(f"MinIO bucket '{self.bucket_name}' doesn't exist")

            filename = os.path.basename(local_file_path)
            minio_file_path = os.path.join(self.REMOTE_DATA_PATH, filename)
            minio_client.fput_object(self.bucket_name, minio_file_path, local_file_path)

            log.info(f"Uploaded file to MinIO: {minio_file_path}")
            return [[minio_file_path]]

        except S3Error as e:
            raise Exception(f"Failed to connect MinIO server {self.minio_endpoint}, error: {e}")

    def write_import_rows_file(
        self,
        collection_name: str,
        rows: list[dict[str, Any]],
        file_format: str,
        *,
        row_group_size: int,
        include_struct: bool = True,
    ) -> str:
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.{file_format}")
        if file_format == "json":
            with open(local_file_path, "w") as f:
                json.dump(rows, f)
            return local_file_path

        if file_format != "parquet":
            raise ValueError(f"Unsupported import file format: {file_format}")

        if include_struct:
            write_scalar_struct_rows_parquet(rows, local_file_path, row_group_size=row_group_size)
            return local_file_path

        table = pa.table(
            {
                PK_FIELD: pa.array([row[PK_FIELD] for row in rows], type=pa.int64()),
                VECTOR_FIELD: pa.array([row[VECTOR_FIELD] for row in rows], type=pa.list_(pa.float32())),
                TAG_FIELD: pa.array([row[TAG_FIELD] for row in rows], type=pa.string()),
            }
        )
        pq.write_table(table, local_file_path, row_group_size=row_group_size)
        return local_file_path

    def upload_import_rows(
        self,
        collection_name: str,
        rows: list[dict[str, Any]],
        file_format: str,
        *,
        row_group_size: int,
        include_struct: bool = True,
    ) -> list[list[str]]:
        local_file_path = self.write_import_rows_file(
            collection_name,
            rows,
            file_format,
            row_group_size=row_group_size,
            include_struct=include_struct,
        )
        return self.upload_to_minio(local_file_path)

    def call_bulkinsert(
        self,
        collection_name: str,
        batch_files: list[list[str]],
        expect_fail: bool = False,
        partition_name: str = "",
    ) -> dict[str, Any]:
        """Call bulk import API and wait for completion."""
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
            if state == "Failed":
                reason = resp.json()["data"].get("reason", "Unknown reason")
                if expect_fail:
                    log.info(f"Bulk import job {job_id} failed as expected: {reason}")
                    return {"state": "Failed", "reason": reason}
                raise Exception(f"Bulk import job {job_id} failed: {reason}")
            if state == "Completed" and progress == 100:
                if expect_fail:
                    raise AssertionError(f"Bulk import job {job_id} unexpectedly completed")
                log.info(f"Bulk import job {job_id} completed successfully")
                return {"state": "Completed", "reason": None}

        raise Exception(f"Bulk import job {job_id} timeout after {timeout}s")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_nullable_scalar_struct_array(self, file_format):
        """
        target: test bulk import for a nullable scalar-only Struct Array field
        method: create a collection with nullable scalar Struct Array, import rows with null, empty, and non-empty
            profile values, then query and search the imported data
        expected: imported row count matches source data, and query/search output preserves nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_{file_format}")
        entities = 3000

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = gen_scalar_profile(row_id)
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"import_row_{row_id}",
                STRUCT_FIELD: profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        remote_files = self.upload_import_rows(collection_name, rows, file_format, row_group_size=entities)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        self.refresh_load(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == entities

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_row = source_by_id[entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit[PK_FIELD] for hit in hits} == set(source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_nullable_scalar_struct_array_partition_query_search(self, file_format):
        """
        target: test bulk import into a specified partition for a nullable scalar-only Struct Array field
        method: create two partitions, import rows into one partition, insert same-vector interference rows into the
            other partition, then query and search with partition_names
        expected: imported row count is scoped to the target partition, and partition-scoped query/search never returns
            rows from the other partition while preserving nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_{file_format}_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        entities = 3000
        other_entities = 3000

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        self.create_partition(client, collection_name, partition_a)
        self.create_partition(client, collection_name, partition_b)

        other_partition_rows = []
        other_source_by_id = {}
        for offset in range(other_entities):
            row_id = 100000 + offset
            if offset == 0:
                profile = gen_scalar_profile(row_id)
                vector = gen_vector(entities - 1)
                doc_tag = "other_partition_same_vector"
            elif offset % 3 == 1:
                profile = None
                vector = gen_vector(row_id)
                doc_tag = f"other_partition_null_profile_{offset}"
            elif offset % 3 == 2:
                profile = []
                vector = gen_vector(row_id)
                doc_tag = f"other_partition_empty_profile_{offset}"
            else:
                profile = gen_scalar_profile(row_id)
                vector = gen_vector(row_id)
                doc_tag = f"other_partition_profile_{offset}"
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: vector,
                TAG_FIELD: doc_tag,
                STRUCT_FIELD: profile,
            }
            other_partition_rows.append(row)
            other_source_by_id[row_id] = row
        res, _ = self.insert(client, collection_name, other_partition_rows, partition_name=partition_b)
        assert res["insert_count"] == len(other_partition_rows)
        self.flush(client, collection_name)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = gen_scalar_profile(row_id)
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"target_partition_row_{row_id}",
                STRUCT_FIELD: profile,
            }
            rows.append(row)
            source_by_id[row_id] = row
        remote_files = self.upload_import_rows(collection_name, rows, file_format, row_group_size=entities)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        self.load_collection(client, collection_name)
        self.refresh_load(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == entities + len(other_partition_rows)
        target_partition_stats, _ = self.get_partition_stats(client, collection_name, partition_a)
        assert target_partition_stats["row_count"] == entities
        other_partition_stats, _ = self.get_partition_stats(client, collection_name, partition_b)
        assert other_partition_stats["row_count"] == len(other_partition_rows)

        other_query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=other_entities,
            partition_names=[partition_b],
        )
        assert {row[PK_FIELD] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
            partition_names=[partition_a],
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(other_source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_row = source_by_id[entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
            partition_names=[partition_a],
        )
        hits = search_results[0]
        hit_ids = {hit[PK_FIELD] for hit in hits}
        assert hit_ids == set(source_by_id)
        assert not hit_ids.intersection(other_source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_nullable_scalar_struct_array_all_null(self, file_format):
        """
        target: test bulk import for a nullable scalar-only Struct Array whose parent value is always null
        method: create a collection with nullable scalar Struct Array, import rows where every profile value is null,
            then query and search the imported data
        expected: import succeeds, row count matches source data, and every returned profile value is null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_all_null_{file_format}")
        entities = 3000

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"import_null_row_{row_id}",
                STRUCT_FIELD: None,
            }
            rows.append(row)
            source_by_id[row_id] = row

        remote_files = self.upload_import_rows(collection_name, rows, file_format, row_group_size=entities)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        self.refresh_load(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == entities

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert row[STRUCT_FIELD] is None

        subfield_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, STRUCT_INT_FIELD, STRUCT_TAG_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            assert set(row) == {PK_FIELD, STRUCT_FIELD}
            assert row[STRUCT_FIELD] is None

        search_row = source_by_id[entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit[PK_FIELD] for hit in hits} == set(source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert entity[STRUCT_FIELD] is None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_nullable_scalar_struct_array_partial_null_subfield_rejected(self, file_format):
        """
        target: test bulk import rejects partial null values inside a nullable scalar-only Struct Array element
        method: create a collection with nullable scalar Struct Array and import rows where profile is non-null but one
            struct element has p_int=null or p_tag=null
        expected: import fails because StructArray nullable is parent-level only, and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_partial_null_{file_format}")
        entities = 8

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        rows = []
        for row_id in range(entities):
            if row_id == 0:
                profile = None
            elif row_id == 1:
                profile = []
            elif row_id == 2:
                profile = [
                    {INT_SUBFIELD: None, TAG_SUBFIELD: "partial_null_int"},
                    {INT_SUBFIELD: 21, TAG_SUBFIELD: "valid_int"},
                ]
            elif row_id == 3:
                profile = [{INT_SUBFIELD: 30, TAG_SUBFIELD: None}]
            else:
                profile = gen_scalar_profile(row_id)
            rows.append(
                {
                    PK_FIELD: row_id,
                    VECTOR_FIELD: gen_vector(row_id),
                    TAG_FIELD: f"import_row_{row_id}",
                    STRUCT_FIELD: profile,
                }
            )

        remote_files = self.upload_import_rows(collection_name, rows, file_format, row_group_size=entities)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        reason = import_result["reason"].lower()
        assert any(keyword in reason for keyword in ["null", "expected element type", "not allowed"])

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_non_nullable_scalar_struct_array_rejects_null(self, file_format):
        """
        target: test bulk import rejects null for a non-nullable scalar-only Struct Array field
        method: create a collection with non-nullable scalar Struct Array, import rows where one row uses profile=null
        expected: import job fails with a nullable-related error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_{file_format}")
        entities = 3000

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = []
        for row_id in range(entities):
            if row_id == 0:
                profile = None
            elif row_id % 2 == 0:
                profile = []
            else:
                profile = gen_scalar_profile(row_id)
            rows.append(
                {
                    PK_FIELD: row_id,
                    VECTOR_FIELD: gen_vector(row_id),
                    TAG_FIELD: f"import_row_{row_id}",
                    STRUCT_FIELD: profile,
                }
            )

        remote_files = self.upload_import_rows(collection_name, rows, file_format, row_group_size=entities)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        assert any(keyword in import_result["reason"].lower() for keyword in ["null", "nullable"])

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_non_nullable_scalar_struct_array_rejects_omit_field(self, file_format):
        """
        target: test bulk import rejects omitted data for a non-nullable scalar-only Struct Array field
        method: create a collection with non-nullable scalar Struct Array, import rows that omit profile entirely
        expected: import job fails with a schema/data mismatch error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_omit_{file_format}")
        entities = 3000

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = []
        for row_id in range(entities):
            rows.append(
                {
                    PK_FIELD: row_id,
                    VECTOR_FIELD: gen_vector(row_id),
                    TAG_FIELD: f"import_row_{row_id}",
                }
            )

        remote_files = self.upload_import_rows(
            collection_name,
            rows,
            file_format,
            row_group_size=entities,
            include_struct=False,
        )
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        reason = import_result["reason"].lower()
        assert any(keyword in reason for keyword in [STRUCT_FIELD, "null", "nullable", "missing", "required"])

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_after_add_nullable_scalar_struct_array(self, file_format):
        """
        target: test bulk import after dynamically adding a nullable scalar-only Struct Array field
        method: insert and flush old rows, add nullable scalar Struct Array, import rows with null, empty, and
            non-empty profile values, then query and search all rows
        expected: old sealed rows expose the added struct field as null, imported rows preserve nullable struct
            semantics, and normal vector search requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_{file_format}")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)
        self.create_collection(client, collection_name, schema=schema)

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: None}
        res, _ = self.insert(client, collection_name, old_rows)
        assert res["insert_count"] == old_entities
        self.flush(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_index(client, collection_name, index_params)

        import_rows = []
        for row_id in range(old_entities, total_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = gen_scalar_profile(row_id)
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"import_row_{row_id}",
                STRUCT_FIELD: profile,
            }
            import_rows.append(row)
            source_by_id[row_id] = row

        remote_files = self.upload_import_rows(
            collection_name, import_rows, file_format, row_group_size=import_entities
        )
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        self.load_collection(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == total_entities

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_entities,
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_row = source_by_id[total_entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit[PK_FIELD] for hit in hits} == set(source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_after_add_nullable_scalar_struct_array_partition_query_search(self, file_format):
        """
        target: test bulk import into a specified partition after dynamically adding a nullable scalar-only Struct Array
            field
        method: insert and flush old rows in both target and non-target partitions, add nullable scalar Struct Array,
            import rows into the target partition, and keep a same-vector interference row in another partition
        expected: old target-partition rows expose the added struct field as null, imported rows preserve nullable
            struct semantics, and partition-scoped query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_{file_format}_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)
        self.create_collection(client, collection_name, schema=schema)

        self.create_partition(client, collection_name, partition_a)
        self.create_partition(client, collection_name, partition_b)

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: None}
        res, _ = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert res["insert_count"] == old_entities

        other_source_by_id = {}
        other_old_rows = []
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"other_partition_old_{row_id}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, STRUCT_FIELD: None}
        res, _ = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert res["insert_count"] == len(other_old_rows)
        self.flush(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_index(client, collection_name, index_params)

        import_rows = []
        for row_id in range(old_entities, total_target_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = gen_scalar_profile(row_id)
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"import_target_partition_row_{row_id}",
                STRUCT_FIELD: profile,
            }
            import_rows.append(row)
            source_by_id[row_id] = row

        remote_files = self.upload_import_rows(
            collection_name, import_rows, file_format, row_group_size=import_entities
        )
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        self.load_collection(client, collection_name)
        self.refresh_load(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, _ = self.get_partition_stats(client, collection_name, partition_a)
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, _ = self.get_partition_stats(client, collection_name, partition_b)
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            PK_FIELD: 100000 + other_old_entities,
            VECTOR_FIELD: gen_vector(total_target_entities - 1),
            TAG_FIELD: "other_partition_same_vector_after_add",
            STRUCT_FIELD: gen_scalar_profile(100000 + other_old_entities),
        }
        res, _ = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row[PK_FIELD]] = other_post_add_row

        other_query_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} >= 100000",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(other_source_by_id),
            partition_names=[partition_b],
        )
        assert {row[PK_FIELD] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_target_entities,
            partition_names=[partition_a],
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(other_source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_row = source_by_id[total_target_entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_target_entities,
            partition_names=[partition_a],
        )
        hits = search_results[0]
        hit_ids = {hit[PK_FIELD] for hit in hits}
        assert hit_ids == set(source_by_id)
        assert not hit_ids.intersection(other_source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_after_add_nullable_scalar_struct_array_omit_field(self, file_format):
        """
        target: test bulk import omitting a dynamically added nullable scalar-only Struct Array field
        method: insert and flush old rows, add nullable scalar Struct Array, import rows that omit profile entirely,
            then query and search all rows
        expected: both old rows and imported rows expose the added struct field as null, and normal vector search
            requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_{file_format}")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)
        self.create_collection(client, collection_name, schema=schema)

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: None}
        res, _ = self.insert(client, collection_name, old_rows)
        assert res["insert_count"] == old_entities
        self.flush(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_index(client, collection_name, index_params)

        import_rows = []
        for row_id in range(old_entities, total_entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"import_row_{row_id}",
            }
            import_rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: None}

        remote_files = self.upload_import_rows(
            collection_name,
            import_rows,
            file_format,
            row_group_size=import_entities,
            include_struct=False,
        )
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        self.load_collection(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == total_entities

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_entities,
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert row[STRUCT_FIELD] is None

        search_row = source_by_id[total_entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit[PK_FIELD] for hit in hits} == set(source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert entity[STRUCT_FIELD] is None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_partition_query_search(self, file_format):
        """
        target: test bulk import omitting a dynamically added nullable scalar-only Struct Array field into a specified
            partition
        method: insert and flush old rows in both target and non-target partitions, add nullable scalar Struct Array,
            import rows that omit profile into the target partition, and keep a same-vector interference row in another
            partition
        expected: old and imported target-partition rows expose the added struct field as null, and partition-scoped
            query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_{file_format}_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)
        self.create_collection(client, collection_name, schema=schema)

        self.create_partition(client, collection_name, partition_a)
        self.create_partition(client, collection_name, partition_b)

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: None}
        res, _ = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert res["insert_count"] == old_entities

        other_source_by_id = {}
        other_old_rows = []
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"other_partition_old_{row_id}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, STRUCT_FIELD: None}
        res, _ = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert res["insert_count"] == len(other_old_rows)
        self.flush(client, collection_name)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        self.add_collection_struct_field(
            client, collection_name, STRUCT_FIELD, profile_schema, max_capacity=STRUCT_MAX_CAPACITY
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD, index_type=NORMAL_VECTOR_INDEX_TYPE, metric_type=NORMAL_VECTOR_METRIC_TYPE
        )
        self.create_index(client, collection_name, index_params)

        import_rows = []
        for row_id in range(old_entities, total_target_entities):
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"import_target_partition_omit_profile_row_{row_id}",
            }
            import_rows.append(row)
            source_by_id[row_id] = {**row, STRUCT_FIELD: None}

        remote_files = self.upload_import_rows(
            collection_name,
            import_rows,
            file_format,
            row_group_size=import_entities,
            include_struct=False,
        )
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        self.load_collection(client, collection_name)
        self.refresh_load(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, _ = self.get_partition_stats(client, collection_name, partition_a)
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, _ = self.get_partition_stats(client, collection_name, partition_b)
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            PK_FIELD: 100000 + other_old_entities,
            VECTOR_FIELD: gen_vector(total_target_entities - 1),
            TAG_FIELD: "other_partition_same_vector_after_add",
            STRUCT_FIELD: gen_scalar_profile(100000 + other_old_entities),
        }
        res, _ = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row[PK_FIELD]] = other_post_add_row

        other_query_results, _ = self.query(
            client,
            collection_name,
            filter=f"{PK_FIELD} >= 100000",
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=len(other_source_by_id),
            partition_names=[partition_b],
        )
        assert {row[PK_FIELD] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_scalar_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_target_entities,
            partition_names=[partition_a],
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        assert not {row[PK_FIELD] for row in query_results}.intersection(other_source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert row[STRUCT_FIELD] is None

        search_row = source_by_id[total_target_entities - 1]
        search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params=NORMAL_VECTOR_SEARCH_PARAMS,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=total_target_entities,
            partition_names=[partition_a],
        )
        hits = search_results[0]
        hit_ids = {hit[PK_FIELD] for hit in hits}
        assert hit_ids == set(source_by_id)
        assert not hit_ids.intersection(other_source_by_id)
        assert hits[0][PK_FIELD] == search_row[PK_FIELD]
        for hit in hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert entity[STRUCT_FIELD] is None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_format", SCALAR_IMPORT_FORMATS, ids=SCALAR_IMPORT_FORMATS)
    @pytest.mark.parametrize(
        "search_mode",
        [
            pytest.param(
                "embedding_list",
                marks=pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/51414", strict=True),
                id="embedding_list",
            ),
            pytest.param("element", id="element"),
        ],
    )
    def test_import_nullable_struct_array_with_vector(self, file_format, search_mode):
        """
        target: test JSON and Parquet import for a nullable Struct Array field with scalar and vector sub-fields
        method: create a collection with nullable Struct Array, import rows with null, empty, and non-empty profile
            values, build an embedding-list or element-level index on the Struct vector child, then query and search
            the imported data
        expected: imported row count matches source data, indexes are ready, and query/search output preserves nullable
            Struct semantics, vector values, and element offsets in both row file formats
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_struct_vector_{file_format}_{search_mode}")
        entities = 60

        schema = gen_schema(self, client, auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=PK_FIELD, datatype=PK_TYPE, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=VECTOR_TYPE, dim=VECTOR_DIM)
        schema.add_field(field_name=TAG_FIELD, datatype=TAG_TYPE, max_length=TAG_MAX_LENGTH)

        profile_schema = gen_struct_schema(self, client)
        profile_schema.add_field(INT_SUBFIELD, INT_SUBFIELD_TYPE)
        profile_schema.add_field(TAG_SUBFIELD, TAG_SUBFIELD_TYPE, max_length=TAG_MAX_LENGTH)
        profile_schema.add_field(VECTOR_SUBFIELD, VECTOR_SUBFIELD_TYPE, dim=VECTOR_DIM)
        schema.add_field(
            STRUCT_FIELD,
            datatype=STRUCT_TYPE,
            element_type=STRUCT_ELEMENT_TYPE,
            struct_schema=profile_schema,
            max_capacity=STRUCT_MAX_CAPACITY,
            nullable=True,
        )

        index_params = gen_index_params(self, client)
        index_params.add_index(
            field_name=VECTOR_FIELD,
            index_type="HNSW",
            metric_type=NORMAL_VECTOR_METRIC_TYPE,
            params=INDEX_PARAMS,
        )
        if search_mode == "embedding_list":
            index_params.add_index(
                field_name=STRUCT_VECTOR_FIELD,
                index_type=STRUCT_VECTOR_INDEX_TYPE,
                metric_type=STRUCT_VECTOR_METRIC_TYPE,
                params=INDEX_PARAMS,
            )
        else:
            index_params.add_index(
                field_name=STRUCT_VECTOR_FIELD,
                index_type="HNSW",
                metric_type="COSINE",
                params=INDEX_PARAMS,
            )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = gen_profile(row_id)
                for offset, element in enumerate(profile):
                    rng = np.random.RandomState(row_id * STRUCT_MAX_CAPACITY + offset)
                    vector = rng.uniform(-1.0, 1.0, VECTOR_SUBFIELD_DIM).astype(np.float32)
                    element[VECTOR_SUBFIELD] = (vector / np.linalg.norm(vector)).tolist()
            row = {
                PK_FIELD: row_id,
                VECTOR_FIELD: gen_vector(row_id),
                TAG_FIELD: f"import_row_{row_id}",
                STRUCT_FIELD: profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        if file_format == "parquet":
            local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
            write_struct_vector_rows_parquet(rows, local_file_path, row_group_size=entities)
            remote_files = self.upload_to_minio(local_file_path)
        else:
            remote_files = self.upload_import_rows(collection_name, rows, file_format, row_group_size=entities)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, VECTOR_FIELD, timeout=300)
        assert self.wait_for_index_ready(client, collection_name, STRUCT_VECTOR_FIELD, timeout=300)
        self.refresh_load(client, collection_name)

        stats, _ = self.get_collection_stats(client, collection_name)
        assert stats["row_count"] == entities

        query_results, _ = self.query(
            client,
            collection_name,
            filter=ALL_ROWS_FILTER,
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=entities,
        )
        assert {row[PK_FIELD] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row[PK_FIELD]]
            assert row[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(row[STRUCT_FIELD], expected[STRUCT_FIELD])

        search_row = source_by_id[entities - 1]
        ann_limit = 10
        normal_search_results, _ = self.search(
            client,
            collection_name,
            data=[search_row[VECTOR_FIELD]],
            anns_field=VECTOR_FIELD,
            search_params={"metric_type": NORMAL_VECTOR_METRIC_TYPE, "params": {"ef": 128}},
            output_fields=[PK_FIELD, TAG_FIELD, STRUCT_FIELD],
            limit=ann_limit,
        )
        normal_hits = normal_search_results[0]
        exact_normal_ids = [
            row[PK_FIELD]
            for row in sorted(
                rows,
                key=lambda row: np.sum((np.asarray(search_row[VECTOR_FIELD]) - np.asarray(row[VECTOR_FIELD])) ** 2),
            )[:ann_limit]
        ]
        assert_ann_recall([hit[PK_FIELD] for hit in normal_hits], exact_normal_ids)
        for hit in normal_hits:
            expected = source_by_id[hit[PK_FIELD]]
            entity = search_entity(hit)
            assert entity[TAG_FIELD] == expected[TAG_FIELD]
            assert_profile_equal(entity[STRUCT_FIELD], expected[STRUCT_FIELD])

        non_empty_rows = [row for row in rows if row[STRUCT_FIELD]]
        target_row = non_empty_rows[-1]
        if search_mode == "embedding_list":
            query_tensor = EmbeddingList()
            for element in target_row[STRUCT_FIELD]:
                query_tensor.add(element[VECTOR_SUBFIELD])
            struct_results, _ = self.search(
                client,
                collection_name,
                data=[query_tensor],
                anns_field=STRUCT_VECTOR_FIELD,
                search_params={"metric_type": STRUCT_VECTOR_METRIC_TYPE, "params": {"ef": 128}},
                output_fields=[PK_FIELD],
                limit=ann_limit,
            )
            query_vectors = [np.asarray(element[VECTOR_SUBFIELD]) for element in target_row[STRUCT_FIELD]]
            exact_embedding_ids = [
                row[PK_FIELD]
                for row in sorted(
                    non_empty_rows,
                    key=lambda row: sum(
                        max(
                            float(np.dot(query_vector, np.asarray(element[VECTOR_SUBFIELD])))
                            for element in row[STRUCT_FIELD]
                        )
                        for query_vector in query_vectors
                    ),
                    reverse=True,
                )[:ann_limit]
            ]
            actual_ids = [hit[PK_FIELD] for hit in struct_results[0]]
            assert set(actual_ids).issubset({row[PK_FIELD] for row in non_empty_rows})
            assert_ann_recall(actual_ids, exact_embedding_ids)
        else:
            query_vector = np.asarray(target_row[STRUCT_FIELD][0][VECTOR_SUBFIELD])
            struct_results, _ = self.search(
                client,
                collection_name,
                data=[query_vector.tolist()],
                anns_field=STRUCT_VECTOR_FIELD,
                search_params={"metric_type": "COSINE", "params": {"ef": 128}},
                output_fields=[PK_FIELD],
                limit=ann_limit,
            )
            exact_elements = sorted(
                (
                    (
                        row[PK_FIELD],
                        offset,
                        float(np.dot(query_vector, np.asarray(element[VECTOR_SUBFIELD]))),
                    )
                    for row in non_empty_rows
                    for offset, element in enumerate(row[STRUCT_FIELD])
                ),
                key=lambda item: item[2],
                reverse=True,
            )[:ann_limit]
            expected_keys = [(row_id, offset) for row_id, offset, _ in exact_elements]
            actual_keys = [(hit[PK_FIELD], hit["offset"]) for hit in struct_results[0]]
            valid_keys = {(row[PK_FIELD], offset) for row in non_empty_rows for offset in range(len(row[STRUCT_FIELD]))}
            assert set(actual_keys).issubset(valid_keys)
            assert_ann_recall(actual_keys, expected_keys)
