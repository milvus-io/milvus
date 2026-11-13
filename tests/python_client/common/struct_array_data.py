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

"""Schema-driven Struct Array inputs, scenario overrides and independent value/vector oracles."""

import copy

import numpy as np
import pytest
from common import common_func as cf
from pymilvus import DataType

SCALARS = [
    DataType.BOOL,
    DataType.INT8,
    DataType.INT16,
    DataType.INT32,
    DataType.INT64,
    DataType.FLOAT,
    DataType.DOUBLE,
    DataType.VARCHAR,
]


SCALAR_IDS = ["bool", "int8", "int16", "int32", "int64", "float", "double", "varchar"]


DIM = 8


def scalar_boundary_value(dtype, n):
    if dtype == DataType.BOOL:
        return bool(n % 2)
    if dtype == DataType.VARCHAR:
        return ["", '雪"a', "abcd", "λ"][n % 4]
    if dtype in (DataType.FLOAT, DataType.DOUBLE):
        return (n % 7 - 3) * 0.25
    bits = {DataType.INT8: 8, DataType.INT16: 16, DataType.INT32: 32, DataType.INT64: 64}[dtype]
    return [-(2 ** (bits - 1)), 2 ** (bits - 1) - 1, 0, -1, 1][n % 5]


def normalized_vector(n, dim=DIM):
    return cf.gen_vectors(1, dim, seed=n, normalize=True)[0]


def make_nullable_row(schema, n, clips, revision=0):
    """Generate ordinary fields from the request schema; control only the tested null shape."""
    primary = schema.primary_field
    defaults = {
        primary.name: [make_primary_key(n, "str" if primary.dtype == DataType.VARCHAR else "int")],
        "revision": [revision],
        schema.struct_fields[0].name: [copy.deepcopy(clips)],
    }
    names = {field.name for field in [*schema.fields, *schema.struct_fields]}
    defaults = {name: value for name, value in defaults.items() if name in names}
    return cf.gen_row_data_by_schema_with_defaults(nb=1, schema=schema, start=n, default_values=defaults)[0]


def assert_values_equal(actual, expected, path="row", *, exact_fields=True):
    """Compare nested values without converting integers to floating point."""
    if isinstance(expected, dict):
        assert isinstance(actual, dict), (path, actual)
        if exact_fields:
            assert set(actual) == set(expected), (path, set(actual), set(expected))
        for key in expected:
            assert key in actual, (path, key, actual)
            assert_values_equal(actual[key], expected[key], f"{path}.{key}", exact_fields=exact_fields)
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected), (path, actual, expected)
        for n, (a, e) in enumerate(zip(actual, expected)):
            assert_values_equal(a, e, f"{path}[{n}]", exact_fields=exact_fields)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, abs=2e-6, rel=2e-6), (path, actual, expected)
    else:
        assert actual == expected, (path, actual, expected)


def child_schema(dtype, nullable=True, **kwargs):
    if dtype == DataType.VARCHAR:
        kwargs.setdefault("max_length", 32)
    if dtype == DataType.FLOAT_VECTOR:
        kwargs.setdefault("dim", DIM)
    return dtype, {"nullable": nullable, **kwargs}


def nullable_children():
    return {
        "anchor": child_schema(DataType.INT64, False),
        "score": child_schema(DataType.INT64),
        "label": child_schema(DataType.VARCHAR),
        "emb": child_schema(DataType.FLOAT_VECTOR),
        "tags": child_schema(DataType.ARRAY, element_type=DataType.INT64, max_capacity=4, element_nullable=True),
    }


def make_nullable_elements(struct_schema, n, revision=0):
    """Apply the NULL/empty/mixed-slot scenario to values generated from the declared children."""
    shape = (n + revision) % 8
    if shape == 0:
        return None if struct_schema.nullable else []
    if shape == 1:
        return []
    fields = {field.name: field for field in struct_schema.fields}
    elements = []
    for offset in range(min(1 + shape % 5, struct_schema.max_capacity)):
        element = {field.name: cf.gen_data_by_collection_field(field) for field in struct_schema.fields}
        if "anchor" in fields:
            element["anchor"] = offset
        if "score" in fields:
            element["score"] = (
                None
                if fields["score"].nullable and (n + offset + revision) % 3 == 0
                else (n + offset + revision) % 7 - 3
            )
        if "label" in fields:
            element["label"] = (
                None
                if fields["label"].nullable and (n + offset) % 4 == 0
                else f"tag_{(n + offset) % 3}"[: int(fields["label"].params["max_length"])]
            )
        if "emb" in fields and fields["emb"].dtype == DataType.FLOAT_VECTOR:
            element["emb"] = (
                None
                if fields["emb"].nullable and (n + offset + revision) % 3 == 1
                else normalized_vector(n * 31 + offset + revision * 100003, int(fields["emb"].params["dim"]))
            )
        if "tags" in fields and fields["tags"].element_type == DataType.INT64:
            tags = [None, [], [None], [n, None], [1, 2, 3, 4]][(n + offset + revision) % 5]
            if tags is None and not fields["tags"].nullable:
                tags = []
            if tags is not None:
                if not fields["tags"].to_dict().get("element_nullable", False):
                    tags = [value for value in tags if value is not None]
                tags = tags[: int(fields["tags"].params["max_capacity"])]
            element["tags"] = tags
        elements.append(element)
    return elements


CAPACITY = 8


def make_primary_key(number, pk_type):
    return number if pk_type == "int" else f"key_雪_{number:06d}"


def sample_scalar_value(number, value_type):
    number = number % 7 - 3
    if value_type == DataType.VARCHAR:
        return f"v{number}"
    if value_type == DataType.BOOL:
        return number >= 0
    if value_type == DataType.DOUBLE:
        return number + 0.25
    return number


def make_required_element(struct_schema, number, offset, revision=0):
    """Keep predicate controls explicit; derive all other values and types from schema."""
    result = {field.name: cf.gen_data_by_collection_field(field) for field in struct_schema.fields}
    fields = {field.name: field for field in struct_schema.fields}
    if "value" in fields:
        result["value"] = sample_scalar_value(number + offset + revision, fields["value"].dtype)
    if "tag" in fields:
        limit = int(fields["tag"].params["max_length"])
        result["tag"] = (
            "雪" * (limit // 3) + "x" * (limit % 3) if offset % 3 == 0 else f"tag_{(number + offset) % 5}"[:limit]
        )
    if "flag" in fields:
        result["flag"] = (number + offset) % 2 == 0
    return result


def make_required_elements(struct_schema, number, revision=0, capacity=None):
    """Exercise parent NULL/empty, short and capacity-sized arrays using their declared children."""
    capacity = struct_schema.max_capacity if capacity is None else min(capacity, struct_schema.max_capacity)
    shape = (number + revision) % 7
    if shape in (0, 6):
        return None if struct_schema.nullable else []
    if shape == 1:
        return []
    length = {2: 1, 3: 2, 4: capacity, 5: 3}[shape]
    return [make_required_element(struct_schema, number, offset, revision) for offset in range(min(length, capacity))]


def make_metadata_row(schema, number, revision=0):
    """Overlay mutation/predicate controls on schema-generated fields, retaining the actual input as oracle."""
    primary = schema.primary_field
    struct = schema.struct_fields[0]
    defaults = {
        primary.name: [make_primary_key(number, "str" if primary.dtype == DataType.VARCHAR else "int")],
        "revision": [revision],
        "bucket": [number % 3],
        "note": [None if number % 3 == 0 else f"note_{revision}"],
        struct.name: [make_required_elements(struct, number, revision)],
    }
    names = {field.name for field in [*schema.fields, *schema.struct_fields]}
    defaults = {name: value for name, value in defaults.items() if name in names}
    return cf.gen_row_data_by_schema_with_defaults(nb=1, schema=schema, start=number, default_values=defaults)[0]


def vector_score(query, vector, metric):
    query = np.asarray(query, dtype=np.float64)
    vector = np.asarray(vector, dtype=np.float64)
    if metric == "L2":
        return float(np.sum((query - vector) ** 2))
    dot = float(np.dot(query, vector))
    return dot if metric == "IP" else dot / float(np.linalg.norm(query) * np.linalg.norm(vector))


def vector_score_tolerance(index, dtype=DataType.FLOAT_VECTOR, *, indexed=False, abs_tol=2e-4, rel_tol=2e-4):
    """Keep raw controls strict; allow an ANN budget only for an established indexed path.

    Struct children are VECTOR_ARRAY fields. GrowingIndexSet::StageAppender and
    ChunkedSegmentSealedImpl::generate_interim_index exclude this type, so initial
    growing and below-threshold sealed controls do not use interim vector indexes.
    These budgets apply to the fixed corpora, not to arbitrary ANN workloads.
    """
    exact = {"abs": abs_tol, "rel": rel_tol}
    if (
        not indexed
        or index["index_type"] not in ("HNSW", "DISKANN")
        or dtype not in (DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR)
        or (index.get("refine_quant_type") or "").upper() == "NONE"
    ):
        return exact
    # The caller must establish indexed execution; collection metadata alone is
    # insufficient for growing or small sealed populations. Default ANN refinement
    # may be quantized. Keep the same policy on both engines;
    # FLAT, binary/integer vectors and explicitly unquantized refinement stay strict.
    metric = index["metric_type"]
    if metric == "COSINE":
        return {"abs": 5e-3, "rel": 0}
    if metric in ("L2", "IP"):
        # Full mutation/reload runs observed up to 1.22% relative error for BF16/IP.
        # The absolute floor also covers self-matches with an exact L2 score of zero.
        return {"abs": 5e-3, "rel": 2e-2 if dtype == DataType.BFLOAT16_VECTOR and metric == "IP" else 1e-2}
    return exact


def make_typed_vector(dtype, seed, dim=None):
    # Seeded vectors keep precision regressions and exact score oracles reproducible.
    dim = dim if dim is not None else (128 if dtype == DataType.BINARY_VECTOR else 32)
    rng = np.random.RandomState(seed)
    if dtype == DataType.BINARY_VECTOR:
        return rng.bytes(dim // 8)
    if dtype == DataType.INT8_VECTOR:
        return rng.randint(-16, 17, dim).astype(np.int8)
    return cf.gen_vectors(1, dim, dtype, seed=seed, normalize=False)[0]


def decode_vector(value, dtype):
    if dtype == DataType.BINARY_VECTOR:
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes):
            value = value[0]
        return np.unpackbits(np.frombuffer(value, dtype=np.uint8)).astype(np.float64)
    values = np.asarray(value)
    if dtype == DataType.BFLOAT16_VECTOR and values.dtype == np.uint16:
        values = (values.astype(np.uint32) << 16).view(np.float32)
    return values.astype(np.float64)


def normalize_vector_row(row, dtype):
    row = copy.deepcopy(row)
    for element in row["clips"] or []:
        for field in ("emb", "newvec"):
            if element.get(field) is not None:
                element[field] = decode_vector(element[field], dtype).tolist()
    return row


def make_predicate_rows(schema):
    shapes = [
        None,
        [],
        [(None, None)],
        [(1, [])],
        [(-1, [None])],
        [(0, [1, None])],
        [(1, [1, 2])],
        [(None, []), (1, [2])],
        [(-1, [2]), (None, None)],
        [(1, [1]), (-1, [])],
        [(None, [None]), (None, [1, None])],
    ]
    return [
        make_nullable_row(
            schema,
            n,
            None
            if shape is None
            else [
                {"anchor": j, "score": score, "label": None, "emb": None, "tags": tags}
                for j, (score, tags) in enumerate(shape)
            ],
        )
        for n, shape in enumerate(shapes)
    ]
