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

"""Deterministic Struct Array inputs and independent value/vector oracles."""

import copy

import numpy as np
import pytest
from ml_dtypes import bfloat16
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


def normalized_vector(n):
    value = np.random.RandomState(n % (2**32)).normal(size=DIM).astype(np.float32)
    return (value / np.linalg.norm(value)).tolist()


def make_nullable_row(n, clips, pk_type="int", revision=0):
    return {
        "id": n if pk_type == "int" else f"key_雪_{n:06d}",
        "revision": revision,
        "normal_vector": normalized_vector(n + 100003 * revision + 9),
        "clips": copy.deepcopy(clips),
    }


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


def make_nullable_elements(n, revision=0, parent_nullable=True):
    shape = (n + revision) % 8
    if shape == 0:
        return None if parent_nullable else []
    if shape == 1:
        return []
    return [
        {
            "anchor": j,
            "score": None if (n + j + revision) % 3 == 0 else (n + j + revision) % 7 - 3,
            "label": None if (n + j) % 4 == 0 else f"tag_{(n + j) % 3}",
            "emb": None if (n + j + revision) % 3 == 1 else normalized_vector(n * 31 + j + revision * 100003),
            "tags": [None, [], [None], [n, None], [1, 2, 3, 4]][(n + j + revision) % 5],
        }
        for j in range(1 + shape % 5)
    ]


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


def make_required_element(number, offset, revision=0, value_type=DataType.INT64, vectors=True):
    result = {
        "value": sample_scalar_value(number + offset + revision, value_type),
        "tag": "雪" * 21 if offset % 3 == 0 else f"tag_{(number + offset) % 5}",
        "flag": (number + offset) % 2 == 0,
    }
    if vectors:
        result["emb"] = normalized_vector(number * 1009 + offset * 31 + revision * 100003 + 7)
    return result


def make_required_elements(number, revision=0, capacity=CAPACITY, value_type=DataType.INT64, vectors=True):
    shape = (number + revision) % 7
    if shape in (0, 6):
        return None
    if shape == 1:
        return []
    length = {2: 1, 3: 2, 4: capacity, 5: 3}[shape]
    return [
        make_required_element(number, offset, revision, value_type, vectors) for offset in range(min(length, capacity))
    ]


def make_metadata_row(number, revision=0, pk_type="int", capacity=CAPACITY, value_type=DataType.INT64, vectors=True):
    return {
        "id": make_primary_key(number, pk_type),
        "revision": revision,
        "bucket": number % 3,
        "normal_vector": normalized_vector(number + revision * 100003 + 999),
        "note": None if number % 3 == 0 else f"note_{revision}",
        "clips": make_required_elements(number, revision, capacity, value_type, vectors),
    }


def vector_score(query, vector, metric):
    query = np.asarray(query, dtype=np.float64)
    vector = np.asarray(vector, dtype=np.float64)
    if metric == "L2":
        return float(np.sum((query - vector) ** 2))
    dot = float(np.dot(query, vector))
    return dot if metric == "IP" else dot / float(np.linalg.norm(query) * np.linalg.norm(vector))


def make_typed_vector(dtype, seed):
    rng = np.random.RandomState(seed)
    if dtype == DataType.BINARY_VECTOR:
        return rng.bytes(16)
    if dtype == DataType.INT8_VECTOR:
        return rng.randint(-16, 17, 32).astype(np.int8)
    values = rng.uniform(-1, 1, 32)
    if dtype == DataType.FLOAT16_VECTOR:
        return values.astype(np.float16)
    if dtype == DataType.BFLOAT16_VECTOR:
        return values.astype(bfloat16)
    return values.astype(np.float32).tolist()


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


def make_predicate_rows():
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
