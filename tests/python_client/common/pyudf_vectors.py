"""Deterministic encoded vectors and an independent metric oracle for D2."""

import math

import numpy as np
from pymilvus import DataType


def vector_data(kind, count=10000):
    if kind == DataType.BINARY_VECTOR:
        vectors = [index.to_bytes(2, "little") for index in range(count)]
        return vectors, (1234).to_bytes(2, "little"), "BIN_FLAT", {"dim": 16}
    if kind == DataType.SPARSE_FLOAT_VECTOR:
        vectors = [{index % 97: float(index % 5 + 1), 100 + index // 97: 0.5} for index in range(count)]
        return vectors, {3: 2.0, 104: 1.0}, "SPARSE_INVERTED_INDEX", {}
    # Exact binary fractions survive FP16/BF16; int8 gets exact integer values.
    raw = np.array([[index % 97 + 1, index // 97 + 1] + [1] * 14 for index in range(count)], dtype=np.float32)
    if kind != DataType.INT8_VECTOR:
        raw /= 128
    query = raw[1234].copy()

    def encode(value):
        if kind == DataType.FLOAT_VECTOR:
            return value.tolist()
        if kind == DataType.FLOAT16_VECTOR:
            return value.astype(np.float16).tobytes()
        if kind == DataType.BFLOAT16_VECTOR:
            return (value.view(np.uint32) >> 16).astype(np.uint16).tobytes()
        return value.astype(np.int8).tobytes()

    return [encode(value) for value in raw], encode(query), "HNSW", {"dim": 16}


def metric_score(kind, metric, vector, query):
    if kind == DataType.BINARY_VECTOR:
        first, second = int.from_bytes(vector, "little"), int.from_bytes(query, "little")
        if metric == "HAMMING":
            return (first ^ second).bit_count()
        union = (first | second).bit_count()
        return 1 - (first & second).bit_count() / union if union else 0
    if kind == DataType.SPARSE_FLOAT_VECTOR:
        return sum(value * query.get(key, 0) for key, value in vector.items())

    def decode(value):
        if kind == DataType.FLOAT_VECTOR:
            return np.asarray(value, dtype=np.float64)
        if kind == DataType.BFLOAT16_VECTOR:
            return (np.frombuffer(value, dtype=np.uint16).astype(np.uint32) << 16).view(np.float32).astype(np.float64)
        return np.frombuffer(value, dtype=np.float16 if kind == DataType.FLOAT16_VECTOR else np.int8).astype(np.float64)

    first, second = decode(vector), decode(query)
    if metric == "L2":
        return float(np.sum((first - second) ** 2))
    dot = float(np.dot(first, second))
    return dot if metric == "IP" else dot / math.sqrt(float(np.dot(first, first) * np.dot(second, second)))
