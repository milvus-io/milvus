// Licensed under the Apache License, Version 2.0.
#pragma once

namespace milvus {

enum class DataType {
    NONE = 0,
    BOOL = 1,
    INT8 = 2,
    INT16 = 3,
    INT32 = 4,
    INT64 = 5,

    FLOAT = 10,
    DOUBLE = 11,

    STRING = 20,
    VARCHAR = 21,
    ARRAY = 22,
    JSON = 23,
    GEOMETRY = 24,
    TEXT = 25,
    TIMESTAMPTZ = 26,  // Timestamp with timezone, stored as int64

    // Some special Data type, start from after 50
    // just for internal use now, may sync proto in future
    ROW = 50,

    VECTOR_BINARY = 100,
    VECTOR_FLOAT = 101,
    VECTOR_FLOAT16 = 102,
    VECTOR_BFLOAT16 = 103,
    VECTOR_SPARSE_U32_F32 = 104,
    VECTOR_INT8 = 105,
    VECTOR_ARRAY = 106,
};

}  // namespace milvus
