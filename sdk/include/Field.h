// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "Status.h"


namespace milvus {

enum class DataType {
    INT8 = 1,
    INT16 = 2,
    INT32 = 3,
    INT64 = 4,

    STRING = 20,

    BOOL = 30,

    FLOAT = 40,
    DOUBLE = 41,

    VECTOR = 100,
    UNKNOWN = 9999,
};

// Base struct of all fields
struct Field {
    uint64_t field_id;              ///< read-only
    std::string field_name;
    DataType field_type;
    float boost;
    std::string extram_params;
};
using FieldPtr = std::shared_ptr<Field>;

// DistanceMetric
enum class DistanceMetric {
    L2 = 1,        // Euclidean Distance
    IP = 2,        // Cosine Similarity
    HAMMING = 3,   // Hamming Distance
    JACCARD = 4,   // Jaccard Distance
    TANIMOTO = 5,  // Tanimoto Distance
};

// vector field
struct VectorField : Field {
    uint64_t dimension;
};
using VectorFieldPtr = std::shared_ptr<VectorField>;

} // namespace milvus
