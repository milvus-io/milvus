// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>

#include "common/Types.h"

// Point and range predicates. A pure mixin, independent of IIndexReaderBase.

namespace milvus::index {

// Native comparison operators. Translate protobuf operators at the consumer
// boundary rather than exposing plan types in query contracts.
enum class CompareOp {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterEqual,
    LessThan,
    LessEqual,
};

// String families use std::string_view for borrowed inputs. Implementations
// must not retain input views beyond the call.
template <typename T>
class IScalarPredicateReader {
 public:
    virtual ~IScalarPredicateReader() = default;

    // 1 = hit; the returned bitmap has Count() bits in the reader's domain.
    virtual TargetBitmap
    In(size_t n, const T* values) const = 0;

    virtual TargetBitmap
    NotIn(size_t n, const T* values) const = 0;

    virtual TargetBitmap
    Range(const T& value, CompareOp op) const = 0;

    virtual TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const = 0;
};

// Null, pattern, and reverse-lookup operations have separate query mixins.
// Construction and serialization are not part of a predicate reader.

}  // namespace milvus::index
