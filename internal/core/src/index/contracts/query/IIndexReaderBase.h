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

#include <cstdint>
#include <memory>

#include "common/Types.h"
#include "index/contracts/query/ReaderCaps.h"

// Type-erased ownership and metadata base for an opened index. Scalar and
// vector query interfaces are independent pure mixins. Concrete readers inherit
// this base together with the mixins they expose. Create a reader, then cross-cast to
// the required query contract; queries do not need the concrete implementation
// type.
//
// Construction, serialization, and loading belong to IArtifactBuilder,
// Artifact, and Loader.

namespace milvus::index {

// Coordinate domain of result offsets. Nested indexes return element offsets;
// the consumer owns projection to rows using the column's offsets.
enum class Domain {
    Row,
    Element,
};

class IIndexReaderBase {
 public:
    virtual ~IIndexReaderBase() = default;

    // Explicitly prepare query storage using the bound context. Synchronous;
    // callers schedule overlap. Single-cell readers retain the loaded cell for
    // subsequent operations. Already-resident views need no preparation.
    virtual void
    Prefetch() const {
    }

    // Reader self-description for consistency checks. Execution-path selection
    // uses the inventory's metadata-derived copy before creating the reader.
    virtual ReaderCaps
    Caps() const = 0;

    // Row (row-level index) or Element (nested index).
    virtual Domain
    CoordDomain() const = 0;

    // Cardinality in this reader's coordinate domain. Predicate bitmaps have this
    // size: element count for nested indexes, row count for row-level indexes.
    // The consumer, not the index, folds element results to rows.
    virtual int64_t
    Count() const = 0;

    virtual DataType
    ValueType() const = 0;
};

using IIndexReaderBasePtr = std::unique_ptr<IIndexReaderBase>;

}  // namespace milvus::index
