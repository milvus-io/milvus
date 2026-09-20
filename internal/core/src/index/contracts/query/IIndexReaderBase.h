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

#include "cachinglayer/Utils.h"
#include "common/Types.h"
#include "index/contracts/query/ReaderCaps.h"

// Type-erased ownership and metadata base for an opened index. Scalar and
// vector query interfaces are independent pure mixins. Concrete readers inherit
// this base together with the mixins they expose. Pin once, then cross-cast to
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

    // Reader self-description for consistency checks. Execution-path selection
    // uses the inventory's metadata-derived copy before pinning the reader.
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

    // Heap-resident bytes owned by this reader. File-backed ownership is
    // reported separately by CellByteSize(). A documented zero may mean native
    // accounting is unavailable, not that the resident footprint was measured
    // as empty. A documented legacy loader may attach a post-load cache
    // estimate to CellByteSize; it remains an estimate and is charged once with
    // the loaded cell, never once per reader pin.
    virtual int64_t
    MemoryUsage() const = 0;

    // Report resources owned by the opened reader. Heap-resident structures go
    // in the memory half; owned mmap/file-backed bytes go in the file half.
    // When native accounting is unavailable, an implementation may return an
    // explicitly documented all-zero sentinel. A documented legacy path may
    // instead return a post-load cache charge estimate; that estimate is not
    // measured native ownership and must remain separate from pre-load
    // admission. All other non-sentinel values are actual owned resources.
    virtual cachinglayer::ResourceUsage
    CellByteSize() const = 0;
};

using IIndexReaderBasePtr = std::unique_ptr<IIndexReaderBase>;

}  // namespace milvus::index
