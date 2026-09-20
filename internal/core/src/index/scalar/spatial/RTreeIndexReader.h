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
#include <vector>

#include "common/Geometry.h"
#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ISpatialReader.h"
#include "index/scalar/spatial/RTreeEngine.h"

// Spatial candidate and null reader, without unrelated point/range methods.
// Geometry values are WKB strings; no value-type template is needed. It uses
// the same build, artifact, load, and pin lifecycle as other scalar families.

namespace milvus::index {

class RTreeGrowingSpatialIndex;

// Immutable, fully validated query state. Readers share this object so neither
// the Boost shards nor row-sized NULL state is copied. The loader creates a
// one-shard state; growing publication may compose several immutable shards.
class RTreeIndexState final {
 public:
    static std::shared_ptr<const RTreeIndexState>
    Create(std::shared_ptr<const RTreeQueryEngine> engine,
           std::shared_ptr<const std::vector<size_t>> null_offsets,
           int64_t total_num_rows);

    const std::vector<std::shared_ptr<const RTreeQueryEngine>>&
    Engines() const;

    const std::vector<size_t>&
    NullOffsets() const;

    int64_t
    Count() const;

    int64_t
    MemoryUsage() const;

 private:
    friend class RTreeGrowingSpatialIndex;

    // Growing owns the only construction path for these shards and validates
    // each new contiguous batch before it can enter an immutable engine. Avoid
    // rescanning every historical shard on every publication.
    static std::shared_ptr<const RTreeIndexState>
    CreateFromValidatedShards(
        std::vector<std::shared_ptr<const RTreeQueryEngine>> engines,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        int64_t total_num_rows);

    RTreeIndexState(
        std::vector<std::shared_ptr<const RTreeQueryEngine>> engines,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        int64_t total_num_rows,
        int64_t memory_usage);

    std::vector<std::shared_ptr<const RTreeQueryEngine>> engines_;
    std::shared_ptr<const std::vector<size_t>> null_offsets_;
    int64_t total_num_rows_{0};
    int64_t memory_usage_{0};
};

class RTreeIndexReader final : public IIndexReaderBase,
                               public ISpatialReader,
                               public INullReader {
 public:
    explicit RTreeIndexReader(std::shared_ptr<const RTreeIndexState> state);

    ~RTreeIndexReader() override;

    // ---- IIndexReaderBase ----------------------------------------

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    cachinglayer::ResourceUsage
    CellByteSize() const override;

    // ---- ISpatialReader ------------------------------------------

    // Return candidate bits in the reader's domain; execution iterates hits and
    // checks exact geometry. SpatialOp is mapped from the plan by the consumer.
    // STIsValid bypasses the index; DWithin passes an expanded bounding box and
    // performs its exact distance check afterwards.
    TargetBitmap
    Candidates(SpatialOp op, const Geometry& query_geom) const override;

    // ---- INullReader -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    std::shared_ptr<const RTreeIndexState> state_;
};

}  // namespace milvus::index
