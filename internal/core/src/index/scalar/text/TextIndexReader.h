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
#include <cstddef>
#include <memory>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ITextMatchReader.h"

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

// Text-match reader over a composed, stable tantivy snapshot. Construction and
// loading are separate from querying. Null offsets are frozen with the same
// snapshot so match results and SQL validity always share one row generation.

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

class TextIndexReader final : public IIndexReaderBase,
                              public ITextMatchReader,
                              public INullReader {
 public:
    // Constructed by TextIndexLoader::Open or by consuming a completed
    // TextIndexArtifact. The constructor accepts ready state rather than
    // building or loading it.
    TextIndexReader(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        int64_t count,
        DataType value_type,
        bool file_backed,
        size_t payload_bytes);

    ~TextIndexReader() override;

    TextIndexReader(const TextIndexReader&) = delete;
    TextIndexReader&
    operator=(const TextIndexReader&) = delete;

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

    // ---- Resource accounting -----------------------------------------------

    cachinglayer::ResourceUsage
    CellByteSize() const override;

    // ---- ITextMatchReader ----------------------------------------

    TargetBitmap
    MatchQuery(std::string_view query,
               uint32_t min_should_match) const override;

    TargetBitmap
    PhraseMatchQuery(std::string_view query, uint32_t slop) const override;

    TargetBitmap
    FuzzyMatchQuery(std::string_view query,
                    uint32_t max_edit_distance) const override;

    // ---- INullReader ---------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // Allocates the result bitmap and installs the tantivy set-bit callback.
    TargetBitmap
    PrepareBitset() const;

    // Destroy the engine before the optional mmap directory owner.
    std::shared_ptr<storage::LocalDirectory> directory_;

    // Composed immutable engine snapshot. Writer locks, commit timing, and append
    // state belong to the growing owner, not this query reader.
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    // Validity materialized once at construction instead of replaying the
    // frozen null-offset vector on every IsNull/IsNotNull call, which sit on
    // the query hot path. The offset vector is not retained: it would duplicate
    // the same information at 8 bytes per null row. `all_valid_` leaves the
    // bitmap empty rather than allocating rows/8 bytes of all-ones for a
    // non-nullable field or a field with no nulls.
    bool all_valid_{false};
    TargetBitmap valid_bitmap_;

    int64_t count_{0};
    DataType value_type_{DataType::NONE};
    bool file_backed_{false};
    size_t payload_bytes_{0};
};

}  // namespace milvus::index
