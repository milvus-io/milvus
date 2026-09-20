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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"

// FM query reader: IPatternMatchReader and INullReader, without unrelated
// point/range or reverse-lookup methods. Null predicates use its null bitmap.

namespace milvus::index {

namespace fmindex {
class FMIndex;
}

// Owns the family-private mmap and its unique staging directory. The engine
// stores views into Data(), so readers retain this object for their lifetime.
class FmIndexMappedFile final {
 public:
    FmIndexMappedFile(void* data,
                      size_t mapped_bytes,
                      std::string staging_directory);
    FmIndexMappedFile(const FmIndexMappedFile&) = delete;
    FmIndexMappedFile&
    operator=(const FmIndexMappedFile&) = delete;
    ~FmIndexMappedFile();

    const uint8_t*
    Data() const;

    size_t
    MappedBytes() const;

    size_t
    HeapBytes() const;

 private:
    void* data_{nullptr};
    size_t mapped_bytes_{0};
    std::string staging_directory_;
};

enum class FmIndexStateOrigin {
    Builder,
    Persisted,
};

// Immutable owner/state bundle shared by readers and builder artifacts.
// mapped_file precedes engine so the FM-index view is destroyed before its
// backing mapping is unmapped.
class FmIndexStorage final {
 public:
    static std::shared_ptr<const FmIndexStorage>
    Create(std::shared_ptr<const FmIndexMappedFile> mapped_file,
           std::shared_ptr<const fmindex::FMIndex> engine,
           TargetBitmap null_bitmap,
           int64_t total_rows,
           DataType value_type,
           bool nullable,
           FmIndexStateOrigin origin);

    const fmindex::FMIndex&
    Engine() const;

    const TargetBitmap&
    NullBitmap() const;

    int64_t
    Count() const;

    int64_t
    TotalTokens() const;

    DataType
    ValueType() const;

    bool
    Nullable() const;

    int64_t
    MemoryUsage() const;

    int64_t
    FileBytes() const;

 private:
    FmIndexStorage(std::shared_ptr<const FmIndexMappedFile> mapped_file,
                   std::shared_ptr<const fmindex::FMIndex> engine,
                   TargetBitmap null_bitmap,
                   int64_t total_rows,
                   int64_t total_tokens,
                   DataType value_type,
                   bool nullable,
                   int64_t memory_usage,
                   int64_t file_bytes);

    std::shared_ptr<const FmIndexMappedFile> mapped_file_;
    std::shared_ptr<const fmindex::FMIndex> engine_;
    TargetBitmap null_bitmap_;
    int64_t total_rows_{0};
    int64_t total_tokens_{0};
    DataType value_type_{DataType::VARCHAR};
    bool nullable_{false};
    int64_t memory_usage_{0};
    int64_t file_bytes_{0};
};

class FmIndexReader final : public IIndexReaderBase,
                            public IPatternMatchReader,
                            public INullReader {
 public:
    // Query cost guard, injected at construction rather than read from global
    // segment configuration. See ShouldUseForOp below.
    FmIndexReader(std::shared_ptr<const FmIndexStorage> storage,
                  double cost_ratio = 0.001);

    ~FmIndexReader() override;

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

    // ---- IPatternMatchReader -------------------------------------

    // PrefixMatch / PostfixMatch / InnerMatch are answered EXACTLY. General
    // LIKE (`Match`) is answered with CANDIDATES ONLY: the occurrences of the
    // pattern's rarest literal fragment, a superset of the exact answer that
    // the consumer MUST recheck against the raw column
    // (PhyUnaryRangeFilterExpr::ExecFMMatch). `RegexMatch` is declined by
    // `ShouldUseForOp` below and never arrives.
    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    // Exact for the three anchored ops; false for `Match`, which is answered
    // with a candidate superset the consumer must recheck.
    bool
    PatternMatchIsExact(PatternOp op) const override;

    bool
    ShouldUseForOp(PatternOp op, std::string_view pattern) const override;

    // ---- INullReader -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // O(|pattern|) occurrence count; -1 means "unknown, accept".
    int64_t
    PatternCount(std::string_view pattern, PatternOp op) const;

    TargetBitmap
    DocsToBitmap(const std::vector<uint64_t>& docs) const;

    // Count-first guard for general LIKE (Match). Declines an empty pattern
    // and every pattern with no literal fragment (`%`, `%_%`), because phase 1
    // has no seed to search for. Otherwise the rarest fragment is scored as
    // occ * sa_sample_rate < cost_ratio * tokens, the same locate-only bound
    // as the anchored ops. The consumer's phase-2 recheck reads those
    // candidates back from the raw column; its byte cost is deliberately NOT
    // priced here (known approximation for long rows * unselective
    // fragments).
    bool
    MatchGuardAccepts(std::string_view pattern) const;

    // Rarest literal fragment of `pattern`, or nullopt when the pattern has
    // none. Shared by MatchGuardAccepts and the Match branch of PatternMatch
    // so the guard and the query can never disagree on which fragment seeds
    // phase 1.
    struct RarestFragment {
        std::string literal;
        int64_t occurrences{0};
    };

    std::optional<RarestFragment>
    RarestMatchFragment(std::string_view pattern) const;

    std::shared_ptr<const FmIndexStorage> storage_;

    // The cost ratio is fixed for this reader's lifetime. TODO: if live query
    // configuration updates are required, inject a policy callback instead of
    // assuming this construction-time value follows later changes.
    double cost_ratio_{0.001};
};

}  // namespace milvus::index
