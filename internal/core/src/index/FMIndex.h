// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <sys/mman.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/fmindex/FMIndex.h"
#include "pb/schema.pb.h"
#include "segcore/SegcoreConfig.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

// File-name / meta keys used by the V3 packed-file entries. These strings are
// persisted, do not edit them.
constexpr const char* FMINDEX_BLOB_FILE_NAME = "fm_index.bin";
constexpr const char* FMINDEX_NULL_BITMAP_FILE_NAME = "fm_index_null_bitmap";
constexpr const char* FMINDEX_META_TOTAL_ROWS = "total_rows";
constexpr const char* FMINDEX_META_NULLABLE = "nullable";

// Scalar-index wrapper around the self-contained byte-exact FM-index library
// (milvus::index::fmindex::FMIndex). It accelerates LIKE prefix/infix/suffix on
// VARCHAR exactly (no recheck). General LIKE / regex / range fall back to the
// raw-data scan (see ShouldUseOp).
class FMIndex : public ScalarIndex<std::string> {
 public:
    using MemFileManager = storage::MemFileManagerImpl;
    using MemFileManagerPtr = std::shared_ptr<MemFileManager>;

    explicit FMIndex(const storage::FileManagerContext& ctx,
                     const FMIndexParams& params);

    ~FMIndex() {
        // fm_ may view the mmap'd region (LoadView path); unmap after the
        // wrapper body runs and before fm_'s own (view-only) destruction.
        if (is_mmap_ && mmap_data_ != nullptr && mmap_data_ != MAP_FAILED) {
            munmap(mmap_data_, mmap_size_);
            unlink(mmap_filepath_.c_str());
        }
    }

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::FMINDEX;
    }

    // ---- deprecated / unsupported entry points ----

    void
    Load(const BinarySet& binary_set, const Config& config = {}) override {
        ThrowInfo(ErrorCode::NotImplemented, "load v1 deprecated");
    }

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    void
    BuildWithDataset(const DatasetPtr& dataset,
                     const Config& config = {}) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "BuildWithDataset should be deprecated");
    }

    // deprecated, only used in small chunk index.
    void
    Build(size_t n,
          const std::string* values,
          const bool* valid_data) override {
        ThrowInfo(ErrorCode::NotImplemented, "Build should not be called");
    }

    // ---- build ----

    void
    Build(const Config& config = {}) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    // BuildWithRawDataForUT should be only used in ut. Only string is supported.
    void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config = {}) override;

    // ---- serialize / upload / load (V3 packed-file path) ----

    BinarySet
    Serialize(const Config& config) override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    void
    WriteEntries(storage::IndexEntryWriter* writer) override;

    void
    LoadEntries(storage::IndexEntryReader& reader,
                const Config& config) override;

    // ---- query ----

    const TargetBitmap
    In(size_t n, const std::string* values) override;

    const TargetBitmap
    NotIn(size_t n, const std::string* values) override;

    const TargetBitmap
    IsNull() override;

    TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    Range(const std::string& value, OpType op) override {
        ThrowInfo(ErrorCode::Unsupported, "FM-index does not support range");
    }

    const TargetBitmap
    Range(const std::string& lower_bound_value,
          bool lb_inclusive,
          const std::string& upper_bound_value,
          bool ub_inclusive) override {
        ThrowInfo(ErrorCode::Unsupported, "FM-index does not support range");
    }

    const TargetBitmap
    PatternMatch(const std::string& pattern, proto::plan::OpType op) override;

    bool
    SupportPatternMatch() const override {
        return true;
    }

    // Routing gate for the executor (UnaryIndexFunc via CanUseIndexForOp),
    // deciding in two stages inside ONE call:
    //
    // 1. Op ALLOWLIST with a false default: declining an op merely downgrades
    //    to the raw-data scan (correct, just slower), while wrongly accepting
    //    one routes it into a method that throws (Range() is Unsupported;
    //    PatternMatch rejects Match/RegexMatch) and FAILS the query. So the
    //    safe default for any op not explicitly supported — including future
    //    enum additions — is false. The allowlist is exactly the three anchored
    //    pattern ops (PrefixMatch/PostfixMatch/InnerMatch, the LIKE prefix/
    //    infix/suffix forms), all answered exactly by the FM-index. The equality
    //    family (Equal/NotEqual, IN/NOT IN) is deliberately NOT in the allowlist
    //    — see the declines below.
    //
    // 2. Count-first guard on the anchored pattern ops, when the caller
    //    passed the concrete literal: enumeration costs occ x sa_sample_rate
    //    random LF steps (each a cache miss on the wavelet), the scan streams
    //    TotalTokens() bytes sequentially (memchr-fast) — accelerate iff
    //    occ x sr < ratio x tokens, where ratio is the measured random-step /
    //    sequential-byte price crossover: ~0.0008 on 1 GiB of per-row random
    //    text (cache-adversarial for LF, memchr-friendly for the scan),
    //    ~0.002-0.008 on repetitive corpora. The default 0.001 takes the
    //    conservative end, so the index is never left slower than the scan it
    //    replaces; it is configurable as queryNode.fmindexCostRatio (read
    //    from SegcoreConfig, wired at query-node init). The count is
    //    O(|pattern|) backward search (microseconds, no locate). A high-hit
    //    literal (LIKE '%a%' over most rows) therefore declines and the expr
    //    runs the scan — exact either way, this only picks the cheaper path.
    //    An empty pattern means EITHER `LIKE '%'` (answered by an O(rows)
    //    bitmap clone, always accelerate) OR a caller without literal
    //    information — both accelerate, so no flag is needed to tell them
    //    apart.
    bool
    ShouldUseOp(proto::plan::OpType op,
                const std::string& pattern = "") const override {
        switch (op) {
            case proto::plan::OpType::PrefixMatch:
            case proto::plan::OpType::PostfixMatch:
            case proto::plan::OpType::InnerMatch: {
                if (pattern.empty()) {
                    return true;
                }
                int64_t occ = PatternCount(pattern, op);
                if (occ < 0) {
                    return true;
                }
                const double ratio =
                    static_cast<double>(segcore::SegcoreConfig::default_config()
                                            .get_fmindex_cost_ratio());
                return static_cast<double>(occ) *
                           static_cast<double>(fm_.sa_sample_rate()) <
                       ratio * static_cast<double>(TotalTokens());
            }
            // Notable declines (fall back to the scan / another index):
            // - Equal/NotEqual (and IN/NOT IN via TermExpr): FM equality
            //   enumerates ALL prefix candidates of the value before the length
            //   filter, which loses to a scan or an equality-oriented index —
            //   FMINDEX is a substring index, not an equality index.
            // - general LIKE (Match) / RegexMatch: not answered exactly in v1.
            // - lexicographic range (GT/GE/LT/LE): needs forward navigation the
            //   FM-index does not carry.
            default:
                return false;
        }
    }

    // ---- misc ----

    int64_t
    Count() override {
        return total_rows_;
    }

    int64_t
    Size() override {
        return Count();
    }

    const bool
    HasRawData() const override {
        return false;
    }

    std::optional<std::string>
    Reverse_Lookup(size_t offset) const override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "Reverse_Lookup should not be handled by FM index");
    }

 private:
    // Occurrence/document count for `pattern` under `op`, answered by backward
    // search ALONE — O(|pattern|), NO suffix-array locate. The guard's input:
    // Prefix/PostfixMatch return the exact matching-document count; InnerMatch
    // returns the occurrence count, an upper bound on matching rows (a row may
    // contain the pattern more than once — the bias only makes the guard
    // decline earlier, the safe direction). Returns -1 for uncounted ops.
    int64_t
    PatternCount(const std::string& pattern, proto::plan::OpType op) const {
        auto* p = reinterpret_cast<const uint8_t*>(pattern.data());
        switch (op) {
            case proto::plan::OpType::PrefixMatch:
                return static_cast<int64_t>(
                    fm_.CountPrefixDocs(p, pattern.size()));
            case proto::plan::OpType::PostfixMatch:
                return static_cast<int64_t>(
                    fm_.CountSuffixDocs(p, pattern.size()));
            case proto::plan::OpType::InnerMatch:
                return static_cast<int64_t>(fm_.Count(p, pattern.size()));
            default:
                return -1;
        }
    }

    // Total byte length over all non-null rows — the text a brute scan would
    // stream. The guard normalizes occ by TOKENS, not rows: scan cost tracks
    // bytes, so an occ/rows threshold would drift with row length while
    // occ/tokens does not. Computed ONCE eagerly at build/load time (see
    // ComputeTotalTokens); this getter is a pure read, so the concurrent const
    // query path (ShouldUseOp) needs no synchronization.
    int64_t
    TotalTokens() const {
        return total_tokens_;
    }

    // Set total_tokens_ = sum of non-null row byte lengths, DERIVED from the FM
    // blob instead of a per-row length array. The FM index stores one separator
    // per document, so its internal length is (sum of content bytes) + one per
    // row; that internal length is exposed as bwt_size() - 1 (see the FM library
    // Build). Null rows are indexed as empty documents (0 content + 1 separator),
    // so they add nothing to the content sum. Hence:
    //     total_tokens = (bwt_size() - 1) - total_rows.
    // Called ONLY from the single-threaded build/load paths, AFTER fm_ is built
    // or loaded (so bwt_size() is valid). Doing it eagerly here removes the data
    // race a lazy write from the const, concurrently-called TotalTokens() would
    // otherwise introduce.
    void
    ComputeTotalTokens() {
        if (!fm_.valid() || total_rows_ <= 0) {
            total_tokens_ = 0;
            return;
        }
        int64_t text_len = static_cast<int64_t>(fm_.bwt_size()) - 1;
        int64_t t = text_len - total_rows_;
        total_tokens_ = t < 0 ? 0 : t;
    }

    // Turn a sorted, unique list of matching doc ids into a bitmap over rows.
    TargetBitmap
    DocsToBitmap(const std::vector<uint64_t>& docs) const;

    fmindex::FMIndex fm_;

    proto::schema::FieldSchema schema_;
    int64_t field_id_{0};

    // For the mmap load path: streams the FM-index blob to a local file and
    // maps it, so fm_ (via LoadView) serves queries zero-copy from the mapping.
    std::shared_ptr<storage::DiskFileManagerImpl> disk_file_manager_;
    bool is_mmap_{false};
    char* mmap_data_{nullptr};
    size_t mmap_size_{0};
    std::string mmap_filepath_;

    // Build-time input only. Once fm_ is built or loaded, its serialized
    // sa_sample_rate() is the query-side source of truth for the cost guard.
    uint32_t build_sa_sample_rate_{8};
    // rank-directory block granularity (fm_block_bytes); larger = smaller
    // resident directory. Read from the blob on load, so only used at build.
    uint32_t block_bytes_{64};

    // number of rows indexed (one document per row, including nulls)
    int64_t total_rows_{0};

    // total non-null byte length (the guard's scan-cost proxy); computed once
    // eagerly at build/load time (see ComputeTotalTokens), then read-only — so
    // the concurrent const query path (ShouldUseOp) reads it without a lock.
    int64_t total_tokens_{0};

    // bit i set iff row i is null. Built from field validity at build time and
    // persisted bit-packed (1 bit/row) so IsNull/IsNotNull and the empty-pattern
    // (`LIKE '%'`) path can exclude null rows after load. This replaces the old
    // per-row uint32 length array (32 bits/row): FMINDEX declines equality, so
    // no per-row length is needed, and the token total is derived from the blob.
    TargetBitmap null_bitmap_;
};

}  // namespace milvus::index
