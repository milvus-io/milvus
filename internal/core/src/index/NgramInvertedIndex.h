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
#include <string>
#include <boost/filesystem.hpp>
#include <optional>
#include "index/JsonInvertedIndex.h"
#include "index/InvertedIndexTantivy.h"

namespace milvus::exec {
class SegmentExpr;
}  // namespace milvus::exec

namespace milvus::index {

class NgramInvertedIndex : public InvertedIndexTantivy<std::string> {
 public:
    // for string/varchar type
    explicit NgramInvertedIndex(const storage::FileManagerContext& ctx,
                                const NgramParams& params);

    // for json type
    explicit NgramInvertedIndex(const storage::FileManagerContext& ctx,
                                const NgramParams& params,
                                const std::string& nested_path);

    BinarySet
    Serialize(const Config& config) override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config) override;

    void
    LoadIndexMetas(const std::vector<std::string>& index_files,
                   const Config& config) override;

    void
    RetainTantivyIndexFiles(std::vector<std::string>& index_files) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    void
    BuildWithJsonFieldData(const std::vector<FieldDataPtr>& datas);

    // For unit tests only - combines Phase1 and Phase2 in one call
    std::optional<TargetBitmap>
    ExecuteQueryForUT(const std::string& literal,
                      proto::plan::OpType op_type,
                      exec::SegmentExpr* segment,
                      const TargetBitmap* pre_filter = nullptr);

    // Check if literal can be handled by ngram index (length >= min_gram)
    bool
    CanHandleLiteral(const std::string& literal,
                     proto::plan::OpType op_type) const;

    // Phase1: Execute ngram index query, AND-merge result into candidates
    // Requires: CanHandleLiteral(literal, op_type) == true
    // Requires: candidates must be non-empty (caller initializes it)
    // Ngram query result is AND-merged with existing candidates.
    void
    ExecutePhase1(const std::string& literal,
                  proto::plan::OpType op_type,
                  TargetBitmap& candidates);

    // Phase2: Execute post-filter verification on a specific range
    // - segment_offset: starting position in segment
    // - batch_size: number of rows to process
    // - candidates: bitmap of size batch_size (relative to the range)
    // Requires: CanHandleLiteral(literal, op_type) == true
    void
    ExecutePhase2(const std::string& literal,
                  proto::plan::OpType op_type,
                  exec::SegmentExpr* segment,
                  TargetBitmap& candidates,
                  int64_t segment_offset,
                  int64_t batch_size);

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::NGRAM;
    }

    void
    finish() {
        this->wrapper_->finish();
    }

    void
    create_reader(SetBitsetFn set_bitset) {
        this->wrapper_->create_reader(set_bitset);
    }

 private:
    void
    ApplyIterativeNgramFilter(const std::vector<std::string>& sorted_terms,
                              size_t total_count,
                              TargetBitmap& bitset);

    bool
    ShouldUseBatchStrategy(double pre_filter_hit_rate) const;

    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};
    int64_t field_id_{0};
    size_t avg_row_size_{0};
    std::chrono::time_point<std::chrono::system_clock> index_build_begin_;

    // for json type
    std::string nested_path_;
    JsonInvertedIndexParseErrorRecorder error_recorder_;
};
}  // namespace milvus::index