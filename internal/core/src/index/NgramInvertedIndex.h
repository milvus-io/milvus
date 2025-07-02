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

#include "index/InvertedIndexTantivy.h"

namespace milvus::exec {
class SegmentExpr;
}  // namespace milvus::exec

namespace milvus::index {
class NgramInvertedIndex : public InvertedIndexTantivy<std::string> {
 public:
    explicit NgramInvertedIndex(const storage::FileManagerContext& ctx,
                                const NgramParams& params);

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    std::optional<TargetBitmap>
    ExecuteQuery(const std::string& literal,
                 proto::plan::OpType op_type,
                 exec::SegmentExpr* segment);

 private:
    std::optional<TargetBitmap>
    PrefixMatchQuery(const std::string& literal, exec::SegmentExpr* segment);

    std::optional<TargetBitmap>
    PostfixMatchQuery(const std::string& literal, exec::SegmentExpr* segment);

    // InnerMatch is something like %xxx%.
    std::optional<TargetBitmap>
    InnerMatchQuery(const std::string& literal, exec::SegmentExpr* segment);

    // Match is something like xxx%xxx%xxx, xxx%xxx, %xxx%xxx, xxx_x etc.
    std::optional<TargetBitmap>
    MatchQuery(const std::string& literal, exec::SegmentExpr* segment);

 private:
    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};
    int64_t field_id_{0};
    std::chrono::time_point<std::chrono::system_clock> index_build_begin_;
};
}  // namespace milvus::index