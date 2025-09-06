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

#include <cstddef>
#include <optional>
#include <random>
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "Scorer.h"
#include "Utils.h"
#include "log/Log.h"
#include "rescores/Murmur3.h"

namespace milvus::rescores {

void
WeightScorer::batch_score(const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmapView& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    Assert(bitmap.size() == offsets.size());
    for (auto i = 0; i <= offsets.size(); i++) {
        if (bitmap[i] > 0) {
            set_score(boost_scores[i], mode);
        }
    }
}

void
WeightScorer::batch_score(const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmap& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    for (auto i = 0; i <= offsets.size(); i++) {
        if (bitmap[offsets[i]] > 0) {
            set_score(boost_scores[i], mode);
        }
    }
};

void
WeightScorer::batch_score(const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          std::vector<std::optional<float>>& boost_scores) {
    for (auto i = 0; i <= offsets.size(); i++) {
        set_score(boost_scores[i], mode);
    }
};

void
WeightScorer::set_score(std::optional<float>& score,
                        const proto::plan::FunctionMode& mode) {
    if (!score.has_value()) {
        score = std::make_optional(weight_);
    } else {
        score = std::make_optional(
            function_score_merge(score.value(), weight_, mode));
    }
}

void
RandomScorer::batch_score(const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmapView& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    Assert(bitmap.size() == offsets.size());
    FixedVector<int64_t> target_offsets;
    FixedVector<int> idx;
    for (auto i = 0; i <= offsets.size(); i++) {
        if (bitmap[i] > 0) {
            target_offsets.push_back(static_cast<int64_t>(offsets[i]));
            idx.push_back(i);
        }
    }

    // skip if empty
    if (target_offsets.empty()) {
        return;
    }

    random_score(segment, mode, target_offsets, &idx, boost_scores);
}

void
RandomScorer::batch_score(const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmap& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    FixedVector<int64_t> target_offsets;
    FixedVector<int> idx;
    for (auto i = 0; i <= offsets.size(); i++) {
        if (bitmap[offsets[i]] > 0) {
            target_offsets.push_back(static_cast<int64_t>(offsets[i]));
            idx.push_back(i);
        }
    }

    // skip if empty
    if (target_offsets.empty()) {
        return;
    }

    random_score(segment, mode, target_offsets, &idx, boost_scores);
}

void
RandomScorer::batch_score(const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          std::vector<std::optional<float>>& boost_scores) {
    FixedVector<int64_t> target_offsets;
    for (auto i = 0; i <= offsets.size(); i++) {
        target_offsets.push_back(static_cast<int64_t>(offsets[i]));
    }

    random_score(segment, mode, target_offsets, nullptr, boost_scores);
}

void
RandomScorer::random_score(const segcore::SegmentInternalInterface* segment,
                           const proto::plan::FunctionMode& mode,
                           const FixedVector<int64_t>& target_offsets,
                           const FixedVector<int>* idx,
                           std::vector<std::optional<float>>& boost_scores) {
    if (field_.get() != -1) {
        auto array = segment->bulk_subscript(
            field_, target_offsets.data(), target_offsets.size());
        AssertInfo(array->has_scalars(), "seed field must be scalar");
        AssertInfo(array->scalars().has_long_data(),
                   "now only support int64 field as seed");
        // TODO: Support varchar and int32 field as random field.

        auto datas = array->scalars().long_data();
        for (int i = 0; i < datas.data_size(); i++) {
            auto a = datas.data()[i];
            auto random_score =
                hash_to_double(MurmurHash3_x64_64_Special(a, seed_));
            if (idx == nullptr) {
                set_score(random_score, boost_scores[i], mode);
            } else {
                set_score(random_score, boost_scores[idx->at(i)], mode);
            }
        }
    } else {
        // if not set field, use offset and seed to hash.
        for (int i = 0; i < target_offsets.size(); i++) {
            double random_score = hash_to_double(MurmurHash3_x64_64_Special(
                target_offsets[i] + segment->get_segment_id(), seed_));
            if (idx == nullptr) {
                set_score(random_score, boost_scores[i], mode);
            } else {
                set_score(random_score, boost_scores[idx->at(i)], mode);
            }
        }
    }
}

void
RandomScorer::set_score(float random_value,
                        std::optional<float>& score,
                        const proto::plan::FunctionMode& mode) {
    if (!score.has_value()) {
        score = std::make_optional(random_value * weight_);
    } else {
        score = std::make_optional(function_score_merge(
            score.value(), (random_value * weight_), mode));
    }
}
}  // namespace milvus::rescores