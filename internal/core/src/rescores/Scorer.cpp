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
#include <cstdint>
#include <memory>
#include <optional>

#include "Scorer.h"
#include "Utils.h"
#include "bitset/bitset.h"
#include "common/Types.h"
#include "pb/schema.pb.h"
#include "rescores/Murmur3.h"
#include "segcore/SegmentInterface.h"

namespace milvus::rescores {

void
WeightScorer::batch_score(milvus::OpContext* op_ctx,
                          const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmapView& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    Assert(bitmap.size() == offsets.size());
    for (auto i = 0; i < offsets.size(); i++) {
        if (bitmap[i] > 0) {
            set_score(boost_scores[i], mode);
        }
    }
}

void
WeightScorer::batch_score(milvus::OpContext* op_ctx,
                          const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmap& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    auto bitmap_size = bitmap.size();
    for (auto i = 0; i < offsets.size(); i++) {
        auto offset = offsets[i];
        // Bounds check: offset must be within bitmap size.
        // Race condition: text index may lag behind vector index,
        // causing offsets to reference rows not yet in text index.
        if (offset >= 0 && static_cast<size_t>(offset) < bitmap_size) {
            if (bitmap[offset] > 0) {
                set_score(boost_scores[i], mode);
            }
        }
        // If offset is out of bounds, treat as "no match" (don't apply boost)
    }
};

void
WeightScorer::batch_score(milvus::OpContext* op_ctx,
                          const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          std::vector<std::optional<float>>& boost_scores) {
    for (auto i = 0; i < offsets.size(); i++) {
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
RandomScorer::batch_score(milvus::OpContext* op_ctx,
                          const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmapView& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    Assert(bitmap.size() == offsets.size());
    FixedVector<int64_t> target_offsets;
    FixedVector<int> idx;
    target_offsets.reserve(offsets.size());
    idx.reserve(offsets.size());

    for (auto i = 0; i < offsets.size(); i++) {
        if (bitmap[i] > 0) {
            target_offsets.push_back(static_cast<int64_t>(offsets[i]));
            idx.push_back(i);
        }
    }

    // skip if empty
    if (target_offsets.empty()) {
        return;
    }

    random_score(op_ctx, segment, mode, target_offsets, &idx, boost_scores);
}

void
RandomScorer::batch_score(milvus::OpContext* op_ctx,
                          const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          const TargetBitmap& bitmap,
                          std::vector<std::optional<float>>& boost_scores) {
    FixedVector<int64_t> target_offsets;
    FixedVector<int> idx;
    target_offsets.reserve(offsets.size());
    idx.reserve(offsets.size());

    for (auto i = 0; i < offsets.size(); i++) {
        if (bitmap[offsets[i]] > 0) {
            target_offsets.push_back(static_cast<int64_t>(offsets[i]));
            idx.push_back(i);
        }
    }

    // skip if empty
    if (target_offsets.empty()) {
        return;
    }

    random_score(op_ctx, segment, mode, target_offsets, &idx, boost_scores);
}

void
RandomScorer::batch_score(milvus::OpContext* op_ctx,
                          const segcore::SegmentInternalInterface* segment,
                          const proto::plan::FunctionMode& mode,
                          const FixedVector<int32_t>& offsets,
                          std::vector<std::optional<float>>& boost_scores) {
    FixedVector<int64_t> target_offsets;
    target_offsets.reserve(offsets.size());

    for (int offset : offsets) {
        target_offsets.push_back(static_cast<int64_t>(offset));
    }

    random_score(op_ctx, segment, mode, target_offsets, nullptr, boost_scores);
}

void
RandomScorer::random_score(milvus::OpContext* op_ctx,
                           const segcore::SegmentInternalInterface* segment,
                           const proto::plan::FunctionMode& mode,
                           const FixedVector<int64_t>& target_offsets,
                           const FixedVector<int>* idx,
                           std::vector<std::optional<float>>& boost_scores) {
    if (field_.get() != -1) {
        auto array = segment->bulk_subscript(
            op_ctx, field_, target_offsets.data(), target_offsets.size());
        AssertInfo(array->has_scalars(), "seed field must be scalar");
        AssertInfo(array->scalars().has_long_data(),
                   "now only support int64 field as seed");
        // TODO: Support varchar and int32 field as random field.

        const auto& data = array->scalars().long_data();
        for (int i = 0; i < data.data_size(); i++) {
            auto a = data.data()[i];
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
        const auto segment_id = segment->get_segment_id();
        for (int i = 0; i < target_offsets.size(); i++) {
            double random_score = hash_to_double(MurmurHash3_x64_64_Special(
                target_offsets[i] + segment_id, seed_));
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

void
FieldScorer::batch_score(milvus::OpContext* op_ctx,
                         const segcore::SegmentInternalInterface* segment,
                         const proto::plan::FunctionMode& mode,
                         const FixedVector<int32_t>& offsets,
                         const TargetBitmapView& bitmap,
                         std::vector<std::optional<float>>& boost_scores) {
    Assert(bitmap.size() == offsets.size());
    FixedVector<int64_t> target_offsets;
    FixedVector<int> idx;
    target_offsets.reserve(offsets.size());
    idx.reserve(offsets.size());

    for (auto i = 0; i < offsets.size(); i++) {
        if (bitmap[i] > 0) {
            target_offsets.push_back(static_cast<int64_t>(offsets[i]));
            idx.push_back(i);
        }
    }

    if (target_offsets.empty()) {
        return;
    }

    field_score(op_ctx, segment, mode, target_offsets, &idx, boost_scores);
}

void
FieldScorer::batch_score(milvus::OpContext* op_ctx,
                         const segcore::SegmentInternalInterface* segment,
                         const proto::plan::FunctionMode& mode,
                         const FixedVector<int32_t>& offsets,
                         const TargetBitmap& bitmap,
                         std::vector<std::optional<float>>& boost_scores) {
    FixedVector<int64_t> target_offsets;
    FixedVector<int> idx;
    target_offsets.reserve(offsets.size());
    idx.reserve(offsets.size());

    for (auto i = 0; i < offsets.size(); i++) {
        if (bitmap[offsets[i]] > 0) {
            target_offsets.push_back(static_cast<int64_t>(offsets[i]));
            idx.push_back(i);
        }
    }

    if (target_offsets.empty()) {
        return;
    }

    field_score(op_ctx, segment, mode, target_offsets, &idx, boost_scores);
}

void
FieldScorer::batch_score(milvus::OpContext* op_ctx,
                         const segcore::SegmentInternalInterface* segment,
                         const proto::plan::FunctionMode& mode,
                         const FixedVector<int32_t>& offsets,
                         std::vector<std::optional<float>>& boost_scores) {
    FixedVector<int64_t> target_offsets;
    target_offsets.reserve(offsets.size());

    for (int offset : offsets) {
        target_offsets.push_back(static_cast<int64_t>(offset));
    }

    field_score(op_ctx, segment, mode, target_offsets, nullptr, boost_scores);
}

namespace {
// Read DataArray[i] as float32. Returns std::nullopt if the value is null
// (valid_data[i] == false). Supports Int8/16/32/64, Float, Double.
std::optional<float>
read_as_float(const proto::schema::ScalarField& scalars,
              const google::protobuf::RepeatedField<bool>& valid_data,
              int i) {
    if (!valid_data.empty() && !valid_data.Get(i)) {
        return std::nullopt;
    }
    if (scalars.has_int_data()) {
        return static_cast<float>(scalars.int_data().data().Get(i));
    }
    if (scalars.has_long_data()) {
        return static_cast<float>(scalars.long_data().data().Get(i));
    }
    if (scalars.has_float_data()) {
        return scalars.float_data().data().Get(i);
    }
    if (scalars.has_double_data()) {
        return static_cast<float>(scalars.double_data().data().Get(i));
    }
    ThrowInfo(ErrorCode::InvalidParameter,
              "boost field_score only supports numeric field");
}
}  // namespace

void
FieldScorer::field_score(milvus::OpContext* op_ctx,
                         const segcore::SegmentInternalInterface* segment,
                         const proto::plan::FunctionMode& mode,
                         const FixedVector<int64_t>& target_offsets,
                         const FixedVector<int>* idx,
                         std::vector<std::optional<float>>& boost_scores) {
    auto array = segment->bulk_subscript(
        op_ctx, field_, target_offsets.data(), target_offsets.size());
    AssertInfo(array->has_scalars(),
               "boost field_score field must be scalar");

    const auto& scalars = array->scalars();
    const auto& valid_data = array->valid_data();
    int64_t data_size = static_cast<int64_t>(target_offsets.size());

    for (int64_t i = 0; i < data_size; i++) {
        auto value = read_as_float(scalars, valid_data, i);
        float field_value;
        if (value.has_value()) {
            field_value = value.value();
        } else if (missing_value_.has_value()) {
            field_value = missing_value_.value();
        } else {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "boost field_score encountered null value without "
                      "missing_value configured, field_id={}",
                      field_.get());
        }

        if (idx == nullptr) {
            set_score(field_value, boost_scores[i], mode);
        } else {
            set_score(field_value, boost_scores[idx->at(i)], mode);
        }
    }
}

void
FieldScorer::set_score(float field_value,
                       std::optional<float>& score,
                       const proto::plan::FunctionMode& mode) {
    if (!score.has_value()) {
        score = std::make_optional(field_value * weight_);
    } else {
        score = std::make_optional(function_score_merge(
            score.value(), (field_value * weight_), mode));
    }
}
}  // namespace milvus::rescores