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

#include <exception>
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentInterface.h"
#include "common/protobuf_utils.h"

namespace milvus::rescores {
class Scorer {
 public:
    virtual ~Scorer() = default;

    virtual expr::TypedExprPtr
    filter() = 0;

    // filter result of offset[i] was bitmapview[i]
    // add boost score for idx[i] if bitmap[i] was true
    virtual void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmapView& bitmap,
                std::vector<std::optional<float>>& boost_scores) = 0;

    // score by bitmap
    // filter result of offset[i] was bitmap[offset[i]]
    // add boost score for idx[i] if bitmap[i] was true
    virtual void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmap& bitmap,
                std::vector<std::optional<float>>& boost_scores) = 0;

    // score for all offset
    // used when no filter
    virtual void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                std::vector<std::optional<float>>& boost_scores) = 0;

    virtual float
    weight() = 0;
};

class WeightScorer : public Scorer {
 public:
    WeightScorer(expr::TypedExprPtr filter, float weight)
        : filter_(std::move(filter)), weight_(weight){};

    expr::TypedExprPtr
    filter() override {
        return filter_;
    }

    void
    set_score(std::optional<float>& old_score,
              const proto::plan::FunctionMode& mode);

    void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmapView& bitmap,
                std::vector<std::optional<float>>& boost_scores) override;

    void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmap& bitmap,
                std::vector<std::optional<float>>& boost_scores) override;

    void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                std::vector<std::optional<float>>& boost_scores) override;

    float
    weight() override {
        return weight_;
    }

 private:
    expr::TypedExprPtr filter_;
    float weight_;
};

class RandomScorer : public Scorer {
 public:
    RandomScorer(expr::TypedExprPtr& filter,
                 float weight,
                 const ProtoParams& params) {
        auto param_map = RepeatedKeyValToMap(params);
        if (auto it = param_map.find("seed"); it != param_map.end()) {
            try {
                seed_ = std::stoll(it->second);
            } catch (std::exception e) {
                ThrowInfo(ErrorCode::InvalidParameter,
                          "parse boost random seed params failed: {}",
                          e.what());
            }
        }

        if (auto it = param_map.find("field_id"); it != param_map.end()) {
            try {
                field_ = FieldId(std::stoll(it->second));
            } catch (std::exception e) {
                ThrowInfo(ErrorCode::InvalidParameter,
                          "parse boost random seed field ID failed: {}",
                          e.what());
            }
        } else {
            field_ = FieldId(-1);
        }

        weight_ = weight;
        filter_ = filter;
    }

    expr::TypedExprPtr
    filter() override {
        return filter_;
    }

    void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmapView& bitmap,
                std::vector<std::optional<float>>& boost_scores) override;

    void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmap& bitmap,
                std::vector<std::optional<float>>& boost_scores) override;

    void
    batch_score(const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                std::vector<std::optional<float>>& boost_scores) override;

    void
    random_score(const segcore::SegmentInternalInterface* segment,
                 const proto::plan::FunctionMode& mode,
                 const FixedVector<int64_t>& target_offsets,
                 const FixedVector<int>* idx,
                 std::vector<std::optional<float>>& boost_scores);

    void
    set_score(float random_value,
              std::optional<float>& old_score,
              const proto::plan::FunctionMode& mode);

    float
    weight() override {
        return weight_;
    }

 private:
    expr::TypedExprPtr filter_;
    float weight_;
    int64_t seed_;
    FieldId field_;
};
}  // namespace milvus::rescores