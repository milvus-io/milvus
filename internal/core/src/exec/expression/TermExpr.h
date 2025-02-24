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

#include <fmt/core.h>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

template <typename T>
struct TermElementFuncSet {
    bool
    operator()(const std::unordered_set<T>& srcs, T val) {
        return srcs.find(val) != srcs.end();
    }
};

template <typename T>
struct TermIndexFunc {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    TargetBitmap
    operator()(Index* index, size_t n, const IndexInnerType* val) {
        return index->In(n, val);
    }
};

class PhyTermFilterExpr : public SegmentExpr {
 public:
    PhyTermFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::TermFilterExpr>& expr,
        const std::string& name,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        milvus::Timestamp timestamp,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::NONE,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr),
          query_timestamp_(timestamp) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

 private:
    void
    InitPkCacheOffset();

    template <typename T>
    bool
    CanSkipSegment();

    VectorPtr
    ExecPkTermImpl();

    template <typename T>
    VectorPtr
    ExecVisitorImpl(OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    ExecVisitorImplForIndex(OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    ExecVisitorImplForData(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecVisitorImplTemplateJson(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecTermJsonVariableInField(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecTermJsonFieldInVariable(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecVisitorImplTemplateArray(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecTermArrayVariableInField(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecTermArrayFieldInVariable(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecJsonInVariableByKeyIndex();

 private:
    std::shared_ptr<const milvus::expr::TermFilterExpr> expr_;
    milvus::Timestamp query_timestamp_;
    bool cached_bits_inited_{false};
    TargetBitmap cached_bits_;
    int32_t consistency_level_ = 0;
    bool arg_inited_{false};
    std::shared_ptr<MultiElement> arg_set_;
    SingleElement arg_val_;
};
}  //namespace exec
}  // namespace milvus
