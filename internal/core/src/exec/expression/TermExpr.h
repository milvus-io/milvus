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
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

template <typename T>
struct TermElementFuncFlat {
    bool
    operator()(const T* src, size_t n, T val) {
        for (size_t i = 0; i < n; ++i) {
            if (src[i] == val) {
                return true;
            }
        }
    }
};

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
    FixedVector<bool>
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
        Timestamp query_timestamp,
        int64_t batch_size)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      query_timestamp,
                      batch_size),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

 private:
    void
    InitPkCacheOffset();

    VectorPtr
    ExecPkTermImpl();

    template <typename T>
    VectorPtr
    ExecVisitorImpl();

    template <typename T>
    VectorPtr
    ExecVisitorImplForIndex();

    template <typename T>
    VectorPtr
    ExecVisitorImplForData();

    template <typename ValueType>
    VectorPtr
    ExecVisitorImplTemplateJson();

    template <typename ValueType>
    VectorPtr
    ExecTermJsonVariableInField();

    template <typename ValueType>
    VectorPtr
    ExecTermJsonFieldInVariable();

    template <typename ValueType>
    VectorPtr
    ExecVisitorImplTemplateArray();

    template <typename ValueType>
    VectorPtr
    ExecTermArrayVariableInField();

    template <typename ValueType>
    VectorPtr
    ExecTermArrayFieldInVariable();

 private:
    std::shared_ptr<const milvus::expr::TermFilterExpr> expr_;
    // If expr is like "pk in (..)", can use pk index to optimize
    bool cached_offsets_inited_{false};
    ColumnVectorPtr cached_offsets_;
    FixedVector<bool> cached_bits_;
};
}  //namespace exec
}  // namespace milvus
