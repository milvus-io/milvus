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

#include "exec/expression/MembershipFilterExpr.h"

#include <cstdint>
#include <string>
#include <string_view>

#include "storage/MmapManager.h"

namespace milvus {
namespace exec {

template <typename LogicalExpr, typename ProbePolicy>
void
PhyMembershipFilterExpr<LogicalExpr, ProbePolicy>::Eval(EvalCtx& context,
                                                        VectorPtr& result) {
    WaitPrefetch();
    // Honor iterative-filter candidate offsets: EvalKernel evaluates the
    // offset input rows rather than the first N rows sequentially.
    auto* input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);
    auto data_type = expr_->column_.data_type_;
    switch (data_type) {
        case DataType::INT8: {
            result = ExecVisitorImpl<int8_t>(context);
            break;
        }
        case DataType::INT16: {
            result = ExecVisitorImpl<int16_t>(context);
            break;
        }
        case DataType::INT32: {
            result = ExecVisitorImpl<int32_t>(context);
            break;
        }
        case DataType::INT64: {
            result = ExecVisitorImpl<int64_t>(context);
            break;
        }
        case DataType::VARCHAR: {
            if constexpr (ProbePolicy::kSupportsVarChar) {
                if (segment_->type() == SegmentType::Growing &&
                    !storage::MmapManager::GetInstance()
                         .GetMmapConfig()
                         .growing_enable_mmap) {
                    result = ExecVisitorImpl<std::string>(context);
                } else {
                    result = ExecVisitorImpl<std::string_view>(context);
                }
            } else {
                ThrowInfo(ExprInvalid,
                          "{} does not support field data type: {}",
                          ProbePolicy::kKindName,
                          data_type);
            }
            break;
        }
        case DataType::JSON: {
            if constexpr (ProbePolicy::kSupportsJson) {
                result = ExecVisitorImplJson(context);
            } else {
                ThrowInfo(ExprInvalid,
                          "{} does not support field data type: {}",
                          ProbePolicy::kKindName,
                          data_type);
            }
            break;
        }
        default:
            ThrowInfo(ExprInvalid,
                      "{} does not support field data type: {}",
                      ProbePolicy::kKindName,
                      data_type);
    }
}

template <typename LogicalExpr, typename ProbePolicy>
template <typename T>
VectorPtr
PhyMembershipFilterExpr<LogicalExpr, ProbePolicy>::ExecVisitorImpl(
    EvalCtx& context) {
    // Index-only sealed field: EvalKernel reverse-looks-up values from the
    // pinned scalar index (DetermineExecPath() committed to ScalarIndex only
    // if one supports cheap reverse lookup). Without such an index and without
    // raw data, fail with a clear SegcoreError rather than reading zero rows
    // and tripping the batch-size assertion. Field data being absent is a
    // load/state condition, not the request's fault, so this is a System error
    // and stays retriable.
    if (!UseIndexCursor() && segment_->type() == SegmentType::Sealed &&
        !has_field_data_at_init_) {
        ThrowInfo(
            FieldNotLoaded,
            "{} cannot evaluate field {}: raw field data is not loaded and "
            "no scalar index with a cheap per-row reverse lookup is available "
            "(a BITMAP index without its offset cache is excluded because it "
            "reverse-looks-up in O(cardinality) per row; load the raw field "
            "data or set queryNode.indexOffsetCacheEnabled=true)",
            ProbePolicy::kKindName,
            field_id_.get());
    }
    return EvalKernel<T>(context,
                         MembershipScalarKernel<T, ProbePolicy>{&probe_},
                         /*element_level=*/false);
}

template <typename LogicalExpr, typename ProbePolicy>
template <typename Dummy>
VectorPtr
PhyMembershipFilterExpr<LogicalExpr, ProbePolicy>::ExecVisitorImplJson(
    EvalCtx& context) {
    static_assert(ProbePolicy::kSupportsJson,
                  "JSON probing requires a policy that supports it");
    // JSON paths are data-path only: DetermineExecPath() never commits to
    // ScalarIndex for JSON (no per-row reverse lookup exists for a JSON path),
    // so an index-only sealed JSON field cannot be probed at all.
    if (segment_->type() == SegmentType::Sealed && !has_field_data_at_init_) {
        ThrowInfo(FieldNotLoaded,
                  "{} cannot evaluate JSON field {}: raw field data is not "
                  "loaded, and a JSON path has no scalar index with a per-row "
                  "reverse lookup; load the raw JSON field data",
                  ProbePolicy::kKindName,
                  field_id_.get());
    }
    return EvalKernel<milvus::Json>(
        context,
        MembershipJsonKernel<ProbePolicy>{
            &probe_, milvus::Json::pointer(expr_->column_.nested_path_)},
        /*element_level=*/false);
}

// The two kinds of the membership family. The aliases keep the historical
// class names so factory construction strings and logs stay stable.
template class PhyMembershipFilterExpr<milvus::expr::BloomFilterExpr,
                                       BloomMembershipProbe>;
template class PhyMembershipFilterExpr<milvus::expr::RoaringFilterExpr,
                                       RoaringMembershipProbe>;

}  // namespace exec
}  // namespace milvus
