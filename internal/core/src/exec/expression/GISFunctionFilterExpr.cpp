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

#include "GISFunctionFilterExpr.h"
#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/Types.h"
#include "pb/plan.pb.h"
namespace milvus {
namespace exec {

#define GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(method)                     \
    auto execute_sub_batch = [](const std::string_view* data,                  \
                                const bool* valid_data,                        \
                                const int size,                                \
                                TargetBitmapView res,                          \
                                TargetBitmapView valid_res,                    \
                                const Geometry& right_source) {                \
        for (int i = 0; i < size; ++i) {                                       \
            if (valid_data != nullptr && !valid_data[i]) {                     \
                res[i] = valid_res[i] = false;                                 \
                continue;                                                      \
            }                                                                  \
            res[i] =                                                           \
                Geometry(data[i].data(), data[i].size()).method(right_source); \
        }                                                                      \
    };                                                                         \
    int64_t processed_size = ProcessDataChunks<std::string_view>(              \
        execute_sub_batch, std::nullptr_t{}, res, valid_res, right_source);    \
    AssertInfo(processed_size == real_batch_size,                              \
               "internal error: expr processed rows {} not equal "             \
               "expect batch size {}",                                         \
               processed_size,                                                 \
               real_batch_size);                                               \
    return res_vec;

void
PhyGISFunctionFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    AssertInfo(expr_->column_.data_type_ == DataType::GEOMETRY,
               "unsupported data type: {}",
               expr_->column_.data_type_);
    if (is_index_mode_) {
        // result = EvalForIndexSegment();
        PanicInfo(NotImplemented, "index for geos not implement");
    } else {
        result = EvalForDataSegment();
    }
}

VectorPtr
PhyGISFunctionFilterExpr::EvalForDataSegment() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto& right_source = expr_->geometry_;
    switch (expr_->op_) {
        case proto::plan::GISFunctionFilterExpr_GISOp_Equals: {
            GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(equals);
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Touches: {
            GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(touches);
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Overlaps: {
            GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(overlaps);
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Crosses: {
            GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(crosses);
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Contains: {
            GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(contains);
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Intersects: {
            GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(intersects);
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Within: {
            GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(within);
        }
        default: {
            PanicInfo(NotImplemented,
                      "internal error: unknown GIS op : {}",
                      expr_->op_);
        }
    }
    return res_vec;
}

// VectorPtr
// PhyGISFunctionFilterExpr::EvalForIndexSegment() {
//     // TODO
// }

}  //namespace exec
}  // namespace milvus