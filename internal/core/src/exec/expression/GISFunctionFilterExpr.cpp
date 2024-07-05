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
#include "common/GeoSpatial.h"
#include "common/Types.h"
#include "pb/plan.pb.h"
namespace milvus {
namespace exec {

void
PhyGISFunctionFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    AssertInfo(expr_->column_.data_type_ == DataType::GEOSPATIAL,
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
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    auto& str = expr_->wkb_;
    GeoSpatial right_source(str.data(), str.size());
    switch (expr_->op_) {
        case proto::plan::GISFunctionFilterExpr_GISOp_Equals: {
            auto execute_sub_batch = [&right_source](
                                         const std::string_view* data,
                                         const int size,
                                         TargetBitmapView res) {
                for (int i = 0; i < size; ++i) {
                    res[i] = GeoSpatial(data[i].data(), data[i].size())
                                 .equals(right_source);
                }
            };
            int64_t processed_size = ProcessDataChunks<std::string_view>(
                execute_sub_batch, std::nullptr_t{}, res);
            AssertInfo(processed_size == real_batch_size,
                       "internal error: expr processed rows {} not equal "
                       "expect batch size {}",
                       processed_size,
                       real_batch_size);
            return res_vec;
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Touches: {
            auto execute_sub_batch = [&right_source](
                                         const std::string_view* data,
                                         const int size,
                                         TargetBitmapView res) {
                for (int i = 0; i < size; ++i) {
                    res[i] = GeoSpatial(data[i].data(), data[i].size())
                                 .touches(right_source);
                }
            };
            int64_t processed_size = ProcessDataChunks<std::string_view>(
                execute_sub_batch, std::nullptr_t{}, res);
            AssertInfo(processed_size == real_batch_size,
                       "internal error: expr processed rows {} not equal "
                       "expect batch size {}",
                       processed_size,
                       real_batch_size);
            return res_vec;
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Overlaps: {
            auto execute_sub_batch = [&right_source](
                                         const std::string_view* data,
                                         const int size,
                                         TargetBitmapView res) {
                for (int i = 0; i < size; ++i) {
                    res[i] = GeoSpatial(data[i].data(), data[i].size())
                                 .overlaps(right_source);
                }
            };
            int64_t processed_size = ProcessDataChunks<std::string_view>(
                execute_sub_batch, std::nullptr_t{}, res);
            AssertInfo(processed_size == real_batch_size,
                       "internal error: expr processed rows {} not equal "
                       "expect batch size {}",
                       processed_size,
                       real_batch_size);
            return res_vec;
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Crosses: {
            auto execute_sub_batch = [&right_source](
                                         const std::string_view* data,
                                         const int size,
                                         TargetBitmapView res) {
                for (int i = 0; i < size; ++i) {
                    res[i] = GeoSpatial(data[i].data(), data[i].size())
                                 .crosses(right_source);
                }
            };
            int64_t processed_size = ProcessDataChunks<std::string_view>(
                execute_sub_batch, std::nullptr_t{}, res);
            AssertInfo(processed_size == real_batch_size,
                       "internal error: expr processed rows {} not equal "
                       "expect batch size {}",
                       processed_size,
                       real_batch_size);
            return res_vec;
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Contains: {
            auto execute_sub_batch = [&right_source](
                                         const std::string_view* data,
                                         const int size,
                                         TargetBitmapView res) {
                for (int i = 0; i < size; ++i) {
                    res[i] = GeoSpatial(data[i].data(), data[i].size())
                                 .contains(right_source);
                }
            };
            int64_t processed_size = ProcessDataChunks<std::string_view>(
                execute_sub_batch, std::nullptr_t{}, res);
            AssertInfo(processed_size == real_batch_size,
                       "internal error: expr processed rows {} not equal "
                       "expect batch size {}",
                       processed_size,
                       real_batch_size);
            return res_vec;
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Intersects: {
            auto execute_sub_batch = [&right_source](
                                         const std::string_view* data,
                                         const int size,
                                         TargetBitmapView res) {
                for (int i = 0; i < size; ++i) {
                    res[i] = GeoSpatial(data[i].data(), data[i].size())
                                 .intersects(right_source);
                }
            };
            int64_t processed_size = ProcessDataChunks<std::string_view>(
                execute_sub_batch, std::nullptr_t{}, res);
            AssertInfo(processed_size == real_batch_size,
                       "internal error: expr processed rows {} not equal "
                       "expect batch size {}",
                       processed_size,
                       real_batch_size);
            return res_vec;
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Within: {
            auto execute_sub_batch = [&right_source](
                                         const std::string_view* data,
                                         const int size,
                                         TargetBitmapView res) {
                for (int i = 0; i < size; ++i) {
                    res[i] = GeoSpatial(data[i].data(), data[i].size())
                                 .within(right_source);
                }
            };
            int64_t processed_size = ProcessDataChunks<std::string_view>(
                execute_sub_batch, std::nullptr_t{}, res);
            AssertInfo(processed_size == real_batch_size,
                       "internal error: expr processed rows {} not equal "
                       "expect batch size {}",
                       processed_size,
                       real_batch_size);
            return res_vec;
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