// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/retrieve_result_export_c.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>
#include <folly/CancellationToken.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "futures/Future.h"
#include "monitor/scope_metric.h"
#include "query/PlanImpl.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentReadLease.h"
#include "segcore/Utils.h"
#include "segcore/arrow_field_utils.h"

using milvus::DataArray;
using milvus::FieldId;
using milvus::segcore::EmptyExtraFieldArrowType;
using milvus::segcore::FieldDataToArrow;
using milvus::segcore::MergeBase;
using milvus::segcore::MergeDataArray;
using milvus::segcore::MilvusField;
using milvus::segcore::SegmentInternalInterface;

namespace {

void
ReleaseArrowSchemaIfNeeded(ArrowSchema* schema) {
    if (schema != nullptr && schema->release != nullptr) {
        schema->release(schema);
    }
}

void
ReleaseArrowArrayIfNeeded(ArrowArray* array) {
    if (array != nullptr && array->release != nullptr) {
        array->release(array);
    }
}

// Mirrors AcquireSegmentReadLease in segment_c.cpp: growing segments are not
// gated (there is no lazy-load publish to race with), while sealed segments
// require a read lease held for the duration of the bulk_subscript calls to
// prevent torn reads during concurrent lazy-load publish.
std::shared_ptr<milvus::segcore::SegmentReadLease>
AcquireSegmentReadLease(milvus::segcore::SegmentInterface* segment,
                        const folly::CancellationToken& cancel_token) {
    if (segment->type() == SegmentType::Growing) {
        return nullptr;
    }

    auto sealed =
        dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(segment);
    AssertInfo(sealed != nullptr,
               "sealed segment {} does not support request read leases",
               segment->get_segment_id());
    return sealed->AcquireReadLease(cancel_token);
}

// Groups the rows of one FillRetrieveFieldsOrdered call that belong to the
// same segment, mirroring OrderedSegmentFields in search_result_export_c.cpp.
struct RetrieveSegmentFields {
    SegmentInternalInterface* segment{nullptr};
    std::vector<int64_t> segment_offsets;
    std::vector<int64_t> result_positions;
    std::map<FieldId, std::unique_ptr<DataArray>> fields;
};

// Build a RecordBatch with the correctly typed but empty columns for the
// requested output fields. When field_ids_ is empty (zero columns) but
// total_rows > 0, the batch still reports total_rows rows so that callers
// see the right row count.
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
BuildEmptyRetrieveBatch(milvus::query::RetrievePlan* plan, int64_t total_rows) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    fields.reserve(plan->field_ids_.size());
    arrays.reserve(plan->field_ids_.size());

    for (auto field_id : plan->field_ids_) {
        auto& field_meta = plan->schema_->operator[](field_id);
        auto name = std::string(field_meta.get_name().get());
        ARROW_ASSIGN_OR_RAISE(auto arrow_type,
                              EmptyExtraFieldArrowType(field_meta));
        ARROW_ASSIGN_OR_RAISE(auto arr, arrow::MakeEmptyArray(arrow_type));
        fields.push_back(MilvusField(name,
                                     arrow_type,
                                     field_meta.is_nullable(),
                                     field_id,
                                     field_meta.get_data_type()));
        arrays.push_back(std::move(arr));
    }

    // With zero columns there is no array length to stay consistent with,
    // so report the true row count. With columns present, each array was
    // built as MakeEmptyArray (length 0), so the batch must report 0 rows.
    int64_t num_rows = fields.empty() ? total_rows : 0;
    return arrow::RecordBatch::Make(
        arrow::schema(std::move(fields)), num_rows, std::move(arrays));
}

// Build the non-empty RecordBatch from the field data merged into
// caller-requested row order, mirroring BuildExplicitFieldsBatch in
// search_result_export_c.cpp.
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
BuildRetrieveFieldsBatch(
    milvus::query::RetrievePlan* plan,
    const std::map<FieldId, std::unique_ptr<DataArray>>& ordered_fields,
    int64_t total_rows) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    fields.reserve(plan->field_ids_.size());
    arrays.reserve(plan->field_ids_.size());

    for (auto field_id : plan->field_ids_) {
        auto& field_meta = plan->schema_->operator[](field_id);
        auto name = std::string(field_meta.get_name().get());
        auto it = ordered_fields.find(field_id);
        if (it == ordered_fields.end()) {
            return arrow::Status::Invalid(
                "missing ordered field data for field id ", field_id.get());
        }
        ARROW_ASSIGN_OR_RAISE(auto converted,
                              FieldDataToArrow(name, *it->second, total_rows));
        auto array = converted.second;
        fields.push_back(MilvusField(name,
                                     array->type(),
                                     field_meta.is_nullable(),
                                     field_id,
                                     field_meta.get_data_type()));
        arrays.push_back(std::move(array));
    }

    return arrow::RecordBatch::Make(
        arrow::schema(std::move(fields)), total_rows, std::move(arrays));
}

}  // namespace

CStatus
FillRetrieveFieldsOrdered(CSegmentInterface* segments,
                          int64_t num_segments,
                          CRetrievePlan c_plan,
                          const int32_t* seg_indices,
                          const int64_t* seg_offsets,
                          int64_t total_rows,
                          ArrowSchema* out_schema,
                          ArrowArray* out_array,
                          void* cancellation_source) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(segments != nullptr, "null segments");
        AssertInfo(num_segments > 0, "num_segments must be positive");
        AssertInfo(c_plan != nullptr, "null retrieve plan");
        AssertInfo(total_rows >= 0, "total_rows must not be negative");
        AssertInfo(total_rows == 0 || seg_indices != nullptr,
                   "null seg_indices for non-empty rows");
        AssertInfo(total_rows == 0 || seg_offsets != nullptr,
                   "null seg_offsets for non-empty rows");
        AssertInfo(out_schema != nullptr, "null ArrowSchema output");
        AssertInfo(out_array != nullptr, "null ArrowArray output");
        AssertInfo(out_schema->release == nullptr,
                   "ArrowSchema output must be empty before export");
        AssertInfo(out_array->release == nullptr,
                   "ArrowArray output must be empty before export");

        auto cancel_token = folly::CancellationToken();
        if (cancellation_source != nullptr) {
            auto source =
                static_cast<folly::CancellationSource*>(cancellation_source);
            cancel_token = source->getToken();
        }

        auto* plan = static_cast<milvus::query::RetrievePlan*>(c_plan);

        std::shared_ptr<arrow::RecordBatch> batch;
        if (total_rows == 0 || plan->field_ids_.empty()) {
            auto empty_batch_result = BuildEmptyRetrieveBatch(plan, total_rows);
            if (!empty_batch_result.ok()) {
                return milvus::FailureCStatus(
                    milvus::ErrorCode::UnexpectedError,
                    empty_batch_result.status().ToString());
            }
            batch = *empty_batch_result;
        } else {
            // Group rows by segment index (dense, so vector is ideal).
            std::vector<RetrieveSegmentFields> segment_fields(num_segments);
            for (int64_t i = 0; i < num_segments; ++i) {
                segment_fields[i].segment =
                    static_cast<SegmentInternalInterface*>(segments[i]);
            }
            for (int64_t pos = 0; pos < total_rows; ++pos) {
                auto seg_idx = seg_indices[pos];
                AssertInfo(seg_idx >= 0 && seg_idx < num_segments,
                           "segment index {} out of range [0, {})",
                           seg_idx,
                           num_segments);
                segment_fields[seg_idx].result_positions.push_back(pos);
                segment_fields[seg_idx].segment_offsets.push_back(
                    seg_offsets[pos]);
            }

            auto dynamic_field_id = plan->schema_->get_dynamic_field_id();

            // Materialize fields per segment via bulk_subscript.
            for (auto& materialized : segment_fields) {
                if (materialized.segment_offsets.empty()) {
                    continue;
                }
                milvus::futures::throwIfCancelled(cancel_token);
                milvus::OpContext op_ctx(cancel_token);
                auto read_lease =
                    AcquireSegmentReadLease(materialized.segment, cancel_token);
                for (auto field_id : plan->field_ids_) {
                    milvus::futures::throwIfCancelled(cancel_token);
                    auto& field_meta = plan->schema_->operator[](field_id);
                    std::unique_ptr<DataArray> data;
                    if (dynamic_field_id.has_value() &&
                        dynamic_field_id.value() == field_id &&
                        !plan->target_dynamic_fields_.empty()) {
                        data = materialized.segment->bulk_subscript(
                            &op_ctx,
                            field_id,
                            materialized.segment_offsets.data(),
                            materialized.segment_offsets.size(),
                            plan->target_dynamic_fields_);
                    } else if (!materialized.segment->is_field_exist(
                                   field_id)) {
                        data = materialized.segment
                                   ->bulk_subscript_not_exist_field(
                                       field_meta,
                                       materialized.segment_offsets.size());
                    } else {
                        data = materialized.segment->bulk_subscript(
                            &op_ctx,
                            field_id,
                            materialized.segment_offsets.data(),
                            materialized.segment_offsets.size());
                    }
                    materialized.fields[field_id] = std::move(data);
                }
            }

            // Build result_pairs for MergeDataArray.
            std::vector<MergeBase> result_pairs(total_rows);
            for (auto& materialized : segment_fields) {
                for (size_t row_idx = 0;
                     row_idx < materialized.result_positions.size();
                     ++row_idx) {
                    auto position = materialized.result_positions[row_idx];
                    result_pairs[position] = {&materialized.fields, row_idx};
                }
            }

            // Nullable vector materialization may compact physical vector
            // values while retaining a logical validity bitmap. Record the
            // compacted physical offset for each output row before
            // MergeDataArray scatters rows into final order.
            for (auto& materialized : segment_fields) {
                if (materialized.segment_offsets.empty()) {
                    continue;
                }
                for (auto field_id : plan->field_ids_) {
                    auto& field_meta = plan->schema_->operator[](field_id);
                    if (!field_meta.is_vector() || !field_meta.is_nullable()) {
                        continue;
                    }
                    auto it = materialized.fields.find(field_id);
                    if (it == materialized.fields.end()) {
                        continue;
                    }
                    const auto& valid_data =
                        milvus::GetFieldDataRowValidData(*it->second);
                    if (valid_data.empty()) {
                        continue;
                    }
                    int64_t valid_index = 0;
                    for (size_t row_idx = 0;
                         row_idx < materialized.result_positions.size();
                         ++row_idx) {
                        auto position = materialized.result_positions[row_idx];
                        result_pairs[position].setValidDataOffset(field_id,
                                                                  valid_index);
                        if (valid_data[row_idx]) {
                            ++valid_index;
                        }
                    }
                }
            }

            // Merge into ordered fields.
            std::map<FieldId, std::unique_ptr<DataArray>> ordered_fields;
            for (auto field_id : plan->field_ids_) {
                auto& field_meta = plan->schema_->operator[](field_id);
                ordered_fields[field_id] =
                    MergeDataArray(result_pairs, field_meta);
            }

            auto batch_result =
                BuildRetrieveFieldsBatch(plan, ordered_fields, total_rows);
            if (!batch_result.ok()) {
                return milvus::FailureCStatus(
                    milvus::ErrorCode::UnexpectedError,
                    batch_result.status().ToString());
            }
            batch = *batch_result;
        }

        auto export_status =
            arrow::ExportRecordBatch(*batch, out_array, out_schema);
        if (!export_status.ok()) {
            ReleaseArrowArrayIfNeeded(out_array);
            ReleaseArrowSchemaIfNeeded(out_schema);
            return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                          export_status.ToString());
        }
        return milvus::SuccessCStatus();
    } catch (folly::FutureCancellation& e) {
        return milvus::FailureCStatus(milvus::ErrorCode::FollyCancel, e.what());
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
