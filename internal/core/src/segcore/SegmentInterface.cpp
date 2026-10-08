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

#include "SegmentInterface.h"

#include <folly/CancellationToken.h>
#include <folly/ExceptionWrapper.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <ratio>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ChunkedSegmentSealedImpl.h"
#include "NamedType/named_type_impl.hpp"
#include "Utils.h"
#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/GeometryCache.h"
#include "common/OpContext.h"
#include "common/QueryResult.h"
#include "common/SystemProperty.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "expr/ITypeExpr.h"
#include "fmt/core.h"
#include "futures/Future.h"
#include "index/json_stats/JsonKeyStats.h"
#include "monitor/Monitor.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "plan/PlanNodeIdGenerator.h"
#include "prometheus/histogram.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/ConcurrentVector.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus::segcore {

std::shared_ptr<milvus::exec::SimpleGeometryCache>
SegmentInternalInterface::GetGeometryCache(FieldId field_id) const {
    return milvus::exec::SimpleGeometryCacheManager::Instance().GetCache(
        segment_instance_uid(), get_segment_id(), field_id);
}

namespace {

struct FetchedOutputField {
    FieldId field_id;
    std::unique_ptr<DataArray> field_data;
    int64_t scanned_remote_bytes;
    int64_t scanned_total_bytes;
};

void
InheritOpContext(milvus::OpContext& target, const milvus::OpContext* source) {
    if (source == nullptr) {
        return;
    }
    target.cancellation_token = source->cancellation_token;
    target.runtime_load_priority = source->runtime_load_priority;
    target.coload_fields = source->coload_fields;
    target.pinned_segment_state = source->pinned_segment_state;
    target.pinned_state_owner = source->pinned_state_owner;
    target.trace_context = source->trace_context;
    target.trace_span = source->trace_span;
}

template <typename FetchField>
std::vector<FetchedOutputField>
FetchOutputFields(const std::vector<FieldId>& field_ids,
                  int64_t segment_id,
                  milvus::OpContext* op_ctx,
                  FetchField fetch_field) {
    if (field_ids.empty()) {
        return {};
    }

    std::vector<std::future<FetchedOutputField>> futures;
    futures.reserve(field_ids.size());
    std::vector<FetchedOutputField> fetched_fields;
    fetched_fields.reserve(field_ids.size());

    auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::MIDDLE);
    try {
        for (auto field_id : field_ids) {
            futures.emplace_back(pool.Submit(
                [field_id, segment_id, op_ctx, fetch_field]() mutable {
                    milvus::OpContext field_ctx;
                    InheritOpContext(field_ctx, op_ctx);
                    segcore::CheckCancellation(&field_ctx,
                                               segment_id,
                                               field_id.get(),
                                               "FillTargetEntry");
                    auto field_data = fetch_field(field_id, &field_ctx);
                    return FetchedOutputField{
                        field_id,
                        std::move(field_data),
                        field_ctx.storage_usage.scanned_cold_bytes.load(),
                        field_ctx.storage_usage.scanned_total_bytes.load()};
                }));
        }
    } catch (...) {
        storage::DrainFutures(futures);
        throw;
    }

    std::exception_ptr first_error;
    for (auto& future : futures) {
        try {
            fetched_fields.emplace_back(future.get());
        } catch (...) {
            if (first_error == nullptr) {
                first_error = std::current_exception();
            }
        }
    }
    if (first_error != nullptr) {
        std::rethrow_exception(first_error);
    }
    return fetched_fields;
}

}  // namespace

void
SegmentInternalInterface::FillPrimaryKeys(const query::Plan* plan,
                                          SearchResult& results,
                                          milvus::OpContext* op_ctx) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size,
               "Size of result distances is not equal to size of ids");
    Assert(results.primary_keys_.size() == 0);
    results.primary_keys_.resize(size);

    auto schema = get_schema_snapshot();
    auto pk_field_id_opt = schema->get_primary_field_id();
    AssertInfo(pk_field_id_opt.has_value(),
               "Cannot get primary key offset from schema");
    auto pk_field_id = pk_field_id_opt.value();
    AssertInfo(IsPrimaryKeyDataType((*schema)[pk_field_id].get_data_type()),
               "Primary key field is not INT64 or VARCHAR type");

    segcore::CheckCancellation(op_ctx, get_segment_id(), "FillPrimaryKeys");
    // Use a per-call OpContext for storage_usage; sharing op_ctx across
    // segments would make each segment's search_storage_cost_ accumulate
    // every prior segment's bytes.
    milvus::OpContext local_ctx;
    if (op_ctx != nullptr) {
        local_ctx.cancellation_token = op_ctx->cancellation_token;
        local_ctx.runtime_load_priority = op_ctx->runtime_load_priority;
    }
    auto field_data = bulk_subscript(
        &local_ctx, pk_field_id, results.seg_offsets_.data(), size);
    results.pk_type_ = DataType(field_data->type());

    ParsePksFromFieldData(results.primary_keys_, *field_data);
    results.search_storage_cost_.scanned_remote_bytes +=
        local_ctx.storage_usage.scanned_cold_bytes.load();
    results.search_storage_cost_.scanned_total_bytes +=
        local_ctx.storage_usage.scanned_total_bytes.load();
}

void
SegmentInternalInterface::FillSearchResultOutputFields(
    const query::Plan* plan,
    const std::vector<FieldId>& field_ids,
    SearchResult& results,
    milvus::OpContext* op_ctx,
    const std::vector<std::string>* target_dynamic_fields) const {
    const auto size = results.seg_offsets_.size();
    auto fetch_one = [this, plan, &results, size, target_dynamic_fields](
                         FieldId field_id, milvus::OpContext* field_ctx) {
        auto& field_meta = plan->schema_->operator[](field_id);
        std::unique_ptr<DataArray> field_data;
        if (plan->schema_->get_dynamic_field_id().has_value() &&
            plan->schema_->get_dynamic_field_id().value() == field_id &&
            target_dynamic_fields != nullptr &&
            !target_dynamic_fields->empty()) {
            field_data = bulk_subscript(field_ctx,
                                        field_id,
                                        results.seg_offsets_.data(),
                                        size,
                                        *target_dynamic_fields);
        } else if (!is_field_exist(field_id)) {
            field_data = bulk_subscript_not_exist_field(field_meta, size);
        } else {
            field_data = bulk_subscript(
                field_ctx, field_id, results.seg_offsets_.data(), size);
        }
        return field_data;
    };

    auto fetched_fields =
        FetchOutputFields(field_ids, get_segment_id(), op_ctx, fetch_one);

    for (auto& fetched : fetched_fields) {
        results.output_fields_data_[fetched.field_id] =
            std::move(fetched.field_data);
        results.search_storage_cost_.scanned_remote_bytes +=
            fetched.scanned_remote_bytes;
        results.search_storage_cost_.scanned_total_bytes +=
            fetched.scanned_total_bytes;
    }
}

void
SegmentInternalInterface::FillTargetEntry(const query::Plan* plan,
                                          SearchResult& results,
                                          milvus::OpContext* op_ctx) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size,
               "Size of result distances is not equal to size of ids");

    FillSearchResultOutputFields(plan,
                                 plan->target_entries_,
                                 results,
                                 op_ctx,
                                 &plan->target_dynamic_fields_);
}

void
SegmentInternalInterface::FillTargetEntry(const query::Plan* plan,
                                          const std::vector<FieldId>& field_ids,
                                          SearchResult& results,
                                          milvus::OpContext* op_ctx) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size,
               "Size of result distances is not equal to size of ids");

    FillSearchResultOutputFields(plan, field_ids, results, op_ctx, nullptr);
}

std::unique_ptr<SearchResult>
SegmentInternalInterface::Search(
    const query::Plan* plan,
    const query::PlaceholderGroup* placeholder_group,
    Timestamp timestamp,
    const folly::CancellationToken& cancel_token,
    int32_t consistency_level,
    Timestamp collection_ttl,
    int64_t entity_ttl_physical_time_us,
    bool filter_only,
    bool enable_expr_cache,
    milvus::tracer::SpanPtr trace_span) const {
    std::shared_lock lck(mutex_);
    milvus::tracer::AddEvent("obtained_segment_lock_mutex");

    check_search(plan);
    query::ExecPlanNodeVisitor visitor(*this,
                                       timestamp,
                                       placeholder_group,
                                       cancel_token,
                                       consistency_level,
                                       collection_ttl,
                                       entity_ttl_physical_time_us,
                                       std::move(trace_span));
    visitor.SetFilterOnly(filter_only);
    visitor.SetEnableExprCache(enable_expr_cache);
    auto results = std::make_unique<SearchResult>();
    *results = visitor.get_moved_result(*plan->plan_node_);
    results->segment_ = (void*)this;
    return results;
}

// Determine the actual result row count for the output-size guard.
//
// ExecPlanNodeVisitor produces results via two mutually exclusive paths:
//
//   1. Bitmap path (normal query):
//      Pipeline outputs a bitmap covering the full segment, then find_first()
//      selects matching offsets into result_offsets_.  result_offsets_.size()
//      is the true result count.  field_data_ is empty.
//      total_data_cnt_ = segment active count (NOT the match count).
//
//   2. Columnar path (ORDER BY / aggregation):
//      Pipeline outputs final columns directly into field_data_.
//      result_offsets_ is empty (find_first is never called).
//      total_data_cnt_ = first_column->size() = actual output row count.
//
// We must NOT fall back to total_data_cnt_ when result_offsets_ is empty
// on the bitmap path (zero matches), because that would use the full
// segment row count and falsely trigger the output-size guard.
int64_t
GetResultRowCount(const RetrieveResult& retrieve_results) {
    auto offset_count =
        static_cast<int64_t>(retrieve_results.result_offsets_.size());
    if (offset_count > 0) {
        return offset_count;
    }
    // Columnar path: pipeline produced field_data_ directly.
    if (!retrieve_results.field_data_.empty()) {
        return retrieve_results.total_data_cnt_;
    }
    // Bitmap path with zero matches: no offsets, no field_data_.
    return 0;
}

std::unique_ptr<proto::segcore::RetrieveResults>
SegmentInternalInterface::Retrieve(tracer::TraceContext* trace_ctx,
                                   const query::RetrievePlan* plan,
                                   Timestamp timestamp,
                                   int64_t limit_size,
                                   bool ignore_non_pk,
                                   const folly::CancellationToken& cancel_token,
                                   int32_t consistency_level,
                                   Timestamp collection_ttl,
                                   int64_t entity_ttl_physical_time_us) const {
    std::shared_lock lck(mutex_);
    tracer::AutoSpan span("Retrieve", tracer::GetRootSpan(), true);
    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    query::ExecPlanNodeVisitor visitor(*this,
                                       timestamp,
                                       cancel_token,
                                       consistency_level,
                                       collection_ttl,
                                       entity_ttl_physical_time_us);
    auto retrieve_results = visitor.get_retrieve_result(*plan->plan_node_);

    retrieve_results.segment_ = (void*)this;
    results->set_has_more_result(retrieve_results.has_more_result);
    results->set_scanned_remote_bytes(
        retrieve_results.retrieve_storage_cost_.scanned_remote_bytes);
    results->set_scanned_total_bytes(
        retrieve_results.retrieve_storage_cost_.scanned_total_bytes);

    auto result_rows = GetResultRowCount(retrieve_results);
    int64_t output_data_size = 0;
    if (result_rows > 0) {
        for (auto field_id : plan->field_ids_) {
            output_data_size += get_field_avg_size(field_id) * result_rows;
        }
    }
    if (output_data_size > limit_size) {
        ThrowInfo(
            RetrieveError,
            fmt::format("query results exceed the limit size ", limit_size));
    }

    results->set_all_retrieve_count(retrieve_results.total_data_cnt_);
    results->mutable_offset()->Add(retrieve_results.result_offsets_.begin(),
                                   retrieve_results.result_offsets_.end());

    // Element-level query support: serialize element_level flag and element_indices
    if (retrieve_results.element_level_) {
        results->set_element_level(true);
        // element_indices_ is vector<vector<int32_t>>, serialize each doc's indices
        for (const auto& indices : retrieve_results.element_indices_) {
            auto* elem_indices = results->add_element_indices();
            elem_indices->mutable_indices()->Add(indices.begin(),
                                                 indices.end());
        }
    }

    std::chrono::high_resolution_clock::time_point get_target_entry_start =
        std::chrono::high_resolution_clock::now();
    // Carry the upstream cancel_token down into FillTargetEntry so the
    // take()/Arrow-convert path can short-circuit on abort.
    milvus::OpContext fte_op_ctx;
    fte_op_ctx.cancellation_token = cancel_token;
    if (retrieve_results.field_data_.empty()) {
        FillTargetEntry(trace_ctx,
                        plan,
                        results,
                        retrieve_results.result_offsets_.data(),
                        retrieve_results.result_offsets_.size(),
                        ignore_non_pk,
                        true,
                        &fte_op_ctx);
    } else if (!plan->plan_node_->pipeline_field_ids_.empty()) {
        // Non-aggregation ORDER BY (single-project or two-project mode):
        // Pipeline output contains [pk, sort_cols, ..., hidden columns].
        // FillOrderByResult strips hidden columns, sets field_id on each
        // DataArray, bulk-fetches deferred fields (if any), populates system
        // fields, restores element indices when present, and fills PK-based
        // IDs for proxy reduce.
        //
        // Aggregation + ORDER BY does NOT set pipeline_field_ids_ and produces
        // final columns directly, falling through to FillTargetEntryDirectly.
        FillOrderByResult(plan, results, retrieve_results, &fte_op_ctx);
    } else {
        FillTargetEntryDirectly(trace_ctx, results, retrieve_results);
    }
    std::chrono::high_resolution_clock::time_point get_target_entry_end =
        std::chrono::high_resolution_clock::now();
    double get_entry_cost = std::chrono::duration<double, std::micro>(
                                get_target_entry_end - get_target_entry_start)
                                .count();
    milvus::monitor::internal_core_retrieve_get_target_entry_latency.Observe(
        get_entry_cost / 1000);

    milvus::futures::throwIfCancelled(cancel_token);
    return results;
}

void
SegmentInternalInterface::FillTargetEntryDirectly(
    tracer::TraceContext* trace_ctx,
    const std::unique_ptr<proto::segcore::RetrieveResults>& results,
    RetrieveResult& retrieveResult) const {
    auto fields_data = results->mutable_fields_data();
    for (auto& field_data : retrieveResult.field_data_) {
        auto* allocated_data = new DataArray(std::move(field_data));
        fields_data->AddAllocated(allocated_data);
    }
    retrieveResult.field_data_.clear();
}

void
SegmentInternalInterface::FillOrderByResult(
    const query::RetrievePlan* plan,
    const std::unique_ptr<proto::segcore::RetrieveResults>& results,
    RetrieveResult& retrieveResult,
    milvus::OpContext* op_ctx) const {
    auto fields_data = results->mutable_fields_data();
    auto& deferred = plan->plan_node_->deferred_field_ids_;

    // Pipeline layout:
    //   row-level:     [...user_columns..., SegmentOffsetFieldID]
    //   element-level: [...user_columns..., ElementIndexFieldID,
    //                   SegmentOffsetFieldID]
    // The last column is always SegmentOffsetFieldID carrying segment offsets.
    auto total_cols = retrieveResult.field_data_.size();
    AssertInfo(total_cols >= 2,
               "ORDER BY expects at least 2 pipeline columns "
               "(pk + SegmentOffsetFieldID), got: {}",
               total_cols);

    // Move all non-hidden columns to results.
    // Set field_id on each DataArray so QN-side AppendFieldData can match
    // fields correctly (pipeline-produced DataArrays have field_id=0 by default).
    auto& pipeline_ids = plan->plan_node_->pipeline_field_ids_;
    AssertInfo(pipeline_ids.size() == total_cols,
               "pipeline_field_ids size ({}) must match pipeline column "
               "count ({})",
               pipeline_ids.size(),
               total_cols);

    size_t segment_offset_col_idx = total_cols;
    size_t element_index_col_idx = total_cols;
    for (size_t i = 0; i < pipeline_ids.size(); i++) {
        if (pipeline_ids[i] == SegmentOffsetFieldID) {
            segment_offset_col_idx = i;
        } else if (pipeline_ids[i] == ElementIndexFieldID) {
            element_index_col_idx = i;
        }
    }
    AssertInfo(segment_offset_col_idx != total_cols,
               "ORDER BY pipeline must contain SegmentOffsetFieldID");

    for (size_t i = 0; i < total_cols; i++) {
        if (i == segment_offset_col_idx || i == element_index_col_idx) {
            continue;
        }
        auto* data = new DataArray(std::move(retrieveResult.field_data_[i]));
        data->set_field_id(pipeline_ids[i].get());
        fields_data->AddAllocated(data);
    }

    // Extract segment offsets from the hidden SegmentOffsetFieldID column.
    auto& offset_col = retrieveResult.field_data_[segment_offset_col_idx];
    auto& offset_data = offset_col.scalars().long_data().data();
    auto topk_count = offset_data.size();

    // Populate results->offset() so QN-side MergeSegcoreRetrieveResults
    // won't filter out this result (it checks len(r.GetOffset()) == 0).
    results->mutable_offset()->Add(offset_data.begin(), offset_data.end());

    if (element_index_col_idx != total_cols) {
        auto& element_index_col =
            retrieveResult.field_data_[element_index_col_idx];
        auto& element_index_data =
            element_index_col.scalars().long_data().data();
        AssertInfo(element_index_data.size() == topk_count,
                   "element index column size ({}) must match offset column "
                   "size ({})",
                   element_index_data.size(),
                   topk_count);
        results->set_element_level(true);
        results->clear_element_indices();
        for (auto element_index : element_index_data) {
            AssertInfo(element_index >= 0 &&
                           element_index <= std::numeric_limits<int32_t>::max(),
                       "invalid element index: {}",
                       element_index);
            auto* elem_indices = results->add_element_indices();
            elem_indices->add_indices(static_cast<int32_t>(element_index));
        }
    }

    std::vector<FieldId> fields_to_fetch(deferred.begin(), deferred.end());
    for (auto field_id : plan->field_ids_) {
        if (SystemProperty::Instance().IsSystem(field_id)) {
            fields_to_fetch.push_back(field_id);
        }
    }

    auto fetch_one = [this, plan, &offset_data, topk_count](
                         FieldId field_id, milvus::OpContext* field_ctx) {
        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto system_type =
                SystemProperty::Instance().GetSystemFieldType(field_id);
            FixedVector<int64_t> output(topk_count);
            bulk_subscript(field_ctx,
                           system_type,
                           offset_data.data(),
                           topk_count,
                           output.data());

            auto data_array = std::make_unique<DataArray>();
            data_array->set_field_id(field_id.get());
            data_array->set_type(milvus::proto::schema::DataType::Int64);
            auto data = reinterpret_cast<const int64_t*>(output.data());
            data_array->mutable_scalars()
                ->mutable_long_data()
                ->mutable_data()
                ->Add(data, data + topk_count);
            return data_array;
        }

        auto dynamic_field_id = plan->schema_->get_dynamic_field_id();
        std::unique_ptr<DataArray> col;
        if (dynamic_field_id.has_value() &&
            dynamic_field_id.value() == field_id &&
            !plan->target_dynamic_fields_.empty()) {
            col = bulk_subscript(field_ctx,
                                 field_id,
                                 offset_data.data(),
                                 topk_count,
                                 plan->target_dynamic_fields_);
        } else if (!is_field_exist(field_id)) {
            auto& field_meta = plan->schema_->operator[](field_id);
            col = bulk_subscript_not_exist_field(field_meta, topk_count);
        } else {
            col = bulk_subscript(
                field_ctx, field_id, offset_data.data(), topk_count);
        }
        auto& field_meta = plan->schema_->operator[](field_id);
        if (field_meta.get_data_type() == DataType::ARRAY) {
            col->mutable_scalars()->mutable_array_data()->set_element_type(
                proto::schema::DataType(field_meta.get_element_type()));
        }
        return col;
    };

    auto fetched_fields =
        FetchOutputFields(fields_to_fetch, get_segment_id(), op_ctx, fetch_one);
    for (auto& fetched : fetched_fields) {
        fields_data->AddAllocated(fetched.field_data.release());
        results->set_scanned_remote_bytes(results->scanned_remote_bytes() +
                                          fetched.scanned_remote_bytes);
        results->set_scanned_total_bytes(results->scanned_total_bytes() +
                                         fetched.scanned_total_bytes);
    }

    retrieveResult.field_data_.clear();

    // Populate IDs from PK column (position 0) for proxy ReduceByPK.
    if (results->fields_data_size() > 0) {
        auto ids = results->mutable_ids();
        auto& pk_data = results->fields_data(0);
        auto pk_field_id = plan->schema_->get_primary_field_id();
        if (pk_field_id.has_value()) {
            auto pk_type = plan->schema_->GetFieldType(pk_field_id.value());
            if (pk_type == DataType::INT64) {
                auto int_ids = ids->mutable_int_id();
                auto& src = pk_data.scalars().long_data();
                int_ids->mutable_data()->Add(src.data().begin(),
                                             src.data().end());
            } else if (pk_type == DataType::VARCHAR) {
                auto str_ids = ids->mutable_str_id();
                auto& src = pk_data.scalars().string_data();
                for (int i = 0; i < src.data_size(); ++i) {
                    *(str_ids->mutable_data()->Add()) = src.data(i);
                }
            }
        }
    }
}

void
SegmentInternalInterface::FillTargetEntry(
    tracer::TraceContext* trace_ctx,
    const query::RetrievePlan* plan,
    const std::unique_ptr<proto::segcore::RetrieveResults>& results,
    const int64_t* offsets,
    int64_t size,
    bool ignore_non_pk,
    bool fill_ids,
    milvus::OpContext* op_ctx) const {
    tracer::AutoSpan span("FillTargetEntry", tracer::GetRootSpan());

    // Fast path: use take() API for eligible output fields.
    // Use dynamic_cast to avoid adding new virtual methods (vtable layout
    // change causes SIGSEGV in cgo boundary).
    if (auto* chunked = dynamic_cast<const ChunkedSegmentSealedImpl*>(this)) {
        if (chunked->TryTakeForRetrieve(plan,
                                        results,
                                        offsets,
                                        size,
                                        ignore_non_pk,
                                        fill_ids,
                                        op_ctx)) {
            return;
        }
    }

    auto fields_data = results->mutable_fields_data();
    auto ids = results->mutable_ids();
    auto pk_field_id = plan->schema_->get_primary_field_id();

    auto is_pk_field = [pk_field_id](const FieldId& field_id) -> bool {
        return pk_field_id.has_value() && pk_field_id.value() == field_id;
    };

    std::vector<FieldId> field_ids;
    field_ids.reserve(plan->field_ids_.size());
    for (auto field_id : plan->field_ids_) {
        if (SystemProperty::Instance().IsSystem(field_id) || !ignore_non_pk ||
            is_pk_field(field_id)) {
            field_ids.push_back(field_id);
        }
    }

    auto fetch_one = [this, plan, offsets, size](FieldId field_id,
                                                 milvus::OpContext* field_ctx) {
        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto system_type =
                SystemProperty::Instance().GetSystemFieldType(field_id);

            FixedVector<int64_t> output(size);
            bulk_subscript(
                field_ctx, system_type, offsets, size, output.data());

            auto data_array = std::make_unique<DataArray>();
            data_array->set_field_id(field_id.get());
            data_array->set_type(milvus::proto::schema::DataType::Int64);

            auto scalar_array = data_array->mutable_scalars();
            auto data = reinterpret_cast<const int64_t*>(output.data());
            auto obj = scalar_array->mutable_long_data();
            obj->mutable_data()->Add(data, data + size);
            return data_array;
        }

        if (plan->schema_->get_dynamic_field_id().has_value() &&
            plan->schema_->get_dynamic_field_id().value() == field_id &&
            !plan->target_dynamic_fields_.empty()) {
            auto& target_dynamic_fields = plan->target_dynamic_fields_;
            return bulk_subscript(
                field_ctx, field_id, offsets, size, target_dynamic_fields);
        }
        std::unique_ptr<DataArray> col;
        auto& field_meta = plan->schema_->operator[](field_id);
        if (!is_field_exist(field_id)) {
            col = bulk_subscript_not_exist_field(field_meta, size);
        } else {
            col = bulk_subscript(field_ctx, field_id, offsets, size);
        }
        // todo(SpadeA): consider vector array?
        if (field_meta.get_data_type() == DataType::ARRAY) {
            col->mutable_scalars()->mutable_array_data()->set_element_type(
                proto::schema::DataType(field_meta.get_element_type()));
        }
        return col;
    };

    auto fetched_fields =
        FetchOutputFields(field_ids, get_segment_id(), op_ctx, fetch_one);

    for (auto& fetched : fetched_fields) {
        auto field_id = fetched.field_id;
        auto& col = fetched.field_data;
        results->set_scanned_remote_bytes(results->scanned_remote_bytes() +
                                          fetched.scanned_remote_bytes);
        results->set_scanned_total_bytes(results->scanned_total_bytes() +
                                         fetched.scanned_total_bytes);
        if (SystemProperty::Instance().IsSystem(field_id)) {
            fields_data->AddAllocated(col.release());
            continue;
        }

        auto& field_meta = plan->schema_->operator[](field_id);
        if (fill_ids && is_pk_field(field_id)) {
            // fill_ids should be true when the first Retrieve was called. The reduce phase depends on the ids to do
            // merge-sort.
            auto col_data = col.get();
            switch (field_meta.get_data_type()) {
                case DataType::INT64: {
                    auto int_ids = ids->mutable_int_id();
                    auto& src_data = col_data->scalars().long_data();
                    int_ids->mutable_data()->Add(src_data.data().begin(),
                                                 src_data.data().end());
                    break;
                }
                case DataType::VARCHAR: {
                    auto str_ids = ids->mutable_str_id();
                    auto& src_data = col_data->scalars().string_data();
                    for (auto i = 0; i < src_data.data_size(); ++i) {
                        *(str_ids->mutable_data()->Add()) = src_data.data(i);
                    }
                    break;
                }
                default: {
                    ThrowInfo(DataTypeInvalid,
                              fmt::format("unsupported datatype {}",
                                          field_meta.get_data_type()));
                }
            }
        }
        if (!ignore_non_pk) {
            // when ignore_non_pk is false, it indicates two situations:
            //  1. No need to do the two-phase Retrieval, the target entries should be returned as the first Retrieval
            //      is done, below two cases are included:
            //       a. There is only one segment;
            //       b. No pagination is used;
            //  2. The FillTargetEntry was called by the second Retrieval (by offsets).
            fields_data->AddAllocated(col.release());
        }
    }
}

std::unique_ptr<proto::segcore::RetrieveResults>
SegmentInternalInterface::Retrieve(
    tracer::TraceContext* trace_ctx,
    const query::RetrievePlan* Plan,
    const int64_t* offsets,
    int64_t size,
    const folly::CancellationToken& cancel_token) const {
    std::shared_lock lck(mutex_);
    tracer::AutoSpan span("RetrieveByOffsets", tracer::GetRootSpan());
    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::chrono::high_resolution_clock::time_point get_target_entry_start =
        std::chrono::high_resolution_clock::now();
    // Carry the upstream cancel_token down into the take() + Arrow-convert
    // path so RetrieveByOffsets on external fields can short-circuit.
    milvus::OpContext fte_op_ctx;
    fte_op_ctx.cancellation_token = cancel_token;
    FillTargetEntry(
        trace_ctx, Plan, results, offsets, size, false, false, &fte_op_ctx);
    std::chrono::high_resolution_clock::time_point get_target_entry_end =
        std::chrono::high_resolution_clock::now();
    double get_entry_cost = std::chrono::duration<double, std::micro>(
                                get_target_entry_end - get_target_entry_start)
                                .count();
    milvus::monitor::internal_core_retrieve_get_target_entry_latency.Observe(
        get_entry_cost / 1000);
    return results;
}

int64_t
SegmentInternalInterface::get_real_count() const {
    if (get_deleted_count() == 0) {
        return get_row_count();
    }

#if 0
    auto insert_cnt = get_row_count();
    BitsetType bitset_holder;
    bitset_holder.resize(insert_cnt, false);
    mask_with_delete(bitset_holder, insert_cnt, MAX_TIMESTAMP);
    return bitset_holder.size() - bitset_holder.count();
#endif
    auto plan = std::make_unique<query::RetrievePlan>(get_schema_snapshot());
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    milvus::plan::PlanNodePtr plannode;
    std::vector<milvus::plan::PlanNodePtr> sources;
    plannode = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId());
    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

    // ProjectNode consumes the MVCC bitmap and materializes valid rows.
    // Without it, AggregationNode would see input->size() == total rows
    // instead of the actual valid row count after MVCC filtering.
    plannode = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{},
        std::vector<std::string>{},
        std::vector<DataType>{},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    plannode = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<expr::FieldAccessTypeExprPtr>{},
        std::vector<std::string>{agg_name},
        std::move(aggregates),
        sources);

    plan->plan_node_->plannodes_ = plannode;
    auto res = Retrieve(nullptr,
                        plan.get(),
                        MAX_TIMESTAMP,
                        INT64_MAX,
                        false,
                        folly::CancellationToken(),
                        0,
                        0);
    AssertInfo(res->fields_data().size() == 1,
               "count result should only have one column");
    AssertInfo(res->fields_data()[0].has_scalars(),
               "count result should match scalar");
    AssertInfo(res->fields_data()[0].scalars().has_long_data(),
               "count result should match long data");
    AssertInfo(res->fields_data()[0].scalars().long_data().data_size() == 1,
               "count result should only have one row");
    return res->fields_data()[0].scalars().long_data().data(0);
}

int64_t
SegmentInternalInterface::get_field_avg_size(FieldId field_id) const {
    AssertInfo(field_id.get() >= 0,
               "invalid field id, should be greater than or equal to 0");
    if (SystemProperty::Instance().IsSystem(field_id)) {
        if (field_id == TimestampFieldID || field_id == RowFieldID) {
            return sizeof(int64_t);
        }

        ThrowInfo(FieldIDInvalid, "unsupported system field id");
    }

    auto schema = get_schema_snapshot();
    auto& field_meta = (*schema)[field_id];
    auto data_type = field_meta.get_data_type();

    // Retrieve already holds mutex_; acquiring it again may deadlock
    // when a writer is waiting.
    if (IsVariableDataType(data_type)) {
        if (variable_fields_avg_size_.find(field_id) ==
            variable_fields_avg_size_.end()) {
            return 0;
        }

        return variable_fields_avg_size_.at(field_id).second;
    } else {
        return field_meta.get_sizeof();
    }
}

void
SegmentInternalInterface::set_field_avg_size(FieldId field_id,
                                             int64_t num_rows,
                                             int64_t field_size) {
    AssertInfo(field_id.get() >= 0,
               "invalid field id, should be greater than or equal to 0");
    auto schema = get_schema_snapshot();
    auto& field_meta = (*schema)[field_id];
    set_field_avg_size(field_meta, num_rows, field_size);
}

void
SegmentInternalInterface::set_field_avg_size(const FieldMeta& field_meta,
                                             int64_t num_rows,
                                             int64_t field_size) {
    auto field_id = field_meta.get_id();
    AssertInfo(field_id.get() >= 0,
               "invalid field id, should be greater than or equal to 0");
    auto data_type = field_meta.get_data_type();

    std::unique_lock lck(mutex_);
    if (IsVariableDataType(data_type)) {
        AssertInfo(num_rows > 0,
                   "The num rows of field data should be greater than 0");
        if (variable_fields_avg_size_.find(field_id) ==
            variable_fields_avg_size_.end()) {
            variable_fields_avg_size_.emplace(field_id, std::make_pair(0, 0));
        }

        auto& field_info = variable_fields_avg_size_.at(field_id);
        auto size = field_info.first * field_info.second + field_size;
        field_info.first = field_info.first + num_rows;
        field_info.second = size / field_info.first;
    }
}

FieldSkipMetricsView
SegmentInternalInterface::GetFieldSkipMetrics(FieldId field_id) const {
    if (auto* sealed = dynamic_cast<const ChunkedSegmentSealedImpl*>(this)) {
        return sealed->GetFieldSkipMetrics(field_id);
    }
    return {};
}

PinWrapper<index::TextMatchIndex*>
SegmentInternalInterface::GetTextIndex(milvus::OpContext* op_ctx,
                                       FieldId field_id) const {
    std::shared_lock lock(mutex_);
    auto iter = text_indexes_.find(field_id);
    if (iter == text_indexes_.end()) {
        ThrowInfo(milvus::ErrorCode::TextIndexNotFound,
                  "text index not found for field {}",
                  field_id.get());
    }

    auto make_pin = [&](auto&& alt) -> PinWrapper<index::TextMatchIndex*> {
        using Alt = std::decay_t<decltype(alt)>;

        if constexpr (std::is_same_v<
                          Alt,
                          std::unique_ptr<milvus::index::TextMatchIndex>>) {
            return PinWrapper<index::TextMatchIndex*>(alt.get());
        } else if constexpr (std::is_same_v<
                                 Alt,
                                 std::shared_ptr<
                                     milvus::index::TextMatchIndexHolder>>) {
            return PinWrapper<index::TextMatchIndex*>(alt, alt->get());
        } else if constexpr (std::is_same_v<
                                 Alt,
                                 std::shared_ptr<
                                     milvus::cachinglayer::CacheSlot<
                                         milvus::index::TextMatchIndex>>>) {
            auto ca = SemiInlineGet(alt->PinCells(op_ctx, {0}));
            auto index = ca->get_cell_of(0);
            return PinWrapper<index::TextMatchIndex*>(std::move(ca), index);
        } else {
            ThrowInfo(milvus::ErrorCode::UnexpectedError,
                      "text index of segment is not supported for field {}",
                      field_id.get());
        }
    };

    return std::visit(make_pin, iter->second);
}

std::unique_ptr<DataArray>
SegmentInternalInterface::bulk_subscript_not_exist_field(
    const milvus::FieldMeta& field_meta, int64_t count) const {
    auto data_type = field_meta.get_data_type();
    if (IsVectorDataType(data_type)) {
        AssertInfo(field_meta.is_nullable(),
                   "Non-nullable vector field should not reach here");

        auto create_count = IsVectorArrayDataType(data_type) ? count : 0;
        auto result = CreateEmptyVectorDataArray(create_count, field_meta);

        auto valid_data = MutableFieldDataRowValidData(result.get());
        for (int64_t i = 0; i < count; ++i) {
            valid_data->Add(false);
        }
        return result;
    }

    auto result = CreateEmptyScalarDataArray(count, field_meta);
    if (field_meta.default_value().has_value()) {
        if (field_meta.is_nullable()) {
            auto res =
                MutableFieldDataRowValidData(result.get())->mutable_data();
            for (int64_t i = 0; i < count; ++i) {
                res[i] = true;
            }
        }
        switch (field_meta.get_data_type()) {
            case DataType::BOOL: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_bool_data()
                                    ->mutable_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr[i] = field_meta.default_value()->bool_data();
                }
                break;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_int_data()
                                    ->mutable_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr[i] = field_meta.default_value()->int_data();
                }
                break;
            }
            case DataType::INT64: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_long_data()
                                    ->mutable_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr[i] = field_meta.default_value()->long_data();
                }
                break;
            }
            case DataType::FLOAT: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_float_data()
                                    ->mutable_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr[i] = field_meta.default_value()->float_data();
                }
                break;
            }
            case DataType::DOUBLE: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_double_data()
                                    ->mutable_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr[i] = field_meta.default_value()->double_data();
                }
                break;
            }
            case DataType::TIMESTAMPTZ: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_timestamptz_data()
                                    ->mutable_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr[i] =
                        field_meta.default_value()->timestamptz_data();
                }
                break;
            }
            case DataType::VARCHAR: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_string_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr->at(i) = field_meta.default_value()->string_data();
                }
                break;
            }
            // for enabling dynamic field, normal json not support default value yet
            case DataType::JSON: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_json_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr->at(i) = field_meta.default_value()->bytes_data();
                }
                break;
            }
            case DataType::GEOMETRY: {
                auto data_ptr = result->mutable_scalars()
                                    ->mutable_geometry_data()
                                    ->mutable_data();

                for (int64_t i = 0; i < count; ++i) {
                    data_ptr->at(i) = field_meta.default_value()->bytes_data();
                }
                break;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported default value type {}",
                                      field_meta.get_data_type()));
            }
        }
        return result;
    }
    // Without a default value the column can only read as all-null;
    // CreateEmptyScalarDataArray already sized valid_data to all-false.
    AssertInfo(field_meta.is_nullable(),
               "Non-nullable scalar field without default value should not "
               "reach here");
    return result;
}

// Only sealed segment has ngram index
PinWrapper<index::NgramInvertedIndex*>
SegmentInternalInterface::GetNgramIndex(milvus::OpContext* op_ctx,
                                        FieldId field_id) const {
    return PinWrapper<index::NgramInvertedIndex*>(nullptr);
}

PinWrapper<index::NgramInvertedIndex*>
SegmentInternalInterface::GetNgramIndexForJson(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::string& nested_path) const {
    return PinWrapper<index::NgramInvertedIndex*>(nullptr);
}

std::shared_ptr<index::JsonKeyStats>
SegmentInternalInterface::GetJsonStats(milvus::OpContext* op_ctx,
                                       FieldId field_id) const {
    std::shared_lock lock(mutex_);
    auto iter = json_stats_.find(field_id);
    if (iter == json_stats_.end()) {
        return nullptr;
    }
    return iter->second;
}

}  // namespace milvus::segcore
