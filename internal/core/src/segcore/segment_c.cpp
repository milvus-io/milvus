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

#include "segcore/segment_c.h"

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include "common/SystemProperty.h"
#include "common/arrow_c_data_c.h"
#include "segcore/arrow_field_utils.h"
#include "segcore/default_fs.h"

#include <folly/CancellationToken.h>
#include <folly/ExceptionWrapper.h>
#include <folly/ScopeGuard.h>
#include <folly/Try.h>
#include <folly/futures/Promise.h>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <memory>
#include <numeric>
#include <type_traits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/CGoCatch.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/LoadInfo.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/common_type_c.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "exec/expression/ExprCache.h"
#include "fmt/core.h"
#include "folly/CancellationToken.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/futures/Future.h"
#include "futures/Executor.h"
#include "futures/Future.h"
#include "glog/logging.h"
#include "index/Meta.h"
#include "log/Log.h"
#include "milvus-storage/filesystem/fs.h"
#include "monitor/scope_metric.h"
#include "nlohmann/json.hpp"
#include "opentelemetry/trace/span.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "prometheus/histogram.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/Collection.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "storage/FileManager.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"

//////////////////////////////    common interfaces    //////////////////////////////

/**
 * @brief Create a segment from a collection.
 * @param col The collection to create the segment from.
 * @param seg_type The type of segment to create.
 * @param segment_id The ID of the segment to create.
 * @param is_sorted_by_pk Whether the data in the sealed segment is sorted by primary key.
 * @return A unique pointer to a SegmentInterface object.
 */
std::unique_ptr<milvus::segcore::SegmentInterface>
CreateSegment(milvus::segcore::Collection* col,
              SegmentType seg_type,
              int64_t segment_id,
              bool is_sorted_by_pk) {
    auto schema = col->get_schema();
    std::unique_ptr<milvus::segcore::SegmentInterface> segment;
    switch (seg_type) {
        case Growing: {
            auto seg = milvus::segcore::CreateGrowingSegment(
                schema,
                col->get_index_meta(),
                segment_id,
                milvus::segcore::SegcoreConfig::default_config());
            segment = std::move(seg);
            break;
        }
        case Sealed:
        case Indexing: {
            auto scope = milvus::segcore::LoadScope::FromLegacySchema(*schema);
            segment =
                std::make_unique<milvus::segcore::ChunkedSegmentSealedImpl>(
                    col->GetSealedFieldLayout(scope),
                    col->get_index_meta(),
                    milvus::segcore::SegcoreConfig::default_config(),
                    segment_id,
                    is_sorted_by_pk);
        } break;

        default:
            ThrowInfo(
                milvus::UnexpectedError, "invalid segment type: {}", seg_type);
    }
    return segment;
}

CStatus
NewSegment(CCollection collection,
           SegmentType seg_type,
           int64_t segment_id,
           CSegmentInterface* newSegment,
           bool is_sorted_by_pk) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto col = static_cast<milvus::segcore::Collection*>(collection);

        auto segment =
            CreateSegment(col, seg_type, segment_id, is_sorted_by_pk);

        *newSegment = segment.release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
NewSegmentWithLoadInfo(CCollection collection,
                       SegmentType seg_type,
                       int64_t segment_id,
                       CSegmentInterface* newSegment,
                       bool is_sorted_by_pk,
                       const uint8_t* load_info_blob,
                       const int64_t load_info_length) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(load_info_blob, "load info is null");
        milvus::proto::segcore::SegmentLoadInfo load_info;
        auto suc = load_info.ParseFromArray(load_info_blob, load_info_length);
        AssertInfo(suc, "unmarshal load info failed");

        auto col = static_cast<milvus::segcore::Collection*>(collection);

        auto segment =
            CreateSegment(col, seg_type, segment_id, is_sorted_by_pk);
        segment->SetLoadInfo(std::move(load_info));
        *newSegment = segment.release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
NewSealedSegmentWithCollection(CCollection collection,
                               int64_t segment_id,
                               CSegmentInterface* output,
                               bool sorted,
                               const uint8_t* load_blob,
                               int64_t load_length,
                               const int64_t* load_fields,
                               int64_t field_count) {
    SCOPE_CGO_CALL_METRIC();
    try {
        auto* col = static_cast<milvus::segcore::Collection*>(collection);
        auto schema = col->get_schema();
        std::vector<int64_t> fields;
        if (field_count > 0) {
            fields.assign(load_fields, load_fields + field_count);
        }
        milvus::proto::segcore::SegmentLoadInfo info;
        AssertInfo(info.ParseFromArray(load_blob, load_length),
                   "unmarshal load info failed");
        std::vector<int64_t> hints(info.field_hints().begin(),
                                   info.field_hints().end());
        auto segment =
            std::make_unique<milvus::segcore::ChunkedSegmentSealedImpl>(
                col->GetSealedFieldLayout(
                    milvus::segcore::LoadScope(fields, hints)),
                col->get_index_meta(),
                milvus::segcore::SegcoreConfig::default_config(),
                segment_id,
                sorted);
        segment->SetLoadInfo(std::move(info));
        *output = segment.release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

milvus::SchemaPtr
ParseReopenSchema(const void* schema_blob,
                  const int64_t schema_length,
                  const uint64_t schema_version) {
    AssertInfo(schema_blob != nullptr, "schema is null");
    AssertInfo(schema_length > 0, "schema length must be positive");

    milvus::proto::schema::CollectionSchema collection_schema;
    auto suc = collection_schema.ParseFromArray(schema_blob, schema_length);
    AssertInfo(suc, "parse schema proto failed");
    auto schema = milvus::Schema::ParseFrom(collection_schema);
    schema->set_schema_version(schema_version);
    return schema;
}

milvus::SchemaPtr
ParseFlushSchema(const void* schema_blob, const int64_t schema_length) {
    AssertInfo(schema_blob != nullptr, "flush schema is null");
    AssertInfo(schema_length > 0, "flush schema length must be positive");

    milvus::proto::schema::CollectionSchema collection_schema;
    auto suc = collection_schema.ParseFromArray(schema_blob, schema_length);
    AssertInfo(suc, "parse flush schema proto failed");
    return milvus::Schema::ParseFrom(collection_schema);
}

CFuture*
AsyncReopenSealedSegmentWithCollection(CTraceContext trace,
                                       CSegmentInterface c_segment,
                                       CCollection c_collection,
                                       const uint8_t* load_blob,
                                       int64_t load_length,
                                       const int64_t* load_fields,
                                       int64_t load_field_count) {
    try {
        auto* collection =
            static_cast<milvus::segcore::Collection*>(c_collection);
        std::vector<int64_t> fields;
        if (load_field_count > 0) {
            fields.assign(load_fields, load_fields + load_field_count);
        }
        milvus::proto::segcore::SegmentLoadInfo info;
        AssertInfo(info.ParseFromArray(load_blob, load_length),
                   "unmarshal load info failed");
        auto layout =
            collection->GetSealedFieldLayout(milvus::segcore::LoadScope(
                fields,
                std::vector<int64_t>(info.field_hints().begin(),
                                     info.field_hints().end())));
        // Capture the exact metadata before scheduling. The CCollection handle
        // may be released once its Go owner has joined this operation; retained
        // PublishedStates continue owning the layout independently.
        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getLoadCPUExecutor(),
            milvus::futures::ExecutePriority::NORMAL,
            [c_segment, info = std::move(info), layout = std::move(layout)](
                folly::CancellationToken token) -> bool* {
                auto* segment =
                    dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(
                        static_cast<milvus::segcore::SegmentInterface*>(
                            c_segment));
                AssertInfo(segment != nullptr,
                           "borrowed schema reopen requires a sealed segment");
                milvus::OpContext ctx(token);
                segment->Reopen(&ctx, info, layout);
                return nullptr;
            },
            milvus::futures::PoolType::kLoad);
        return static_cast<CFuture*>(static_cast<void*>(
            static_cast<milvus::futures::IFuture*>(future.release())));
    } catch (...) {
        auto error = std::current_exception();
        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getLoadCPUExecutor(),
            milvus::futures::ExecutePriority::NORMAL,
            [error](folly::CancellationToken) -> bool* {
                std::rethrow_exception(error);
            },
            milvus::futures::PoolType::kLoad);
        return static_cast<CFuture*>(static_cast<void*>(
            static_cast<milvus::futures::IFuture*>(future.release())));
    }
}

CFuture*
AsyncReopenSegment(CTraceContext c_trace,
                   CSegmentInterface c_segment,
                   const uint8_t* load_info_blob,
                   const int64_t load_info_length,
                   const void* schema_blob,
                   const int64_t schema_length,
                   const uint64_t schema_version) {
    try {
        AssertInfo(load_info_blob, "load info is null");
        milvus::proto::segcore::SegmentLoadInfo load_info;
        auto suc = load_info.ParseFromArray(load_info_blob, load_info_length);
        AssertInfo(suc, "unmarshal load info failed");
        auto schema =
            ParseReopenSchema(schema_blob, schema_length, schema_version);

        auto segment =
            static_cast<milvus::segcore::SegmentInterface*>(c_segment);

        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getLoadCPUExecutor(),
            milvus::futures::ExecutePriority::NORMAL,
            [c_trace,
             segment,
             load_info = std::move(load_info),
             schema = std::move(schema)](
                folly::CancellationToken cancel_token) -> bool* {
                milvus::OpContext op_ctx(cancel_token);
                segment->Reopen(&op_ctx, load_info, schema);
                return nullptr;
            },
            milvus::futures::PoolType::kLoad);
        return static_cast<CFuture*>(static_cast<void*>(
            static_cast<milvus::futures::IFuture*>(future.release())));
    } catch (std::exception& e) {
        // Keep the ORIGINAL error code (a typed SegcoreError, e.g. a
        // retriable storage failure from preflight, must not collapse into
        // UnexpectedError) while still prefixing the preflight context.
        auto code = milvus::UnexpectedError;
        if (auto* se = dynamic_cast<const milvus::SegcoreError*>(&e)) {
            code = se->get_error_code();
        }
        std::string error_msg = e.what();
        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getLoadCPUExecutor(),
            milvus::futures::ExecutePriority::NORMAL,
            [code, error_msg = std::move(error_msg)](
                folly::CancellationToken cancel_token) -> bool* {
                (void)cancel_token;
                ThrowInfo(
                    code, "AsyncReopenSegment preflight failed: {}", error_msg);
                return nullptr;
            },
            milvus::futures::PoolType::kLoad);
        return static_cast<CFuture*>(static_cast<void*>(
            static_cast<milvus::futures::IFuture*>(future.release())));
    } catch (...) {
        auto eptr = std::current_exception();
        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getLoadCPUExecutor(),
            milvus::futures::ExecutePriority::NORMAL,
            [eptr](folly::CancellationToken cancel_token) -> bool* {
                (void)cancel_token;
                std::rethrow_exception(eptr);
                return nullptr;
            },
            milvus::futures::PoolType::kLoad);
        return static_cast<CFuture*>(static_cast<void*>(
            static_cast<milvus::futures::IFuture*>(future.release())));
    }
}

CLoadCancellationSource
NewLoadCancellationSource() {
    return new folly::CancellationSource();
}

void
CancelLoadCancellationSource(CLoadCancellationSource source) {
    if (source) {
        static_cast<folly::CancellationSource*>(source)->requestCancellation();
    }
}

void
ReleaseLoadCancellationSource(CLoadCancellationSource source) {
    delete static_cast<folly::CancellationSource*>(source);
}

CStatus
SegmentLoad(CTraceContext c_trace,
            CSegmentInterface c_segment,
            CLoadCancellationSource source) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment =
            static_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto trace_ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};

        if (source) {
            // Create OpContext with cancellation token from source
            auto cancellation_source =
                static_cast<folly::CancellationSource*>(source);
            milvus::OpContext op_ctx(cancellation_source->getToken());
            segment->Load(trace_ctx, &op_ctx);
        } else {
            // No cancellation source
            segment->Load(trace_ctx, nullptr);
        }

        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CFuture*
AsyncSegmentLoad(CTraceContext c_trace, CSegmentInterface c_segment) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);

    auto future = milvus::futures::Future<bool>::async(
        milvus::futures::getLoadCPUExecutor(),
        milvus::futures::ExecutePriority::NORMAL,
        [c_trace, segment](folly::CancellationToken cancel_token) -> bool* {
            auto trace_ctx = milvus::tracer::TraceContext{
                c_trace.traceID, c_trace.spanID, c_trace.traceFlags};

            milvus::OpContext op_ctx(cancel_token);
            segment->Load(trace_ctx, &op_ctx);

            return nullptr;
        },
        milvus::futures::PoolType::kLoad);
    return static_cast<CFuture*>(static_cast<void*>(
        static_cast<milvus::futures::IFuture*>(future.release())));
}

CStatus
AdoptSealedSegmentReadState(CSegmentInterface c_segment,
                            CSegmentInterface read_segment) {
    SCOPE_CGO_CALL_METRIC();
    try {
        auto* segment =
            dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(
                static_cast<milvus::segcore::SegmentInterface*>(c_segment));
        auto* read = dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(
            static_cast<milvus::segcore::SegmentInterface*>(read_segment));
        AssertInfo(segment != nullptr && read != nullptr,
                   "read-state adoption requires sealed segments");
        segment->AdoptReadState(*read);
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
PinSealedSegmentReadState(CSegmentInterface c_segment,
                          CSegmentInterface* read_segment) {
    SCOPE_CGO_CALL_METRIC();
    try {
        auto* segment =
            dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(
                static_cast<milvus::segcore::SegmentInterface*>(c_segment));
        AssertInfo(segment != nullptr,
                   "read-state pin requires a sealed segment");
        *read_segment = segment->PinReadState().release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

void
DeleteSegment(CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    auto s = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    delete s;
}

void
ClearSegmentData(CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    auto s = static_cast<milvus::segcore::SegmentSealed*>(c_segment);
    s->ClearData();
}

void
DeleteSearchResult(CSearchResult search_result) {
    SCOPE_CGO_CALL_METRIC();

    auto res = static_cast<milvus::SearchResult*>(search_result);
    delete res;
}

int64_t
GetSearchResultValidCount(CSearchResult search_result) {
    auto res = static_cast<milvus::SearchResult*>(search_result);
    if (res == nullptr) {
        return -1;
    }
    return res->valid_count_;
}

// Verifies the plan's external field references against the loaded manifest,
// after LazyCheckSchema refreshed the segment schema and manifest view.
// Optionally ignores fields that the current execution path will not access.
void
CheckExternalFieldsInLoadedManifest(
    const milvus::SchemaPtr& schema,
    milvus::segcore::SegmentInternalInterface* segment,
    const std::vector<milvus::FieldId>& fields,
    const std::vector<milvus::FieldId>& skipped_fields = {}) {
    if (!schema || !schema->is_external_collection()) {
        return;
    }

    for (auto field_id : fields) {
        if (std::find(skipped_fields.begin(), skipped_fields.end(), field_id) !=
            skipped_fields.end()) {
            continue;
        }
        if (!schema->has_field(field_id)) {
            continue;
        }

        if (!schema->IsExternalManifestStoredField(field_id)) {
            continue;
        }
        const auto& field_meta = schema->operator[](field_id);
        auto column_name = schema->GetPhysicalColumnName(field_id);
        // External output may be served through take(), so "ready" here means
        // the loaded manifest contains the storage column. It intentionally
        // does not require field data or index accessibility.
        if (!segment->HasColumnInLoadedManifest(column_name)) {
            throw milvus::SegcoreError(
                milvus::FieldNotLoaded,
                fmt::format(
                    "external field \"{}\" (storage column \"{}\") is not "
                    "available in the current loaded external collection "
                    "manifest; run RefreshExternalCollection and reload the "
                    "collection before accessing this field",
                    field_meta.get_name().get(),
                    column_name));
        }
    }
}

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

void
ValidateSegmentSchemaCompatibility(
    milvus::segcore::SegmentInterface* segment,
    const milvus::SchemaPtr& plan_schema,
    const std::vector<milvus::FieldId>& access_fields) {
    if (segment->type() == SegmentType::Growing) {
        return;
    }

    auto sealed =
        dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(segment);
    AssertInfo(sealed != nullptr,
               "sealed segment {} does not support schema validation",
               segment->get_segment_id());
    sealed->ValidateSchemaCompatibility(plan_schema);
    sealed->ValidateLogicalFields(access_fields);
}

//////////////////////////////    public C API wrappers    //////////////////////////////

CFuture*  // Future<milvus::SearchResult*>
AsyncSearch(CTraceContext c_trace,
            CSegmentInterface c_segment,
            CSearchPlan c_plan,
            CPlaceholderGroup c_placeholder_group,
            uint64_t timestamp,
            int32_t consistency_level,
            uint64_t collection_ttl,
            uint64_t entity_ttl_physical_time_us,
            bool filter_only,
            bool enable_expr_cache) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto plan = static_cast<milvus::query::Plan*>(c_plan);
    auto phg_ptr = reinterpret_cast<const milvus::query::PlaceholderGroup*>(
        c_placeholder_group);
    auto future = milvus::futures::Future<milvus::SearchResult>::async(
        milvus::futures::getSearchCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace,
         segment,
         plan,
         phg_ptr,
         timestamp,
         consistency_level,
         collection_ttl,
         entity_ttl_physical_time_us,
         filter_only,
         enable_expr_cache](folly::CancellationToken cancel_token) {
            // save trace context into search_info
            auto& trace_ctx = plan->plan_node_->search_info_.trace_ctx_;
            trace_ctx.traceID = c_trace.traceID;
            trace_ctx.spanID = c_trace.spanID;
            trace_ctx.traceFlags = c_trace.traceFlags;

            auto span = milvus::tracer::StartSpan("SegCoreSearch", &trace_ctx);
            milvus::tracer::SetRootSpan(span);
            AssertInfo(phg_ptr != nullptr && !phg_ptr->empty(),
                       "search requires non-empty placeholder group");
            const int64_t num_queries = milvus::query::GetNumOfQueries(phg_ptr);
            auto target_vector_field_id =
                plan->plan_node_->search_info_.field_id_;

            milvus::OpContext op_ctx(cancel_token);
            op_ctx.runtime_load_priority =
                milvus::proto::common::LoadPriority::HIGH;
            segment->LazyCheckSchema(plan->schema_, &op_ctx);
            auto read_lease = AcquireSegmentReadLease(segment, cancel_token);
            ValidateSegmentSchemaCompatibility(
                segment, plan->schema_, plan->access_entries_);
            auto internal_segment =
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment);
            std::vector<milvus::FieldId> skipped_manifest_fields;
            if (filter_only) {
                skipped_manifest_fields.push_back(target_vector_field_id);
                for (auto field_id : plan->target_entries_) {
                    skipped_manifest_fields.push_back(field_id);
                }
            }
            CheckExternalFieldsInLoadedManifest(plan->schema_,
                                                internal_segment,
                                                plan->access_entries_,
                                                skipped_manifest_fields);
            std::unique_ptr<milvus::SearchResult> search_result;
            if (!filter_only &&
                !internal_segment->FieldAccessible(target_vector_field_id)) {
                search_result = std::make_unique<milvus::SearchResult>();
                search_result->total_nq_ = num_queries;
                search_result->unity_topK_ = 0;
                search_result->total_data_cnt_ = 0;
                search_result->segment_ = internal_segment;
            } else {
                search_result = segment->Search(plan,
                                                phg_ptr,
                                                timestamp,
                                                cancel_token,
                                                consistency_level,
                                                collection_ttl,
                                                entity_ttl_physical_time_us,
                                                filter_only,
                                                enable_expr_cache,
                                                span);
            }
            search_result->read_lease_ = std::move(read_lease);
            if (!filter_only &&
                !milvus::PositivelyRelated(
                    plan->plan_node_->search_info_.metric_type_)) {
                for (auto& dis : search_result->distances_) {
                    dis *= -1;
                }
            }
            span->End();
            milvus::tracer::CloseRootSpan();

            return search_result.release();
        });

    return static_cast<CFuture*>(static_cast<void*>(
        static_cast<milvus::futures::IFuture*>(future.release())));
}

void
DeleteRetrieveResult(CRetrieveResult* retrieve_result) {
    delete[] static_cast<uint8_t*>(
        const_cast<void*>(retrieve_result->proto_blob));
    delete retrieve_result;
}

/// Create a leaked CRetrieveResult from a proto.
/// Should be released by DeleteRetrieveResult.
CRetrieveResult*
CreateLeakedCRetrieveResultFromProto(
    std::unique_ptr<milvus::proto::segcore::RetrieveResults> retrieve_result) {
    for (auto& field_data : *retrieve_result->mutable_fields_data()) {
        milvus::SyncFieldDataRowValidData(&field_data);
    }
    auto size = retrieve_result->ByteSizeLong();
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[size]);
    retrieve_result->SerializePartialToArray(buffer.get(), size);

    auto result = std::make_unique<CRetrieveResult>();
    result->proto_blob = buffer.release();
    result->proto_size = size;
    return result.release();
}

CFuture*  // Future<CRetrieveResult>
AsyncRetrieve(CTraceContext c_trace,
              CSegmentInterface c_segment,
              CRetrievePlan c_plan,
              uint64_t timestamp,
              int64_t limit_size,
              bool ignore_non_pk,
              int32_t consistency_level,
              uint64_t collection_ttl,
              uint64_t entity_ttl_physical_time_us) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);
    auto future = milvus::futures::Future<CRetrieveResult>::async(
        milvus::futures::getSearchCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace,
         segment,
         plan,
         timestamp,
         limit_size,
         ignore_non_pk,
         consistency_level,
         collection_ttl,
         entity_ttl_physical_time_us](folly::CancellationToken cancel_token) {
            auto trace_ctx = milvus::tracer::TraceContext{
                c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
            milvus::tracer::AutoSpan span("SegCoreRetrieve", &trace_ctx, true);

            milvus::OpContext op_ctx(cancel_token);
            op_ctx.runtime_load_priority =
                milvus::proto::common::LoadPriority::HIGH;
            segment->LazyCheckSchema(plan->schema_, &op_ctx);
            auto read_lease = AcquireSegmentReadLease(segment, cancel_token);
            ValidateSegmentSchemaCompatibility(
                segment, plan->schema_, plan->access_entries_);
            auto internal_segment =
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment);
            CheckExternalFieldsInLoadedManifest(
                plan->schema_, internal_segment, plan->access_entries_);

            auto retrieve_result =
                segment->Retrieve(&trace_ctx,
                                  plan,
                                  timestamp,
                                  limit_size,
                                  ignore_non_pk,
                                  cancel_token,
                                  consistency_level,
                                  collection_ttl,
                                  entity_ttl_physical_time_us);

            auto c_result = CreateLeakedCRetrieveResultFromProto(
                std::move(retrieve_result));
            read_lease.reset();
            return c_result;
        });
    return static_cast<CFuture*>(static_cast<void*>(
        static_cast<milvus::futures::IFuture*>(future.release())));
}

namespace {

// RowCountOfDataArray derives a column's row count from the column itself.
//
// offset_size() is NOT a safe substitute. ExecPlanNodeVisitor picks between two
// shapes on `first_column->IsBitmap()` (ExecPlanNodeVisitor.cpp, setupRetrieveResult):
// a bitmap output goes through find_first into result_offsets_, where
// offset_size() IS the row count; a COLUMNAR output populates field_data_
// directly and leaves result_offsets_ empty while the columns hold rows.
// GetResultRowCount (SegmentInterface.cpp) is the same decision for the internal
// struct -- there is no protobuf-side equivalent, which is why this exists.
//
// Today's routing only sends bitmap-shaped plans here (shouldUseArrowTransport
// excludes aggregation and ORDER BY, which are what currently produce columnar
// output), so offset_size() would in fact be correct. It is deliberately not
// trusted: the real discriminator is the pipeline's output shape, not the
// routing predicate, and if any non-bitmap plan ever reaches this export
// offset_size() would build a zero-row batch and silently drop EVERY row --
// the Arrow builders only assert when the source is SHORTER than the requested
// count, so nothing would catch it. Deriving from the column converts that
// silent data loss into a loud mismatch.
int64_t
RowCountOfDataArray(const milvus::DataArray& fd) {
    const auto& valid = milvus::GetFieldDataRowValidData(fd);
    if (!valid.empty()) {
        return static_cast<int64_t>(valid.size());
    }
    if (fd.has_scalars()) {
        const auto& sc = fd.scalars();
        if (sc.has_bool_data())
            return sc.bool_data().data_size();
        if (sc.has_int_data())
            return sc.int_data().data_size();
        if (sc.has_long_data())
            return sc.long_data().data_size();
        if (sc.has_float_data())
            return sc.float_data().data_size();
        if (sc.has_double_data())
            return sc.double_data().data_size();
        if (sc.has_string_data())
            return sc.string_data().data_size();
        if (sc.has_json_data())
            return sc.json_data().data_size();
        if (sc.has_array_data())
            return sc.array_data().data_size();
        if (sc.has_geometry_data())
            return sc.geometry_data().data_size();
        if (sc.has_timestamptz_data())
            return sc.timestamptz_data().data_size();
    }
    if (fd.has_vectors()) {
        const auto& v = fd.vectors();
        auto dim = v.dim();
        if (v.has_float_vector() && dim > 0)
            return v.float_vector().data_size() / dim;
        // dim >= 8, not dim > 0: a binary vector packs 8 dimensions per byte,
        // so each row occupies dim/8 bytes -- a divisor that would be zero,
        // not merely wrong, for dim < 8.
        if (!v.binary_vector().empty() && dim >= 8)
            return static_cast<int64_t>(v.binary_vector().size()) / (dim / 8);
        if (!v.float16_vector().empty() && dim > 0)
            return static_cast<int64_t>(v.float16_vector().size()) / (dim * 2);
        if (!v.bfloat16_vector().empty() && dim > 0)
            return static_cast<int64_t>(v.bfloat16_vector().size()) / (dim * 2);
        if (!v.int8_vector().empty() && dim > 0)
            return static_cast<int64_t>(v.int8_vector().size()) / dim;
        if (v.has_sparse_float_vector())
            return v.sparse_float_vector().contents_size();
        if (v.has_vector_array())
            return v.vector_array().data_size();
    }
    return -1;  // unknown shape
}

// Schema-level metadata key carrying the original fields_data column order.
constexpr const char* kRetrieveFieldOrderKey = "milvus.field_order";

// Schema-level metadata key listing the field ids whose source DataArray
// carried a valid_data bitmap, comma separated.
//
// Arrow cannot express the distinction on its own: an all-true bitmap and an
// absent one both arrive as NullN() == 0, and the Go reconstruction otherwise
// has to guess from schema nullability. Guessing diverges, because the ORDER
// BY pipeline allocates valid_data for every scalar column regardless of
// whether the field is nullable (ExecPlanNodeVisitor.cpp:364-374,
// "Always allocate valid_data").
//
// The divergence is not cosmetic. AppendFieldData appends a validity bit only
// for source columns that have one (schema.go:1367-1371), so merging an
// Arrow-path result with a protobuf-path result for the same column yields a
// ValidData shorter than the row count. The reachable mix is per-NODE config
// differences across the proxy reduce; there is no per-segment fallback inside
// one QueryNode, since RetrieveArrow has no fallback and the routing decision is
// made once per request.
constexpr const char* kRetrieveValidDataFieldsKey = "milvus.valid_data_fields";

// RetrieveUserColumns owns the user output columns moved out of a retrieve
// result, and is what makes it sound for the exported Arrow arrays to ALIAS
// their protobuf buffers instead of copying them.
//
// Lifetime: the holder is handed to FieldDataToArrow as the arrays' owner, so
// ExportRecordBatch's C release callback owns the RecordBatch, which owns the
// ArrayData, which owns the aliasing buffers, which own this holder. The Go
// side releasing the record unwinds the whole chain; nothing here is freed
// before then.
//
// Storage choice matters: columns is a protobuf RepeatedPtrField, whose
// elements are separately heap-allocated, so their addresses are stable. A
// std::vector<DataArray> would move its elements on growth and invalidate
// every pointer already handed to Arrow.
struct RetrieveUserColumns : milvus::segcore::ProtoStorageOwner {
    // The user columns, moved (not copied) out of the result.
    milvus::proto::segcore::RetrieveResults columns;
    // Field ids of `columns`, positionally aligned with it.
    std::vector<milvus::FieldId> ids;
    // Every column of the ORIGINAL fields_data in order, system ones included.
    std::vector<milvus::FieldId> field_order;
    // The subset of `ids` whose source DataArray carried a valid_data bitmap.
    std::vector<milvus::FieldId> valid_data_fields;
};

// JoinFieldIds renders field ids as the comma-separated list the two schema
// metadata keys carry.
std::string
JoinFieldIds(const std::vector<milvus::FieldId>& ids) {
    std::string out;
    for (const auto& id : ids) {
        if (!out.empty()) {
            out += ",";
        }
        out += std::to_string(id.get());
    }
    return out;
}

// MakeRetrieveBatch assembles the exported RecordBatch and attaches the two
// schema metadata keys the Go side reads to recover column identity.
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeRetrieveBatch(std::vector<std::shared_ptr<arrow::Field>> fields,
                  std::vector<std::shared_ptr<arrow::Array>> arrays,
                  const std::string& field_order,
                  const std::string& valid_data_fields,
                  int64_t total_rows) {
    auto metadata = arrow::key_value_metadata(
        {kRetrieveFieldOrderKey, kRetrieveValidDataFieldsKey},
        {field_order, valid_data_fields});
    auto schema = arrow::schema(std::move(fields), std::move(metadata));
    return arrow::RecordBatch::Make(schema, total_rows, std::move(arrays));
}

// WorthCarryingAsArrow decides whether a column travels as Arrow or stays in
// fields_data for the protobuf reduce to merge. The reduce addresses rows by
// the same selection either way, so the result is identical.
//
// This is an ALLOW-list, and deliberately keyed on the same payload oneof that
// FieldDataToArrow dispatches on -- not on fd.type(). Two reasons:
//
//   - Fail closed. There is no fallback behind this predicate: a column that is
//     carried but cannot be exported reaches NotImplemented, and RetrieveArrow
//     returns the error rather than retrying on the protobuf path. A deny-list
//     over DataType would hand `true` to every type added to the enum later --
//     Mol, Date, Time, Decimal, UUID and Struct are already in schema.proto
//     with no exporter branch -- and turn each into a failed user query.
//   - No level mismatch. fd.type() and the payload oneof are set independently,
//     so a DataType-keyed check can pass a column whose actual payload the
//     exporter has no branch for (bytes_data, geometry_wkt_data, mol_data).
//
// Keep the two lists in step: if FieldDataToArrow gains a branch, add it here.
//
// Element-level (ArrayOfVector) queries depend on this staying in step with the
// GO routing too. They work today only because vector_array is excluded below:
// the column stays in the protobuf header alongside element_indices, which the
// reduce reads from the header and re-emits via buildMergedElementIndices. The
// Arrow materializer has no ArrayOfVector case and no element-indices handling,
// so admitting vector_array here without adding both would silently drop the
// indices.
//
// Three types are excluded as POLICY even though the exporter handles them:
//
//   - array_data: exported as a protobuf blob SERIALIZED PER ROW (a
//     SerializeToString into a BinaryBuilder), so carrying it would cost that
//     serialize here plus a matching Unmarshal in Go to move bytes the
//     protobuf path already had in final form. A property of this export, not
//     of the type: a native Arrow list would remove the reason.
//   - vector_array: already exports as a native list(fixed_size_binary) with no
//     serialization, and arrowconv CAN read it back
//     (arrowListToVectorArray). The gap is narrower than it looks -- the Go
//     gathers accept only STRING/BINARY/BOOL and materializeColumn has no
//     ArrayOfVector case -- so lifting it means adding a LIST case there, not
//     writing a converter.
//   - sparse_float_vector: a byte-identity reason. The protobuf reduce sets
//     SparseFloatArray.Dim to the max over each contributing SEGMENT's
//     DECLARED dim (queryutil buildMergedVectorField), which covers every row
//     that segment retrieved; an Arrow column holds only the rows the reduce
//     kept, so compactSparseVector computes the max over the SELECTED rows.
//     Those differ whenever the selection is a strict subset. Downstream
//     AppendFieldData recomputes Dim per appended row
//     (appendSparseFloatArraySingleRow), so the difference does not survive
//     that hop -- but MergeFieldData propagates the declared value, and this
//     change holds itself to byte-identity AT THIS BOUNDARY. The deeper fix is
//     to tighten buildMergedVectorField's maxDim to SparseFloatRowDim over the
//     selected rows, which would align the two conventions; carrying a
//     per-segment dim in the metadata would instead propagate the looser one.
bool
WorthCarryingAsArrow(const milvus::DataArray& fd) {
    if (fd.has_scalars()) {
        const auto& s = fd.scalars();
        // Policy exclusion, not a capability one: FieldDataToArrow CAN export
        // array_data, but only as a per-row SerializeToString blob.
        if (s.has_array_data()) {
            return false;
        }
        return s.has_bool_data() || s.has_int_data() || s.has_long_data() ||
               s.has_timestamptz_data() || s.has_float_data() ||
               s.has_double_data() || s.has_string_data() ||
               s.has_json_data() || s.has_geometry_data();
    }
    if (fd.has_vectors()) {
        const auto& v = fd.vectors();
        // Policy exclusions, both exportable but not carried -- see above.
        if (v.has_sparse_float_vector() || v.has_vector_array()) {
            return false;
        }
        return v.has_float_vector() || v.has_binary_vector() ||
               v.has_float16_vector() || v.has_bfloat16_vector() ||
               v.has_int8_vector();
    }
    return false;
}

// PartitionRetrieveResult moves the user output columns out of `results` into a
// holder, leaving only the system columns behind in their original relative
// order.
//
// The system columns must stay in the protobuf header, for two reasons in the
// reduce (internal/util/queryutil/reduce_by_pk_op.go):
//   - a result with len(GetFieldsData()) == 0 is skipped outright, so moving
//     EVERY column out would drop the segment's rows entirely;
//   - newTimestampedResult finds the Timestamp column in fields_data by id. Its
//     error is tolerated, not fatal -- the caller falls back to timestamps=nil,
//     which makes every timestamp read 0 and silently changes how duplicate PKs
//     break ties. A silent wrong answer, not a loud failure.
//
// Moving rather than copying is what the aliasing depends on: Swap transfers
// the inner buffers, so the payload is never duplicated and its address is the
// one Arrow will point at.
std::shared_ptr<RetrieveUserColumns>
PartitionRetrieveResult(milvus::proto::segcore::RetrieveResults* results) {
    auto holder = std::make_shared<RetrieveUserColumns>();
    auto* fields_data = results->mutable_fields_data();
    milvus::proto::segcore::RetrieveResults retained;

    // Decide up front whether ANY column will stay behind, because emptying
    // fields_data entirely is a silent wrong answer rather than an error: the
    // Go reduce skips a result with no fields_data, so this segment's rows
    // would vanish while the Arrow batch still held the whole payload.
    //
    // The invariant holds today only because the proxy appends
    // common.TimeStampField to every non-aggregation retrieve's
    // output_field_ids (proxy/dql/task_query.go) -- three layers away, in
    // another process, with nothing here depending on it visibly. Keep the
    // first column in the header instead of trusting that. Degrading by one
    // carried column is invisible in the result: Go derives the Arrow set as
    // field_order minus whatever the header retained, so a retained user
    // column is simply merged by the protobuf path as usual.
    //
    // This covers two or more columns. It cannot help a result that arrives with
    // NO columns at all, and a single carryable column would still leave the
    // Arrow set empty -- both unreachable for the same proxy reason, and neither
    // is something this function can repair.
    bool any_retained = false;
    for (const auto& fd : *fields_data) {
        if (milvus::SystemProperty::Instance().IsSystem(
                milvus::FieldId(fd.field_id())) ||
            !WorthCarryingAsArrow(fd)) {
            any_retained = true;
            break;
        }
    }

    bool first = true;
    for (auto& fd : *fields_data) {
        auto field_id = milvus::FieldId(fd.field_id());
        holder->field_order.push_back(field_id);
        const bool force_retain = !any_retained && first;
        first = false;

        if (force_retain ||
            milvus::SystemProperty::Instance().IsSystem(field_id) ||
            !WorthCarryingAsArrow(fd)) {
            retained.mutable_fields_data()->Add()->Swap(&fd);
            continue;
        }
        if (!milvus::GetFieldDataRowValidData(fd).empty()) {
            holder->valid_data_fields.push_back(field_id);
        }
        holder->ids.push_back(field_id);
        holder->columns.mutable_fields_data()->Add()->Swap(&fd);
    }
    fields_data->Clear();
    for (auto& fd : *retained.mutable_fields_data()) {
        fields_data->Add()->Swap(&fd);
    }
    return holder;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
BuildRetrieveUserColumnBatch(const std::shared_ptr<RetrieveUserColumns>& holder,
                             const milvus::query::RetrievePlan* plan,
                             int64_t total_rows) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    const auto& columns = holder->columns.fields_data();
    fields.reserve(columns.size());
    arrays.reserve(columns.size());

    for (int i = 0; i < columns.size(); ++i) {
        const auto& field_data = columns.Get(i);
        auto field_id = holder->ids[i];
        auto& field_meta = plan->schema_->operator[](field_id);
        auto name = std::string(field_meta.get_name().get());
        // holder is passed as the owner, so every column whose protobuf payload
        // already has Arrow's value-buffer layout is aliased rather than
        // copied. See FieldDataToArrow's contract: the holder is kept alive by
        // the returned arrays, and its storage must not be mutated after this.
        ARROW_ASSIGN_OR_RAISE(
            auto converted,
            milvus::segcore::FieldDataToArrow(
                name, field_data, total_rows, false, holder, &field_meta));
        auto array = converted.second;
        // Plain arrow::field, NOT MilvusField: no per-field metadata.
        //
        // MilvusField attaches milvus.field_id and milvus.data_type to every
        // column, and on this path both are dead weight. milvus.data_type is
        // never read for a retrieve result -- its only reader is the search
        // path (querynodev2/tasks/arrow_import.go) -- and milvus.field_id is
        // redundant with kRetrieveFieldOrderKey below, which already carries
        // every id in order; the Arrow columns are the non-system subset in
        // that same order, so Go matches them positionally via
        // ArrowFieldsToProtoOrdered.
        //
        // This is not a micro-optimization. Profiling attributed ~44 of the
        // ~126 extra allocations per call to that metadata, because Go decodes
        // it per field and then arrow::StructOf clones each one again. It is a
        // third of the Arrow path's fixed per-column cost, and the fixed cost
        // is what decides the break-even point against protobuf.
        //
        // MilvusField is left alone: the search export depends on it.
        fields.push_back(arrow::field(
            std::move(name), array->type(), field_meta.is_nullable()));
        arrays.push_back(std::move(array));
    }

    return MakeRetrieveBatch(std::move(fields),
                             std::move(arrays),
                             JoinFieldIds(holder->field_order),
                             JoinFieldIds(holder->valid_data_fields),
                             total_rows);
}

// QueryScan transports row-sized state in Arrow. The protobuf header retains
// only scalar statistics. Fixed-width arrays alias moved protobuf storage;
// their shared owner survives the future and the C result wrapper.
struct QueryScanColumns : milvus::segcore::ProtoStorageOwner {
    std::vector<milvus::DataArray> columns;
    std::vector<std::string> names;
};

CRetrieveArrowResult*
ExportQueryScanResult(
    std::unique_ptr<milvus::proto::segcore::RetrieveResults> results,
    const milvus::query::RetrievePlan* plan,
    bool identities) {
    auto owner = std::make_shared<QueryScanColumns>();
    const int64_t rows = results->offset_size();
    owner->columns.reserve(results->fields_data_size() + 2);
    auto add = [&](const std::string& name) -> milvus::DataArray& {
        owner->names.push_back(name);
        owner->columns.emplace_back();
        return owner->columns.back();
    };
    auto& offsets = add("$seg_offset");
    offsets.set_type(milvus::proto::schema::DataType::Int64);
    offsets.mutable_scalars()->mutable_long_data()->mutable_data()->Swap(
        results->mutable_offset());
    if (identities) {
        const auto pk = plan->schema_->get_primary_field_id();
        AssertInfo(pk.has_value(), "identity export requires a primary key");
        auto& ids = add("$id");
        const auto type = (*plan->schema_)[*pk].get_data_type();
        if (type == milvus::DataType::INT64) {
            AssertInfo(results->ids().int_id().data_size() == rows,
                       "identity PK count differs from offsets");
            ids.set_type(milvus::proto::schema::DataType::Int64);
            ids.mutable_scalars()->mutable_long_data()->Swap(
                results->mutable_ids()->mutable_int_id());
        } else {
            AssertInfo(type == milvus::DataType::VARCHAR &&
                           results->ids().str_id().data_size() == rows,
                       "invalid identity string PK layout");
            ids.set_type(milvus::proto::schema::DataType::VarChar);
            ids.mutable_scalars()->mutable_string_data()->Swap(
                results->mutable_ids()->mutable_str_id());
        }
        AssertInfo(
            results->fields_data_size() == 1 &&
                results->fields_data(0).field_id() == TimestampFieldID.get() &&
                results->fields_data(0).scalars().long_data().data_size() ==
                    rows,
            "identity timestamp column does not match offsets");
        add("$timestamp").Swap(results->mutable_fields_data(0));
    }
    results->clear_ids();
    results->clear_fields_data();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (size_t i = 0; i < owner->columns.size(); ++i) {
        auto converted = milvus::segcore::FieldDataToArrow(
            owner->names[i], owner->columns[i], rows, false, owner);
        if (!converted.ok()) {
            ThrowInfo(milvus::segcore::ArrowExportErrorCode(converted.status()),
                      "failed to build QueryScan column: {}",
                      converted.status().ToString());
        }
        auto array = converted->second;
        fields.push_back(arrow::field(owner->names[i], array->type(), false));
        arrays.push_back(std::move(array));
        // Strings are copied by FieldDataToArrow, unlike fixed-width columns.
        // Do not retain the old string payload through the offset buffer owner.
        if (owner->columns[i].type() ==
            milvus::proto::schema::DataType::VarChar) {
            milvus::DataArray released;
            released.Swap(&owner->columns[i]);
        }
    }
    auto batch = arrow::RecordBatch::Make(
        arrow::schema(std::move(fields)), rows, std::move(arrays));
    // Allocate the C structs under RAII, including allocation-failure paths.
    auto schema = std::make_unique<ArrowSchema>();
    auto array = std::make_unique<ArrowArray>();
    auto cleanup = folly::makeGuard([&] {
        MilvusGoArrowArrayRelease(array.get());
        MilvusGoArrowSchemaRelease(schema.get());
    });
    auto status = arrow::ExportRecordBatch(*batch, array.get(), schema.get());
    if (!status.ok()) {
        ThrowInfo(milvus::segcore::ArrowExportErrorCode(status),
                  "failed to export QueryScan batch: {}",
                  status.ToString());
    }
    auto out = std::make_unique<CRetrieveArrowResult>();
    out->header = CreateLeakedCRetrieveResultFromProto(std::move(results));
    out->array = array.release();
    out->schema = schema.release();
    cleanup.dismiss();
    return out.release();
}

}  // namespace

// CRetrieveArrowResult owns its header blob and both Arrow C structs through
// raw pointers, so the default `delete` in ~LeakyResult would free the struct
// and leak everything it points at -- including the RecordBatch buffers, which
// are the whole result set. Route it to the real free function.
//
// This must precede the Future<CRetrieveArrowResult> instantiation below.
namespace milvus::futures {
template <>
inline void
DestroyLeakyResult<CRetrieveArrowResult>(CRetrieveArrowResult* r) {
    DeleteRetrieveArrowResult(r);
}
}  // namespace milvus::futures

CFuture*  // Future<CRetrieveArrowResult>
AsyncRetrieveAsArrow(CTraceContext c_trace,
                     CSegmentInterface c_segment,
                     CRetrievePlan c_plan,
                     uint64_t timestamp,
                     int64_t limit_size,
                     int32_t consistency_level,
                     uint64_t collection_ttl,
                     uint64_t entity_ttl_physical_time_us) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);
    auto future = milvus::futures::Future<CRetrieveArrowResult>::async(
        milvus::futures::getSearchCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace,
         segment,
         plan,
         timestamp,
         limit_size,
         consistency_level,
         collection_ttl,
         entity_ttl_physical_time_us](folly::CancellationToken cancel_token) {
            auto trace_ctx = milvus::tracer::TraceContext{
                c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
            milvus::tracer::AutoSpan span(
                "SegCoreRetrieveAsArrow", &trace_ctx, true);

            milvus::OpContext op_ctx(cancel_token);
            op_ctx.runtime_load_priority =
                milvus::proto::common::LoadPriority::HIGH;
            segment->LazyCheckSchema(plan->schema_, &op_ctx);
            auto read_lease = AcquireSegmentReadLease(segment, cancel_token);
            ValidateSegmentSchemaCompatibility(
                segment, plan->schema_, plan->access_entries_);
            auto internal_segment =
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment);
            CheckExternalFieldsInLoadedManifest(
                plan->schema_, internal_segment, plan->access_entries_);

            // Ordinary Arrow retrieval needs user columns. Candidate mode
            // ignores this flag and returns only offsets and scalar stats.
            auto results =
                internal_segment->Retrieve(&trace_ctx,
                                           plan,
                                           timestamp,
                                           limit_size,
                                           /*ignore_non_pk=*/false,
                                           cancel_token,
                                           consistency_level,
                                           collection_ttl,
                                           entity_ttl_physical_time_us);

            if (plan->mode_ == milvus::query::RetrieveMode::Candidates) {
                return ExportQueryScanResult(std::move(results), plan, false);
            }

            // The routing precondition -- no aggregation, no
            // ignore_non_pk -- is enforced in Go by
            // shouldUseArrowTransport, with a backstop duplicate-field-id
            // check in MaterializeArrowSelection.
            //
            // It deliberately is NOT re-asserted here: Go decides the
            // routing, so a C++ copy of the same predicate could only go
            // stale against it. The backstop that matters is on the
            // consuming side, where a violation would actually corrupt
            // output, and that is where it lives.
            //
            // An AssertInfo here would also be the wrong tool: a routing
            // mistake is a caller contract violation, not data corruption,
            // and throwing on it would turn a Go-side bug into a QueryNode
            // crash.

            // Derive the row count from the columns, not from offset. See
            // RowCountOfDataArray: offset is empty on the aggregation branch
            // while the columns hold rows, and trusting it there drops every
            // row silently. Fall back to offset_size() only when there are no
            // columns to measure.
            int64_t total_rows = results->offset_size();
            bool measured = false;
            for (const auto& fd : results->fields_data()) {
                auto n = RowCountOfDataArray(fd);
                if (n < 0) {
                    continue;  // unknown shape: leave the offset-derived value
                }
                // Adopt the first measurement only when the offset-derived
                // seed told us nothing (no offsets). After that every column
                // must agree.
                //
                // `measured` is tracked separately rather than testing
                // total_rows != 0 because overloading 0 to mean both "not yet
                // measured" and "zero rows" made this one-sided:
                // {col A = 0, col B = 5} was accepted while
                // {col A = 5, col B = 0} threw. A column whose length
                // disagrees with the batch's num_rows is precisely what lets
                // materializeColumn index past that column's value buffer into
                // aliased protobuf memory, so the check has to be
                // order-independent.
                if (!measured && total_rows == 0) {
                    total_rows = n;
                    measured = true;
                    continue;
                }
                AssertInfo(n == total_rows,
                           "retrieve column {} has {} rows but the result "
                           "claims {}; the Arrow export cannot reconcile them",
                           fd.field_id(),
                           n,
                           total_rows);
                measured = true;
            }
            auto out = std::make_unique<CRetrieveArrowResult>();
            out->header = nullptr;
            out->schema = new ArrowSchema{};
            out->array = new ArrowArray{};
            auto cleanup = folly::makeGuard([&] {
                MilvusGoArrowArrayRelease(out->array);
                MilvusGoArrowSchemaRelease(out->schema);
                delete out->array;
                delete out->schema;
            });

            // Partition first, then build Arrow from the moved-out columns.
            //
            // The order is forced by the aliasing: the Arrow arrays point into
            // the user columns' protobuf buffers, so those buffers must already
            // be owned by something that outlives the export. Partitioning
            // afterwards would free them while Arrow still referenced them.
            auto user_columns = PartitionRetrieveResult(results.get());
            auto batch_result =
                BuildRetrieveUserColumnBatch(user_columns, plan, total_rows);
            // Classify at the throw site. The copy paths -- every nullable
            // column, BOOL, VARCHAR, JSON, geometry -- allocate through Arrow
            // builders that RETURN Status::OutOfMemory rather than throwing, so
            // this is the normal out-of-memory route here, and
            // MemAllocateFailed is retriable where UnexpectedError is not.
            // Status text goes through a {} placeholder, never concatenated
            // into the format string: ThrowInfo expands to
            // fmt::format(fmt::runtime(info), ...), so a '{' anywhere in
            // third-party text would raise fmt::format_error instead -- which
            // is not a SegcoreError, so Future.h's std::exception arm would
            // flatten it to UnexpectedError and destroy the retriable
            // MemAllocateFailed this call exists to carry.
            if (!batch_result.ok()) {
                ThrowInfo(milvus::segcore::ArrowExportErrorCode(
                              batch_result.status()),
                          "failed to build Arrow batch for retrieve result: {}",
                          batch_result.status().ToString());
            }
            auto export_status = arrow::ExportRecordBatch(
                **batch_result, out->array, out->schema);
            if (!export_status.ok()) {
                ThrowInfo(milvus::segcore::ArrowExportErrorCode(export_status),
                          "failed to export retrieve RecordBatch: {}",
                          export_status.ToString());
            }

            // The user columns are already out of the header: dropping them
            // is now PartitionRetrieveResult's job, above, because the aliasing
            // needs it to happen before the export rather than after.
            out->header =
                CreateLeakedCRetrieveResultFromProto(std::move(results));
            cleanup.dismiss();
            read_lease.reset();
            return out.release();
        });
    return static_cast<CFuture*>(static_cast<void*>(
        static_cast<milvus::futures::IFuture*>(future.release())));
}

void
DeleteRetrieveArrowResult(CRetrieveArrowResult* result) {
    if (result == nullptr) {
        return;
    }
    // Null-safe: importing the Arrow structs on the Go side nulls their
    // release callbacks, so these become no-ops. Releasing unconditionally
    // matters for the paths where Go errors out before importing them.
    MilvusGoArrowArrayRelease(result->array);
    MilvusGoArrowSchemaRelease(result->schema);
    delete result->array;
    delete result->schema;
    // DeleteRetrieveResult is NOT null-safe: it dereferences proto_blob.
    if (result->header != nullptr) {
        DeleteRetrieveResult(result->header);
    }
    delete result;
}

template <bool identities_only>
static CFuture*
AsyncRetrieveByOffsetsImpl(CTraceContext c_trace,
                           CSegmentInterface c_segment,
                           CRetrievePlan c_plan,
                           int64_t* offsets,
                           int64_t len,
                           bool identity_take_allowed) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);

    using Result = std::
        conditional_t<identities_only, CRetrieveArrowResult, CRetrieveResult>;
    auto future = milvus::futures::Future<Result>::async(
        milvus::futures::getSearchCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace, segment, plan, offsets, len, identity_take_allowed](
            folly::CancellationToken cancel_token) {
            auto trace_ctx = milvus::tracer::TraceContext{
                c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
            milvus::tracer::AutoSpan span(
                "SegCoreRetrieveByOffsets", &trace_ctx, true);

            milvus::OpContext op_ctx(cancel_token);
            op_ctx.runtime_load_priority =
                milvus::proto::common::LoadPriority::HIGH;
            segment->LazyCheckSchema(plan->schema_, &op_ctx);
            auto read_lease = AcquireSegmentReadLease(segment, cancel_token);
            ValidateSegmentSchemaCompatibility(
                segment, plan->schema_, plan->access_entries_);
            auto internal_segment =
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment);
            CheckExternalFieldsInLoadedManifest(
                plan->schema_, internal_segment, plan->access_entries_);

            auto retrieve_result =
                identities_only
                    ? internal_segment->RetrieveIdentitiesByOffsets(
                          &trace_ctx,
                          plan,
                          offsets,
                          len,
                          identity_take_allowed,
                          cancel_token)
                    : segment->Retrieve(
                          &trace_ctx, plan, offsets, len, cancel_token);

            if constexpr (identities_only) {
                return ExportQueryScanResult(
                    std::move(retrieve_result), plan, true);
            } else {
                return CreateLeakedCRetrieveResultFromProto(
                    std::move(retrieve_result));
            }
        });
    return static_cast<CFuture*>(static_cast<void*>(
        static_cast<milvus::futures::IFuture*>(future.release())));
}

CFuture*
AsyncRetrieveByOffsets(CTraceContext trace,
                       CSegmentInterface segment,
                       CRetrievePlan plan,
                       int64_t* offsets,
                       int64_t len) {
    return AsyncRetrieveByOffsetsImpl<false>(
        trace, segment, plan, offsets, len, false);
}

CFuture*
AsyncRetrieveIdentitiesByOffsets(CTraceContext trace,
                                 CSegmentInterface segment,
                                 CRetrievePlan plan,
                                 int64_t* offsets,
                                 int64_t len,
                                 bool take_for_output_allowed) {
    return AsyncRetrieveByOffsetsImpl<true>(
        trace, segment, plan, offsets, len, take_for_output_allowed);
}

int64_t
GetMemoryUsageInBytes(CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto mem_size = segment->GetMemoryUsageInBytes();
    return mem_size;
}

int64_t
GetRowCount(CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto row_count = segment->get_row_count();
    return row_count;
}

// TODO: segmentInterface implement get_deleted_count()
int64_t
GetDeletedCount(CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    auto segment =
        reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto deleted_count = segment->get_deleted_count();
    return deleted_count;
}

int64_t
GetRealCount(CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    // not accurate, pk may exist in deleted record and not in insert record.
    // return GetRowCount(c_segment) - GetDeletedCount(c_segment);
    auto segment =
        reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
    return segment->get_real_count();
}

bool
HasRawData(CSegmentInterface c_segment, int64_t field_id) {
    SCOPE_CGO_CALL_METRIC();

    auto segment =
        reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
    try {
        return segment->HasRawData(field_id);
    } catch (const std::exception& e) {
        // Returning through a plain bool leaves no channel for an error, and
        // the impls AssertInfo-throw on a field the published schema has not
        // caught up with yet (normal schema-evolution timing, no malformed
        // data needed) as well as on not-ready indexes. "No raw data" is the
        // safe answer -- the caller just loads the field data -- and it must
        // not terminate the process.
        LOG_WARN("HasRawData({}) failed, reporting no raw data: {}",
                 field_id,
                 e.what());
        return false;
    } catch (...) {
        LOG_WARN(
            "HasRawData({}) failed with an unknown exception, reporting "
            "no raw data",
            field_id);
        return false;
    }
}

bool
HasFieldData(CSegmentInterface c_segment, int64_t field_id) {
    SCOPE_CGO_CALL_METRIC();

    auto segment =
        reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
    return segment->HasFieldData(milvus::FieldId(field_id));
}

//////////////////////////////    interfaces for growing segment    //////////////////////////////
CStatus
Insert(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps,
       const uint8_t* data_info,
       const uint64_t data_info_len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(data_info_len < std::numeric_limits<int>::max(),
                   "insert data length ({}) exceeds max int",
                   data_info_len);
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        auto insert_record_proto =
            std::make_shared<milvus::InsertRecordProto>();
        auto suc =
            insert_record_proto->ParseFromArray(data_info, data_info_len);
        AssertInfo(suc, "failed to parse insert data from records");

        segment->Insert(reserved_offset,
                        size,
                        row_ids,
                        timestamps,
                        std::move(insert_record_proto));
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        *offset = segment->PreInsert(size);
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
Delete(CSegmentInterface c_segment,
       int64_t size,
       const uint8_t* ids,
       const uint64_t ids_size,
       const uint64_t* timestamps) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    try {
        // Parse inside the try: a malformed IDs blob makes AssertInfo throw a
        // SegcoreError, and it would leave this CStatus-returning function as a
        // live exception across the C ABI (Insert and LoadDeletedRecord already
        // parse inside theirs).
        auto pks = std::make_unique<milvus::proto::schema::IDs>();
        auto suc = pks->ParseFromArray(ids, ids_size);
        AssertInfo(suc, "failed to parse pks from ids");
        auto res = segment->Delete(size, pks.get(), timestamps);
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

//////////////////////////////    interfaces for sealed segment    //////////////////////////////
CStatus
LoadFieldData(CSegmentInterface c_segment,
              CLoadFieldDataInfo c_load_field_data_info) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_info = (LoadFieldDataInfo*)c_load_field_data_info;
        segment->LoadFieldData(*load_info);
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
LoadDeletedRecord(CSegmentInterface c_segment,
                  CLoadDeletedRecordInfo deleted_record_info) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        AssertInfo(segment_interface != nullptr, "segment conversion failed");
        auto pks = std::make_unique<milvus::proto::schema::IDs>();
        auto suc = pks->ParseFromArray(deleted_record_info.primary_keys,
                                       deleted_record_info.primary_keys_size);
        AssertInfo(suc, "unmarshal field data string failed");
        auto load_info = LoadDeletedRecordInfo{deleted_record_info.timestamps,
                                               pks.get(),
                                               deleted_record_info.row_count};
        segment_interface->LoadDeletedRecord(load_info);
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropFieldData(milvus::FieldId(field_id));
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

void
RemoveFieldFile(CSegmentInterface c_segment, int64_t field_id) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = reinterpret_cast<milvus::segcore::SegmentSealed*>(c_segment);
    segment->RemoveFieldFile(milvus::FieldId(field_id));
}

CStatus
ExprResCacheEraseSegment(int64_t segment_id) {
    SCOPE_CGO_CALL_METRIC();

    try {
        milvus::exec::ExprResCacheManager::Instance().EraseSegment(segment_id);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::UnexpectedError, e.what());
    } catch (...) {
        return milvus::FailureCStatus(milvus::UnexpectedError,
                                      "unknown exception");
    }
}

CStatus
SegmentSetCommitTimestamp(CSegmentInterface c_segment, uint64_t commit_ts) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    segment->SetCommitTimestamp(commit_ts);
    return milvus::SuccessCStatus();
}
