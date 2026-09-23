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
#include "segcore/default_fs.h"

#include <folly/CancellationToken.h>
#include <folly/ExceptionWrapper.h>
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
#include "segcore/TextLobSpillover.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "storage/FileManager.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/loon_ffi/property_singleton.h"

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
    std::unique_ptr<milvus::segcore::SegmentInterface> segment;
    switch (seg_type) {
        case Growing: {
            auto seg = milvus::segcore::CreateGrowingSegment(
                col->get_schema(),
                col->get_index_meta(),
                segment_id,
                milvus::segcore::SegcoreConfig::default_config());
            segment = std::move(seg);
            break;
        }
        case Sealed:
        case Indexing:
            segment = milvus::segcore::CreateSealedSegment(
                col->get_schema(),
                col->get_index_meta(),
                segment_id,
                milvus::segcore::SegcoreConfig::default_config(),
                is_sorted_by_pk);
            break;

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
ValidateSegmentSchemaCompatibility(milvus::segcore::SegmentInterface* segment,
                                   const milvus::SchemaPtr& plan_schema) {
    if (segment->type() == SegmentType::Growing) {
        return;
    }

    auto sealed =
        dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(segment);
    AssertInfo(sealed != nullptr,
               "sealed segment {} does not support schema validation",
               segment->get_segment_id());
    sealed->ValidateSchemaCompatibility(plan_schema);
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
            segment->LazyCheckSchema(plan->schema_, &op_ctx);
            auto read_lease = AcquireSegmentReadLease(segment, cancel_token);
            ValidateSegmentSchemaCompatibility(segment, plan->schema_);
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
            segment->LazyCheckSchema(plan->schema_, &op_ctx);
            auto read_lease = AcquireSegmentReadLease(segment, cancel_token);
            ValidateSegmentSchemaCompatibility(segment, plan->schema_);
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

CFuture*  // Future<CRetrieveResult>
AsyncRetrieveByOffsets(CTraceContext c_trace,
                       CSegmentInterface c_segment,
                       CRetrievePlan c_plan,
                       int64_t* offsets,
                       int64_t len) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);

    auto future = milvus::futures::Future<CRetrieveResult>::async(
        milvus::futures::getSearchCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace, segment, plan, offsets, len](
            folly::CancellationToken cancel_token) {
            auto trace_ctx = milvus::tracer::TraceContext{
                c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
            milvus::tracer::AutoSpan span(
                "SegCoreRetrieveByOffsets", &trace_ctx, true);

            milvus::OpContext op_ctx(cancel_token);
            segment->LazyCheckSchema(plan->schema_, &op_ctx);
            auto read_lease = AcquireSegmentReadLease(segment, cancel_token);
            ValidateSegmentSchemaCompatibility(segment, plan->schema_);
            auto internal_segment =
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment);
            CheckExternalFieldsInLoadedManifest(
                plan->schema_, internal_segment, plan->access_entries_);

            auto retrieve_result =
                segment->Retrieve(&trace_ctx, plan, offsets, len, cancel_token);

            auto c_result = CreateLeakedCRetrieveResultFromProto(
                std::move(retrieve_result));
            read_lease.reset();
            return c_result;
        });
    return static_cast<CFuture*>(static_cast<void*>(
        static_cast<milvus::futures::IFuture*>(future.release())));
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
            std::make_unique<milvus::InsertRecordProto>();
        auto suc =
            insert_record_proto->ParseFromArray(data_info, data_info_len);
        AssertInfo(suc, "failed to parse insert data from records");

        segment->Insert(reserved_offset,
                        size,
                        row_ids,
                        timestamps,
                        insert_record_proto.get());
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
