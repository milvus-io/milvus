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
#include <cstring>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/LoadInfo.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/ScopedTimer.h"
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
#include "index/json_stats/JsonKeyStats.h"
#include "log/Log.h"
#include "milvus-storage/filesystem/fs.h"
#include "monitor/Monitor.h"
#include "monitor/scope_metric.h"
#include "nlohmann/json.hpp"
#include "opentelemetry/trace/span.h"
#include "pb/index_cgo_msg.pb.h"
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
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/loon_ffi/property_singleton.h"

// milvus-storage headers for FlushGrowingSegmentData
#include "milvus-storage/segment/segment_writer.h"
#include "milvus-storage/transaction/transaction.h"
#include "milvus-storage/common/layout.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/properties.h"

// Arrow headers for FlushGrowingSegmentData
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

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
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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
        std::string error_msg = e.what();
        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getLoadCPUExecutor(),
            milvus::futures::ExecutePriority::NORMAL,
            [error_msg = std::move(error_msg)](
                folly::CancellationToken cancel_token) -> bool* {
                (void)cancel_token;
                ThrowInfo(milvus::UnexpectedError,
                          "AsyncReopenSegment preflight failed: {}",
                          error_msg);
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
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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
            auto internal_segment =
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment);
            std::unique_ptr<milvus::SearchResult> search_result;
            if (!filter_only &&
                !internal_segment->FieldAccessible(target_vector_field_id)) {
                search_result = std::make_unique<milvus::SearchResult>();
                search_result->total_nq_ = num_queries;
                search_result->unity_topK_ = 0;
                search_result->total_data_cnt_ = 0;
            } else {
                search_result = segment->Search(plan,
                                                phg_ptr,
                                                timestamp,
                                                cancel_token,
                                                consistency_level,
                                                collection_ttl,
                                                entity_ttl_physical_time_us,
                                                filter_only,
                                                enable_expr_cache);
            }
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
    auto buffer = new uint8_t[size];
    try {
        retrieve_result->SerializePartialToArray(buffer, size);
    } catch (std::exception& e) {
        delete[] buffer;
        throw;
    }

    auto result = new CRetrieveResult();
    result->proto_blob = buffer;
    result->proto_size = size;
    return result;
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

            return CreateLeakedCRetrieveResultFromProto(
                std::move(retrieve_result));
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

            auto retrieve_result =
                segment->Retrieve(&trace_ctx, plan, offsets, len, cancel_token);

            return CreateLeakedCRetrieveResultFromProto(
                std::move(retrieve_result));
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
    return segment->HasRawData(field_id);
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
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment = static_cast<milvus::segcore::SegmentGrowing*>(c_segment);
        *offset = segment->PreInsert(size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
Delete(CSegmentInterface c_segment,
       int64_t size,
       const uint8_t* ids,
       const uint64_t ids_size,
       const uint64_t* timestamps) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto pks = std::make_unique<milvus::proto::schema::IDs>();
    auto suc = pks->ParseFromArray(ids, ids_size);
    AssertInfo(suc, "failed to parse pks from ids");
    try {
        auto res = segment->Delete(size, pks.get(), timestamps);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment,
                         CLoadIndexInfo c_load_index_info) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_index_info =
            static_cast<milvus::segcore::LoadIndexInfo*>(c_load_index_info);
        segment->LoadIndex(*load_index_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
LoadJsonKeyIndex(CTraceContext c_trace,
                 CSegmentInterface c_segment,
                 const uint8_t* serialized_load_json_key_index_info,
                 const uint64_t len,
                 CLoadCancellationSource source) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");

        // Check for cancellation before starting
        if (source) {
            auto cancellation_source =
                static_cast<folly::CancellationSource*>(source);
            if (cancellation_source->getToken().isCancellationRequested()) {
                throw milvus::SegcoreError(
                    milvus::ErrorCode::FollyCancel,
                    fmt::format("Load cancelled for segment {} json stats",
                                segment->get_segment_id()));
            }
        }

        auto info_proto =
            std::make_unique<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>();
        info_proto->ParseFromArray(serialized_load_json_key_index_info, len);
        if (!milvus::JSON_KEY_STATS_ENABLED.load()) {
            LOG_WARN(
                "skip load json stats because json key stats is disabled, "
                "segment:{}, field:{}, build:{}, version:{}",
                segment->get_segment_id(),
                info_proto->fieldid(),
                info_proto->buildid(),
                info_proto->version());
            return milvus::SuccessCStatus();
        }

        milvus::storage::FieldDataMeta field_meta{info_proto->collectionid(),
                                                  info_proto->partitionid(),
                                                  segment->get_segment_id(),
                                                  info_proto->fieldid(),
                                                  info_proto->schema()};
        milvus::storage::IndexMeta index_meta{segment->get_segment_id(),
                                              info_proto->fieldid(),
                                              info_proto->buildid(),
                                              info_proto->version()};
        auto remote_chunk_manager =
            milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        AssertInfo(fs != nullptr, "arrow file system is null");

        milvus::Config config;
        std::vector<std::string> files;
        files.reserve(info_proto->files_size());
        for (const auto& f : info_proto->files()) {
            files.push_back(f);
        }
        config[milvus::index::INDEX_FILES] = files;
        config[milvus::LOAD_PRIORITY] = info_proto->load_priority();
        config[milvus::index::ENABLE_MMAP] = info_proto->enable_mmap();
        if (info_proto->enable_mmap()) {
            config[milvus::index::MMAP_FILE_PATH] = info_proto->mmap_dir_path();
        }
        if (info_proto->warmup_policy() != "") {
            config[milvus::index::WARMUP] = info_proto->warmup_policy();
        }
        config[milvus::index::INDEX_SIZE] = info_proto->stats_size();
        if (!info_proto->base_path().empty()) {
            config[STATS_BASE_PATH_KEY] = info_proto->base_path();
        }

        milvus::storage::FileManagerContext file_ctx(
            field_meta, index_meta, remote_chunk_manager, fs);

        auto index =
            std::make_shared<milvus::index::JsonKeyStats>(file_ctx, true);
        {
            milvus::ScopedTimer timer(
                "json_stats_load",
                [](double us) {
                    milvus::monitor::internal_json_stats_latency_load.Observe(
                        us / 1000.0);
                },
                milvus::ScopedTimer::LogLevel::Info);
            index->Load(ctx, config);
        }

        segment->LoadJsonStats(milvus::FieldId(info_proto->fieldid()),
                               std::move(index));

        LOG_INFO("load json stats success for field:{} of segment:{}",
                 info_proto->fieldid(),
                 segment->get_segment_id());

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
UpdateFieldRawDataSize(CSegmentInterface c_segment,
                       int64_t field_id,
                       int64_t num_rows,
                       int64_t field_data_size) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        AssertInfo(segment_interface != nullptr, "segment conversion failed");
        segment_interface->set_field_avg_size(
            milvus::FieldId(field_id), num_rows, field_data_size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropIndex(milvus::FieldId(field_id));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
DropSealedSegmentJSONIndex(CSegmentInterface c_segment,
                           int64_t field_id,
                           const char* nested_path) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        segment->DropJSONIndex(milvus::FieldId(field_id), nested_path);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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
    }
}

//////////////////////////////    interfaces for growing segment flush    //////////////////////////////

namespace {

// struct to hold field info for building Arrow arrays
struct FieldInfo {
    milvus::FieldId field_id;
    std::string field_name;
    milvus::DataType data_type;
    milvus::DataType element_type;
    bool nullable;
    int64_t dim;  // for vector types
    const milvus::segcore::VectorBase* vec_base;
    milvus::segcore::ThreadSafeValidDataPtr valid_data;
    // For TEXT fields with spillover: reader for temp LOB file
    milvus::segcore::TextLobSpillover* text_lob_spillover = nullptr;
};

struct BM25StatsAccumulator {
    std::unordered_map<uint32_t, int32_t> rows_with_token;
    int64_t num_row = 0;
    int64_t num_token = 0;

    void
    Merge(const BM25StatsAccumulator& other) {
        for (const auto& [token, count] : other.rows_with_token) {
            rows_with_token[token] += count;
        }
        num_row += other.num_row;
        num_token += other.num_token;
    }
};

void
AppendSparseRowToBM25Stats(
    const knowhere::sparse::SparseRow<milvus::SparseValueType>* row,
    BM25StatsAccumulator& stats) {
    stats.num_row++;
    if (row == nullptr) {
        return;
    }
    auto byte_size = row->data_byte_size();
    if (byte_size == 0) {
        return;
    }
    auto data = static_cast<const uint8_t*>(row->data());
    auto element_count =
        byte_size /
        knowhere::sparse::SparseRow<milvus::SparseValueType>::element_size();
    for (size_t i = 0; i < element_count; i++) {
        uint32_t token = 0;
        float value = 0;
        std::memcpy(&token, data + i * 8, sizeof(token));
        std::memcpy(&value, data + i * 8 + 4, sizeof(value));
        stats.rows_with_token[token] += 1;
        stats.num_token += static_cast<int64_t>(value);
    }
}

std::vector<uint8_t>
SerializeBM25Stats(const BM25StatsAccumulator& stats) {
    constexpr int32_t version = 0;
    std::vector<uint8_t> out;
    out.reserve(
        sizeof(version) + sizeof(stats.num_row) + sizeof(stats.num_token) +
        stats.rows_with_token.size() * (sizeof(uint32_t) + sizeof(int32_t)));
    auto append = [&out](const void* ptr, size_t size) {
        auto bytes = static_cast<const uint8_t*>(ptr);
        out.insert(out.end(), bytes, bytes + size);
    };
    append(&version, sizeof(version));
    append(&stats.num_row, sizeof(stats.num_row));
    append(&stats.num_token, sizeof(stats.num_token));
    for (const auto& [token, row_count] : stats.rows_with_token) {
        append(&token, sizeof(token));
        append(&row_count, sizeof(row_count));
    }
    return out;
}

arrow::Result<BM25StatsAccumulator>
DeserializeBM25Stats(const uint8_t* data, int64_t size) {
    if (size < 20 || (size - 20) % 8 != 0) {
        return arrow::Status::Invalid("invalid BM25 stats blob size");
    }
    BM25StatsAccumulator stats;
    int32_t version = 0;
    std::memcpy(&version, data, sizeof(version));
    if (version != 0) {
        return arrow::Status::Invalid("unsupported BM25 stats version");
    }
    std::memcpy(&stats.num_row, data + 4, sizeof(stats.num_row));
    std::memcpy(&stats.num_token, data + 12, sizeof(stats.num_token));
    auto entries = (size - 20) / 8;
    for (int64_t i = 0; i < entries; i++) {
        uint32_t token = 0;
        int32_t count = 0;
        std::memcpy(&token, data + 20 + i * 8, sizeof(token));
        std::memcpy(&count, data + 20 + i * 8 + 4, sizeof(count));
        stats.rows_with_token[token] += count;
    }
    return stats;
}

arrow::Status
WriteRawFile(const milvus_storage::ArrowFileSystemPtr& fs,
             const std::string& path,
             const std::vector<uint8_t>& data) {
    auto slash = path.find_last_of('/');
    if (slash != std::string::npos && slash > 0) {
        ARROW_RETURN_NOT_OK(fs->CreateDir(path.substr(0, slash), true));
    }
    ARROW_ASSIGN_OR_RAISE(auto output, fs->OpenOutputStream(path));
    ARROW_RETURN_NOT_OK(output->Write(data.data(), data.size()));
    return output->Close();
}

arrow::Result<BM25StatsAccumulator>
ReadBM25StatsFile(const milvus_storage::ArrowFileSystemPtr& fs,
                  const std::string& path) {
    ARROW_ASSIGN_OR_RAISE(auto input, fs->OpenInputFile(path));
    ARROW_ASSIGN_OR_RAISE(auto size, input->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto buffer, input->Read(size));
    return DeserializeBM25Stats(buffer->data(), buffer->size());
}

bool
IsCompoundStatsPath(const std::string& path) {
    auto slash = path.find_last_of('/');
    auto basename = slash == std::string::npos ? path : path.substr(slash + 1);
    return basename == "1";
}

arrow::Status
CollectBM25StatsForChunk(const FieldInfo& field_info,
                         int64_t start_offset,
                         int64_t num_rows,
                         BM25StatsAccumulator& stats) {
    for (int64_t i = 0; i < num_rows; i++) {
        auto logical_offset = start_offset + i;
        if (field_info.valid_data &&
            !field_info.valid_data->is_valid(logical_offset)) {
            stats.num_row++;
            continue;
        }

        auto physical_offset =
            field_info.vec_base->get_physical_offset(logical_offset);
        if (physical_offset < 0) {
            return arrow::Status::Invalid(
                "valid nullable sparse vector row missing physical data");
        }

        auto size_per_chunk = field_info.vec_base->get_size_per_chunk();
        auto chunk_id = physical_offset / size_per_chunk;
        auto offset_in_chunk = physical_offset % size_per_chunk;
        auto chunk_data = field_info.vec_base->get_chunk_data(chunk_id);
        auto rows = static_cast<
            const knowhere::sparse::SparseRow<milvus::SparseValueType>*>(
            chunk_data);
        AppendSparseRowToBM25Stats(rows + offset_in_chunk, stats);
    }
    return arrow::Status::OK();
}

// get element byte width for a data type
int64_t
GetElementByteWidth(milvus::DataType data_type, int64_t dim) {
    switch (data_type) {
        case milvus::DataType::BOOL:
        case milvus::DataType::INT8:
            return 1;
        case milvus::DataType::INT16:
            return 2;
        case milvus::DataType::INT32:
        case milvus::DataType::FLOAT:
            return 4;
        case milvus::DataType::INT64:
        case milvus::DataType::TIMESTAMPTZ:
        case milvus::DataType::DOUBLE:
            return 8;
        case milvus::DataType::VECTOR_FLOAT:
            return dim * sizeof(float);
        case milvus::DataType::VECTOR_BINARY:
            return dim / 8;
        case milvus::DataType::VECTOR_FLOAT16:
            return dim * sizeof(milvus::float16);
        case milvus::DataType::VECTOR_BFLOAT16:
            return dim * sizeof(milvus::bfloat16);
        case milvus::DataType::VECTOR_INT8:
            return dim * sizeof(milvus::int8);
        default:
            return 0;  // variable length
    }
}

bool
IsSupportedNullableVectorDataType(milvus::DataType data_type) {
    switch (data_type) {
        case milvus::DataType::VECTOR_FLOAT:
        case milvus::DataType::VECTOR_BINARY:
        case milvus::DataType::VECTOR_FLOAT16:
        case milvus::DataType::VECTOR_BFLOAT16:
        case milvus::DataType::VECTOR_INT8:
        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            return true;
        default:
            return false;
    }
}

bool
IsFixedWidthVectorDataType(milvus::DataType data_type) {
    return data_type == milvus::DataType::VECTOR_FLOAT ||
           data_type == milvus::DataType::VECTOR_BINARY ||
           data_type == milvus::DataType::VECTOR_FLOAT16 ||
           data_type == milvus::DataType::VECTOR_BFLOAT16 ||
           data_type == milvus::DataType::VECTOR_INT8;
}

arrow::Result<int64_t>
GetFixedWidthVectorValueAlignment(milvus::DataType data_type) {
    switch (data_type) {
        case milvus::DataType::VECTOR_FLOAT:
            return alignof(float);
        case milvus::DataType::VECTOR_BINARY:
            return alignof(uint8_t);
        case milvus::DataType::VECTOR_FLOAT16:
            return alignof(milvus::float16);
        case milvus::DataType::VECTOR_BFLOAT16:
            return alignof(milvus::bfloat16);
        case milvus::DataType::VECTOR_INT8:
            return alignof(milvus::int8);
        default:
            return arrow::Status::Invalid(fmt::format(
                "unsupported fixed-width vector data type {}", data_type));
    }
}

bool
IsBufferAligned(const void* data, int64_t alignment) {
    if (data == nullptr || alignment <= 1) {
        return true;
    }
    return reinterpret_cast<std::uintptr_t>(data) %
               static_cast<std::uintptr_t>(alignment) ==
           0;
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
WrapOrCopyArrowBuffer(const void* data, int64_t size, int64_t alignment) {
    if (size < 0) {
        return arrow::Status::Invalid("negative Arrow buffer size");
    }
    if (data == nullptr && size > 0) {
        return arrow::Status::Invalid("null Arrow buffer data");
    }
    auto raw_data = static_cast<const uint8_t*>(data);
    if (IsBufferAligned(data, alignment)) {
        return arrow::Buffer::Wrap(raw_data, size);
    }

    ARROW_ASSIGN_OR_RAISE(auto copied_buffer, arrow::AllocateBuffer(size));
    if (size > 0) {
        std::memcpy(copied_buffer->mutable_data(), raw_data, size);
    }
    return std::shared_ptr<arrow::Buffer>(std::move(copied_buffer));
}

const uint8_t*
GetPhysicalVectorValue(const milvus::segcore::VectorBase* vec_base,
                       int64_t physical_offset,
                       int64_t byte_width) {
    if (physical_offset < 0) {
        return nullptr;
    }

    auto size_per_chunk = vec_base->get_size_per_chunk();
    auto chunk_id = physical_offset / size_per_chunk;
    auto offset_in_chunk = physical_offset % size_per_chunk;
    auto chunk_data = vec_base->get_chunk_data(chunk_id);
    return static_cast<const uint8_t*>(chunk_data) +
           offset_in_chunk * byte_width;
}

arrow::Result<std::shared_ptr<arrow::Array>>
BuildNullableFixedWidthVectorArray(const FieldInfo& field_info,
                                   int64_t start_offset,
                                   int64_t num_rows,
                                   int64_t byte_width,
                                   int64_t data_alignment) {
    if (!field_info.valid_data) {
        return arrow::Status::Invalid(
            "nullable vector field missing ValidData");
    }

    bool all_valid = true;
    for (int64_t i = 0; i < num_rows; i++) {
        if (!field_info.valid_data->is_valid(start_offset + i)) {
            all_valid = false;
            break;
        }
    }
    if (all_valid && num_rows > 0) {
        auto physical_offset =
            field_info.vec_base->get_physical_offset(start_offset);
        auto size_per_chunk = field_info.vec_base->get_size_per_chunk();
        if (physical_offset >= 0 &&
            physical_offset / size_per_chunk ==
                (physical_offset + num_rows - 1) / size_per_chunk &&
            num_rows * byte_width <= std::numeric_limits<int32_t>::max()) {
            auto value = GetPhysicalVectorValue(
                field_info.vec_base, physical_offset, byte_width);
            if (value == nullptr) {
                return arrow::Status::Invalid(
                    "valid nullable vector row missing physical data");
            }

            ARROW_ASSIGN_OR_RAISE(
                auto offsets_buffer,
                arrow::AllocateBuffer((num_rows + 1) * sizeof(int32_t)));
            auto offsets =
                reinterpret_cast<int32_t*>(offsets_buffer->mutable_data());
            for (int64_t i = 0; i <= num_rows; i++) {
                offsets[i] = static_cast<int32_t>(i * byte_width);
            }
            std::shared_ptr<arrow::Buffer> offsets_buffer_shared(
                std::move(offsets_buffer));
            ARROW_ASSIGN_OR_RAISE(
                auto data_buffer,
                WrapOrCopyArrowBuffer(
                    value, num_rows * byte_width, data_alignment));
            return std::make_shared<arrow::BinaryArray>(
                num_rows, offsets_buffer_shared, data_buffer, nullptr, 0);
        }
    }

    arrow::BinaryBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));

    for (int64_t i = 0; i < num_rows; i++) {
        auto logical_offset = start_offset + i;
        if (!field_info.valid_data->is_valid(logical_offset)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }

        auto physical_offset =
            field_info.vec_base->get_physical_offset(logical_offset);
        auto value = GetPhysicalVectorValue(
            field_info.vec_base, physical_offset, byte_width);
        if (value == nullptr) {
            return arrow::Status::Invalid(
                "valid nullable vector row missing physical data");
        }
        ARROW_RETURN_NOT_OK(builder.Append(value, byte_width));
    }

    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
BuildSparseFloatVectorArrayForChunk(const FieldInfo& field_info,
                                    int64_t start_offset,
                                    int64_t num_rows) {
    arrow::BinaryBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));

    for (int64_t i = 0; i < num_rows; i++) {
        auto logical_offset = start_offset + i;
        if (field_info.valid_data &&
            !field_info.valid_data->is_valid(logical_offset)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }

        auto physical_offset =
            field_info.vec_base->get_physical_offset(logical_offset);
        if (physical_offset < 0) {
            return arrow::Status::Invalid(
                "valid nullable sparse vector row missing physical data");
        }

        auto size_per_chunk = field_info.vec_base->get_size_per_chunk();
        auto chunk_id = physical_offset / size_per_chunk;
        auto offset_in_chunk = physical_offset % size_per_chunk;
        auto chunk_data = field_info.vec_base->get_chunk_data(chunk_id);
        auto rows = static_cast<
            const knowhere::sparse::SparseRow<milvus::SparseValueType>*>(
            chunk_data);
        auto row = rows + offset_in_chunk;
        auto byte_size = row->data_byte_size();
        if (byte_size == 0) {
            ARROW_RETURN_NOT_OK(builder.Append(""));
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(
                static_cast<const uint8_t*>(row->data()), byte_size));
        }
    }

    return builder.Finish();
}

// build Arrow Array for a single chunk of fixed-size data (zero-copy when possible)
// this wraps the chunk data directly without copying
template <typename ArrayType>
arrow::Result<std::shared_ptr<arrow::Array>>
WrapChunkAsArrowArray(const void* chunk_data,
                      int64_t num_rows,
                      int64_t element_size,
                      const milvus::segcore::ThreadSafeValidDataPtr& valid_data,
                      int64_t validity_offset) {
    ARROW_ASSIGN_OR_RAISE(
        auto data_buffer,
        WrapOrCopyArrowBuffer(
            chunk_data, num_rows * element_size, element_size));

    // build validity bitmap if needed
    std::shared_ptr<arrow::Buffer> null_bitmap = nullptr;
    int64_t null_count = 0;

    if (valid_data) {
        int64_t bitmap_bytes = (num_rows + 7) / 8;
        ARROW_ASSIGN_OR_RAISE(auto bitmap_buffer,
                              arrow::AllocateBuffer(bitmap_bytes));
        uint8_t* dst = bitmap_buffer->mutable_data();
        std::memset(dst, 0, bitmap_bytes);

        for (int64_t i = 0; i < num_rows; i++) {
            bool is_valid = valid_data->is_valid(validity_offset + i);
            if (is_valid) {
                dst[i / 8] |= (1 << (i % 8));
            } else {
                null_count++;
            }
        }
        null_bitmap = std::move(bitmap_buffer);
    }

    return std::make_shared<ArrayType>(
        num_rows, data_buffer, null_bitmap, null_count);
}

// build Arrow Array for a single chunk of FixedSizeBinary data (zero-copy)
arrow::Result<std::shared_ptr<arrow::Array>>
WrapChunkAsFixedSizeBinaryArray(
    const void* chunk_data,
    int64_t num_rows,
    int64_t byte_width,
    int64_t data_alignment,
    const std::shared_ptr<arrow::DataType>& data_type,
    const milvus::segcore::ThreadSafeValidDataPtr& valid_data,
    int64_t validity_offset) {
    ARROW_ASSIGN_OR_RAISE(
        auto data_buffer,
        WrapOrCopyArrowBuffer(
            chunk_data, num_rows * byte_width, data_alignment));

    // build validity bitmap if needed
    std::shared_ptr<arrow::Buffer> null_bitmap = nullptr;
    int64_t null_count = 0;

    if (valid_data) {
        int64_t bitmap_bytes = (num_rows + 7) / 8;
        ARROW_ASSIGN_OR_RAISE(auto bitmap_buffer,
                              arrow::AllocateBuffer(bitmap_bytes));
        uint8_t* dst = bitmap_buffer->mutable_data();
        std::memset(dst, 0, bitmap_bytes);

        for (int64_t i = 0; i < num_rows; i++) {
            bool is_valid = valid_data->is_valid(validity_offset + i);
            if (is_valid) {
                dst[i / 8] |= (1 << (i % 8));
            } else {
                null_count++;
            }
        }
        null_bitmap = std::move(bitmap_buffer);
    }

    return std::make_shared<arrow::FixedSizeBinaryArray>(
        data_type, num_rows, data_buffer, null_bitmap, null_count);
}

// build string array for a chunk - strings need to be copied since they're not contiguous
arrow::Result<std::shared_ptr<arrow::Array>>
BuildStringArrayForChunk(
    const milvus::segcore::ConcurrentVector<std::string>* string_vec,
    int64_t start_offset,
    int64_t num_rows,
    const milvus::segcore::ThreadSafeValidDataPtr& valid_data) {
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));

    for (int64_t i = 0; i < num_rows; i++) {
        int64_t offset = start_offset + i;
        if (valid_data && !valid_data->is_valid(offset)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            auto str_view = string_vec->view_element(offset);
            ARROW_RETURN_NOT_OK(
                builder.Append(str_view.data(), str_view.length()));
        }
    }

    return builder.Finish();
}

// build TEXT array for a chunk when spillover is enabled
// reads LOB references from ConcurrentVector, decodes and reads actual text from spillover
arrow::Result<std::shared_ptr<arrow::Array>>
BuildTextArrayForChunkWithSpillover(
    const milvus::segcore::ConcurrentVector<std::string>* ref_vec,
    milvus::segcore::TextLobSpillover* spillover,
    int64_t start_offset,
    int64_t num_rows,
    const milvus::segcore::ThreadSafeValidDataPtr& valid_data) {
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));

    // Collect non-null refs for batch read
    std::vector<int64_t> pending_indices;
    std::vector<std::string_view> pending_refs;
    for (int64_t i = 0; i < num_rows; i++) {
        int64_t offset = start_offset + i;
        if (valid_data && !valid_data->is_valid(offset)) {
            continue;
        }
        pending_refs.push_back(ref_vec->view_element(offset));
        pending_indices.push_back(i);
    }

    // Batch pread all refs
    auto texts = spillover->DecodeAndReadBatch(pending_refs);

    // Build arrow array
    size_t batch_idx = 0;
    for (int64_t i = 0; i < num_rows; i++) {
        if (batch_idx < pending_indices.size() &&
            pending_indices[batch_idx] == i) {
            ARROW_RETURN_NOT_OK(builder.Append(texts[batch_idx].data(),
                                               texts[batch_idx].length()));
            batch_idx++;
        } else {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        }
    }

    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
BuildVectorArrayForChunk(const FieldInfo& field_info,
                         int64_t start_offset,
                         int64_t num_rows) {
    auto vector_array_vec = dynamic_cast<
        const milvus::segcore::ConcurrentVector<milvus::VectorArray>*>(
        field_info.vec_base);
    if (!vector_array_vec) {
        return arrow::Status::Invalid("Expected ConcurrentVector<VectorArray>");
    }

    auto byte_width = milvus::vector_bytes_per_element(field_info.element_type,
                                                       field_info.dim);
    auto value_builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
        arrow::fixed_size_binary(byte_width));
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));

    for (int64_t i = 0; i < num_rows; i++) {
        auto logical_offset = start_offset + i;
        if (field_info.valid_data &&
            !field_info.valid_data->is_valid(logical_offset)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }

        auto physical_offset =
            field_info.vec_base->get_physical_offset(logical_offset);
        if (physical_offset < 0) {
            return arrow::Status::Invalid(
                "valid nullable vector array row missing physical data");
        }

        const auto& vector_array = (*vector_array_vec)[physical_offset];
        if (vector_array.get_element_type() != field_info.element_type) {
            return arrow::Status::Invalid("VECTOR_ARRAY element type mismatch");
        }
        if (vector_array.dim() != field_info.dim) {
            return arrow::Status::Invalid("VECTOR_ARRAY dim mismatch");
        }

        ARROW_RETURN_NOT_OK(builder.Append());
        ARROW_RETURN_NOT_OK(value_builder->AppendValues(
            reinterpret_cast<const uint8_t*>(vector_array.data()),
            vector_array.length()));
    }

    return builder.Finish();
}

// build boolean array for a chunk - booleans need special handling
arrow::Result<std::shared_ptr<arrow::Array>>
BuildBoolArrayForChunk(
    const void* chunk_data,
    int64_t num_rows,
    const milvus::segcore::ThreadSafeValidDataPtr& valid_data,
    int64_t validity_offset) {
    const uint8_t* bool_data = static_cast<const uint8_t*>(chunk_data);
    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));

    for (int64_t i = 0; i < num_rows; i++) {
        if (valid_data && !valid_data->is_valid(validity_offset + i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(bool_data[i] != 0));
        }
    }

    return builder.Finish();
}

// build Arrow Array for a single chunk based on data type
arrow::Result<std::shared_ptr<arrow::Array>>
BuildArrayForChunk(const FieldInfo& field_info,
                   int64_t chunk_id,
                   int64_t offset_in_chunk,
                   int64_t num_rows,
                   int64_t global_offset) {
    int64_t element_size =
        GetElementByteWidth(field_info.data_type, field_info.dim);

    auto get_data_ptr = [&]() {
        const void* chunk_data = field_info.vec_base->get_chunk_data(chunk_id);
        return static_cast<const uint8_t*>(chunk_data) +
               offset_in_chunk * element_size;
    };

    switch (field_info.data_type) {
        case milvus::DataType::BOOL:
            return BuildBoolArrayForChunk(
                get_data_ptr(), num_rows, field_info.valid_data, global_offset);

        case milvus::DataType::INT8:
            return WrapChunkAsArrowArray<arrow::Int8Array>(
                get_data_ptr(),
                num_rows,
                1,
                field_info.valid_data,
                global_offset);

        case milvus::DataType::INT16:
            return WrapChunkAsArrowArray<arrow::Int16Array>(
                get_data_ptr(),
                num_rows,
                2,
                field_info.valid_data,
                global_offset);

        case milvus::DataType::INT32:
            return WrapChunkAsArrowArray<arrow::Int32Array>(
                get_data_ptr(),
                num_rows,
                4,
                field_info.valid_data,
                global_offset);

        case milvus::DataType::INT64:
        case milvus::DataType::TIMESTAMPTZ:
            return WrapChunkAsArrowArray<arrow::Int64Array>(
                get_data_ptr(),
                num_rows,
                8,
                field_info.valid_data,
                global_offset);

        case milvus::DataType::FLOAT:
            return WrapChunkAsArrowArray<arrow::FloatArray>(
                get_data_ptr(),
                num_rows,
                4,
                field_info.valid_data,
                global_offset);

        case milvus::DataType::DOUBLE:
            return WrapChunkAsArrowArray<arrow::DoubleArray>(
                get_data_ptr(),
                num_rows,
                8,
                field_info.valid_data,
                global_offset);

        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING: {
            auto string_vec = dynamic_cast<
                const milvus::segcore::ConcurrentVector<std::string>*>(
                field_info.vec_base);
            if (!string_vec) {
                return arrow::Status::Invalid(
                    "Expected ConcurrentVector<std::string>");
            }
            return BuildStringArrayForChunk(
                string_vec, global_offset, num_rows, field_info.valid_data);
        }

        case milvus::DataType::TEXT: {
            auto string_vec = dynamic_cast<
                const milvus::segcore::ConcurrentVector<std::string>*>(
                field_info.vec_base);
            if (!string_vec) {
                return arrow::Status::Invalid(
                    "Expected ConcurrentVector<std::string>");
            }
            // TEXT with spillover: read from LOB file
            if (field_info.text_lob_spillover) {
                return BuildTextArrayForChunkWithSpillover(
                    string_vec,
                    field_info.text_lob_spillover,
                    global_offset,
                    num_rows,
                    field_info.valid_data);
            }
            // Fallback for TEXT without spillover
            return BuildStringArrayForChunk(
                string_vec, global_offset, num_rows, field_info.valid_data);
        }

        case milvus::DataType::JSON: {
            auto json_vec = dynamic_cast<
                const milvus::segcore::ConcurrentVector<milvus::Json>*>(
                field_info.vec_base);
            if (!json_vec) {
                return arrow::Status::Invalid(
                    "Expected ConcurrentVector<Json>");
            }
            arrow::BinaryBuilder builder;
            ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
            for (int64_t i = 0; i < num_rows; i++) {
                int64_t offset = global_offset + i;
                if (field_info.valid_data &&
                    !field_info.valid_data->is_valid(offset)) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    auto sv = json_vec->view_element(offset);
                    ARROW_RETURN_NOT_OK(builder.Append(sv.data(), sv.length()));
                }
            }
            return builder.Finish();
        }

        case milvus::DataType::ARRAY: {
            auto array_vec = dynamic_cast<
                const milvus::segcore::ConcurrentVector<milvus::Array>*>(
                field_info.vec_base);
            if (!array_vec) {
                return arrow::Status::Invalid(
                    "Expected ConcurrentVector<Array>");
            }
            arrow::BinaryBuilder builder;
            ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
            for (int64_t i = 0; i < num_rows; i++) {
                int64_t offset = global_offset + i;
                if (field_info.valid_data &&
                    !field_info.valid_data->is_valid(offset)) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    auto array_view = array_vec->view_element(offset);
                    auto serialized =
                        array_view.output_data().SerializeAsString();
                    ARROW_RETURN_NOT_OK(
                        builder.Append(serialized.data(), serialized.size()));
                }
            }
            return builder.Finish();
        }

        case milvus::DataType::GEOMETRY: {
            auto geometry_vec = dynamic_cast<
                const milvus::segcore::ConcurrentVector<std::string>*>(
                field_info.vec_base);
            if (!geometry_vec) {
                return arrow::Status::Invalid(
                    "Expected ConcurrentVector<std::string> for GEOMETRY");
            }
            arrow::BinaryBuilder builder;
            ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
            for (int64_t i = 0; i < num_rows; i++) {
                int64_t offset = global_offset + i;
                if (field_info.valid_data &&
                    !field_info.valid_data->is_valid(offset)) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    auto wkb = geometry_vec->view_element(offset);
                    ARROW_RETURN_NOT_OK(builder.Append(wkb.data(), wkb.size()));
                }
            }
            return builder.Finish();
        }

        case milvus::DataType::VECTOR_FLOAT:
        case milvus::DataType::VECTOR_BINARY:
        case milvus::DataType::VECTOR_FLOAT16:
        case milvus::DataType::VECTOR_BFLOAT16:
        case milvus::DataType::VECTOR_INT8: {
            ARROW_ASSIGN_OR_RAISE(
                auto data_alignment,
                GetFixedWidthVectorValueAlignment(field_info.data_type));
            if (field_info.nullable) {
                return BuildNullableFixedWidthVectorArray(field_info,
                                                          global_offset,
                                                          num_rows,
                                                          element_size,
                                                          data_alignment);
            }
            auto arrow_type =
                milvus::GetArrowDataType(field_info.data_type, field_info.dim);
            return WrapChunkAsFixedSizeBinaryArray(get_data_ptr(),
                                                   num_rows,
                                                   element_size,
                                                   data_alignment,
                                                   arrow_type,
                                                   field_info.valid_data,
                                                   global_offset);
        }

        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            return BuildSparseFloatVectorArrayForChunk(
                field_info, global_offset, num_rows);

        case milvus::DataType::VECTOR_ARRAY:
            return BuildVectorArrayForChunk(
                field_info, global_offset, num_rows);

        default:
            return arrow::Status::NotImplemented("Unsupported data type");
    }
}

}  // anonymous namespace

CStatus
FlushGrowingSegmentData(CSegmentInterface c_segment,
                        int64_t start_offset,
                        int64_t end_offset,
                        const CFlushConfig* config,
                        CFlushResult* result) {
    SCOPE_CGO_CALL_METRIC();

    try {
        // validate inputs
        if (!c_segment || !config || !result) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          "invalid arguments: segment, config, "
                                          "and result must not be null");
        }
        result->manifest_path = nullptr;
        result->committed_version = 0;
        result->num_rows = 0;
        result->bm25_field_ids = nullptr;
        result->bm25_stats = nullptr;
        result->bm25_stats_sizes = nullptr;
        result->num_bm25_stats = 0;

        if (start_offset < 0 || end_offset < start_offset) {
            return milvus::FailureCStatus(
                milvus::UnexpectedError,
                "invalid offsets: start_offset must be >= 0 and <= end_offset");
        }
        if (config->num_bm25_fields > 0 && config->bm25_field_ids == nullptr) {
            return milvus::FailureCStatus(
                milvus::UnexpectedError,
                "invalid BM25 config: bm25_field_ids is null");
        }
        if (config->num_bm25_fields > 0 &&
            config->bm25_stats_log_ids == nullptr) {
            return milvus::FailureCStatus(
                milvus::UnexpectedError,
                "invalid BM25 config: bm25_stats_log_ids is null");
        }
        if (config->num_allowed_fields > 0 &&
            config->allowed_field_ids == nullptr) {
            return milvus::FailureCStatus(
                milvus::UnexpectedError,
                "invalid allowed field config: allowed_field_ids is null");
        }
        if (config->schema_blob == nullptr || config->schema_length <= 0) {
            return milvus::FailureCStatus(
                milvus::UnexpectedError,
                "invalid flush schema config: schema_blob is null or "
                "schema_length is not positive");
        }

        // no data to flush
        if (start_offset == end_offset) {
            return milvus::SuccessCStatus();
        }

        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto growing_segment =
            dynamic_cast<milvus::segcore::SegmentGrowingImpl*>(
                segment_interface);
        if (!growing_segment) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          "segment is not a growing segment");
        }

        // Use the schema selected by the flush task. The growing segment's
        // runtime schema may be advanced by concurrent LazyCheckSchema/Reopen.
        auto flush_schema =
            ParseFlushSchema(config->schema_blob, config->schema_length);
        const auto& schema = *flush_schema;
        auto& insert_record = growing_segment->get_insert_record();

        int64_t total_rows = end_offset - start_offset;
        std::unordered_set<int64_t> bm25_field_ids;
        std::unordered_map<int64_t, int64_t> bm25_stats_log_ids;
        for (size_t i = 0; i < config->num_bm25_fields; i++) {
            bm25_field_ids.insert(config->bm25_field_ids[i]);
            bm25_stats_log_ids[config->bm25_field_ids[i]] =
                config->bm25_stats_log_ids[i];
        }
        std::unordered_set<int64_t> allowed_field_ids;
        for (size_t i = 0; i < config->num_allowed_fields; i++) {
            allowed_field_ids.insert(config->allowed_field_ids[i]);
        }
        std::unordered_map<int64_t, BM25StatsAccumulator> bm25_stats;

        // Use get_field_ids() (ordered vector) instead of get_fields() (unordered_map)
        // to ensure deterministic column order matching the reader's expected order.
        std::vector<FieldInfo> field_infos;
        std::vector<std::shared_ptr<arrow::Field>> arrow_fields;

        {
            const auto& field_meta = milvus::FieldMeta::RowIdMeta;
            FieldInfo info;
            info.field_id = RowFieldID;
            info.field_name = field_meta.get_name().get();
            info.data_type = field_meta.get_data_type();
            info.element_type = milvus::DataType::NONE;
            info.nullable = field_meta.is_nullable();
            info.dim = 0;
            info.vec_base = &insert_record.row_ids_;
            info.valid_data = nullptr;
            info.text_lob_spillover = nullptr;
            field_infos.push_back(std::move(info));

            auto metadata = arrow::KeyValueMetadata::Make(
                {milvus_storage::ARROW_FIELD_ID_KEY},
                {std::to_string(RowFieldID.get())});
            auto arrow_type =
                milvus::GetArrowDataType(field_meta.get_data_type(), 0);
            arrow_fields.push_back(
                arrow::field(std::to_string(RowFieldID.get()),
                             arrow_type,
                             field_meta.is_nullable(),
                             metadata));
        }

        for (const auto& field_id : schema.get_field_ids()) {
            if (field_id == RowFieldID) {
                continue;  // skip RowID system field
            }
            if (!allowed_field_ids.empty() && field_id != TimestampFieldID &&
                allowed_field_ids.find(field_id.get()) ==
                    allowed_field_ids.end()) {
                continue;
            }

            const auto& field_meta = schema[field_id];

            // Timestamp is stored in insert_record.timestamps_, not in data_ map
            const milvus::segcore::VectorBase* vec_base;
            if (field_id == TimestampFieldID) {
                vec_base = &insert_record.timestamps_;
            } else {
                vec_base = insert_record.get_data_base(field_id);
                if (!vec_base) {
                    LOG_ERROR("no data base for field {} of segment {}",
                              field_meta.get_name().get(),
                              growing_segment->get_segment_id());
                    return milvus::FailureCStatus(
                        milvus::UnexpectedError,
                        fmt::format("no data base for field {} of segment {}",
                                    field_meta.get_name().get(),
                                    growing_segment->get_segment_id()));
                }
            }

            auto data_type = field_meta.get_data_type();
            auto dim = field_meta.is_vector() &&
                               !milvus::IsSparseFloatVectorDataType(data_type)
                           ? field_meta.get_dim()
                           : 0;
            auto arrow_type = data_type == milvus::DataType::VECTOR_ARRAY
                                  ? milvus::GetArrowDataTypeForVectorArray(
                                        field_meta.get_element_type(), dim)
                                  : milvus::GetArrowDataType(data_type, dim);
            if (field_meta.is_nullable() &&
                IsSupportedNullableVectorDataType(data_type)) {
                arrow_type = arrow::binary();
            }

            FieldInfo info;
            info.field_id = field_id;
            info.field_name = field_meta.get_name().get();
            info.data_type = data_type;
            info.element_type = field_meta.get_element_type();
            info.nullable = field_meta.is_nullable();
            info.dim = dim;
            info.vec_base = vec_base;
            info.valid_data = nullptr;
            if (field_meta.is_nullable() &&
                insert_record.is_valid_data_exist(field_id)) {
                info.valid_data = insert_record.get_valid_data(field_id);
            }

            info.text_lob_spillover = nullptr;
            if (field_meta.get_data_type() == milvus::DataType::TEXT &&
                growing_segment->HasTextLobSpillover(field_id)) {
                info.text_lob_spillover =
                    growing_segment->GetTextLobSpillover(field_id);
            }
            if (bm25_field_ids.find(field_id.get()) != bm25_field_ids.end()) {
                if (field_meta.get_data_type() !=
                    milvus::DataType::VECTOR_SPARSE_U32_F32) {
                    return milvus::FailureCStatus(
                        milvus::UnexpectedError,
                        fmt::format("BM25 stats field {} is not sparse vector",
                                    field_id.get()));
                }
                bm25_stats.emplace(field_id.get(), BM25StatsAccumulator{});
            }

            field_infos.push_back(std::move(info));

            // create Arrow field with metadata
            std::vector<std::string> metadata_keys = {
                milvus_storage::ARROW_FIELD_ID_KEY};
            std::vector<std::string> metadata_values = {
                std::to_string(field_id.get())};
            if (field_meta.is_nullable() &&
                IsFixedWidthVectorDataType(data_type)) {
                metadata_keys.push_back(DIM_KEY);
                metadata_values.push_back(std::to_string(dim));
            }
            if (data_type == milvus::DataType::VECTOR_ARRAY) {
                metadata_keys.push_back(ELEMENT_TYPE_KEY_FOR_ARROW);
                metadata_values.push_back(std::to_string(
                    static_cast<int>(field_meta.get_element_type())));
                metadata_keys.push_back(DIM_KEY);
                metadata_values.push_back(std::to_string(dim));
            }
            auto metadata =
                arrow::KeyValueMetadata::Make(metadata_keys, metadata_values);
            arrow_fields.push_back(arrow::field(std::to_string(field_id.get()),
                                                arrow_type,
                                                field_meta.is_nullable(),
                                                metadata));
        }

        if (field_infos.empty()) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          "no fields to flush");
        }

        auto arrow_schema = arrow::schema(arrow_fields);

        // build SegmentWriterConfig
        milvus_storage::segment::SegmentWriterConfig writer_config;
        writer_config.segment_path =
            config->segment_path ? config->segment_path : "";
        int64_t read_version = config->read_version;
        int retry_limit = config->retry_limit > 0 ? config->retry_limit : 1;

        // copy filesystem properties from global storage config
        auto global_properties =
            milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                .GetProperties();
        if (global_properties) {
            writer_config.properties = *global_properties;
        }

        // set required properties for ColumnGroupPolicy
        if (config->schema_based_pattern &&
            config->schema_based_pattern[0] != '\0') {
            milvus_storage::api::SetValue(
                writer_config.properties,
                PROPERTY_WRITER_POLICY,
                LOON_COLUMN_GROUP_POLICY_SCHEMA_BASED);
            milvus_storage::api::SetValue(writer_config.properties,
                                          PROPERTY_WRITER_SCHEMA_BASE_PATTERNS,
                                          config->schema_based_pattern);
            if (config->schema_based_formats &&
                config->schema_based_formats[0] != '\0') {
                milvus_storage::api::SetValue(
                    writer_config.properties,
                    PROPERTY_WRITER_SCHEMA_BASE_FORMATS,
                    config->schema_based_formats);
            }
        } else {
            milvus_storage::api::SetValue(writer_config.properties,
                                          PROPERTY_WRITER_POLICY,
                                          LOON_COLUMN_GROUP_POLICY_SINGLE);
        }
        auto writer_format =
            config->writer_format && config->writer_format[0] != '\0'
                ? std::string(config->writer_format)
                : std::string(LOON_FORMAT_PARQUET);
        milvus_storage::api::SetValue(writer_config.properties,
                                      PROPERTY_WRITER_FORMAT,
                                      writer_format.c_str());

        // add TEXT column configs
        for (size_t i = 0; i < config->num_text_columns; i++) {
            milvus_storage::lob_column::LobColumnConfig text_config;
            text_config.field_id = config->text_field_ids[i];
            if (config->text_lob_paths && config->text_lob_paths[i]) {
                text_config.lob_base_path = config->text_lob_paths[i];
            }
            if (config->text_inline_threshold > 0) {
                text_config.inline_threshold =
                    static_cast<size_t>(config->text_inline_threshold);
            }
            if (config->text_max_lob_file_bytes > 0) {
                text_config.max_lob_file_bytes =
                    static_cast<size_t>(config->text_max_lob_file_bytes);
            }
            if (config->text_flush_threshold_bytes > 0) {
                text_config.flush_threshold_bytes =
                    static_cast<size_t>(config->text_flush_threshold_bytes);
            }
            text_config.properties = writer_config.properties;
            writer_config.lob_columns[text_config.field_id] = text_config;
        }

        // get filesystem from singleton
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        if (!fs) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          "filesystem not initialized");
        }

        // create segment writer
        auto writer_result = milvus_storage::segment::SegmentWriter::Create(
            fs, arrow_schema, writer_config);
        if (!writer_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          writer_result.status().ToString());
        }
        auto writer = std::move(writer_result).ValueOrDie();

        // iterate over chunks and write each one (zero-copy approach)
        // this avoids copying all data into a single contiguous buffer
        int64_t size_per_chunk = field_infos[0].vec_base->get_size_per_chunk();
        int64_t current_offset = start_offset;
        int64_t rows_written = 0;

        while (current_offset < end_offset) {
            int64_t chunk_id = current_offset / size_per_chunk;
            int64_t offset_in_chunk = current_offset % size_per_chunk;
            int64_t chunk_size =
                field_infos[0].vec_base->get_chunk_size(chunk_id);

            // how many rows we can process from this chunk
            int64_t available_in_chunk = chunk_size - offset_in_chunk;
            int64_t remaining = end_offset - current_offset;
            int64_t batch_rows = std::min(available_in_chunk, remaining);

            if (batch_rows <= 0) {
                break;
            }

            // build arrays for each field
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            arrays.reserve(field_infos.size());

            for (const auto& field_info : field_infos) {
                auto arr_result = BuildArrayForChunk(field_info,
                                                     chunk_id,
                                                     offset_in_chunk,
                                                     batch_rows,
                                                     current_offset);
                if (!arr_result.ok()) {
                    return milvus::FailureCStatus(
                        milvus::UnexpectedError,
                        arr_result.status().ToString());
                }
                arrays.push_back(arr_result.ValueOrDie());

                auto stats_iter = bm25_stats.find(field_info.field_id.get());
                if (stats_iter != bm25_stats.end()) {
                    auto status = CollectBM25StatsForChunk(field_info,
                                                           current_offset,
                                                           batch_rows,
                                                           stats_iter->second);
                    if (!status.ok()) {
                        return milvus::FailureCStatus(milvus::UnexpectedError,
                                                      status.ToString());
                    }
                }
            }

            // create RecordBatch and write
            auto batch =
                arrow::RecordBatch::Make(arrow_schema, batch_rows, arrays);
            auto write_status = writer->Write(batch);
            if (!write_status.ok()) {
                return milvus::FailureCStatus(milvus::UnexpectedError,
                                              write_status.ToString());
            }

            current_offset += batch_rows;
            rows_written += batch_rows;
        }

        // close writer — returns ColumnGroups + LobFiles, does NOT commit
        auto close_result = writer->Close();
        if (!close_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          close_result.status().ToString());
        }
        auto output = std::move(close_result).ValueOrDie();

        // commit via Transaction externally
        auto transaction_result =
            milvus_storage::api::transaction::Transaction::Open(
                fs,
                writer_config.segment_path,
                read_version,
                milvus_storage::api::transaction::OverwriteResolver,
                retry_limit);
        if (!transaction_result.ok()) {
            return milvus::FailureCStatus(
                milvus::UnexpectedError,
                transaction_result.status().ToString());
        }
        auto transaction = std::move(transaction_result).ValueOrDie();

        auto manifest_result = transaction->GetManifest();
        if (!manifest_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          manifest_result.status().ToString());
        }
        auto manifest = manifest_result.ValueOrDie();

        // append column groups
        transaction->AppendFiles(*output.column_groups);

        // add LOB files
        for (const auto& lob_file : output.lob_files) {
            transaction->AddLobFile(lob_file);
        }

        // add BM25 stats files and register their manifest entries in the same
        // transaction as the flushed data files.
        for (const auto& [field_id, stats] : bm25_stats) {
            auto stat_key = fmt::format("bm25.{}", field_id);
            milvus_storage::api::Statistics stat_entry;
            auto existing_it = manifest->stats().find(stat_key);
            if (existing_it != manifest->stats().end()) {
                stat_entry = existing_it->second;
            }

            auto serialized = SerializeBM25Stats(stats);
            auto stats_log_id = bm25_stats_log_ids[field_id];
            auto rel_path =
                fmt::format("_stats/bm25.{}/{}", field_id, stats_log_id);
            auto full_path =
                fmt::format("{}/{}", writer_config.segment_path, rel_path);
            auto write_status = WriteRawFile(fs, full_path, serialized);
            if (!write_status.ok()) {
                return milvus::FailureCStatus(milvus::UnexpectedError,
                                              write_status.ToString());
            }
            stat_entry.paths.push_back(full_path);

            int64_t memory_size = serialized.size();
            auto mem_it = stat_entry.metadata.find("memory_size");
            if (mem_it != stat_entry.metadata.end()) {
                try {
                    memory_size += std::stoll(mem_it->second);
                } catch (...) {
                    // Ignore malformed historical metadata and rewrite it below.
                }
            }

            if (config->write_merged_bm25_stats) {
                BM25StatsAccumulator merged_stats;
                std::vector<std::string> paths_to_merge;
                for (const auto& existing_path : stat_entry.paths) {
                    if (IsCompoundStatsPath(existing_path)) {
                        paths_to_merge = {existing_path};
                        break;
                    }
                    if (existing_path != full_path) {
                        paths_to_merge.push_back(existing_path);
                    }
                }
                for (const auto& existing_path : paths_to_merge) {
                    auto existing_result = ReadBM25StatsFile(fs, existing_path);
                    if (!existing_result.ok()) {
                        return milvus::FailureCStatus(
                            milvus::UnexpectedError,
                            existing_result.status().ToString());
                    }
                    merged_stats.Merge(existing_result.ValueOrDie());
                }
                merged_stats.Merge(stats);

                auto merged_serialized = SerializeBM25Stats(merged_stats);
                auto merged_rel_path =
                    fmt::format("_stats/bm25.{}/{}", field_id, int64_t(1));
                auto merged_full_path = fmt::format(
                    "{}/{}", writer_config.segment_path, merged_rel_path);
                write_status =
                    WriteRawFile(fs, merged_full_path, merged_serialized);
                if (!write_status.ok()) {
                    return milvus::FailureCStatus(milvus::UnexpectedError,
                                                  write_status.ToString());
                }
                stat_entry.paths.push_back(merged_full_path);
                memory_size += merged_serialized.size();
            }

            stat_entry.metadata["memory_size"] = std::to_string(memory_size);
            transaction->UpdateStat(stat_key, stat_entry);
        }

        // commit
        auto commit_result = transaction->Commit();
        if (!commit_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          commit_result.status().ToString());
        }
        auto committed_version = commit_result.ValueOrDie();

        // fill output
        auto manifest_path = milvus_storage::get_manifest_filepath(
            writer_config.segment_path, committed_version);
        result->manifest_path = strdup(manifest_path.c_str());
        result->committed_version = committed_version;
        result->num_rows = output.rows_written;
        if (!bm25_stats.empty()) {
            result->num_bm25_stats = bm25_stats.size();
            result->bm25_field_ids = static_cast<int64_t*>(
                malloc(sizeof(int64_t) * result->num_bm25_stats));
            result->bm25_stats = static_cast<uint8_t**>(
                malloc(sizeof(uint8_t*) * result->num_bm25_stats));
            result->bm25_stats_sizes = static_cast<size_t*>(
                malloc(sizeof(size_t) * result->num_bm25_stats));
            size_t idx = 0;
            for (const auto& [field_id, stats] : bm25_stats) {
                auto serialized = SerializeBM25Stats(stats);
                result->bm25_field_ids[idx] = field_id;
                result->bm25_stats_sizes[idx] = serialized.size();
                result->bm25_stats[idx] =
                    static_cast<uint8_t*>(malloc(serialized.size()));
                std::memcpy(result->bm25_stats[idx],
                            serialized.data(),
                            serialized.size());
                idx++;
            }
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::UnexpectedError, e.what());
    }
}

void
FreeFlushResult(CFlushResult* result) {
    if (result && result->manifest_path) {
        free(result->manifest_path);
        result->manifest_path = nullptr;
    }
    if (result && result->bm25_stats) {
        for (size_t i = 0; i < result->num_bm25_stats; i++) {
            free(result->bm25_stats[i]);
        }
        free(result->bm25_stats);
        result->bm25_stats = nullptr;
    }
    if (result && result->bm25_field_ids) {
        free(result->bm25_field_ids);
        result->bm25_field_ids = nullptr;
    }
    if (result && result->bm25_stats_sizes) {
        free(result->bm25_stats_sizes);
        result->bm25_stats_sizes = nullptr;
    }
    if (result) {
        result->num_bm25_stats = 0;
    }
}

CStatus
SegmentSetCommitTimestamp(CSegmentInterface c_segment, uint64_t commit_ts) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    segment->SetCommitTimestamp(commit_ts);
    return milvus::SuccessCStatus();
}
