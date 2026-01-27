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

#include <memory>
#include <limits>

#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "pb/cgo_msg.pb.h"
#include "pb/index_cgo_msg.pb.h"

#include "common/FieldData.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/Tracer.h"
#include "common/type_c.h"
#include "common/ScopedTimer.h"
#include "google/protobuf/text_format.h"
#include "log/Log.h"
#include "mmap/Types.h"
#include "monitor/scope_metric.h"
#include "pb/segcore.pb.h"
#include "segcore/Collection.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/TextLobSpillover.h"
#include "segcore/Utils.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "futures/Future.h"
#include "futures/Executor.h"
#include "segcore/SegmentSealed.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "mmap/Types.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "exec/expression/ExprCache.h"
#include "monitor/Monitor.h"
#include "common/GeometryCache.h"

// milvus-storage headers for FlushGrowingSegmentData
#include "milvus-storage/segment/segment_writer.h"
#include "milvus-storage/segment/growing_segment_flusher.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/common/constants.h"

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
        segment->SetLoadInfo(load_info);
        *newSegment = segment.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
ReopenSegment(CTraceContext c_trace,
              CSegmentInterface c_segment,
              const uint8_t* load_info_blob,
              const int64_t load_info_length) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(load_info_blob, "load info is null");
        milvus::proto::segcore::SegmentLoadInfo load_info;
        auto suc = load_info.ParseFromArray(load_info_blob, load_info_length);
        AssertInfo(suc, "unmarshal load info failed");

        auto segment =
            static_cast<milvus::segcore::SegmentInterface*>(c_segment);

        segment->Reopen(load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
SegmentLoad(CTraceContext c_trace, CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment =
            static_cast<milvus::segcore::SegmentInterface*>(c_segment);
        // TODO unify trace context to op context after supported
        auto trace_ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        segment->Load(trace_ctx);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
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

CFuture*  // Future<milvus::SearchResult>
AsyncSearch(CTraceContext c_trace,
            CSegmentInterface c_segment,
            CSearchPlan c_plan,
            CPlaceholderGroup c_placeholder_group,
            uint64_t timestamp,
            int32_t consistency_level,
            uint64_t collection_ttl) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto plan = static_cast<milvus::query::Plan*>(c_plan);
    auto phg_ptr = reinterpret_cast<const milvus::query::PlaceholderGroup*>(
        c_placeholder_group);

    auto future = milvus::futures::Future<milvus::SearchResult>::async(
        milvus::futures::getGlobalCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace,
         segment,
         plan,
         phg_ptr,
         timestamp,
         consistency_level,
         collection_ttl](folly::CancellationToken cancel_token) {
            // save trace context into search_info
            auto& trace_ctx = plan->plan_node_->search_info_.trace_ctx_;
            trace_ctx.traceID = c_trace.traceID;
            trace_ctx.spanID = c_trace.spanID;
            trace_ctx.traceFlags = c_trace.traceFlags;

            auto span = milvus::tracer::StartSpan("SegCoreSearch", &trace_ctx);
            milvus::tracer::SetRootSpan(span);

            segment->LazyCheckSchema(plan->schema_);

            auto search_result = segment->Search(plan,
                                                 phg_ptr,
                                                 timestamp,
                                                 cancel_token,
                                                 consistency_level,
                                                 collection_ttl);
            if (!milvus::PositivelyRelated(
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
              uint64_t collection_ttl) {
    auto segment = static_cast<milvus::segcore::SegmentInterface*>(c_segment);
    auto plan = static_cast<const milvus::query::RetrievePlan*>(c_plan);
    auto future = milvus::futures::Future<CRetrieveResult>::async(
        milvus::futures::getGlobalCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace,
         segment,
         plan,
         timestamp,
         limit_size,
         ignore_non_pk,
         consistency_level,
         collection_ttl](folly::CancellationToken cancel_token) {
            auto trace_ctx = milvus::tracer::TraceContext{
                c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
            milvus::tracer::AutoSpan span("SegCoreRetrieve", &trace_ctx, true);

            segment->LazyCheckSchema(plan->schema_);

            auto retrieve_result = segment->Retrieve(&trace_ctx,
                                                     plan,
                                                     timestamp,
                                                     limit_size,
                                                     ignore_non_pk,
                                                     cancel_token,
                                                     consistency_level,
                                                     collection_ttl);

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
        milvus::futures::getGlobalCPUExecutor(),
        milvus::futures::ExecutePriority::HIGH,
        [c_trace, segment, plan, offsets, len](
            folly::CancellationToken cancel_token) {
            auto trace_ctx = milvus::tracer::TraceContext{
                c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
            milvus::tracer::AutoSpan span(
                "SegCoreRetrieveByOffsets", &trace_ctx, true);

            auto retrieve_result =
                segment->Retrieve(&trace_ctx, plan, offsets, len);

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
LoadTextIndex(CSegmentInterface c_segment,
              const uint8_t* serialized_load_text_index_info,
              const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");

        auto info_proto =
            std::make_unique<milvus::proto::indexcgo::LoadTextIndexInfo>();
        info_proto->ParseFromArray(serialized_load_text_index_info, len);

        segment->LoadTextIndex(std::move(info_proto));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
LoadJsonKeyIndex(CTraceContext c_trace,
                 CSegmentInterface c_segment,
                 const uint8_t* serialized_load_json_key_index_info,
                 const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");

        auto info_proto =
            std::make_unique<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>();
        info_proto->ParseFromArray(serialized_load_json_key_index_info, len);

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
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
        AssertInfo(fs != nullptr, "arrow file system is null");

        milvus::Config config;
        std::vector<std::string> files;
        for (const auto& f : info_proto->files()) {
            files.push_back(f);
        }
        config[milvus::index::INDEX_FILES] = files;
        config[milvus::LOAD_PRIORITY] = info_proto->load_priority();
        config[milvus::index::ENABLE_MMAP] = info_proto->enable_mmap();
        if (info_proto->enable_mmap()) {
            config[milvus::index::MMAP_FILE_PATH] = info_proto->mmap_dir_path();
        }
        config[milvus::index::INDEX_SIZE] = info_proto->stats_size();

        milvus::storage::FileManagerContext file_ctx(
            field_meta, index_meta, remote_chunk_manager, fs);

        auto index =
            std::make_shared<milvus::index::JsonKeyStats>(file_ctx, true);
        {
            milvus::ScopedTimer timer(
                "json_stats_load",
                [](double ms) {
                    milvus::monitor::internal_json_stats_latency_load.Observe(
                        ms);
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

CStatus
AddFieldDataInfoForSealed(CSegmentInterface c_segment,
                          CLoadFieldDataInfo c_load_field_data_info) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        auto segment =
            dynamic_cast<milvus::segcore::SegmentSealed*>(segment_interface);
        AssertInfo(segment != nullptr, "segment conversion failed");
        auto load_info =
            static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
        segment->AddFieldDataInfoForSealed(*load_info);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::UnexpectedError, e.what());
    }
}

void
RemoveFieldFile(CSegmentInterface c_segment, int64_t field_id) {
    SCOPE_CGO_CALL_METRIC();

    auto segment = reinterpret_cast<milvus::segcore::SegmentSealed*>(c_segment);
    segment->RemoveFieldFile(milvus::FieldId(field_id));
}

CStatus
CreateTextIndex(CSegmentInterface c_segment, int64_t field_id) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        segment_interface->CreateTextIndex(milvus::FieldId(field_id));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::UnexpectedError, e.what());
    }
}

CStatus
FinishLoad(CSegmentInterface c_segment) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto segment_interface =
            reinterpret_cast<milvus::segcore::SegmentInterface*>(c_segment);
        segment_interface->FinishLoad();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::UnexpectedError, e.what());
    }
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
    bool nullable;
    int64_t dim;  // for vector types
    milvus::segcore::VectorBase* vec_base;
    milvus::segcore::ThreadSafeValidDataPtr valid_data;
    // For TEXT fields with spillover: reader for temp LOB file
    milvus::segcore::TextLobSpillover* text_lob_spillover = nullptr;
};

// get Arrow data type from Milvus data type
std::shared_ptr<arrow::DataType>
GetArrowType(milvus::DataType data_type, int64_t dim) {
    switch (data_type) {
        case milvus::DataType::BOOL:
            return arrow::boolean();
        case milvus::DataType::INT8:
            return arrow::int8();
        case milvus::DataType::INT16:
            return arrow::int16();
        case milvus::DataType::INT32:
            return arrow::int32();
        case milvus::DataType::INT64:
            return arrow::int64();
        case milvus::DataType::FLOAT:
            return arrow::float32();
        case milvus::DataType::DOUBLE:
            return arrow::float64();
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING:
        case milvus::DataType::TEXT:
            return arrow::utf8();
        case milvus::DataType::VECTOR_FLOAT:
            return arrow::fixed_size_binary(dim * sizeof(float));
        case milvus::DataType::VECTOR_BINARY:
            return arrow::fixed_size_binary(dim / 8);
        case milvus::DataType::VECTOR_FLOAT16:
            return arrow::fixed_size_binary(dim * sizeof(milvus::float16));
        case milvus::DataType::VECTOR_BFLOAT16:
            return arrow::fixed_size_binary(dim * sizeof(milvus::bfloat16));
        default:
            return nullptr;
    }
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
        default:
            return 0;  // variable length
    }
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
    // wrap data buffer (zero-copy)
    auto data_buffer = arrow::Buffer::Wrap(
        static_cast<const uint8_t*>(chunk_data), num_rows * element_size);

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
    const std::shared_ptr<arrow::DataType>& data_type,
    const milvus::segcore::ThreadSafeValidDataPtr& valid_data,
    int64_t validity_offset) {
    // wrap data buffer (zero-copy)
    auto data_buffer = arrow::Buffer::Wrap(
        static_cast<const uint8_t*>(chunk_data), num_rows * byte_width);

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

    for (int64_t i = 0; i < num_rows; i++) {
        int64_t offset = start_offset + i;
        if (valid_data && !valid_data->is_valid(offset)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            // Read LOB reference from ConcurrentVector
            auto ref_str = ref_vec->view_element(offset);
            // Decode and read actual text from spillover
            std::string text = spillover->DecodeAndRead(ref_str);
            ARROW_RETURN_NOT_OK(builder.Append(text.data(), text.length()));
        }
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
    const void* chunk_data = field_info.vec_base->get_chunk_data(chunk_id);
    int64_t element_size =
        GetElementByteWidth(field_info.data_type, field_info.dim);

    // adjust data pointer for offset within chunk
    const uint8_t* data_ptr = static_cast<const uint8_t*>(chunk_data) +
                              offset_in_chunk * element_size;

    switch (field_info.data_type) {
        case milvus::DataType::BOOL:
            return BuildBoolArrayForChunk(
                data_ptr, num_rows, field_info.valid_data, global_offset);

        case milvus::DataType::INT8:
            return WrapChunkAsArrowArray<arrow::Int8Array>(
                data_ptr, num_rows, 1, field_info.valid_data, global_offset);

        case milvus::DataType::INT16:
            return WrapChunkAsArrowArray<arrow::Int16Array>(
                data_ptr, num_rows, 2, field_info.valid_data, global_offset);

        case milvus::DataType::INT32:
            return WrapChunkAsArrowArray<arrow::Int32Array>(
                data_ptr, num_rows, 4, field_info.valid_data, global_offset);

        case milvus::DataType::INT64:
            return WrapChunkAsArrowArray<arrow::Int64Array>(
                data_ptr, num_rows, 8, field_info.valid_data, global_offset);

        case milvus::DataType::FLOAT:
            return WrapChunkAsArrowArray<arrow::FloatArray>(
                data_ptr, num_rows, 4, field_info.valid_data, global_offset);

        case milvus::DataType::DOUBLE:
            return WrapChunkAsArrowArray<arrow::DoubleArray>(
                data_ptr, num_rows, 8, field_info.valid_data, global_offset);

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

        case milvus::DataType::VECTOR_FLOAT:
        case milvus::DataType::VECTOR_BINARY:
        case milvus::DataType::VECTOR_FLOAT16:
        case milvus::DataType::VECTOR_BFLOAT16: {
            auto arrow_type =
                GetArrowType(field_info.data_type, field_info.dim);
            return WrapChunkAsFixedSizeBinaryArray(data_ptr,
                                                   num_rows,
                                                   element_size,
                                                   arrow_type,
                                                   field_info.valid_data,
                                                   global_offset);
        }

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

        if (start_offset < 0 || end_offset < start_offset) {
            return milvus::FailureCStatus(
                milvus::UnexpectedError,
                "invalid offsets: start_offset must be >= 0 and <= end_offset");
        }

        // no data to flush
        if (start_offset == end_offset) {
            result->manifest_path = nullptr;
            result->committed_version = 0;
            result->num_rows = 0;
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

        // get schema from segment
        auto& schema = growing_segment->get_schema();
        auto& insert_record = growing_segment->get_insert_record();

        int64_t total_rows = end_offset - start_offset;

        // collect field info
        std::vector<FieldInfo> field_infos;
        std::vector<std::shared_ptr<arrow::Field>> arrow_fields;

        for (auto& [field_id, field_meta] : schema.get_fields()) {
            if (field_id == RowFieldID || field_id == TimestampFieldID) {
                continue;  // skip system fields (RowID, Timestamp)
            }

            auto vec_base = insert_record.get_data_base(field_id);
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

            auto arrow_type =
                GetArrowType(field_meta.get_data_type(),
                             field_meta.is_vector() ? field_meta.get_dim() : 0);
            if (!arrow_type) {
                return milvus::FailureCStatus(
                    milvus::UnexpectedError,
                    fmt::format(
                        "unsupported data type: {} for field {} of segment {}",
                        field_meta.get_data_type(),
                        field_meta.get_name().get(),
                        growing_segment->get_segment_id()));
            }

            FieldInfo info;
            info.field_id = field_id;
            info.field_name = field_meta.get_name().get();
            info.data_type = field_meta.get_data_type();
            info.nullable = field_meta.is_nullable();
            info.dim = field_meta.is_vector() ? field_meta.get_dim() : 0;
            info.vec_base = vec_base;
            info.valid_data = nullptr;
            if (field_meta.is_nullable() &&
                insert_record.is_valid_data_exist(field_id)) {
                info.valid_data = insert_record.get_valid_data(field_id);
            }

            // For TEXT fields with spillover, get the spillover pointer
            info.text_lob_spillover = nullptr;
            if (field_meta.get_data_type() == milvus::DataType::TEXT &&
                growing_segment->HasTextLobSpillover(field_id)) {
                info.text_lob_spillover =
                    growing_segment->GetTextLobSpillover(field_id);
            }

            field_infos.push_back(std::move(info));

            // create Arrow field with metadata
            auto metadata = arrow::KeyValueMetadata::Make(
                {milvus_storage::ARROW_FIELD_ID_KEY},
                {std::to_string(field_id.get())});
            arrow_fields.push_back(arrow::field(field_meta.get_name().get(),
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
        writer_config.lob_base_path =
            config->lob_base_path ? config->lob_base_path : "";
        writer_config.read_version = config->read_version;
        writer_config.retry_limit =
            config->retry_limit > 0 ? config->retry_limit : 1;

        // add TEXT column configs
        for (size_t i = 0; i < config->num_text_columns; i++) {
            milvus_storage::text_column::TextColumnConfig text_config;
            text_config.field_id = config->text_field_ids[i];
            if (config->text_lob_paths && config->text_lob_paths[i]) {
                text_config.lob_base_path = config->text_lob_paths[i];
            }
            writer_config.text_columns[text_config.field_id] = text_config;
        }

        // get filesystem from singleton
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
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

        // close writer and get result
        auto close_result = writer->Close();
        if (!close_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          close_result.status().ToString());
        }
        auto cpp_result = close_result.ValueOrDie();

        // fill output
        result->manifest_path = strdup(cpp_result.manifest_path.c_str());
        result->committed_version = cpp_result.committed_version;
        result->num_rows = cpp_result.rows_written;

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
}
