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

#include "segcore/load_index_c.h"

#include <folly/ExceptionWrapper.h>
#include <folly/ScopeGuard.h>
#include <stdlib.h>
#include <string.h>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iosfwd>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "cachinglayer/Manager.h"
#include "cachinglayer/Utils.h"
#include "common/CGoCatch.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "glog/logging.h"
#include "index/LoadResource.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/utils.h"
#include "log/Log.h"
#include "monitor/scope_metric.h"
#include "nlohmann/json.hpp"
#include "opentelemetry/trace/span.h"
#include "pb/cgo_msg.pb.h"
#include "pb/schema.pb.h"
#include "segcore/Types.h"
#include "segcore/Utils.h"
#include "storage/FileManager.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Util.h"

namespace {

CStatus
FailureCStatusNoThrow(int error_code, const char* message) noexcept {
    CStatus status{};
    status.error_code = error_code;
    status.error_msg = message == nullptr ? nullptr : strdup(message);
    return status;
}

}  // namespace

CStatus
IsLoadWithDisk(const char* index_type,
               int index_engine_version,
               bool* is_load_with_disk) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(is_load_with_disk != nullptr,
                   "load-with-disk output is null");
        *is_load_with_disk = false;
        AssertInfo(index_type != nullptr, "index type is null");
        *is_load_with_disk =
            knowhere::UseDiskLoad(index_type, index_engine_version) ||
            strcmp(index_type, milvus::index::INVERTED_INDEX_TYPE) == 0;
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
NewLoadIndexInfo(CLoadIndexInfo* c_load_index_info) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(c_load_index_info != nullptr,
                   "load index info output is null");
        *c_load_index_info = nullptr;
        auto load_index_info =
            std::make_unique<milvus::segcore::LoadIndexInfo>();

        *c_load_index_info = load_index_info.release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

void
DeleteLoadIndexInfo(CLoadIndexInfo c_load_index_info) {
    SCOPE_CGO_CALL_METRIC();

    auto info = (milvus::segcore::LoadIndexInfo*)c_load_index_info;
    delete info;
}

CStatus
EstimateLoadIndexResource(CLoadIndexInfo c_load_index_info,
                          LoadResourceRequest* request) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(request != nullptr, "load resource output is null");
        *request = LoadResourceRequest{};
        AssertInfo(c_load_index_info != nullptr, "load index info is null");
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        auto field_type = load_index_info->field_type;
        auto element_type = load_index_info->element_type;
        auto& index_params = load_index_info->index_params;
        bool find_index_type =
            index_params.count("index_type") > 0 ? true : false;
        if (!(find_index_type == true)) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "Can't find index type in index_params");
        }

        // Segment Loader calls this API while deciding whether a segment may
        // start loading. Keep that admission path metadata-only: exact scalar
        // V3 directory inspection belongs to SealedIndexTranslator, where the
        // result is used for the actual MCL loading reservation.
        *request = milvus::index::IndexLoadResource(
            field_type,
            element_type,
            load_index_info->index_engine_version,
            load_index_info->index_size,
            index_params,
            load_index_info->enable_mmap,
            load_index_info->num_rows,
            load_index_info->dim);
        return milvus::SuccessCStatus();
    }
    // Estimation failure must reach the Go caller as an error: a swallowed
    // exception here would read as a zero-resource estimate and let the
    // segment pass load admission without any memory/disk reservation.
    CGO_CATCH_AND_RETURN_CSTATUS
}

bool
TryReserveLoadingResourceWithTimeout(CResourceUsage size,
                                     int64_t millisecond_timeout) {
    // Failure direction is safe: false means "reservation failed" and the Go
    // caller backs off, whereas an escaping exception would cross the C ABI
    // and terminate the process.
    try {
        return milvus::cachinglayer::Manager::GetInstance()
            .ReserveLoadingResourceWithTimeout(
                milvus::cachinglayer::ResourceUsage(size.memory_bytes,
                                                    size.disk_bytes),
                std::chrono::milliseconds(millisecond_timeout));
    }
    CGO_CATCH_AND_LOG("TryReserveLoadingResourceWithTimeout")
    return false;
}

void
ReleaseLoadingResource(CResourceUsage size) {
    try {
        milvus::cachinglayer::Manager::GetInstance().ReleaseLoadingResource(
            milvus::cachinglayer::ResourceUsage(size.memory_bytes,
                                                size.disk_bytes));
    }
    CGO_CATCH_AND_LOG("ReleaseLoadingResource")
}

void
ChargeLoadedResource(CResourceUsage size) {
    try {
        milvus::cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            milvus::cachinglayer::ResourceUsage(size.memory_bytes,
                                                size.disk_bytes));
    }
    CGO_CATCH_AND_LOG("ChargeLoadedResource")
}

void
RefundLoadedResource(CResourceUsage size) {
    try {
        milvus::cachinglayer::Manager::GetInstance().RefundLoadedResource(
            milvus::cachinglayer::ResourceUsage(size.memory_bytes,
                                                size.disk_bytes));
    }
    CGO_CATCH_AND_LOG("RefundLoadedResource")
}

CStatus
AppendIndexV2(CTraceContext c_trace, CLoadIndexInfo c_load_index_info) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto load_index_info =
            static_cast<milvus::segcore::LoadIndexInfo*>(c_load_index_info);
        AssertInfo(load_index_info != nullptr, "load index info is null");

        auto ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        auto span = milvus::tracer::StartSpan("SegCoreLoadIndex", &ctx);
        milvus::tracer::SetRootSpan(span);
        auto trace_guard = folly::makeGuard([&]() {
            span->End();
            milvus::tracer::CloseRootSpan();
        });

        LoadIndexData(ctx, load_index_info);

        LOG_INFO(
            "[collection={}][segment={}][field={}][enable_mmap={}] load index "
            "{} done, mmap_dir_path={}",
            load_index_info->collection_id,
            load_index_info->segment_id,
            load_index_info->field_id,
            load_index_info->enable_mmap,
            load_index_info->index_id,
            load_index_info->mmap_dir_path);

        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
CleanLoadedIndex(CLoadIndexInfo c_load_index_info) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        AssertInfo(load_index_info != nullptr, "load index info is null");
        load_index_info->cache_index.reset();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
FinishLoadIndexInfo(CLoadIndexInfo c_load_index_info,
                    const uint8_t* serialized_load_index_info,
                    const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(c_load_index_info != nullptr, "load index info is null");
        AssertInfo(serialized_load_index_info != nullptr,
                   "serialized load index info is null");
        AssertInfo(len <= std::numeric_limits<int>::max(),
                   "serialized load index info is too large");
        auto info_proto = std::make_unique<milvus::proto::cgo::LoadIndexInfo>();
        AssertInfo(info_proto->ParseFromArray(serialized_load_index_info,
                                              static_cast<int>(len)),
                   "failed to parse serialized load index info");
        auto load_index_info =
            static_cast<milvus::segcore::LoadIndexInfo*>(c_load_index_info);
        // TODO: keep this since LoadIndexInfo is used by SegmentSealed.
        {
            load_index_info->collection_id = info_proto->collectionid();
            load_index_info->partition_id = info_proto->partitionid();
            load_index_info->segment_id = info_proto->segmentid();
            load_index_info->field_id = info_proto->field().fieldid();
            load_index_info->field_type =
                static_cast<milvus::DataType>(info_proto->field().data_type());
            load_index_info->element_type = static_cast<milvus::DataType>(
                info_proto->field().element_type());
            load_index_info->enable_mmap = info_proto->enable_mmap();
            load_index_info->index_id = info_proto->indexid();
            load_index_info->index_build_id = info_proto->index_buildid();
            load_index_info->index_version = info_proto->index_version();
            load_index_info->index_store_path_version =
                info_proto->index_store_path_version();
            for (const auto& [k, v] : info_proto->index_params()) {
                load_index_info->index_params[k] = v;
            }
            load_index_info->index_files.assign(
                info_proto->index_files().begin(),
                info_proto->index_files().end());
            load_index_info->uri = info_proto->uri();
            load_index_info->index_engine_version =
                info_proto->index_engine_version();
            // Inject scalar index version into index_params for scalar indexes
            auto scalar_version = info_proto->current_scalar_index_version();
            if (scalar_version > 0) {
                load_index_info
                    ->index_params[milvus::index::SCALAR_INDEX_ENGINE_VERSION] =
                    std::to_string(scalar_version);
            }
            load_index_info->schema = info_proto->field();
            load_index_info->index_size = info_proto->index_file_size();
            load_index_info->num_rows = info_proto->num_rows();
            auto field_schema =
                milvus::FieldMeta::ParseFrom(load_index_info->schema);
            size_t dim = IsVectorDataType(field_schema.get_data_type()) &&
                                 !IsSparseFloatVectorDataType(
                                     field_schema.get_data_type())
                             ? field_schema.get_dim()
                             : 1;
            load_index_info->dim = dim;
            // Extract warmup_policy from index_params (keep it for Knowhere)
            auto warmup_it = load_index_info->index_params.find("warmup");
            if (warmup_it != load_index_info->index_params.end()) {
                load_index_info->warmup_policy = warmup_it->second;
                LOG_INFO("Index warmup_policy extracted from index_params: {}",
                         load_index_info->warmup_policy);
            } else {
                LOG_INFO(
                    "No warmup key in index_params, warmup_policy will be "
                    "empty");
            }

            auto remote_chunk_manager =
                milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                    .GetRemoteChunkManager();
            load_index_info->mmap_dir_path =
                milvus::storage::LocalChunkManagerSingleton::GetInstance()
                    .GetChunkManager()
                    ->GetRootPath();
        }
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
SetLoadIndexInfoShard(CLoadIndexInfo c_load_index_info, const char* shard) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto load_index_info =
            static_cast<milvus::segcore::LoadIndexInfo*>(c_load_index_info);
        AssertInfo(load_index_info != nullptr, "load index info is null");
        load_index_info->shard = shard == nullptr ? "" : shard;
        return milvus::SuccessCStatus();
    } catch (milvus::SegcoreError& e) {
        return milvus::FailureCStatus(&e);
    } catch (std::bad_alloc& e) {
        return FailureCStatusNoThrow(milvus::MemAllocateFailed, e.what());
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    } catch (...) {
        return FailureCStatusNoThrow(milvus::UnexpectedError,
                                     "unknown exception");
    }
}
