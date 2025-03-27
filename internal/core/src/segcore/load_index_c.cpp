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

#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "log/Log.h"
#include "storage/FileManager.h"
#include "segcore/Types.h"
#include "storage/Util.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "pb/cgo_msg.pb.h"
#include "knowhere/index/index_static.h"
#include "knowhere/comp/knowhere_check.h"

bool
IsLoadWithDisk(const char* index_type, int index_engine_version) {
    return knowhere::UseDiskLoad(index_type, index_engine_version) ||
           strcmp(index_type, milvus::index::INVERTED_INDEX_TYPE) == 0;
}

CStatus
NewLoadIndexInfo(CLoadIndexInfo* c_load_index_info) {
    try {
        auto load_index_info =
            std::make_unique<milvus::segcore::LoadIndexInfo>();

        *c_load_index_info = load_index_info.release();
        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

void
DeleteLoadIndexInfo(CLoadIndexInfo c_load_index_info) {
    auto info = (milvus::segcore::LoadIndexInfo*)c_load_index_info;
    delete info;
}

CStatus
AppendIndexParam(CLoadIndexInfo c_load_index_info,
                 const char* c_index_key,
                 const char* c_index_value) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        std::string index_key(c_index_key);
        std::string index_value(c_index_value);
        load_index_info->index_params[index_key] = index_value;

        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
AppendFieldInfo(CLoadIndexInfo c_load_index_info,
                int64_t collection_id,
                int64_t partition_id,
                int64_t segment_id,
                int64_t field_id,
                enum CDataType field_type,
                bool enable_mmap,
                const char* mmap_dir_path) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        load_index_info->collection_id = collection_id;
        load_index_info->partition_id = partition_id;
        load_index_info->segment_id = segment_id;
        load_index_info->field_id = field_id;
        load_index_info->field_type = milvus::DataType(field_type);
        load_index_info->enable_mmap = enable_mmap;
        load_index_info->mmap_dir_path = std::string(mmap_dir_path);

        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
appendVecIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        auto binary_set = (knowhere::BinarySet*)c_binary_set;
        auto& index_params = load_index_info->index_params;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = load_index_info->field_type;
        index_info.index_engine_version = load_index_info->index_engine_version;

        // get index type
        AssertInfo(index_params.find("index_type") != index_params.end(),
                   "index type is empty");
        index_info.index_type = index_params.at("index_type");

        // get metric type
        AssertInfo(index_params.find("metric_type") != index_params.end(),
                   "metric type is empty");
        index_info.metric_type = index_params.at("metric_type");

        // init file manager
        milvus::storage::FieldDataMeta field_meta{
            load_index_info->collection_id,
            load_index_info->partition_id,
            load_index_info->segment_id,
            load_index_info->field_id};
        milvus::storage::IndexMeta index_meta{load_index_info->segment_id,
                                              load_index_info->field_id,
                                              load_index_info->index_build_id,
                                              load_index_info->index_version};
        auto remote_chunk_manager =
            milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();

        auto config = milvus::index::ParseConfigFromIndexParams(
            load_index_info->index_params);
        config["index_files"] = load_index_info->index_files;

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, remote_chunk_manager);
        fileManagerContext.set_for_loading_index(true);

        load_index_info->index =
            milvus::index::IndexFactory::GetInstance().CreateIndex(
                index_info, fileManagerContext);
        load_index_info->index->Load(*binary_set, config);
        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
appendScalarIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        auto field_type = load_index_info->field_type;
        auto binary_set = (knowhere::BinarySet*)c_binary_set;
        auto& index_params = load_index_info->index_params;
        bool find_index_type =
            index_params.count("index_type") > 0 ? true : false;
        AssertInfo(find_index_type == true,
                   "Can't find index type in index_params");

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = milvus::DataType(field_type);
        index_info.index_type = index_params["index_type"];

        auto config = milvus::index::ParseConfigFromIndexParams(
            load_index_info->index_params);

        // Config should have value for milvus::index::SCALAR_INDEX_ENGINE_VERSION for production calling chain.
        // Use value_or(1) for unit test without setting this value
        index_info.scalar_index_engine_version =
            milvus::index::GetValueFromConfig<int32_t>(
                config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
                .value_or(1);

        index_info.tantivy_index_version =
            milvus::index::GetValueFromConfig<int32_t>(
                config, milvus::index::TANTIVY_INDEX_VERSION)
                .value_or(milvus::index::TANTIVY_INDEX_LATEST_VERSION);

        load_index_info->index =
            milvus::index::IndexFactory::GetInstance().CreateIndex(
                index_info, milvus::storage::FileManagerContext());
        load_index_info->index->Load(*binary_set);
        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

LoadResourceRequest
EstimateLoadIndexResource(CLoadIndexInfo c_load_index_info) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        auto field_type = load_index_info->field_type;
        auto& index_params = load_index_info->index_params;
        bool find_index_type =
            index_params.count("index_type") > 0 ? true : false;
        AssertInfo(find_index_type == true,
                   "Can't find index type in index_params");

        LoadResourceRequest request =
            milvus::index::IndexFactory::GetInstance().IndexLoadResource(
                field_type,
                load_index_info->index_engine_version,
                load_index_info->index_size,
                index_params,
                load_index_info->enable_mmap);
        return request;
    } catch (std::exception& e) {
        PanicInfo(milvus::UnexpectedError,
                  fmt::format("failed to estimate index load resource, "
                              "encounter exception : {}",
                              e.what()));
        return LoadResourceRequest{0, 0, 0, 0, false};
    }
}

CStatus
AppendIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set) {
    auto load_index_info = (milvus::segcore::LoadIndexInfo*)c_load_index_info;
    auto field_type = load_index_info->field_type;
    if (milvus::IsVectorDataType(field_type)) {
        return appendVecIndex(c_load_index_info, c_binary_set);
    }
    return appendScalarIndex(c_load_index_info, c_binary_set);
}

CStatus
AppendIndexV2(CTraceContext c_trace, CLoadIndexInfo c_load_index_info) {
    try {
        auto load_index_info =
            static_cast<milvus::segcore::LoadIndexInfo*>(c_load_index_info);
        auto& index_params = load_index_info->index_params;
        auto field_type = load_index_info->field_type;
        auto engine_version = load_index_info->index_engine_version;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = load_index_info->field_type;
        index_info.index_engine_version = engine_version;

        auto config = milvus::index::ParseConfigFromIndexParams(
            load_index_info->index_params);

        // Config should have value for milvus::index::SCALAR_INDEX_ENGINE_VERSION for production calling chain.
        // Use value_or(1) for unit test without setting this value
        index_info.scalar_index_engine_version =
            milvus::index::GetValueFromConfig<int32_t>(
                config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
                .value_or(1);

        index_info.tantivy_index_version =
            milvus::index::GetValueFromConfig<int32_t>(
                config, milvus::index::TANTIVY_INDEX_VERSION)
                .value_or(milvus::index::TANTIVY_INDEX_LATEST_VERSION);

        auto ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        auto span = milvus::tracer::StartSpan("SegCoreLoadIndex", &ctx);
        milvus::tracer::SetRootSpan(span);

        LOG_INFO(
            "[collection={}][segment={}][field={}][enable_mmap={}] load index "
            "{}",
            load_index_info->collection_id,
            load_index_info->segment_id,
            load_index_info->field_id,
            load_index_info->enable_mmap,
            load_index_info->index_id);

        // get index type
        AssertInfo(index_params.find("index_type") != index_params.end(),
                   "index type is empty");
        index_info.index_type = index_params.at("index_type");

        // get metric type
        if (milvus::IsVectorDataType(field_type)) {
            AssertInfo(index_params.find("metric_type") != index_params.end(),
                       "metric type is empty for vector index");
            index_info.metric_type = index_params.at("metric_type");
        }

        // init file manager
        milvus::storage::FieldDataMeta field_meta{
            load_index_info->collection_id,
            load_index_info->partition_id,
            load_index_info->segment_id,
            load_index_info->field_id,
            load_index_info->schema};
        milvus::storage::IndexMeta index_meta{load_index_info->segment_id,
                                              load_index_info->field_id,
                                              load_index_info->index_build_id,
                                              load_index_info->index_version};
        auto remote_chunk_manager =
            milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();

        config[milvus::index::INDEX_FILES] = load_index_info->index_files;

        if (load_index_info->field_type == milvus::DataType::JSON) {
            index_info.json_cast_type = static_cast<milvus::DataType>(
                std::stoi(config.at(JSON_CAST_TYPE).get<std::string>()));
            index_info.json_path = config.at(JSON_PATH).get<std::string>();
        }
        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, remote_chunk_manager);
        fileManagerContext.set_for_loading_index(true);

        load_index_info->index =
            milvus::index::IndexFactory::GetInstance().CreateIndex(
                index_info, fileManagerContext);

        if (load_index_info->enable_mmap &&
            load_index_info->index->IsMmapSupported()) {
            AssertInfo(!load_index_info->mmap_dir_path.empty(),
                       "mmap directory path is empty");
            auto filepath =
                std::filesystem::path(load_index_info->mmap_dir_path) /
                "index_files" / std::to_string(load_index_info->index_id) /
                std::to_string(load_index_info->segment_id) /
                std::to_string(load_index_info->field_id);

            config[milvus::index::ENABLE_MMAP] = "true";
            config[milvus::index::MMAP_FILE_PATH] = filepath.string();
        }

        LOG_DEBUG("load index with configs: {}", config.dump());
        load_index_info->index->Load(ctx, config);

        span->End();
        milvus::tracer::CloseRootSpan();

        LOG_INFO(
            "[collection={}][segment={}][field={}][enable_mmap={}] load index "
            "{} done",
            load_index_info->collection_id,
            load_index_info->segment_id,
            load_index_info->field_id,
            load_index_info->enable_mmap,
            load_index_info->index_id);

        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
AppendIndexFilePath(CLoadIndexInfo c_load_index_info, const char* c_file_path) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        std::string index_file_path(c_file_path);
        load_index_info->index_files.emplace_back(index_file_path);

        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
AppendIndexInfo(CLoadIndexInfo c_load_index_info,
                int64_t index_id,
                int64_t build_id,
                int64_t version) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        load_index_info->index_id = index_id;
        load_index_info->index_build_id = build_id;
        load_index_info->index_version = version;

        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
AppendIndexEngineVersionToLoadInfo(CLoadIndexInfo c_load_index_info,
                                   int32_t index_engine_version) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        load_index_info->index_engine_version = index_engine_version;

        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
CleanLoadedIndex(CLoadIndexInfo c_load_index_info) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        auto local_chunk_manager =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager();
        auto index_file_path_prefix =
            milvus::storage::GenIndexPathPrefix(local_chunk_manager,
                                                load_index_info->index_build_id,
                                                load_index_info->index_version);
        local_chunk_manager->RemoveDir(index_file_path_prefix);
        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

void
AppendStorageInfo(CLoadIndexInfo c_load_index_info,
                  const char* uri,
                  int64_t version) {
    auto load_index_info = (milvus::segcore::LoadIndexInfo*)c_load_index_info;
    load_index_info->uri = uri;
    load_index_info->index_store_version = version;
}

CStatus
FinishLoadIndexInfo(CLoadIndexInfo c_load_index_info,
                    const uint8_t* serialized_load_index_info,
                    const uint64_t len) {
    try {
        auto info_proto = std::make_unique<milvus::proto::cgo::LoadIndexInfo>();
        info_proto->ParseFromArray(serialized_load_index_info, len);
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
            load_index_info->enable_mmap = info_proto->enable_mmap();
            load_index_info->mmap_dir_path = info_proto->mmap_dir_path();
            load_index_info->index_id = info_proto->indexid();
            load_index_info->index_build_id = info_proto->index_buildid();
            load_index_info->index_version = info_proto->index_version();
            for (const auto& [k, v] : info_proto->index_params()) {
                load_index_info->index_params[k] = v;
            }
            load_index_info->index_files.assign(
                info_proto->index_files().begin(),
                info_proto->index_files().end());
            load_index_info->uri = info_proto->uri();
            load_index_info->index_store_version =
                info_proto->index_store_version();
            load_index_info->index_engine_version =
                info_proto->index_engine_version();
            load_index_info->schema = info_proto->field();
            load_index_info->index_size = info_proto->index_file_size();
        }
        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}
