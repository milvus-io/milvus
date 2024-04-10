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

#include <memory>

#ifdef __linux__
#include <malloc.h>
#endif

#include "analyze_c.h"
#include "common/type_c.h"
#include "type_c.h"
#include "types.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "storage/Util.h"
#include "indexbuilder/IndexFactory.h"

using namespace milvus;

CStatus
Analyze(CAnalyze* res_analyze, CAnalyzeInfo c_analyze_info) {
    try {
        auto analyze_info = (AnalyzeInfo*)c_analyze_info;
        auto field_type = analyze_info->field_type;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = analyze_info->field_type;

        auto& config = analyze_info->config;
        config["insert_files"] = analyze_info->insert_files;
        config["segment_size"] = analyze_info->segment_size;
        config["train_size"] = analyze_info->train_size;

        //        auto engine_version = analyze_info->index_engine_version;
        //
        //        //        index_info.index_engine_version = engine_version;
        //        config[milvus::index::INDEX_ENGINE_VERSION] =
        //            std::to_string(engine_version);

        // init file manager
        milvus::storage::FieldDataMeta field_meta{analyze_info->collection_id,
                                                  analyze_info->partition_id,
                                                  0,
                                                  analyze_info->field_id};

        milvus::storage::IndexMeta index_meta{0,
                                              analyze_info->field_id,
                                              analyze_info->task_id,
                                              analyze_info->version};
        auto chunk_manager =
            milvus::storage::CreateChunkManager(analyze_info->storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager);

        auto compactionJob =
            milvus::indexbuilder::IndexFactory::GetInstance()
                .CreateCompactionJob(
                    analyze_info->field_type, config, fileManagerContext);
        compactionJob->Train();
        *res_analyze = compactionJob.release();
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
DeleteAnalyze(CAnalyze analyze) {
    auto status = CStatus();
    try {
        AssertInfo(analyze, "failed to delete analyze, passed index was null");
        auto real_analyze =
            reinterpret_cast<milvus::indexbuilder::MajorCompaction*>(analyze);
        delete real_analyze;
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
NewAnalyzeInfo(CAnalyzeInfo* c_analyze_info, CStorageConfig c_storage_config) {
    try {
        auto analyze_info = std::make_unique<AnalyzeInfo>();
        auto& storage_config = analyze_info->storage_config;
        storage_config.address = std::string(c_storage_config.address);
        storage_config.bucket_name = std::string(c_storage_config.bucket_name);
        storage_config.access_key_id =
            std::string(c_storage_config.access_key_id);
        storage_config.access_key_value =
            std::string(c_storage_config.access_key_value);
        storage_config.root_path = std::string(c_storage_config.root_path);
        storage_config.storage_type =
            std::string(c_storage_config.storage_type);
        storage_config.cloud_provider =
            std::string(c_storage_config.cloud_provider);
        storage_config.iam_endpoint =
            std::string(c_storage_config.iam_endpoint);
        storage_config.cloud_provider =
            std::string(c_storage_config.cloud_provider);
        storage_config.useSSL = c_storage_config.useSSL;
        storage_config.useIAM = c_storage_config.useIAM;
        storage_config.region = c_storage_config.region;
        storage_config.useVirtualHost = c_storage_config.useVirtualHost;
        storage_config.requestTimeoutMs = c_storage_config.requestTimeoutMs;

        *c_analyze_info = analyze_info.release();
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

void
DeleteAnalyzeInfo(CAnalyzeInfo c_analyze_info) {
    auto info = (AnalyzeInfo*)c_analyze_info;
    delete info;
}

CStatus
AppendAnalyzeInfo(CAnalyzeInfo c_analyze_info,
                  int64_t collection_id,
                  int64_t partition_id,
                  int64_t field_id,
                  int64_t task_id,
                  int64_t version,
                  const char* field_name,
                  enum CDataType field_type,
                  int64_t dim,
                  int64_t segment_size,
                  int64_t train_size) {
    try {
        auto analyze_info = (AnalyzeInfo*)c_analyze_info;
        analyze_info->collection_id = collection_id;
        analyze_info->partition_id = partition_id;
        analyze_info->field_id = field_id;
        analyze_info->task_id = task_id;
        analyze_info->version = version;
        analyze_info->field_type = milvus::DataType(field_type);
        analyze_info->field_name = field_name;
        analyze_info->dim = dim;
        analyze_info->segment_size = segment_size;
        analyze_info->train_size = train_size;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendSegmentInsertFile(CAnalyzeInfo c_analyze_info,
                        int64_t segID,
                        const char* c_file_path) {
    try {
        auto analyze_info = (AnalyzeInfo*)c_analyze_info;
        std::string insert_file_path(c_file_path);
        analyze_info->insert_files[segID].emplace_back(insert_file_path);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendSegmentNumRows(CAnalyzeInfo c_analyze_info,
                     int64_t segID,
                     int64_t num_rows) {
    try {
        auto analyze_info = (AnalyzeInfo*)c_analyze_info;
        analyze_info->num_rows[segID] = num_rows;

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
SerializeAnalyzeAndUpLoad(CAnalyze analyze, CBinarySet* c_binary_set) {
    auto status = CStatus();
    try {
        AssertInfo(analyze,
                   "failed to serialize analyze to binary set, passed index "
                   "was null");
        auto real_analyze =
            reinterpret_cast<milvus::indexbuilder::MajorCompaction*>(analyze);
        auto binary =
            std::make_unique<knowhere::BinarySet>(real_analyze->Upload());
        *c_binary_set = binary.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}
