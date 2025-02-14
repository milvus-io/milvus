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
#include "pb/clustering.pb.h"
#include "clustering/KmeansClustering.h"

using namespace milvus;

milvus::storage::StorageConfig
get_storage_config(const milvus::proto::clustering::StorageConfig& config) {
    auto storage_config = milvus::storage::StorageConfig();
    storage_config.address = std::string(config.address());
    storage_config.bucket_name = std::string(config.bucket_name());
    storage_config.access_key_id = std::string(config.access_keyid());
    storage_config.access_key_value = std::string(config.secret_access_key());
    storage_config.root_path = std::string(config.root_path());
    storage_config.storage_type = std::string(config.storage_type());
    storage_config.cloud_provider = std::string(config.cloud_provider());
    storage_config.iam_endpoint = std::string(config.iamendpoint());
    storage_config.cloud_provider = std::string(config.cloud_provider());
    storage_config.useSSL = config.usessl();
    storage_config.sslCACert = config.sslcacert();
    storage_config.useIAM = config.useiam();
    storage_config.region = config.region();
    storage_config.useVirtualHost = config.use_virtual_host();
    storage_config.requestTimeoutMs = config.request_timeout_ms();
    storage_config.gcp_credential_json =
        std::string(config.gcpcredentialjson());
    return storage_config;
}

CStatus
Analyze(CAnalyze* res_analyze,
        const uint8_t* serialized_analyze_info,
        const uint64_t len) {
    try {
        auto analyze_info =
            std::make_unique<milvus::proto::clustering::AnalyzeInfo>();
        auto res = analyze_info->ParseFromArray(serialized_analyze_info, len);
        AssertInfo(res, "Unmarshal analyze info failed");
        auto field_type =
            static_cast<DataType>(analyze_info->field_schema().data_type());
        auto field_id = analyze_info->field_schema().fieldid();

        // init file manager
        milvus::storage::FieldDataMeta field_meta{analyze_info->collectionid(),
                                                  analyze_info->partitionid(),
                                                  0,
                                                  field_id};

        milvus::storage::IndexMeta index_meta{
            0, field_id, analyze_info->buildid(), analyze_info->version()};
        auto storage_config =
            get_storage_config(analyze_info->storage_config());
        auto chunk_manager =
            milvus::storage::CreateChunkManager(storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager);

        if (field_type != DataType::VECTOR_FLOAT) {
            throw SegcoreError(
                DataTypeInvalid,
                fmt::format("invalid data type for clustering is {}",
                            std::to_string(int(field_type))));
        }
        auto clusteringJob =
            std::make_unique<milvus::clustering::KmeansClustering>(
                fileManagerContext);

        clusteringJob->Run<float>(*analyze_info);
        *res_analyze = clusteringJob.release();
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (SegcoreError& e) {
        auto status = CStatus();
        status.error_code = e.get_error_code();
        status.error_msg = strdup(e.what());
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
            reinterpret_cast<milvus::clustering::KmeansClustering*>(analyze);
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
GetAnalyzeResultMeta(CAnalyze analyze,
                     char** centroid_path,
                     int64_t* centroid_file_size,
                     void* id_mapping_paths,
                     int64_t* id_mapping_sizes) {
    auto status = CStatus();
    try {
        AssertInfo(analyze,
                   "failed to serialize analyze to binary set, passed index "
                   "was null");
        auto real_analyze =
            reinterpret_cast<milvus::clustering::KmeansClustering*>(analyze);
        auto res = real_analyze->GetClusteringResultMeta();
        *centroid_path = res.centroid_path.data();
        *centroid_file_size = res.centroid_file_size;

        auto& map_ = res.id_mappings;
        const char** id_mapping_paths_ = (const char**)id_mapping_paths;
        size_t i = 0;
        for (auto it = map_.begin(); it != map_.end(); ++it, i++) {
            id_mapping_paths_[i] = it->first.data();
            id_mapping_sizes[i] = it->second;
        }
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}
