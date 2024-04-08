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

#include "analysis_c.h"
#include "common/type_c.h"
#include "type_c.h"
#include "types.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "storage/Util.h"
#include "indexbuilder/IndexFactory.h"

using namespace milvus;

CStatus
Analysis(CAnalysis* res_analysis, CAnalysisInfo c_analysis_info) {
    try {
        auto analysis_info = (AnalysisInfo*)c_analysis_info;
        auto field_type = analysis_info->field_type;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = analysis_info->field_type;

        auto& config = analysis_info->config;
        config["insert_files"] = analysis_info->insert_files;
        config["segment_size"] = analysis_info->segment_size;
        config["train_size"] = analysis_info->train_size;

        // get index type
        //        auto index_type = milvus::index::GetValueFromConfig<std::string>(
        //            config, "index_type");
        //        AssertInfo(index_type.has_value(), "index type is empty");
        //        index_info.index_type = index_type.value();

        auto engine_version = analysis_info->index_engine_version;

        //        index_info.index_engine_version = engine_version;
        config[milvus::index::INDEX_ENGINE_VERSION] =
            std::to_string(engine_version);

        // get metric type
        //        if (milvus::datatype_is_vector(field_type)) {
        //            auto metric_type = milvus::index::GetValueFromConfig<std::string>(
        //                config, "metric_type");
        //            AssertInfo(metric_type.has_value(), "metric type is empty");
        //            index_info.metric_type = metric_type.value();
        //        }

        // init file manager
        milvus::storage::FieldDataMeta field_meta{analysis_info->collection_id,
                                                  analysis_info->partition_id,
                                                  0,
                                                  analysis_info->field_id};

        milvus::storage::IndexMeta index_meta{0,
                                              analysis_info->field_id,
                                              analysis_info->task_id,
                                              analysis_info->version};
        auto chunk_manager =
            milvus::storage::CreateChunkManager(analysis_info->storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager);

        auto compactionJob =
            milvus::indexbuilder::IndexFactory::GetInstance()
                .CreateCompactionJob(
                    analysis_info->field_type, config, fileManagerContext);
        compactionJob->Train();
        *res_analysis = compactionJob.release();
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
DeleteAnalysis(CAnalysis analysis) {
    auto status = CStatus();
    try {
        AssertInfo(analysis,
                   "failed to delete analysis, passed index was null");
        auto real_analysis =
            reinterpret_cast<milvus::indexbuilder::MajorCompaction*>(analysis);
        delete real_analysis;
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
CleanAnalysisLocalData(CAnalysis analysis) {
    auto status = CStatus();
    try {
        AssertInfo(analysis, "failed to build analysis, passed index was null");
        auto real_analysis =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(analysis);
        auto cAnalysis =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_analysis);
        cAnalysis->CleanLocalData();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
NewAnalysisInfo(CAnalysisInfo* c_analysis_info,
                CStorageConfig c_storage_config) {
    try {
        auto analysis_info = std::make_unique<AnalysisInfo>();
        auto& storage_config = analysis_info->storage_config;
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

        *c_analysis_info = analysis_info.release();
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
DeleteAnalysisInfo(CAnalysisInfo c_analysis_info) {
    auto info = (AnalysisInfo*)c_analysis_info;
    delete info;
}

CStatus
AppendAnalysisFieldMetaInfo(CAnalysisInfo c_analysis_info,
                            int64_t collection_id,
                            int64_t partition_id,
                    int64_t field_id,
                    const char* field_name,
                    enum CDataType field_type,
                    int64_t dim) {
    try {
        auto analysis_info = (AnalysisInfo*)c_analysis_info;
        analysis_info->collection_id = collection_id;
        analysis_info->partition_id = partition_id;
        analysis_info->field_id = field_id;
        analysis_info->field_type = milvus::DataType(field_type);
        analysis_info->field_name = field_name;
        analysis_info->dim = dim;

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendAnalysisInfo(CAnalysisInfo c_analysis_info,
                   int64_t task_id,
                   int64_t version) {
    try {
        auto analysis_info = (AnalysisInfo*)c_analysis_info;
        analysis_info->task_id = task_id;
        analysis_info->version = version;

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
AppendSegmentID(CAnalysisInfo c_analysis_info, int64_t segment_id) {
    try {
        auto analysis_info = (AnalysisInfo*)c_analysis_info;
        //        analysis_info->segment_ids.emplace_back(segment_id);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendSegmentInsertFile(CAnalysisInfo c_analysis_info,
                        int64_t segID,
                        const char* c_file_path) {
    try {
        auto analysis_info = (AnalysisInfo*)c_analysis_info;
        std::string insert_file_path(c_file_path);
        analysis_info->insert_files[segID].emplace_back(insert_file_path);
        //        analysis_info->insert_files.emplace_back(insert_file_path);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendSegmentSize(CAnalysisInfo c_analysis_info, int64_t size) {
    try {
        auto analysis_info = (AnalysisInfo*)c_analysis_info;
        analysis_info->segment_size = size;

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendTrainSize(CAnalysisInfo c_analysis_info, int64_t size) {
    try {
        auto analysis_info = (AnalysisInfo*)c_analysis_info;
        analysis_info->train_size = size;

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
SerializeAnalysisAndUpLoad(CAnalysis analysis) {
    auto status = CStatus();
    try {
        AssertInfo(analysis,
                   "failed to serialize analysis to binary set, passed index "
                   "was null");
        auto real_analysis =
            reinterpret_cast<milvus::indexbuilder::MajorCompaction*>(analysis);
        auto binary =
            std::make_unique<knowhere::BinarySet>(real_analysis->Upload());
        //        *c_binary_set = binary.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}
