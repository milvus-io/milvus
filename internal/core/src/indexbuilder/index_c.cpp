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

#include <glog/logging.h>
#include <memory>
#include <string>
#include "fmt/core.h"
#include "indexbuilder/type_c.h"
#include "log/Log.h"

#ifdef __linux__
#include <malloc.h>
#endif

#include "common/EasyAssert.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"

#include "index/TextMatchIndex.h"

#include "indexbuilder/IndexFactory.h"
#include "common/type_c.h"
#include "storage/Types.h"
#include "indexbuilder/types.h"
#include "index/Utils.h"
#include "pb/index_cgo_msg.pb.h"
#include "storage/Util.h"
#include "index/Meta.h"

using namespace milvus;
CStatus
CreateIndexV0(enum CDataType dtype,
              const char* serialized_type_params,
              const char* serialized_index_params,
              CIndex* res_index) {
    auto status = CStatus();
    try {
        AssertInfo(res_index, "failed to create index, passed index was null");

        milvus::proto::indexcgo::TypeParams type_params;
        milvus::proto::indexcgo::IndexParams index_params;
        milvus::index::ParseFromString(type_params, serialized_type_params);
        milvus::index::ParseFromString(index_params, serialized_index_params);

        milvus::Config config;
        for (auto i = 0; i < type_params.params_size(); ++i) {
            const auto& param = type_params.params(i);
            config[param.key()] = param.value();
        }

        for (auto i = 0; i < index_params.params_size(); ++i) {
            const auto& param = index_params.params(i);
            config[param.key()] = param.value();
        }

        config[milvus::index::INDEX_ENGINE_VERSION] = std::to_string(
            knowhere::Version::GetCurrentVersion().VersionNumber());

        auto& index_factory = milvus::indexbuilder::IndexFactory::GetInstance();
        auto index =
            index_factory.CreateIndex(milvus::DataType(dtype),
                                      config,
                                      milvus::storage::FileManagerContext());

        *res_index = index.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (SegcoreError& e) {
        auto status = CStatus();
        status.error_code = e.get_error_code();
        status.error_msg = strdup(e.what());
        return status;
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

milvus::storage::StorageConfig
get_storage_config(const milvus::proto::indexcgo::StorageConfig& config) {
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

milvus::OptFieldT
get_opt_field(const ::google::protobuf::RepeatedPtrField<
              milvus::proto::indexcgo::OptionalFieldInfo>& field_infos) {
    milvus::OptFieldT opt_fields_map;
    for (const auto& field_info : field_infos) {
        auto field_id = field_info.fieldid();
        if (opt_fields_map.find(field_id) == opt_fields_map.end()) {
            opt_fields_map[field_id] = {
                field_info.field_name(),
                static_cast<milvus::DataType>(field_info.field_type()),
                {}};
        }
        for (const auto& str : field_info.data_paths()) {
            std::get<2>(opt_fields_map[field_id]).emplace_back(str);
        }
    }

    return opt_fields_map;
}

milvus::Config
get_config(std::unique_ptr<milvus::proto::indexcgo::BuildIndexInfo>& info) {
    milvus::Config config;
    for (auto i = 0; i < info->index_params().size(); ++i) {
        const auto& param = info->index_params(i);
        config[param.key()] = param.value();
    }

    for (auto i = 0; i < info->type_params().size(); ++i) {
        const auto& param = info->type_params(i);
        config[param.key()] = param.value();
    }

    config["insert_files"] = info->insert_files();
    if (info->opt_fields().size()) {
        config["opt_fields"] = get_opt_field(info->opt_fields());
    }
    if (info->partition_key_isolation()) {
        config["partition_key_isolation"] = info->partition_key_isolation();
    }

    return config;
}

CStatus
CreateIndex(CIndex* res_index,
            const uint8_t* serialized_build_index_info,
            const uint64_t len) {
    try {
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshal build index info failed");

        auto field_type =
            static_cast<DataType>(build_index_info->field_schema().data_type());

        auto storage_config =
            get_storage_config(build_index_info->storage_config());
        auto config = get_config(build_index_info);

        auto engine_version = build_index_info->current_index_version();
        config[milvus::index::INDEX_ENGINE_VERSION] =
            std::to_string(engine_version);
        auto scalar_index_engine_version =
            build_index_info->current_scalar_index_version();
        config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] =
            scalar_index_engine_version;

        // init file manager
        milvus::storage::FieldDataMeta field_meta{
            build_index_info->collectionid(),
            build_index_info->partitionid(),
            build_index_info->segmentid(),
            build_index_info->field_schema().fieldid(),
            build_index_info->field_schema()};

        milvus::storage::IndexMeta index_meta{
            build_index_info->segmentid(),
            build_index_info->field_schema().fieldid(),
            build_index_info->buildid(),
            build_index_info->index_version(),
            "",
            build_index_info->field_schema().name(),
            field_type,
            build_index_info->dim(),
        };
        auto chunk_manager =
            milvus::storage::CreateChunkManager(storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager);

        auto index =
            milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(
                field_type, config, fileManagerContext);
        index->Build();
        *res_index = index.release();
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
BuildTextIndex(ProtoLayoutInterface result,
               const uint8_t* serialized_build_index_info,
               const uint64_t len) {
    try {
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshal build index info failed");

        auto field_type =
            static_cast<DataType>(build_index_info->field_schema().data_type());

        auto storage_config =
            get_storage_config(build_index_info->storage_config());
        auto config = get_config(build_index_info);

        // init file manager
        milvus::storage::FieldDataMeta field_meta{
            build_index_info->collectionid(),
            build_index_info->partitionid(),
            build_index_info->segmentid(),
            build_index_info->field_schema().fieldid(),
            build_index_info->field_schema()};

        milvus::storage::IndexMeta index_meta{
            build_index_info->segmentid(),
            build_index_info->field_schema().fieldid(),
            build_index_info->buildid(),
            build_index_info->index_version(),
            "",
            build_index_info->field_schema().name(),
            field_type,
            build_index_info->dim(),
        };
        auto chunk_manager =
            milvus::storage::CreateChunkManager(storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager);

        auto field_schema =
            FieldMeta::ParseFrom(build_index_info->field_schema());
        auto index = std::make_unique<index::TextMatchIndex>(
            fileManagerContext,
            "milvus_tokenizer",
            field_schema.get_analyzer_params().c_str());
        index->Build(config);
        auto create_index_result = index->Upload(config);
        create_index_result->SerializeAt(
            reinterpret_cast<milvus::ProtoLayout*>(result));
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
DeleteIndex(CIndex index) {
    auto status = CStatus();
    try {
        AssertInfo(index, "failed to delete index, passed index was null");
        auto cIndex =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        delete cIndex;
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildFloatVecIndex(CIndex index,
                   int64_t float_value_num,
                   const float* vectors) {
    auto status = CStatus();
    try {
        AssertInfo(index,
                   "failed to build float vector index, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto ds = knowhere::GenDataSet(row_nums, dim, vectors);
        cIndex->Build(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildFloat16VecIndex(CIndex index,
                     int64_t float16_value_num,
                     const uint8_t* vectors) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to build float16 vector index, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto dim = cIndex->dim();
        auto row_nums = float16_value_num / dim / 2;
        auto ds = knowhere::GenDataSet(row_nums, dim, vectors);
        cIndex->Build(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildBFloat16VecIndex(CIndex index,
                      int64_t bfloat16_value_num,
                      const uint8_t* vectors) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to build bfloat16 vector index, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto dim = cIndex->dim();
        auto row_nums = bfloat16_value_num / dim / 2;
        auto ds = knowhere::GenDataSet(row_nums, dim, vectors);
        cIndex->Build(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to build binary vector index, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto ds = knowhere::GenDataSet(row_nums, dim, vectors);
        cIndex->Build(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildSparseFloatVecIndex(CIndex index,
                         int64_t row_num,
                         int64_t dim,
                         const uint8_t* vectors) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to build sparse float vector index, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto ds = knowhere::GenDataSet(row_num, dim, vectors);
        ds->SetIsSparse(true);
        cIndex->Build(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildInt8VecIndex(CIndex index, int64_t int8_value_num, const int8_t* vectors) {
    auto status = CStatus();
    try {
        AssertInfo(index,
                   "failed to build int8 vector index, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto dim = cIndex->dim();
        auto row_nums = int8_value_num / dim;
        auto ds = knowhere::GenDataSet(row_nums, dim, vectors);
        cIndex->Build(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

// field_data:
//  1, serialized proto::schema::BoolArray, if type is bool;
//  2, serialized proto::schema::StringArray, if type is string;
//  3, raw pointer, if type is of fundamental except bool type;
// TODO: optimize here if necessary.
CStatus
BuildScalarIndex(CIndex c_index, int64_t size, const void* field_data) {
    auto status = CStatus();
    try {
        AssertInfo(c_index,
                   "failed to build scalar index, passed index was null");

        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(c_index);
        const int64_t dim = 8;  // not important here
        auto dataset = knowhere::GenDataSet(size, dim, field_data);
        real_index->Build(dataset);

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
SerializeIndexToBinarySet(CIndex index, CBinarySet* c_binary_set) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to serialize index to binary set, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto binary =
            std::make_unique<knowhere::BinarySet>(real_index->Serialize());
        *c_binary_set = binary.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
LoadIndexFromBinarySet(CIndex index, CBinarySet c_binary_set) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to load index from binary set, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto binary_set = reinterpret_cast<knowhere::BinarySet*>(c_binary_set);
        real_index->Load(*binary_set);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
CleanLocalData(CIndex index) {
    auto status = CStatus();
    try {
        AssertInfo(index,
                   "failed to build float vector index, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex =
            dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        cIndex->CleanLocalData();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
NewBuildIndexInfo(CBuildIndexInfo* c_build_index_info,
                  CStorageConfig c_storage_config) {
    try {
        auto build_index_info = std::make_unique<BuildIndexInfo>();
        auto& storage_config = build_index_info->storage_config;
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
        storage_config.sslCACert = c_storage_config.sslCACert;
        storage_config.useIAM = c_storage_config.useIAM;
        storage_config.region = c_storage_config.region;
        storage_config.useVirtualHost = c_storage_config.useVirtualHost;
        storage_config.requestTimeoutMs = c_storage_config.requestTimeoutMs;
        storage_config.gcp_credential_json =
            std::string(c_storage_config.gcp_credential_json);

        *c_build_index_info = build_index_info.release();
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
DeleteBuildIndexInfo(CBuildIndexInfo c_build_index_info) {
    auto info = (BuildIndexInfo*)c_build_index_info;
    delete info;
}

CStatus
AppendBuildIndexParam(CBuildIndexInfo c_build_index_info,
                      const uint8_t* serialized_index_params,
                      const uint64_t len) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        auto index_params =
            std::make_unique<milvus::proto::indexcgo::IndexParams>();
        auto res = index_params->ParseFromArray(serialized_index_params, len);
        AssertInfo(res, "Unmarshal index params failed");
        for (auto i = 0; i < index_params->params_size(); ++i) {
            const auto& param = index_params->params(i);
            build_index_info->config[param.key()] = param.value();
        }

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
AppendBuildTypeParam(CBuildIndexInfo c_build_index_info,
                     const uint8_t* serialized_type_params,
                     const uint64_t len) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        auto type_params =
            std::make_unique<milvus::proto::indexcgo::TypeParams>();
        auto res = type_params->ParseFromArray(serialized_type_params, len);
        AssertInfo(res, "Unmarshal index build type params failed");
        for (auto i = 0; i < type_params->params_size(); ++i) {
            const auto& param = type_params->params(i);
            build_index_info->config[param.key()] = param.value();
        }

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
AppendFieldMetaInfoV2(CBuildIndexInfo c_build_index_info,
                      int64_t collection_id,
                      int64_t partition_id,
                      int64_t segment_id,
                      int64_t field_id,
                      const char* field_name,
                      enum CDataType field_type,
                      int64_t dim) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        build_index_info->collection_id = collection_id;
        build_index_info->partition_id = partition_id;
        build_index_info->segment_id = segment_id;
        build_index_info->field_id = field_id;
        build_index_info->field_type = milvus::DataType(field_type);
        build_index_info->field_name = field_name;
        build_index_info->dim = dim;

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendFieldMetaInfo(CBuildIndexInfo c_build_index_info,
                    int64_t collection_id,
                    int64_t partition_id,
                    int64_t segment_id,
                    int64_t field_id,
                    enum CDataType field_type) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        build_index_info->collection_id = collection_id;
        build_index_info->partition_id = partition_id;
        build_index_info->segment_id = segment_id;
        build_index_info->field_id = field_id;

        build_index_info->field_type = milvus::DataType(field_type);

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
AppendIndexMetaInfo(CBuildIndexInfo c_build_index_info,
                    int64_t index_id,
                    int64_t build_id,
                    int64_t version) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        build_index_info->index_id = index_id;
        build_index_info->index_build_id = build_id;
        build_index_info->index_version = version;

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
AppendInsertFilePath(CBuildIndexInfo c_build_index_info,
                     const char* c_file_path) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        std::string insert_file_path(c_file_path);
        build_index_info->insert_files.emplace_back(insert_file_path);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendIndexEngineVersionToBuildInfo(CBuildIndexInfo c_load_index_info,
                                    int32_t index_engine_version) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_load_index_info;
        build_index_info->index_engine_version = index_engine_version;

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendIndexStorageInfo(CBuildIndexInfo c_build_index_info,
                       const char* c_data_store_path,
                       const char* c_index_store_path,
                       int64_t data_store_version) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        std::string data_store_path(c_data_store_path),
            index_store_path(c_index_store_path);
        build_index_info->data_store_path = data_store_path;
        build_index_info->index_store_path = index_store_path;
        build_index_info->data_store_version = data_store_version;

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
SerializeIndexAndUpLoad(CIndex index, ProtoLayoutInterface result) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to serialize index to binary set, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto create_index_result = real_index->Upload();
        create_index_result->SerializeAt(
            reinterpret_cast<milvus::ProtoLayout*>(result));
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
AppendOptionalFieldDataPath(CBuildIndexInfo c_build_index_info,
                            const int64_t field_id,
                            const char* field_name,
                            const int32_t field_type,
                            const char* c_file_path) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        std::string field_name_str(field_name);
        auto& opt_fields_map = build_index_info->opt_fields;
        if (opt_fields_map.find(field_id) == opt_fields_map.end()) {
            opt_fields_map[field_id] = {
                field_name, static_cast<milvus::DataType>(field_type), {}};
        }
        std::get<2>(opt_fields_map[field_id]).emplace_back(c_file_path);
        return CStatus{Success, ""};
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
