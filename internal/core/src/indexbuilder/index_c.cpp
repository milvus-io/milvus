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

#include <string>

#ifdef __linux__
#include <malloc.h>
#endif

#include "exceptions/EasyAssert.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/IndexFactory.h"
#include "common/type_c.h"
#include "storage/Types.h"
#include "indexbuilder/types.h"
#include "index/Utils.h"
#include "pb/index_cgo_msg.pb.h"
#include "storage/Util.h"

CStatus
CreateIndex(enum CDataType dtype,
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

        auto& index_factory = milvus::indexbuilder::IndexFactory::GetInstance();
        auto index =
            index_factory.CreateIndex(milvus::DataType(dtype), config, nullptr);

        *res_index = index.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
CreateIndexV2(CIndex* res_index, CBuildIndexInfo c_build_index_info) {
    try {
        auto build_index_info = (BuildIndexInfo*)c_build_index_info;
        auto field_type = build_index_info->field_type;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = build_index_info->field_type;

        auto& config = build_index_info->config;
        config["insert_files"] = build_index_info->insert_files;

        // get index type
        auto index_type = milvus::index::GetValueFromConfig<std::string>(
            config, "index_type");
        AssertInfo(index_type.has_value(), "index type is empty");
        index_info.index_type = index_type.value();

        // get metric type
        if (milvus::datatype_is_vector(field_type)) {
            auto metric_type = milvus::index::GetValueFromConfig<std::string>(
                config, "metric_type");
            AssertInfo(metric_type.has_value(), "metric type is empty");
            index_info.metric_type = metric_type.value();
        }

        // init file manager
        milvus::storage::FieldDataMeta field_meta{
            build_index_info->collection_id,
            build_index_info->partition_id,
            build_index_info->segment_id,
            build_index_info->field_id};
        milvus::storage::IndexMeta index_meta{build_index_info->segment_id,
                                              build_index_info->field_id,
                                              build_index_info->index_build_id,
                                              build_index_info->index_version};
        auto chunk_manager = milvus::storage::CreateChunkManager(
            build_index_info->storage_config);
        auto file_manager = milvus::storage::CreateFileManager(
            index_info.index_type, field_meta, index_meta, chunk_manager);
        AssertInfo(file_manager != nullptr, "create file manager failed!");

        auto index =
            milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(
                build_index_info->field_type, config, file_manager);
        index->Build();
        *res_index = index.release();
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
        storage_config.iam_endpoint =
            std::string(c_storage_config.iam_endpoint);
        storage_config.useSSL = c_storage_config.useSSL;
        storage_config.useIAM = c_storage_config.useIAM;

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
        AssertInfo(res, "Unmarshall index params failed");
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
        AssertInfo(res, "Unmarshall index build type params failed");
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
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
SerializeIndexAndUpLoad(CIndex index, CBinarySet* c_binary_set) {
    auto status = CStatus();
    try {
        AssertInfo(
            index,
            "failed to serialize index to binary set, passed index was null");
        auto real_index =
            reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto binary =
            std::make_unique<knowhere::BinarySet>(real_index->Upload());
        *c_binary_set = binary.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}
