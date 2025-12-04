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
#include "common/Consts.h"
#include "fmt/core.h"
#include "indexbuilder/type_c.h"
#include "log/Log.h"
#include "storage/PluginLoader.h"
#include "storage/loon_ffi/util.h"

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
#include "index/json_stats/JsonKeyStats.h"
#include "milvus-storage/filesystem/fs.h"
#include "monitor/scope_metric.h"

using namespace milvus;
CStatus
CreateIndexForUT(enum CDataType dtype,
                 const char* serialized_type_params,
                 const char* serialized_index_params,
                 CIndex* res_index) {
    SCOPE_CGO_CALL_METRIC();

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
                static_cast<milvus::DataType>(field_info.element_type()),
                {}};
        }
        for (const auto& str : field_info.data_paths()) {
            std::get<3>(opt_fields_map[field_id]).emplace_back(str);
        }
    }

    return opt_fields_map;
}

milvus::SegmentInsertFiles
get_segment_insert_files(
    const milvus::proto::indexcgo::SegmentInsertFiles& segment_insert_files) {
    milvus::SegmentInsertFiles files;
    for (const auto& column_group_files :
         segment_insert_files.field_insert_files()) {
        std::vector<std::string> paths;
        paths.reserve(column_group_files.file_paths().size());
        for (const auto& path : column_group_files.file_paths()) {
            paths.push_back(path);
        }
        files.emplace_back(std::move(paths));
    }
    return files;
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

    config[INSERT_FILES_KEY] = info->insert_files();
    if (info->opt_fields().size()) {
        config[VEC_OPT_FIELDS] = get_opt_field(info->opt_fields());
    }
    if (info->partition_key_isolation()) {
        config[PARTITION_KEY_ISOLATION_KEY] = info->partition_key_isolation();
    }
    config[INDEX_NUM_ROWS_KEY] = info->num_rows();
    config[STORAGE_VERSION_KEY] = info->storage_version();
    if (info->storage_version() == STORAGE_V2) {
        config[SEGMENT_INSERT_FILES_KEY] =
            get_segment_insert_files(info->segment_insert_files());
        config[SEGMENT_MANIFEST_KEY] = info->manifest();
    }
    config[DIM_KEY] = info->dim();
    config[DATA_TYPE_KEY] = info->field_schema().data_type();
    config[ELEMENT_TYPE_KEY] = info->field_schema().element_type();

    return config;
}

CStatus
CreateIndex(CIndex* res_index,
            const uint8_t* serialized_build_index_info,
            const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

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
        auto tantivy_index_version =
            scalar_index_engine_version <= 1
                ? milvus::index::TANTIVY_INDEX_MINIMUM_VERSION
                : milvus::index::TANTIVY_INDEX_LATEST_VERSION;
        config[milvus::index::TANTIVY_INDEX_VERSION] = tantivy_index_version;

        // check index encoding config
        auto index_non_encoding_str =
            config.value(milvus::index::INDEX_NON_ENCODING, "false");
        bool index_non_encoding = index_non_encoding_str == "true";

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
            index_non_encoding};
        auto chunk_manager =
            milvus::storage::CreateChunkManager(storage_config);
        LOG_INFO("create chunk manager success, build_id: {}",
                 build_index_info->buildid());
        auto fs = milvus::storage::InitArrowFileSystem(storage_config);
        LOG_INFO("init arrow file system success, build_id: {}",
                 build_index_info->buildid());

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager, fs);
        if (build_index_info->manifest() != "") {
            auto loon_properties = MakeInternalPropertiesFromStorageConfig(
                ToCStorageConfig(storage_config));
            fileManagerContext.set_loon_ffi_properties(loon_properties);
        }

        if (build_index_info->has_storage_plugin_context()) {
            auto cipherPlugin =
                milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
            AssertInfo(cipherPlugin != nullptr, "failed to get cipher plugin");
            cipherPlugin->Update(
                build_index_info->storage_plugin_context().encryption_zone_id(),
                build_index_info->storage_plugin_context().collection_id(),
                build_index_info->storage_plugin_context().encryption_key());

            auto plugin_context = std::make_shared<CPluginContext>();
            plugin_context->ez_id =
                build_index_info->storage_plugin_context().encryption_zone_id();
            plugin_context->collection_id =
                build_index_info->storage_plugin_context().collection_id();
            fileManagerContext.set_plugin_context(plugin_context);
        }

        auto index =
            milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(
                field_type, config, fileManagerContext);
        LOG_INFO("create index instance success, build_id: {}",
                 build_index_info->buildid());
        index->Build();
        LOG_INFO("build index done, build_id: {}", build_index_info->buildid());
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
BuildJsonKeyIndex(ProtoLayoutInterface result,
                  const uint8_t* serialized_build_index_info,
                  const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshall build index info failed");

        auto field_type = static_cast<milvus::DataType>(
            build_index_info->field_schema().data_type());

        auto storage_config =
            get_storage_config(build_index_info->storage_config());
        auto config = get_config(build_index_info);

        auto loon_properties =
            MakePropertiesFromStorageConfig(ToCStorageConfig(storage_config));

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

        auto scalar_index_engine_version =
            build_index_info->current_scalar_index_version();
        config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] =
            scalar_index_engine_version;
        auto tantivy_index_version =
            scalar_index_engine_version <= 1
                ? milvus::index::TANTIVY_INDEX_MINIMUM_VERSION
                : milvus::index::TANTIVY_INDEX_LATEST_VERSION;
        config[milvus::index::TANTIVY_INDEX_VERSION] = tantivy_index_version;

        auto chunk_manager =
            milvus::storage::CreateChunkManager(storage_config);
        auto fs = milvus::storage::InitArrowFileSystem(storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager, fs);

        if (build_index_info->manifest() != "") {
            auto loon_properties = MakeInternalPropertiesFromStorageConfig(
                ToCStorageConfig(storage_config));
            fileManagerContext.set_loon_ffi_properties(loon_properties);
        }

        if (build_index_info->has_storage_plugin_context()) {
            auto cipherPlugin =
                milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
            AssertInfo(cipherPlugin != nullptr, "failed to get cipher plugin");
            cipherPlugin->Update(
                build_index_info->storage_plugin_context().encryption_zone_id(),
                build_index_info->storage_plugin_context().collection_id(),
                build_index_info->storage_plugin_context().encryption_key());

            auto plugin_context = std::make_shared<CPluginContext>();
            plugin_context->ez_id =
                build_index_info->storage_plugin_context().encryption_zone_id();
            plugin_context->collection_id =
                build_index_info->storage_plugin_context().collection_id();
            fileManagerContext.set_plugin_context(plugin_context);
        }

        auto field_schema =
            FieldMeta::ParseFrom(build_index_info->field_schema());
        auto index = std::make_unique<index::JsonKeyStats>(
            fileManagerContext,
            false,
            build_index_info->json_stats_max_shredding_columns(),
            build_index_info->json_stats_shredding_ratio_threshold(),
            build_index_info->json_stats_write_batch_size(),
            tantivy_index_version);
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
BuildTextIndex(ProtoLayoutInterface result,
               const uint8_t* serialized_build_index_info,
               const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshal build index info failed");

        auto field_type = static_cast<milvus::DataType>(
            build_index_info->field_schema().data_type());

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
        auto fs = milvus::storage::InitArrowFileSystem(storage_config);

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, chunk_manager, fs);

        if (build_index_info->manifest() != "") {
            auto loon_properties = MakeInternalPropertiesFromStorageConfig(
                ToCStorageConfig(storage_config));
            fileManagerContext.set_loon_ffi_properties(loon_properties);
        }

        if (build_index_info->has_storage_plugin_context()) {
            auto cipherPlugin =
                milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
            AssertInfo(cipherPlugin != nullptr, "failed to get cipher plugin");
            cipherPlugin->Update(
                build_index_info->storage_plugin_context().encryption_zone_id(),
                build_index_info->storage_plugin_context().collection_id(),
                build_index_info->storage_plugin_context().encryption_key());
            auto plugin_context = std::make_shared<CPluginContext>();
            plugin_context->ez_id =
                build_index_info->storage_plugin_context().encryption_zone_id();
            plugin_context->collection_id =
                build_index_info->storage_plugin_context().collection_id();
            fileManagerContext.set_plugin_context(plugin_context);
        }

        auto scalar_index_engine_version =
            build_index_info->current_scalar_index_version();
        config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] =
            scalar_index_engine_version;
        auto tantivy_index_version =
            scalar_index_engine_version <= 1
                ? milvus::index::TANTIVY_INDEX_MINIMUM_VERSION
                : milvus::index::TANTIVY_INDEX_LATEST_VERSION;
        config[milvus::index::TANTIVY_INDEX_VERSION] = tantivy_index_version;

        auto field_schema =
            FieldMeta::ParseFrom(build_index_info->field_schema());
        auto index = std::make_unique<index::TextMatchIndex>(
            fileManagerContext,
            tantivy_index_version,
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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

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
SerializeIndexAndUpLoad(CIndex index, ProtoLayoutInterface result) {
    SCOPE_CGO_CALL_METRIC();

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
