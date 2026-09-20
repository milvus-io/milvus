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
#include <string.h>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/CGoCatch.h"
#include "common/EasyAssert.h"
#include "common/Exception.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "filemanager/InputStream.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "segcore/json_stats/JsonKeyStats.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/type_c.h"
#include "log/Log.h"
#include "monitor/scope_metric.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/schema.pb.h"
#include "storage/FileManager.h"
#include "storage/PluginLoader.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"
#include "storage/plugin/PluginInterface.h"

using namespace milvus;

namespace {

struct CIndexBuildSessionHandle {
    explicit CIndexBuildSessionHandle(
        std::unique_ptr<milvus::indexbuilder::BuildSession> native_session)
        : session(std::move(native_session)) {
    }

    std::unique_ptr<milvus::indexbuilder::BuildSession> session;
};

CIndexBuildSessionHandle&
RequireBuildSessionHandle(CIndex index) {
    AssertInfo(index != nullptr, "passed index handle was null");
    return *reinterpret_cast<CIndexBuildSessionHandle*>(index);
}

milvus::indexbuilder::BuildSession&
RequireBuildSession(CIndex index) {
    auto& handle = RequireBuildSessionHandle(index);
    AssertInfo(handle.session != nullptr, "native index session was null");
    return *handle.session;
}

std::unique_ptr<CIndexBuildSessionHandle>
MakeBuildSessionHandle(
    std::unique_ptr<milvus::indexbuilder::BuildSession> session) {
    AssertInfo(session != nullptr, "cannot publish a null native session");
    return std::make_unique<CIndexBuildSessionHandle>(std::move(session));
}

void
WriteArtifactStats(const milvus::storage::ArtifactStats& stats,
                   ProtoLayoutInterface result) {
    AssertInfo(result != nullptr, "index stats output was null");
    auto proto = milvus::indexbuilder::AdaptArtifactStats(stats);
    auto* layout = reinterpret_cast<milvus::ProtoLayout*>(result);
    AssertInfo(layout->SerializeAndHoldProto(proto),
               "failed to serialize index artifact stats");
}

}  // namespace

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
    storage_config.useSSL = config.usessl();
    storage_config.sslCACert = config.sslcacert();
    storage_config.useIAM = config.useiam();
    storage_config.region = config.region();
    storage_config.useVirtualHost = config.use_virtual_host();
    storage_config.requestTimeoutMs = config.request_timeout_ms();
    storage_config.gcp_credential_json =
        std::string(config.gcpcredentialjson());
    storage_config.max_connections = config.max_connections();
    storage_config.tls_min_version = std::string(config.ssl_tls_min_version());
    storage_config.use_crc32c_checksum = config.use_crc32c_checksum();
    return storage_config;
}

milvus::OptFieldT
get_opt_field(const ::google::protobuf::RepeatedPtrField<
              milvus::proto::indexcgo::OptionalFieldInfo>& field_infos) {
    milvus::OptFieldT opt_fields_map;
    for (const auto& field_info : field_infos) {
        auto field_id = field_info.fieldid();
        auto it = opt_fields_map.find(field_id);
        if (it == opt_fields_map.end()) {
            it = opt_fields_map
                     .emplace(field_id,
                              std::make_tuple(field_info.field_name(),
                                              static_cast<milvus::DataType>(
                                                  field_info.field_type()),
                                              static_cast<milvus::DataType>(
                                                  field_info.element_type()),
                                              std::vector<std::string>{}))
                     .first;
        }
        for (const auto& str : field_info.data_paths()) {
            std::get<3>(it->second).emplace_back(str);
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

milvus::storage::StorageColumnMapping
get_storage_column_mapping(
    const milvus::proto::schema::FieldSchema& field_schema,
    bool is_milvus_table) {
    auto physical_mapping =
        milvus::ResolvePhysicalColumnMapping(is_milvus_table, field_schema);
    milvus::storage::StorageColumnMapping mapping;
    mapping.schema_column_name = physical_mapping.schema_column_name;
    mapping.storage_column_name = physical_mapping.storage_column_name;
    mapping.is_external_column = physical_mapping.is_external_column;
    return mapping;
}

milvus::storage::StorageColumnMapping
get_storage_column_mapping(
    const milvus::proto::indexcgo::OptionalFieldInfo& field_info,
    bool is_milvus_table) {
    milvus::storage::StorageColumnMapping mapping;
    mapping.schema_column_name = field_info.field_name();
    mapping.storage_column_name = std::to_string(field_info.fieldid());
    mapping.is_external_column = is_milvus_table;
    return mapping;
}

void
configure_manifest_file_manager_context(
    milvus::storage::FileManagerContext& file_manager_context,
    const milvus::proto::indexcgo::BuildIndexInfo& build_index_info,
    const milvus::storage::StorageConfig& storage_config) {
    if (build_index_info.manifest().empty()) {
        return;
    }

    auto loon_properties = MakeInternalPropertiesFromStorageConfig(
        ToCStorageConfig(storage_config));
    if (!build_index_info.external_source().empty()) {
        InjectExternalSpecProperties(*loon_properties,
                                     build_index_info.collectionid(),
                                     build_index_info.external_source(),
                                     build_index_info.external_spec());
    }
    // Widen the per-round read window for index-build manifest reads when
    // configured. With loon's 32MB default each prefetch round admits a
    // single 64MB-class row group, so the whole raw-data download degrades
    // to one S3 range read at a time; a wider window lets one round span
    // multiple row groups whose column chunks are prefetched in parallel
    // on the arrow IO thread pool.
    milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
        .ApplyIndexBuildReadWindow(*loon_properties);
    file_manager_context.set_loon_ffi_properties(loon_properties);

    auto is_milvus_table =
        milvus::IsMilvusTableExternalSpec(build_index_info.external_spec());
    file_manager_context.set_storage_column_mapping(
        build_index_info.field_schema().fieldid(),
        get_storage_column_mapping(build_index_info.field_schema(),
                                   is_milvus_table));
    for (const auto& field_info : build_index_info.opt_fields()) {
        file_manager_context.set_storage_column_mapping(
            field_info.fieldid(),
            get_storage_column_mapping(field_info, is_milvus_table));
    }
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
    if (info->storage_version() == STORAGE_V2 ||
        info->storage_version() == STORAGE_V3) {
        config[SEGMENT_INSERT_FILES_KEY] =
            get_segment_insert_files(info->segment_insert_files());
        config[SEGMENT_MANIFEST_KEY] = info->manifest();
    }
    config[DIM_KEY] = info->dim();
    config[DATA_TYPE_KEY] = info->field_schema().data_type();
    config[ELEMENT_TYPE_KEY] = info->field_schema().element_type();
    if (!info->stats_base_path().empty()) {
        config[STATS_BASE_PATH_KEY] = info->stats_base_path();
    }

    if (!info->analyzer_extra_info().empty()) {
        config["analyzer_extra_info"] = info->analyzer_extra_info();
    }

    return config;
}

CStatus
CreateIndex(CIndex* res_index,
            const uint8_t* serialized_build_index_info,
            const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(res_index != nullptr, "index output handle was null");
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshal build index info failed");

        const auto field_type =
            static_cast<DataType>(build_index_info->field_schema().data_type());
        const auto purpose =
            milvus::IsVectorDataType(field_type)
                ? milvus::indexbuilder::BuildPurpose::VectorIndex
                : milvus::indexbuilder::BuildPurpose::ScalarIndex;
        auto prepared = milvus::indexbuilder::AdaptBuildIndexInfo(
            *build_index_info, purpose);
        auto session = std::make_unique<milvus::indexbuilder::BuildSession>(
            std::move(prepared.request),
            std::move(prepared.file_manager_context));
        session->BuildFromSource();
        *res_index = MakeBuildSessionHandle(std::move(session)).release();
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

// Build the JSON shredding layout through JsonKeyStats, independently of the
// index-family builder registry. This path still uses its own Build/Upload
// pipeline rather than storage::Artifact; typed sub-column integration remains
// separate work.
CStatus
BuildJsonKeyIndex(ProtoLayoutInterface result,
                  const uint8_t* serialized_build_index_info,
                  const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(result != nullptr, "index stats output was null");
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
        fileManagerContext.set_stats_base_path(
            build_index_info->stats_base_path());

        configure_manifest_file_manager_context(
            fileManagerContext, *build_index_info, storage_config);

        if (build_index_info->has_storage_plugin_context()) {
            fileManagerContext.set_plugin_context(
                milvus::storage::PluginLoader::GetInstance()
                    .registerCipherPluginContext(
                        build_index_info->storage_plugin_context()
                            .encryption_zone_id(),
                        build_index_info->storage_plugin_context()
                            .collection_id(),
                        build_index_info->storage_plugin_context()
                            .encryption_key()));
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
        WriteArtifactStats(index->Upload(config), result);
        return milvus::SuccessCStatus();
    } catch (SegcoreError& e) {
        auto status = CStatus();
        status.error_code = e.get_error_code();
        status.error_msg = strdup(e.what());
        return status;
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
BuildTextIndex(ProtoLayoutInterface result,
               const uint8_t* serialized_build_index_info,
               const uint64_t len) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(result != nullptr, "index stats output was null");
        auto build_index_info =
            std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();
        auto res =
            build_index_info->ParseFromArray(serialized_build_index_info, len);
        AssertInfo(res, "Unmarshal build index info failed");

        auto prepared = milvus::indexbuilder::AdaptBuildIndexInfo(
            *build_index_info, milvus::indexbuilder::BuildPurpose::TextIndex);
        milvus::indexbuilder::BuildSession session(
            std::move(prepared.request),
            std::move(prepared.file_manager_context));
        session.BuildFromSource();
        WriteArtifactStats(session.Publish(), result);
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
DeleteIndex(CIndex index) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto* handle = &RequireBuildSessionHandle(index);
        delete handle;
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
SerializeIndexAndUpLoad(CIndex index, ProtoLayoutInterface result) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(result != nullptr, "index stats output was null");
        WriteArtifactStats(RequireBuildSession(index).Publish(), result);
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
