// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/IndexBuildCapiAdapter.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Utils.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/Utils.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/PluginLoader.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

namespace milvus::indexbuilder {
namespace {

storage::StorageConfig
AdaptStorageConfig(const proto::indexcgo::StorageConfig& config) {
    storage::StorageConfig result;
    result.address = config.address();
    result.bucket_name = config.bucket_name();
    result.access_key_id = config.access_keyid();
    result.access_key_value = config.secret_access_key();
    result.root_path = config.root_path();
    result.storage_type = config.storage_type();
    result.cloud_provider = config.cloud_provider();
    result.iam_endpoint = config.iamendpoint();
    result.useSSL = config.usessl();
    result.sslCACert = config.sslcacert();
    result.useIAM = config.useiam();
    result.region = config.region();
    result.useVirtualHost = config.use_virtual_host();
    result.requestTimeoutMs = config.request_timeout_ms();
    result.gcp_credential_json = config.gcpcredentialjson();
    result.max_connections = config.max_connections();
    result.tls_min_version = config.ssl_tls_min_version();
    result.use_crc32c_checksum = config.use_crc32c_checksum();
    return result;
}

OptFieldT
AdaptOptionalFields(const google::protobuf::RepeatedPtrField<
                    proto::indexcgo::OptionalFieldInfo>& fields) {
    OptFieldT result;
    for (const auto& field : fields) {
        const auto field_id = field.fieldid();
        auto [it, inserted] = result.emplace(
            field_id,
            std::make_tuple(field.field_name(),
                            static_cast<DataType>(field.field_type()),
                            static_cast<DataType>(field.element_type()),
                            std::vector<std::string>{}));
        if (!inserted) {
            AssertInfo(std::get<0>(it->second) == field.field_name() &&
                           std::get<1>(it->second) ==
                               static_cast<DataType>(field.field_type()) &&
                           std::get<2>(it->second) ==
                               static_cast<DataType>(field.element_type()),
                       "optional field {} has conflicting metadata",
                       field_id);
        }
        auto& paths = std::get<3>(it->second);
        const auto added = static_cast<size_t>(field.data_paths_size());
        AssertInfo(added <= std::numeric_limits<size_t>::max() - paths.size(),
                   "optional field {} path count overflows size_t",
                   field_id);
        paths.reserve(paths.size() + added);
        for (const auto& path : field.data_paths()) {
            paths.push_back(path);
        }
    }
    return result;
}

std::vector<std::vector<std::string>>
AdaptSegmentFiles(const proto::indexcgo::SegmentInsertFiles& files) {
    std::vector<std::vector<std::string>> result;
    result.reserve(static_cast<size_t>(files.field_insert_files_size()));
    for (const auto& group : files.field_insert_files()) {
        std::vector<std::string> paths;
        paths.reserve(static_cast<size_t>(group.file_paths_size()));
        for (const auto& path : group.file_paths()) {
            paths.push_back(path);
        }
        result.push_back(std::move(paths));
    }
    return result;
}

std::vector<std::string>
AdaptInsertFiles(const google::protobuf::RepeatedPtrField<std::string>& files) {
    return {files.begin(), files.end()};
}

storage::StorageColumnMapping
AdaptStorageColumnMapping(const proto::schema::FieldSchema& field_schema,
                          bool is_milvus_table) {
    const auto mapping =
        ResolvePhysicalColumnMapping(is_milvus_table, field_schema);
    return {.schema_column_name = mapping.schema_column_name,
            .storage_column_name = mapping.storage_column_name,
            .is_external_column = mapping.is_external_column};
}

storage::StorageColumnMapping
AdaptStorageColumnMapping(const proto::indexcgo::OptionalFieldInfo& field,
                          bool is_milvus_table) {
    return {.schema_column_name = field.field_name(),
            .storage_column_name = std::to_string(field.fieldid()),
            .is_external_column = is_milvus_table};
}

int64_t
CanonicalBuildDimension(const proto::indexcgo::BuildIndexInfo& info) {
    const auto dim = info.dim();
    const auto field_type =
        static_cast<DataType>(info.field_schema().data_type());
    return IsSparseFloatVectorDataType(field_type) && dim == -1 ? 0 : dim;
}

void
ConfigureManifestContext(storage::FileManagerContext& context,
                         const proto::indexcgo::BuildIndexInfo& info,
                         const storage::StorageConfig& storage_config) {
    if (info.manifest().empty()) {
        return;
    }

    auto properties = MakeInternalPropertiesFromStorageConfig(
        ToCStorageConfig(storage_config));
    if (!info.external_source().empty()) {
        InjectExternalSpecProperties(*properties,
                                     info.collectionid(),
                                     info.external_source(),
                                     info.external_spec());
    }
    storage::LoonFFIPropertiesSingleton::GetInstance()
        .ApplyIndexBuildReadWindow(*properties);
    context.set_loon_ffi_properties(std::move(properties));

    const auto is_milvus_table =
        IsMilvusTableExternalSpec(info.external_spec());
    context.set_storage_column_mapping(
        info.field_schema().fieldid(),
        AdaptStorageColumnMapping(info.field_schema(), is_milvus_table));
    for (const auto& field : info.opt_fields()) {
        context.set_storage_column_mapping(
            field.fieldid(), AdaptStorageColumnMapping(field, is_milvus_table));
    }
}

index::BuildParams
AdaptParams(const proto::indexcgo::BuildIndexInfo& info) {
    index::BuildParams params = index::BuildParams::object();
    for (const auto& param : info.index_params()) {
        params[param.key()] = param.value();
    }
    for (const auto& param : info.type_params()) {
        params[param.key()] = param.value();
    }

    if (!info.opt_fields().empty()) {
        params[VEC_OPT_FIELDS] = AdaptOptionalFields(info.opt_fields());
    }
    if (info.partition_key_isolation()) {
        params[PARTITION_KEY_ISOLATION_KEY] = true;
    }
    params[INDEX_NUM_ROWS_KEY] = info.num_rows();
    params[STORAGE_VERSION_KEY] = info.storage_version();
    params[DIM_KEY] = CanonicalBuildDimension(info);
    params[DATA_TYPE_KEY] = info.field_schema().data_type();
    params[ELEMENT_TYPE_KEY] = info.field_schema().element_type();
    if (!info.stats_base_path().empty()) {
        params[STATS_BASE_PATH_KEY] = info.stats_base_path();
    }
    if (!info.analyzer_extra_info().empty()) {
        params["analyzer_extra_info"] = info.analyzer_extra_info();
    }
    return params;
}

std::string
LocalStagingParent();

std::string
RequiredString(const index::BuildParams& params, std::string_view key) {
    AssertInfo(params.contains(key),
               "index-build request is missing parameter {}",
               key);
    AssertInfo(params.at(key).is_string(),
               "index-build parameter {} must be a string",
               key);
    auto value = params.at(key).get<std::string>();
    AssertInfo(!value.empty(), "index-build parameter {} is empty", key);
    return value;
}

std::string
NormalizeIndexType(index::BuildParams& params, BuildPurpose purpose) {
    if (purpose != BuildPurpose::TextIndex) {
        return RequiredString(params, index::INDEX_TYPE);
    }

    if (params.contains(index::INDEX_TYPE)) {
        const auto configured = RequiredString(params, index::INDEX_TYPE);
        AssertInfo(configured == index::INVERTED_INDEX_TYPE,
                   "text index build requires {} index type, got {}",
                   index::INVERTED_INDEX_TYPE,
                   configured);
    }
    params[index::INDEX_TYPE] = index::INVERTED_INDEX_TYPE;
    return index::INVERTED_INDEX_TYPE;
}

std::string
CanonicalJsonPath(const index::BuildParams& params, DataType field_type) {
    if (field_type != DataType::JSON) {
        return {};
    }

    const auto has_path = params.contains(JSON_PATH);
    const auto has_nested = params.contains("nested_path");
    auto read = [&](std::string_view key) {
        AssertInfo(params.at(key).is_string(),
                   "JSON index parameter {} must be a string",
                   key);
        return params.at(key).get<std::string>();
    };
    const auto path = has_path ? read(JSON_PATH) : std::string{};
    const auto nested = has_nested ? read("nested_path") : std::string{};
    AssertInfo(!has_path || !has_nested || path == nested,
               "JSON path parameters {} and nested_path conflict",
               JSON_PATH);
    return has_path ? path : nested;
}

BuildSource
AdaptSource(const proto::indexcgo::BuildIndexInfo& info) {
    if (!info.manifest().empty()) {
        return ManifestBuildSource{info.manifest()};
    }
    if (info.storage_version() == STORAGE_V2 ||
        info.storage_version() == STORAGE_V3) {
        return StorageV2BuildSource{
            AdaptSegmentFiles(info.segment_insert_files())};
    }
    return V1BinlogBuildSource{AdaptInsertFiles(info.insert_files())};
}

std::string
LocalStagingParent() {
    const auto local =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    AssertInfo(local != nullptr,
               "index-build local chunk manager is not initialized");
    const auto& root = local->GetRootPath();
    AssertInfo(!root.empty(), "index-build local staging root is empty");
    return root;
}

bool
IndexNonEncoding(const index::BuildParams& params) {
    return index::GetValueFromConfigOrFallback<bool>(
        params, index::INDEX_NON_ENCODING, false);
}

void
ConfigureTextParams(index::BuildParams& params,
                    const proto::indexcgo::BuildIndexInfo& info) {
    auto field = FieldMeta::ParseFrom(info.field_schema());
    params["analyzer_name"] = "milvus_tokenizer";
    params["analyzer_params"] = field.get_analyzer_params();
    params["analyzer_extra_info"] = info.analyzer_extra_info();
}

storage::FileManagerContext
MakeFileManagerContext(const proto::indexcgo::BuildIndexInfo& info,
                       const storage::StorageConfig& storage_config,
                       const index::BuildParams& params,
                       DataType field_type) {
    storage::FieldDataMeta field_meta{info.collectionid(),
                                      info.partitionid(),
                                      info.segmentid(),
                                      info.field_schema().fieldid(),
                                      info.field_schema()};
    storage::IndexMeta index_meta{info.segmentid(),
                                  info.field_schema().fieldid(),
                                  info.buildid(),
                                  info.index_version(),
                                  "",
                                  info.field_schema().name(),
                                  field_type,
                                  params.at(DIM_KEY).get<int64_t>(),
                                  IndexNonEncoding(params),
                                  info.index_store_path_version()};

    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext context(
        field_meta, index_meta, chunk_manager, std::move(fs));
    if (!info.stats_base_path().empty()) {
        context.set_stats_base_path(info.stats_base_path());
    }
    ConfigureManifestContext(context, info, storage_config);
    if (info.has_storage_plugin_context()) {
        const auto& plugin = info.storage_plugin_context();
        context.set_plugin_context(
            storage::PluginLoader::GetInstance().registerCipherPluginContext(
                plugin.encryption_zone_id(),
                plugin.collection_id(),
                plugin.encryption_key()));
    }
    return context;
}

}  // namespace

PreparedBuild
AdaptBuildIndexInfo(const proto::indexcgo::BuildIndexInfo& info,
                    BuildPurpose purpose) {
    const auto field_type =
        static_cast<DataType>(info.field_schema().data_type());
    const bool vector_field = IsVectorDataType(field_type);
    AssertInfo((purpose == BuildPurpose::VectorIndex) == vector_field,
               "index-build purpose does not match field type {}",
               field_type);
    if (purpose == BuildPurpose::TextIndex) {
        AssertInfo(IsStringDataType(field_type),
                   "text index build requires a string field");
    }

    auto params = AdaptParams(info);
    const auto index_type = NormalizeIndexType(params, purpose);
    const auto json_path = CanonicalJsonPath(params, field_type);
    const auto is_nested =
        vector_field ? false : IsStructSubField(info.field_schema().name());
    const auto element_type =
        static_cast<DataType>(info.field_schema().element_type());

    params[index::SCALAR_INDEX_ENGINE_VERSION] =
        info.current_scalar_index_version();
    params[index::TANTIVY_INDEX_VERSION] =
        info.current_scalar_index_version() <= 1
            ? index::TANTIVY_INDEX_MINIMUM_VERSION
            : index::TANTIVY_INDEX_LATEST_VERSION;

    index::IndexTypeAdapterRequest adapter_request{
        .index_type = index_type,
        .field_type = field_type,
        .element_type = element_type,
        .index_engine_version = info.current_index_version(),
        .params = std::move(params),
        .is_nested = is_nested,
        .is_text_match = purpose == BuildPurpose::TextIndex,
    };
    auto adapted = index::AdaptIndexType(adapter_request);
    if (adapted.family == index::families::kText) {
        adapted.params["is_text_match"] = true;
        ConfigureTextParams(adapted.params, info);
    } else {
        AssertInfo(purpose != BuildPurpose::TextIndex,
                   "text build did not resolve to the text family");
    }

    const auto staging_parent = LocalStagingParent();
    BuildOutputSpec output;
    output.generation =
        !vector_field && info.current_scalar_index_version() >= 3
            ? storage::Generation::V3
            : storage::Generation::V1V2;
    output.storage_path = adapted.family == index::families::kText
                              ? storage::ArtifactStoragePath::TextLog
                              : storage::ArtifactStoragePath::Index;
    if (output.generation == storage::Generation::V3) {
        output.packed_file_name =
            index::PackedScalarIndexFileName(adapted.artifact_type);
    }

    auto source = AdaptSource(info);
    const bool legacy_binlog_source =
        std::holds_alternative<V1BinlogBuildSource>(source);
    BuildRequest request{
        .family = adapted.family,
        .params = std::move(adapted.params),
        .value_type = adapted.value_type,
        .field_id = FieldId(info.field_schema().fieldid()),
        .source = std::move(source),
        .expected_rows = info.num_rows(),
        // lack_binlog_rows describes legacy per-field binlogs and cannot
        // describe StorageV2 column groups. Columnar source handling decides
        // missing-row semantics from the field data it visits instead.
        .missing_rows =
            legacy_binlog_source ? info.lack_binlog_rows() : 0,
        .staging_parent = staging_parent,
        .output = std::move(output),
        .json_path = json_path,
    };

    auto storage_config = AdaptStorageConfig(info.storage_config());
    auto context = MakeFileManagerContext(
        info, storage_config, request.params, field_type);
    return {.request = std::move(request),
            .file_manager_context = std::move(context)};
}

proto::cgo::IndexStats
AdaptArtifactStats(const storage::ArtifactStats& stats) {
    proto::cgo::IndexStats result;
    result.set_mem_size(stats.MemSize());
    for (const auto& file : stats.Files()) {
        auto* output = result.add_serialized_index_infos();
        output->set_file_name(file.file_name);
        output->set_file_size(file.file_size);
    }
    return result;
}

}  // namespace milvus::indexbuilder
