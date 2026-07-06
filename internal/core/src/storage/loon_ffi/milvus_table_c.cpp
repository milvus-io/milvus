// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_internal/result.h"
#include "milvus-storage/filesystem/fs.h"

using json = nlohmann::json;

namespace {

// This adapter builds milvus-table target manifests from source StorageV3
// manifests. It intentionally talks to milvus-storage only through the public C
// FFI so URI-aware transaction open and manifest serialization stay owned by
// milvus-storage.

constexpr const char* kMilvusTableSourceManifestPathProperty =
    "milvus_table.source_manifest_path";
constexpr const char* kMilvusTableSourceRowCountProperty =
    "milvus_table.source_row_count";

struct TransactionGuard {
    LoonTransactionHandle handle = 0;

    TransactionGuard() = default;
    TransactionGuard(const TransactionGuard&) = delete;
    TransactionGuard&
    operator=(const TransactionGuard&) = delete;

    ~TransactionGuard() {
        if (handle != 0) {
            loon_transaction_destroy(handle);
        }
    }
};

struct ManifestGuard {
    LoonManifest* manifest = nullptr;

    ManifestGuard() = default;
    ManifestGuard(const ManifestGuard&) = delete;
    ManifestGuard&
    operator=(const ManifestGuard&) = delete;

    ManifestGuard(ManifestGuard&& other) noexcept : manifest(other.manifest) {
        other.manifest = nullptr;
    }

    ManifestGuard&
    operator=(ManifestGuard&& other) noexcept {
        if (this != &other) {
            if (manifest != nullptr) {
                loon_manifest_destroy(manifest);
            }
            manifest = other.manifest;
            other.manifest = nullptr;
        }
        return *this;
    }

    ~ManifestGuard() {
        if (manifest != nullptr) {
            loon_manifest_destroy(manifest);
        }
    }
};

void
FreeOwnedColumnGroups(LoonColumnGroups* groups) {
    if (groups == nullptr) {
        return;
    }
    if (groups->column_group_array != nullptr) {
        for (uint32_t i = 0; i < groups->num_of_column_groups; ++i) {
            auto& group = groups->column_group_array[i];
            if (group.columns != nullptr) {
                for (uint32_t j = 0; j < group.num_of_columns; ++j) {
                    free(const_cast<char*>(group.columns[j]));
                }
                free(const_cast<char**>(group.columns));
            }
            free(const_cast<char*>(group.format));
            if (group.files != nullptr) {
                for (uint32_t j = 0; j < group.num_of_files; ++j) {
                    auto& file = group.files[j];
                    free(const_cast<char*>(file.path));
                    if (file.property_keys != nullptr) {
                        for (uint32_t k = 0; k < file.num_properties; ++k) {
                            free(const_cast<char*>(file.property_keys[k]));
                        }
                        free(const_cast<char**>(file.property_keys));
                    }
                    if (file.property_values != nullptr) {
                        for (uint32_t k = 0; k < file.num_properties; ++k) {
                            free(const_cast<char*>(file.property_values[k]));
                        }
                        free(const_cast<char**>(file.property_values));
                    }
                }
                free(group.files);
            }
        }
        free(groups->column_group_array);
    }
    free(groups);
}

struct OwnedColumnGroups {
    LoonColumnGroups* groups = nullptr;

    OwnedColumnGroups() = default;
    OwnedColumnGroups(const OwnedColumnGroups&) = delete;
    OwnedColumnGroups&
    operator=(const OwnedColumnGroups&) = delete;

    OwnedColumnGroups(OwnedColumnGroups&& other) noexcept
        : groups(other.groups) {
        other.groups = nullptr;
    }

    ~OwnedColumnGroups() {
        FreeOwnedColumnGroups(groups);
    }
};

void
ThrowIfFFIError(LoonFFIResult result, const std::string& context) {
    if (loon_ffi_is_success(&result) != 0) {
        loon_ffi_free_result(&result);
        return;
    }

    const char* msg = loon_ffi_get_errmsg(&result);
    std::string detail = msg != nullptr ? msg : "unknown error";
    loon_ffi_free_result(&result);
    throw std::runtime_error(context + ": " + detail);
}

char*
DupString(const std::string& value) {
    auto* copied = strdup(value.c_str());
    if (copied == nullptr) {
        throw std::bad_alloc();
    }
    return copied;
}

std::string
TrimSlashes(std::string value) {
    while (!value.empty() && value.front() == '/') {
        value.erase(value.begin());
    }
    while (!value.empty() && value.back() == '/') {
        value.pop_back();
    }
    return value;
}

std::string
FindExtfsProperty(const LoonProperties* properties, const std::string& suffix) {
    if (properties == nullptr || properties->properties == nullptr) {
        return "";
    }
    for (size_t i = 0; i < properties->count; ++i) {
        const auto& property = properties->properties[i];
        if (property.key == nullptr || property.value == nullptr) {
            continue;
        }
        std::string key(property.key);
        if (key.rfind("extfs.", 0) == 0 && key.size() >= suffix.size() &&
            key.compare(key.size() - suffix.size(), suffix.size(), suffix) ==
                0) {
            return property.value;
        }
    }
    return "";
}

std::string
MakeStorageUri(const milvus_storage::StorageUri& uri, bool include_address) {
    auto result = milvus_storage::StorageUri::Make(uri, include_address);
    if (!result.ok()) {
        throw std::runtime_error("make storage URI: " +
                                 result.status().ToString());
    }
    return result.ValueOrDie();
}

milvus_storage::StorageUri
ParseStorageUri(const std::string& uri, bool include_address) {
    auto result = milvus_storage::StorageUri::Parse(uri, include_address);
    if (!result.ok()) {
        throw std::runtime_error("parse storage URI " + uri + ": " +
                                 result.status().ToString());
    }
    return result.ValueOrDie();
}

bool
TryParseStorageUri(const std::string& uri,
                   bool include_address,
                   milvus_storage::StorageUri& parsed) {
    auto result = milvus_storage::StorageUri::Parse(uri, include_address);
    if (!result.ok()) {
        return false;
    }
    parsed = result.ValueOrDie();
    return true;
}

std::string
NormalizeExtfsAddress(const std::string& address) {
    if (address.empty()) {
        return "";
    }
    // StorageUri normalizes HTTP(S) addresses while building a URI; dummy
    // bucket/key let us reuse that behavior for the endpoint-only extfs address.
    milvus_storage::StorageUri uri;
    uri.scheme = "s3";
    uri.address = address;
    uri.bucket_name = "__bucket__";
    uri.key = "__key__";
    auto normalized_uri = ParseStorageUri(MakeStorageUri(uri, true), true);
    return normalized_uri.address;
}

std::string
NormalizeExternalPathForStorage(const std::string& source_path,
                                const LoonProperties* properties) {
    if (properties == nullptr) {
        return source_path;
    }

    auto uri = ParseStorageUri(source_path, false);
    if (uri.IsRelativeUri()) {
        return source_path;
    }
    auto bucket_name = FindExtfsProperty(properties, ".bucket_name");
    auto address_host =
        NormalizeExtfsAddress(FindExtfsProperty(properties, ".address"));
    if (bucket_name.empty() || address_host.empty() ||
        uri.bucket_name != bucket_name || address_host == uri.bucket_name) {
        return source_path;
    }

    uri.address = address_host;
    return MakeStorageUri(uri, true);
}

std::string
MakeExternalUriForSourcePath(const std::string& source_path,
                             const char* external_source,
                             const LoonProperties* properties) {
    if (source_path.empty()) {
        return source_path;
    }
    auto path_uri = ParseStorageUri(source_path, false);
    if (path_uri.IsAbsoluteUri()) {
        return NormalizeExternalPathForStorage(source_path, properties);
    }
    if (std::filesystem::path(source_path).is_absolute()) {
        return source_path;
    }
    if (external_source == nullptr || external_source[0] == '\0' ||
        properties == nullptr) {
        throw std::runtime_error(
            "milvus-table relative source path requires external_source: " +
            source_path);
    }

    auto source = ParseStorageUri(external_source, false);
    if (source.IsRelativeUri()) {
        throw std::runtime_error(
            "milvus-table external_source must be a storage URI: " +
            std::string(external_source));
    }
    auto bucket_name = FindExtfsProperty(properties, ".bucket_name");
    if (bucket_name.empty()) {
        throw std::runtime_error(
            "milvus-table relative source path requires extfs bucket_name: " +
            source_path);
    }
    auto address_host =
        NormalizeExtfsAddress(FindExtfsProperty(properties, ".address"));

    milvus_storage::StorageUri resolved;
    resolved.scheme = source.scheme;
    resolved.bucket_name = bucket_name;
    resolved.key = TrimSlashes(source_path);
    if (!address_host.empty()) {
        resolved.address = address_host;
        return MakeStorageUri(resolved, true);
    }

    if (source.bucket_name == bucket_name) {
        return MakeStorageUri(resolved, false);
    }

    milvus_storage::StorageUri source_with_address;
    if (TryParseStorageUri(external_source, true, source_with_address) &&
        source_with_address.IsAbsoluteUri() &&
        source_with_address.bucket_name == bucket_name &&
        !source_with_address.address.empty()) {
        resolved.address = source_with_address.address;
        return MakeStorageUri(resolved, true);
    }

    resolved.bucket_name = source.bucket_name;
    return MakeStorageUri(resolved, false);
}

uint32_t
CheckedUint32(size_t value, const char* field_name) {
    if (value > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(std::string(field_name) +
                                 " exceeds uint32_t range");
    }
    return static_cast<uint32_t>(value);
}

struct FileData {
    std::string path;
    int64_t start_index = 0;
    int64_t end_index = 0;
    std::vector<std::pair<std::string, std::string>> properties;
};

struct ColumnGroupData {
    std::vector<std::string> columns;
    std::string format;
    std::vector<FileData> files;
};

void
SetProperty(std::vector<std::pair<std::string, std::string>>& properties,
            const std::string& key,
            const std::string& value) {
    for (auto& property : properties) {
        if (property.first == key) {
            property.second = value;
            return;
        }
    }
    properties.emplace_back(key, value);
}

std::vector<std::pair<std::string, std::string>>
CopyProperties(const LoonColumnGroupFile& file) {
    std::vector<std::pair<std::string, std::string>> properties;
    if (file.num_properties == 0) {
        return properties;
    }
    if (file.property_keys == nullptr || file.property_values == nullptr) {
        throw std::runtime_error("column group file has malformed properties");
    }
    properties.reserve(file.num_properties);
    for (uint32_t i = 0; i < file.num_properties; ++i) {
        if (file.property_keys[i] == nullptr ||
            file.property_values[i] == nullptr) {
            throw std::runtime_error(
                "column group file has null property entry");
        }
        properties.emplace_back(file.property_keys[i], file.property_values[i]);
    }
    return properties;
}

std::vector<ColumnGroupData>
BuildTargetColumnGroups(const LoonColumnGroups& source_groups,
                        const std::unordered_set<std::string>& target_columns,
                        const std::string& source_manifest_path,
                        int64_t source_row_count,
                        const char* external_source,
                        const LoonProperties* properties) {
    std::vector<ColumnGroupData> result;
    if (source_groups.num_of_column_groups == 0) {
        return result;
    }
    if (source_groups.column_group_array == nullptr) {
        throw std::runtime_error(
            "source manifest has column group count but no column groups");
    }

    const std::string source_row_count_text = std::to_string(source_row_count);
    for (uint32_t i = 0; i < source_groups.num_of_column_groups; ++i) {
        const auto& source_group = source_groups.column_group_array[i];
        if (source_group.columns == nullptr &&
            source_group.num_of_columns > 0) {
            throw std::runtime_error(
                "source column group has column count but no columns");
        }

        ColumnGroupData target_group;
        std::unordered_set<std::string> seen_columns;
        for (uint32_t j = 0; j < source_group.num_of_columns; ++j) {
            if (source_group.columns[j] == nullptr) {
                throw std::runtime_error(
                    "source column group has null column name");
            }
            std::string column(source_group.columns[j]);
            if (target_columns.find(column) != target_columns.end() &&
                seen_columns.insert(column).second) {
                target_group.columns.push_back(std::move(column));
            }
        }
        if (target_group.columns.empty()) {
            continue;
        }

        if (source_group.format == nullptr) {
            throw std::runtime_error("source column group has null format");
        }
        target_group.format = source_group.format;
        if (source_group.files == nullptr && source_group.num_of_files > 0) {
            throw std::runtime_error(
                "source column group has file count but no files");
        }
        target_group.files.reserve(source_group.num_of_files);
        for (uint32_t j = 0; j < source_group.num_of_files; ++j) {
            const auto& source_file = source_group.files[j];
            if (source_file.path == nullptr) {
                throw std::runtime_error("source column group has null path");
            }
            FileData target_file;
            target_file.path = MakeExternalUriForSourcePath(
                source_file.path, external_source, properties);
            target_file.start_index = source_file.start_index;
            target_file.end_index = source_file.end_index;
            target_file.properties = CopyProperties(source_file);
            SetProperty(target_file.properties,
                        kMilvusTableSourceManifestPathProperty,
                        source_manifest_path);
            SetProperty(target_file.properties,
                        kMilvusTableSourceRowCountProperty,
                        source_row_count_text);
            target_group.files.push_back(std::move(target_file));
        }

        result.push_back(std::move(target_group));
    }
    return result;
}

OwnedColumnGroups
MaterializeColumnGroups(const std::vector<ColumnGroupData>& groups) {
    OwnedColumnGroups owned;
    owned.groups =
        static_cast<LoonColumnGroups*>(calloc(1, sizeof(LoonColumnGroups)));
    if (owned.groups == nullptr) {
        throw std::bad_alloc();
    }

    owned.groups->num_of_column_groups =
        CheckedUint32(groups.size(), "num_of_column_groups");
    if (groups.empty()) {
        return owned;
    }
    owned.groups->column_group_array = static_cast<LoonColumnGroup*>(
        calloc(groups.size(), sizeof(LoonColumnGroup)));
    if (owned.groups->column_group_array == nullptr) {
        throw std::bad_alloc();
    }

    for (size_t i = 0; i < groups.size(); ++i) {
        const auto& group_data = groups[i];
        auto& group = owned.groups->column_group_array[i];
        group.num_of_columns =
            CheckedUint32(group_data.columns.size(), "num_of_columns");
        group.columns = static_cast<const char**>(
            calloc(group_data.columns.size(), sizeof(char*)));
        if (group.columns == nullptr && !group_data.columns.empty()) {
            throw std::bad_alloc();
        }
        for (size_t j = 0; j < group_data.columns.size(); ++j) {
            group.columns[j] = DupString(group_data.columns[j]);
        }

        group.format = DupString(group_data.format);
        group.num_of_files =
            CheckedUint32(group_data.files.size(), "num_of_files");
        group.files = static_cast<LoonColumnGroupFile*>(
            calloc(group_data.files.size(), sizeof(LoonColumnGroupFile)));
        if (group.files == nullptr && !group_data.files.empty()) {
            throw std::bad_alloc();
        }
        for (size_t j = 0; j < group_data.files.size(); ++j) {
            const auto& file_data = group_data.files[j];
            auto& file = group.files[j];
            file.path = DupString(file_data.path);
            file.start_index = file_data.start_index;
            file.end_index = file_data.end_index;
            file.num_properties =
                CheckedUint32(file_data.properties.size(), "num_properties");
            file.property_keys = static_cast<const char**>(
                calloc(file_data.properties.size(), sizeof(char*)));
            file.property_values = static_cast<const char**>(
                calloc(file_data.properties.size(), sizeof(char*)));
            if ((file.property_keys == nullptr ||
                 file.property_values == nullptr) &&
                !file_data.properties.empty()) {
                throw std::bad_alloc();
            }
            for (size_t k = 0; k < file_data.properties.size(); ++k) {
                file.property_keys[k] =
                    DupString(file_data.properties[k].first);
                file.property_values[k] =
                    DupString(file_data.properties[k].second);
            }
        }
    }
    return owned;
}

ManifestGuard
ReadManifestWithFFI(const std::string& manifest_path,
                    const LoonProperties* properties) {
    json j = json::parse(manifest_path);
    auto base_path = j.at("base_path").get<std::string>();
    auto version = j.at("ver").get<int64_t>();

    TransactionGuard transaction;
    ThrowIfFFIError(loon_transaction_begin(base_path.c_str(),
                                           properties,
                                           version,
                                           LOON_TRANSACTION_RESOLVE_FAIL,
                                           1,
                                           &transaction.handle),
                    "open source manifest transaction " + manifest_path);

    ManifestGuard manifest;
    ThrowIfFFIError(
        loon_transaction_get_manifest(transaction.handle, &manifest.manifest),
        "get source manifest " + manifest_path);
    if (manifest.manifest == nullptr) {
        throw std::runtime_error("source manifest is nil: " + manifest_path);
    }
    return manifest;
}

bool
IsBloomFilterStatKey(const std::string& key) {
    return key.rfind("bloom_filter.", 0) == 0;
}

struct StatData {
    std::vector<std::string> paths;
    std::map<std::string, std::string> metadata;
};

void
MergeStatMetadata(std::map<std::string, std::string>& target,
                  const std::map<std::string, std::string>& source) {
    for (const auto& [key, value] : source) {
        if (key == "memory_size") {
            auto existing = target.find(key);
            if (existing == target.end()) {
                target.emplace(key, value);
                continue;
            }
            try {
                existing->second = std::to_string(std::stoll(existing->second) +
                                                  std::stoll(value));
            } catch (const std::exception&) {
                existing->second = value;
            }
            continue;
        }
        target.emplace(key, value);
    }
}

std::map<std::string, std::string>
ReadStatMetadata(const LoonStatsLog& stats, uint32_t index) {
    std::map<std::string, std::string> metadata;
    auto metadata_count = stats.stat_metadata_counts[index];
    if (metadata_count == 0) {
        return metadata;
    }
    if (stats.stat_metadata_keys == nullptr ||
        stats.stat_metadata_values == nullptr ||
        stats.stat_metadata_keys[index] == nullptr ||
        stats.stat_metadata_values[index] == nullptr) {
        throw std::runtime_error("manifest stat has malformed metadata");
    }

    for (uint32_t i = 0; i < metadata_count; ++i) {
        const char* key = stats.stat_metadata_keys[index][i];
        const char* value = stats.stat_metadata_values[index][i];
        if (key == nullptr || value == nullptr) {
            throw std::runtime_error("manifest stat has null metadata entry");
        }
        metadata.emplace(key, value);
    }
    return metadata;
}

void
CollectBloomFilterStats(const LoonStatsLog& source_stats,
                        const char* external_source,
                        const LoonProperties* properties,
                        std::unordered_set<std::string>& added_stat_paths,
                        std::map<std::string, StatData>& imported_stats) {
    if (source_stats.num_stats == 0) {
        return;
    }
    if (source_stats.stat_keys == nullptr ||
        source_stats.stat_files == nullptr ||
        source_stats.stat_file_counts == nullptr ||
        source_stats.stat_metadata_counts == nullptr) {
        throw std::runtime_error("manifest has malformed stats");
    }

    for (uint32_t i = 0; i < source_stats.num_stats; ++i) {
        if (source_stats.stat_keys[i] == nullptr) {
            throw std::runtime_error("manifest stat has null key");
        }
        std::string key(source_stats.stat_keys[i]);
        auto file_count = source_stats.stat_file_counts[i];
        if (!IsBloomFilterStatKey(key) || file_count == 0) {
            continue;
        }
        if (source_stats.stat_files[i] == nullptr) {
            throw std::runtime_error(
                "manifest stat has file count but no files");
        }

        auto metadata = ReadStatMetadata(source_stats, i);
        auto& target = imported_stats[key];
        bool added_path = false;
        for (uint32_t j = 0; j < file_count; ++j) {
            if (source_stats.stat_files[i][j] == nullptr) {
                throw std::runtime_error("manifest stat has null file path");
            }
            auto normalized_path = MakeExternalUriForSourcePath(
                source_stats.stat_files[i][j], external_source, properties);
            auto dedupe_key = key + "\n" + normalized_path;
            if (added_stat_paths.insert(dedupe_key).second) {
                target.paths.push_back(std::move(normalized_path));
                added_path = true;
            }
        }
        if (added_path) {
            MergeStatMetadata(target.metadata, metadata);
        }
    }
}

void
AppendImportedStats(LoonTransactionHandle transaction,
                    const std::map<std::string, StatData>& imported_stats) {
    for (const auto& [key, stat] : imported_stats) {
        if (stat.paths.empty()) {
            continue;
        }

        std::vector<const char*> files;
        files.reserve(stat.paths.size());
        for (const auto& path : stat.paths) {
            files.push_back(path.c_str());
        }

        std::vector<const char*> metadata_keys;
        std::vector<const char*> metadata_values;
        metadata_keys.reserve(stat.metadata.size());
        metadata_values.reserve(stat.metadata.size());
        for (const auto& [meta_key, meta_value] : stat.metadata) {
            metadata_keys.push_back(meta_key.c_str());
            metadata_values.push_back(meta_value.c_str());
        }

        ThrowIfFFIError(
            loon_transaction_update_stat(
                transaction,
                key.c_str(),
                files.empty() ? nullptr : files.data(),
                files.size(),
                metadata_keys.empty() ? nullptr : metadata_keys.data(),
                metadata_values.empty() ? nullptr : metadata_values.data(),
                metadata_keys.size()),
            "update milvus-table bloom-filter stat " + key);
    }
}

void
AppendSourceDeltalogs(LoonTransactionHandle transaction,
                      const LoonDeltaLogs& delta_logs,
                      const char* external_source,
                      const LoonProperties* properties,
                      std::unordered_set<std::string>& added_delta_paths) {
    if (delta_logs.num_delta_logs == 0) {
        return;
    }
    if (delta_logs.delta_log_paths == nullptr ||
        delta_logs.delta_log_num_entries == nullptr) {
        throw std::runtime_error("manifest has malformed delta logs");
    }
    for (uint32_t i = 0; i < delta_logs.num_delta_logs; ++i) {
        if (delta_logs.delta_log_paths[i] == nullptr) {
            throw std::runtime_error("manifest has null delta log path");
        }
        auto normalized_path = MakeExternalUriForSourcePath(
            delta_logs.delta_log_paths[i], external_source, properties);
        if (!added_delta_paths.insert(normalized_path).second) {
            continue;
        }
        ThrowIfFFIError(
            loon_transaction_add_delta_log(
                transaction,
                normalized_path.c_str(),
                static_cast<int64_t>(delta_logs.delta_log_num_entries[i])),
            "add milvus-table source deltalog " + normalized_path);
    }
}

}  // namespace

// loon_milvus_table_create_manifest_from_segment_manifests creates one target
// StorageV3 manifest from source StorageV3 segment manifests. Real-PK mode also
// imports source deltalogs and bloom-filter stats; virtual-PK mode leaves delete
// translation to DataNode.
extern "C" LoonFFIResult
loon_milvus_table_create_manifest_from_segment_manifests(
    const char* base_path,
    char** source_manifest_paths,
    const int64_t* source_row_counts,
    size_t num_source_manifests,
    char** target_columns,
    size_t num_target_columns,
    const char* external_source,
    const LoonProperties* properties,
    int has_external_primary_key,
    char** out_manifest_path) {
    if (base_path == nullptr || source_manifest_paths == nullptr ||
        source_row_counts == nullptr || num_source_manifests == 0 ||
        target_columns == nullptr || num_target_columns == 0 ||
        out_manifest_path == nullptr) {
        RETURN_ERROR(LOON_INVALID_ARGS,
                     "base_path, source_manifest_paths, source_row_counts, "
                     "target_columns, and out_manifest_path must not be empty");
    }
    *out_manifest_path = nullptr;

    try {
        std::unordered_set<std::string> target_column_set;
        target_column_set.reserve(num_target_columns);
        for (size_t i = 0; i < num_target_columns; ++i) {
            if (target_columns[i] == nullptr) {
                RETURN_ERROR(LOON_INVALID_ARGS,
                             "target_columns contains null entry");
            }
            target_column_set.emplace(target_columns[i]);
        }

        TransactionGuard transaction;
        ThrowIfFFIError(
            loon_transaction_begin(base_path,
                                   properties,
                                   0,
                                   LOON_TRANSACTION_RESOLVE_OVERWRITE,
                                   10,
                                   &transaction.handle),
            "open milvus-table target manifest transaction");

        std::unordered_set<std::string> added_delta_paths;
        std::unordered_set<std::string> added_stat_paths;
        std::map<std::string, StatData> imported_stats;

        for (size_t i = 0; i < num_source_manifests; ++i) {
            if (source_manifest_paths[i] == nullptr) {
                RETURN_ERROR(LOON_INVALID_ARGS,
                             "source_manifest_paths contains null entry");
            }
            if (source_row_counts[i] <= 0) {
                RETURN_ERROR(LOON_INVALID_ARGS,
                             "source_row_counts contains non-positive entry");
            }

            std::string source_manifest_path(source_manifest_paths[i]);
            auto source_manifest =
                ReadManifestWithFFI(source_manifest_path, properties);
            auto target_groups_data =
                BuildTargetColumnGroups(source_manifest.manifest->column_groups,
                                        target_column_set,
                                        source_manifest_path,
                                        source_row_counts[i],
                                        external_source,
                                        properties);
            if (target_groups_data.empty()) {
                RETURN_ERROR(LOON_ARROW_ERROR,
                             "milvus-table source manifest does not contain "
                             "any target external columns");
            }

            auto target_groups = MaterializeColumnGroups(target_groups_data);
            ThrowIfFFIError(loon_transaction_append_files(transaction.handle,
                                                          target_groups.groups),
                            "append milvus-table source column groups");

            if (has_external_primary_key) {
                AppendSourceDeltalogs(transaction.handle,
                                      source_manifest.manifest->delta_logs,
                                      external_source,
                                      properties,
                                      added_delta_paths);
                CollectBloomFilterStats(source_manifest.manifest->stats,
                                        external_source,
                                        properties,
                                        added_stat_paths,
                                        imported_stats);
            }
        }

        AppendImportedStats(transaction.handle, imported_stats);

        int64_t committed_version = 0;
        ThrowIfFFIError(
            loon_transaction_commit(transaction.handle, &committed_version),
            "commit milvus-table target manifest");
        json manifest_path = {
            {"base_path", std::string(base_path)},
            {"ver", committed_version},
        };
        *out_manifest_path = strdup(manifest_path.dump().c_str());
        if (*out_manifest_path == nullptr) {
            RETURN_ERROR(LOON_MEMORY_ERROR,
                         "failed to allocate milvus-table manifest path");
        }
        RETURN_SUCCESS();
    } catch (const std::exception& e) {
        RETURN_EXCEPTION(e.what());
    }

    RETURN_UNREACHABLE();
}
