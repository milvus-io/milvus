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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/result.h>
#include <nlohmann/json.hpp>

#include "milvus-storage/common/layout.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_internal/result.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/transaction/transaction.h"

using json = nlohmann::json;

namespace {

constexpr const char* kMilvusTableSourceManifestPathProperty =
    "milvus_table.source_manifest_path";
constexpr const char* kMilvusTableSourceRowCountProperty =
    "milvus_table.source_row_count";

arrow::Result<std::shared_ptr<milvus_storage::api::Manifest>>
ReadManifestPathAware(const std::string& manifest_path,
                      const milvus_storage::api::Properties& properties) {
    json j = json::parse(manifest_path);
    auto base_path = j.at("base_path").get<std::string>();
    auto version = j.at("ver").get<int64_t>();

    ARROW_ASSIGN_OR_RAISE(auto fs,
                          milvus_storage::FilesystemCache::getInstance().get(
                              properties, base_path));
    ARROW_ASSIGN_OR_RAISE(auto transaction,
                          milvus_storage::api::transaction::Transaction::Open(
                              fs,
                              base_path,
                              version,
                              milvus_storage::api::transaction::FailResolver,
                              1));
    return transaction->GetManifest();
}

arrow::Result<milvus_storage::api::Properties>
ConvertProperties(const LoonProperties* properties) {
    if (properties == nullptr) {
        return arrow::Status::Invalid("properties is null");
    }
    milvus_storage::api::Properties converted;
    auto opt = milvus_storage::api::ConvertFFIProperties(converted, properties);
    if (opt != std::nullopt) {
        return arrow::Status::Invalid("failed to parse properties: ", *opt);
    }
    return converted;
}

arrow::Result<std::optional<milvus_storage::StorageUri>>
ParseExternalSourceUri(const char* external_source) {
    if (external_source == nullptr || external_source[0] == '\0') {
        return std::nullopt;
    }
    auto uri_result =
        milvus_storage::StorageUri::Parse(std::string(external_source));
    if (!uri_result.ok()) {
        return uri_result.status();
    }
    auto uri = uri_result.ValueOrDie();
    if (uri.scheme.empty() || uri.bucket_name.empty()) {
        return arrow::Status::Invalid(
            "milvus-table external_source must include scheme and bucket: ",
            external_source);
    }
    return uri;
}

arrow::Result<std::string>
MakeExternalUriForSourcePath(
    const std::string& path,
    const std::optional<milvus_storage::StorageUri>& source_uri) {
    auto path_uri = milvus_storage::StorageUri::Parse(path);
    if (path_uri.ok() && !path_uri.ValueOrDie().scheme.empty()) {
        return path;
    }
    if (std::filesystem::path(path).is_absolute()) {
        return path;
    }
    if (!source_uri.has_value()) {
        return arrow::Status::Invalid(
            "milvus-table relative source path requires a valid "
            "external_source: ",
            path);
    }

    auto uri = source_uri.value();
    uri.key = path;
    return milvus_storage::StorageUri::Make(uri);
}

arrow::Status
NormalizeSourceColumnGroupPaths(
    milvus_storage::api::ColumnGroups& column_groups,
    const std::optional<milvus_storage::StorageUri>& source_uri) {
    for (auto& column_group : column_groups) {
        if (column_group == nullptr) {
            continue;
        }
        for (auto& file : column_group->files) {
            ARROW_ASSIGN_OR_RAISE(
                file.path, MakeExternalUriForSourcePath(file.path, source_uri));
        }
    }
    return arrow::Status::OK();
}

arrow::Result<milvus_storage::api::ColumnGroups>
FilterColumnGroupsToTargetColumns(
    const milvus_storage::api::ColumnGroups& column_groups,
    const std::unordered_set<std::string>& target_columns) {
    milvus_storage::api::ColumnGroups filtered_groups;
    for (const auto& column_group : column_groups) {
        if (column_group == nullptr) {
            continue;
        }

        auto filtered_group =
            std::make_shared<milvus_storage::api::ColumnGroup>(*column_group);
        filtered_group->columns.clear();

        std::unordered_set<std::string> seen;
        for (const auto& column : column_group->columns) {
            if (target_columns.find(column) != target_columns.end() &&
                seen.insert(column).second) {
                filtered_group->columns.push_back(column);
            }
        }

        if (!filtered_group->columns.empty()) {
            filtered_groups.push_back(std::move(filtered_group));
        }
    }

    if (filtered_groups.empty()) {
        return arrow::Status::Invalid(
            "milvus-table source manifest does not contain any target "
            "external columns");
    }
    return filtered_groups;
}

void
TagSourceFragmentIdentity(milvus_storage::api::ColumnGroups& column_groups,
                          const std::string& source_manifest_path,
                          uint64_t source_row_count) {
    for (auto& column_group : column_groups) {
        if (column_group == nullptr) {
            continue;
        }
        for (auto& file : column_group->files) {
            file.Set(kMilvusTableSourceManifestPathProperty,
                     source_manifest_path);
            file.Set(kMilvusTableSourceRowCountProperty,
                     std::to_string(source_row_count));
        }
    }
}

arrow::Result<milvus_storage::api::DeltaLog>
NormalizeSourceDeltaLogPath(
    const milvus_storage::api::DeltaLog& delta_log,
    const std::optional<milvus_storage::StorageUri>& source_uri) {
    auto normalized = delta_log;
    ARROW_ASSIGN_OR_RAISE(
        normalized.path,
        MakeExternalUriForSourcePath(delta_log.path, source_uri));
    return normalized;
}

bool
IsBloomFilterStatKey(const std::string& key) {
    return key.rfind("bloom_filter.", 0) == 0;
}

arrow::Result<milvus_storage::api::Statistics>
NormalizeSourceStatPaths(
    const milvus_storage::api::Statistics& stat,
    const std::optional<milvus_storage::StorageUri>& source_uri) {
    auto normalized = stat;
    for (auto& stat_path : normalized.paths) {
        ARROW_ASSIGN_OR_RAISE(
            stat_path, MakeExternalUriForSourcePath(stat_path, source_uri));
    }
    return normalized;
}

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

}  // namespace

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

    try {
        auto properties_result = ConvertProperties(properties);
        if (!properties_result.ok()) {
            RETURN_ERROR(LOON_INVALID_PROPERTIES,
                         properties_result.status().ToString());
        }
        auto converted = properties_result.ValueOrDie();
        auto source_uri_result = ParseExternalSourceUri(external_source);
        if (!source_uri_result.ok()) {
            RETURN_ERROR(LOON_INVALID_ARGS,
                         source_uri_result.status().ToString());
        }
        auto source_uri = source_uri_result.ValueOrDie();
        std::unordered_set<std::string> target_column_set;
        target_column_set.reserve(num_target_columns);
        for (size_t i = 0; i < num_target_columns; ++i) {
            if (target_columns[i] == nullptr) {
                RETURN_ERROR(LOON_INVALID_ARGS,
                             "target_columns contains null entry");
            }
            target_column_set.emplace(target_columns[i]);
        }

        auto fs_result =
            milvus_storage::FilesystemCache::getInstance().get(converted);
        if (!fs_result.ok()) {
            RETURN_ERROR(LOON_ARROW_ERROR, fs_result.status().ToString());
        }
        auto fs = fs_result.ValueOrDie();

        auto transaction_result =
            milvus_storage::api::transaction::Transaction::Open(
                fs,
                base_path,
                0,
                milvus_storage::api::transaction::OverwriteResolver,
                10);
        if (!transaction_result.ok()) {
            RETURN_ERROR(LOON_ARROW_ERROR,
                         transaction_result.status().ToString());
        }
        auto transaction = std::move(transaction_result).ValueOrDie();
        std::unordered_set<std::string> added_delta_paths;
        std::unordered_set<std::string> added_stat_paths;
        std::map<std::string, milvus_storage::api::Statistics> imported_stats;

        for (size_t i = 0; i < num_source_manifests; ++i) {
            if (source_manifest_paths[i] == nullptr) {
                RETURN_ERROR(LOON_INVALID_ARGS,
                             "source_manifest_paths contains null entry");
            }
            if (source_row_counts[i] <= 0) {
                RETURN_ERROR(LOON_INVALID_ARGS,
                             "source_row_counts contains non-positive entry");
            }
            auto source_manifest_result = ReadManifestPathAware(
                std::string(source_manifest_paths[i]), converted);
            if (!source_manifest_result.ok()) {
                RETURN_ERROR(LOON_ARROW_ERROR,
                             source_manifest_result.status().ToString());
            }
            auto source_manifest = source_manifest_result.ValueOrDie();
            auto source_column_groups = milvus_storage::api::copy_column_groups(
                source_manifest->columnGroups());
            auto normalize_status = NormalizeSourceColumnGroupPaths(
                source_column_groups, source_uri);
            if (!normalize_status.ok()) {
                RETURN_ERROR(LOON_ARROW_ERROR, normalize_status.ToString());
            }
            auto target_column_groups_result =
                FilterColumnGroupsToTargetColumns(source_column_groups,
                                                  target_column_set);
            if (!target_column_groups_result.ok()) {
                RETURN_ERROR(LOON_ARROW_ERROR,
                             target_column_groups_result.status().ToString());
            }
            auto target_column_groups =
                target_column_groups_result.ValueOrDie();
            TagSourceFragmentIdentity(
                target_column_groups,
                std::string(source_manifest_paths[i]),
                static_cast<uint64_t>(source_row_counts[i]));
            transaction->AppendFiles(target_column_groups);

            if (has_external_primary_key) {
                for (const auto& delta_log : source_manifest->deltaLogs()) {
                    auto normalized_delta_result =
                        NormalizeSourceDeltaLogPath(delta_log, source_uri);
                    if (!normalized_delta_result.ok()) {
                        RETURN_ERROR(
                            LOON_ARROW_ERROR,
                            normalized_delta_result.status().ToString());
                    }
                    auto normalized_delta =
                        normalized_delta_result.ValueOrDie();
                    if (added_delta_paths.insert(normalized_delta.path)
                            .second) {
                        transaction->AddDeltaLog(normalized_delta);
                    }
                }

                for (const auto& [key, stat] : source_manifest->stats()) {
                    if (!IsBloomFilterStatKey(key) || stat.paths.empty()) {
                        continue;
                    }
                    auto normalized_stat_result =
                        NormalizeSourceStatPaths(stat, source_uri);
                    if (!normalized_stat_result.ok()) {
                        RETURN_ERROR(
                            LOON_ARROW_ERROR,
                            normalized_stat_result.status().ToString());
                    }
                    auto normalized_stat = normalized_stat_result.ValueOrDie();
                    auto& merged_stat = imported_stats[key];
                    bool added_path = false;
                    for (const auto& stat_path : normalized_stat.paths) {
                        auto dedupe_key = key + "\n" + stat_path;
                        if (added_stat_paths.insert(dedupe_key).second) {
                            merged_stat.paths.push_back(stat_path);
                            added_path = true;
                        }
                    }
                    if (added_path) {
                        MergeStatMetadata(merged_stat.metadata,
                                          normalized_stat.metadata);
                    }
                }
            }
        }

        for (const auto& [key, stat] : imported_stats) {
            if (!stat.paths.empty()) {
                transaction->UpdateStat(key, stat);
            }
        }

        auto commit_result = transaction->Commit();
        if (!commit_result.ok()) {
            RETURN_ERROR(LOON_LOGICAL_ERROR, commit_result.status().ToString());
        }
        auto committed_version = commit_result.ValueOrDie();
        json manifest_path = {
            {"base_path", std::string(base_path)},
            {"ver", committed_version},
        };
        *out_manifest_path = strdup(manifest_path.dump().c_str());
        RETURN_SUCCESS();
    } catch (const std::exception& e) {
        RETURN_EXCEPTION(e.what());
    }

    RETURN_UNREACHABLE();
}
