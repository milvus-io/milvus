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

#include <algorithm>
#include <cctype>
#include <iterator>
#include <memory>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "index/Meta.h"
#include "common/resource_c.h"
#include "index/IndexFactory.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/manifest.h"
#include "pb/schema.pb.h"
#include "segcore/SegmentLoadInfo.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

namespace milvus::segcore {

std::shared_ptr<milvus_storage::api::ColumnGroups>
SegmentLoadInfo::GetColumnGroups() {
    auto manifest_path = GetManifestPath();
    if (manifest_path.empty()) {
        return nullptr;
    }
    // return cached result if exists
    if (column_groups_ != nullptr) {
        return column_groups_;
    }
    auto properties = milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                          .GetProperties();

    auto loon_manifest = ::GetLoonManifest(manifest_path, properties);
    column_groups_ = std::make_shared<milvus_storage::api::ColumnGroups>(
        loon_manifest->columnGroups());
    return column_groups_;
}

LoadIndexInfo
SegmentLoadInfo::ConvertFieldIndexInfoToLoadIndexInfo(
    const proto::segcore::FieldIndexInfo* field_index_info,
    int64_t segment_id) const {
    LoadIndexInfo load_index_info;

    load_index_info.segment_id = segment_id;
    // Extract field ID
    auto field_id = FieldId(field_index_info->fieldid());
    load_index_info.field_id = field_id.get();
    load_index_info.partition_id = GetPartitionID();

    // Get field type from schema
    const auto& field_meta = schema_->operator[](field_id);
    load_index_info.field_type = field_meta.get_data_type();
    load_index_info.element_type = field_meta.get_element_type();

    // Set index metadata
    load_index_info.index_id = field_index_info->indexid();
    load_index_info.index_build_id = field_index_info->buildid();
    load_index_info.index_version = field_index_info->index_version();
    load_index_info.index_engine_version =
        static_cast<IndexVersion>(field_index_info->current_index_version());
    load_index_info.index_size = field_index_info->index_size();
    load_index_info.num_rows = field_index_info->num_rows();
    load_index_info.schema = field_meta.ToProto();

    // Copy index file paths, excluding indexParams file
    for (const auto& file_path : field_index_info->index_file_paths()) {
        size_t last_slash = file_path.find_last_of('/');
        std::string filename = (last_slash != std::string::npos)
                                   ? file_path.substr(last_slash + 1)
                                   : file_path;

        if (filename != "indexParams") {
            load_index_info.index_files.push_back(file_path);
        }
    }

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    auto use_mmap = IsVectorDataType(field_meta.get_data_type())
                        ? mmap_config.GetVectorIndexEnableMmap()
                        : mmap_config.GetScalarIndexEnableMmap();

    // Set index params
    for (const auto& kv_pair : field_index_info->index_params()) {
        if (kv_pair.key() == "mmap.enabled") {
            std::string lower;
            std::transform(kv_pair.value().begin(),
                           kv_pair.value().end(),
                           std::back_inserter(lower),
                           ::tolower);
            use_mmap = (lower == "true");
        }
        // Extract warmup policy from index params
        if (kv_pair.key() == "warmup") {
            load_index_info.warmup_policy = kv_pair.value();
        }
        load_index_info.index_params[kv_pair.key()] = kv_pair.value();
    }

    // Inject scalar index version into index_params for scalar indexes
    auto scalar_version = field_index_info->current_scalar_index_version();
    if (scalar_version > 0) {
        load_index_info
            .index_params[milvus::index::SCALAR_INDEX_ENGINE_VERSION] =
            std::to_string(scalar_version);
    }
    size_t dim =
        IsVectorDataType(field_meta.get_data_type()) &&
                !IsSparseFloatVectorDataType(field_meta.get_data_type())
            ? field_meta.get_dim()
            : 1;
    load_index_info.dim = dim;
    load_index_info.mmap_dir_path =
        milvus::storage::LocalChunkManagerSingleton::GetInstance()
            .GetChunkManager()
            ->GetRootPath();
    load_index_info.enable_mmap = use_mmap;

    return load_index_info;
}

bool
SegmentLoadInfo::CheckIndexHasRawData(const LoadIndexInfo& load_index_info) {
    auto request = milvus::index::IndexFactory::GetInstance().IndexLoadResource(
        load_index_info.field_type,
        load_index_info.element_type,
        load_index_info.index_engine_version,
        load_index_info.index_size,
        load_index_info.index_params,
        load_index_info.enable_mmap,
        load_index_info.num_rows,
        load_index_info.dim);

    return request.has_raw_data;
}

std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>
SegmentLoadInfo::ConvertTextIndexStatsToLoadTextIndexInfo(
    const proto::segcore::TextIndexStats& text_index_stats,
    FieldId field_id) const {
    auto info = std::make_shared<proto::indexcgo::LoadTextIndexInfo>();

    info->set_fieldid(text_index_stats.fieldid());
    info->set_version(text_index_stats.version());
    info->set_buildid(text_index_stats.buildid());
    for (const auto& f : text_index_stats.files()) {
        info->add_files(f);
    }

    const auto& field_meta = schema_->operator[](field_id);
    *info->mutable_schema() = field_meta.ToProto();

    info->set_collectionid(GetCollectionID());
    info->set_partitionid(GetPartitionID());
    info->set_load_priority(GetPriority());

    // Text match index mmap config is based on the scalar field mmap
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    auto [field_has_setting, field_mmap_enabled] =
        schema_->MmapEnabled(field_id);
    bool enable_mmap = field_has_setting
                           ? field_mmap_enabled
                           : mmap_config.GetScalarFieldEnableMmap();
    info->set_enable_mmap(enable_mmap);
    info->set_index_size(text_index_stats.memory_size());
    info->set_current_scalar_index_version(
        text_index_stats.current_scalar_index_version());

    // Text match index warmup policy based on scalar field's warmup
    auto [field_has_warmup, field_warmup_policy] = schema_->WarmupPolicy(
        field_id, /*is_vector=*/false, /*is_index=*/false);
    if (field_has_warmup) {
        info->set_warmup_policy(field_warmup_policy);
    }

    return info;
}

void
SegmentLoadInfo::ComputeDiffIndexes(LoadDiff& diff, SegmentLoadInfo& new_info) {
    // Get current index IDs from converted cache
    std::set<int64_t> current_index_ids;
    // Build a set of field IDs that currently have indexes loaded
    std::set<FieldId> current_indexed_fields;
    for (const auto& load_index_info : converted_index_infos_) {
        current_index_ids.insert(load_index_info.index_id);
        current_indexed_fields.insert(FieldId(load_index_info.field_id));
    }

    std::set<int64_t> new_index_ids;
    // Find indexes to load/replace: indexes in new_info but not in current
    // Use converted_field_index_cache_ from new_info
    for (const auto& [field_id, load_index_infos] :
         new_info.converted_field_index_cache_) {
        for (const auto& load_index_info : load_index_infos) {
            new_index_ids.insert(load_index_info.index_id);
            if (current_index_ids.find(load_index_info.index_id) ==
                current_index_ids.end()) {
                // New index_id: check if field already has an index loaded
                if (current_indexed_fields.find(field_id) !=
                    current_indexed_fields.end()) {
                    diff.indexes_to_replace[field_id].push_back(
                        load_index_info);
                } else {
                    diff.indexes_to_load[field_id].push_back(load_index_info);
                }
            }
        }
    }

    // Find indexes to drop: fields that have indexes in current but not in new_info
    for (const auto& load_index_info : converted_index_infos_) {
        if (new_index_ids.find(load_index_info.index_id) ==
            new_index_ids.end()) {
            diff.indexes_to_drop.insert(FieldId(load_index_info.field_id));
        }
    }
}

void
SegmentLoadInfo::ComputeDiffBinlogs(LoadDiff& diff, SegmentLoadInfo& new_info) {
    // field id -> binlog group id
    std::map<int64_t, int64_t> current_fields;
    for (int i = 0; i < GetBinlogPathCount(); i++) {
        auto& field_binlog = GetBinlogPath(i);
        std::vector<int64_t> child_fields(field_binlog.child_fields().begin(),
                                          field_binlog.child_fields().end());
        // v1 or legacy, group id == field id
        if (child_fields.empty()) {
            child_fields.emplace_back(field_binlog.fieldid());
        }

        for (auto child_id : child_fields) {
            current_fields[child_id] = field_binlog.fieldid();
        }
    }

    std::map<int64_t, int64_t> new_binlog_fields;
    for (int i = 0; i < new_info.GetBinlogPathCount(); i++) {
        auto& new_field_binlog = new_info.GetBinlogPath(i);
        std::vector<FieldId> ids_to_load;
        std::vector<FieldId> ids_to_replace;
        std::vector<int64_t> child_fields(
            new_field_binlog.child_fields().begin(),
            new_field_binlog.child_fields().end());
        // v1 or legacy, group id == field id
        if (child_fields.empty()) {
            child_fields.emplace_back(new_field_binlog.fieldid());
        }
        for (auto child_id : child_fields) {
            new_binlog_fields[child_id] = new_field_binlog.fieldid();
            auto iter = current_fields.find(new_field_binlog.fieldid());
            // Find binlogs to load/replace: fields in new_info not matching current
            if (iter == current_fields.end() ||
                iter->second != new_field_binlog.fieldid()) {
                // Check if this child field already exists in current
                // (either from binlogs or from default value filling)
                if (current_fields.find(child_id) != current_fields.end() ||
                    fields_filled_with_default_.count(FieldId(child_id)) > 0) {
                    ids_to_replace.emplace_back(child_id);
                } else {
                    ids_to_load.emplace_back(child_id);
                }
            }
        }
        if (!ids_to_load.empty()) {
            diff.binlogs_to_load.emplace_back(ids_to_load, new_field_binlog);
        }
        if (!ids_to_replace.empty()) {
            diff.binlogs_to_replace.emplace_back(ids_to_replace,
                                                 new_field_binlog);
        }
    }

    // Find field data to drop: fields in current but not in new_info
    for (const auto& [field_id, group_id] : current_fields) {
        if (new_binlog_fields.find(field_id) == new_binlog_fields.end()) {
            diff.field_data_to_drop.emplace(field_id);
        }
    }
}

void
SegmentLoadInfo::ComputeDiffColumnGroups(LoadDiff& diff,
                                         SegmentLoadInfo& new_info) {
    auto cur_column_group = GetColumnGroups();
    auto new_column_group = new_info.GetColumnGroups();

    AssertInfo(cur_column_group, "current column groups shall not be null");
    AssertInfo(new_column_group, "new column groups shall not be null");

    // Build a set of current FieldIds from current column groups
    std::map<int64_t, int> cur_field_ids;
    for (int i = 0; i < cur_column_group->size(); i++) {
        auto cg = cur_column_group->at(i);
        for (const auto& column : cg->columns) {
            auto field_id = std::stoll(column);
            cur_field_ids.emplace(field_id, i);
        }
    }

    // Build a set of new FieldIds and find column groups to load/replace
    std::map<int64_t, int> new_field_ids;
    for (int i = 0; i < new_column_group->size(); i++) {
        auto cg = new_column_group->at(i);
        std::vector<FieldId> fields;
        std::vector<FieldId> replace_fields;
        std::vector<FieldId> lazy_fields;
        std::vector<FieldId> lazy_replace_fields;
        for (const auto& column : cg->columns) {
            auto field_id = std::stoll(column);
            new_field_ids.emplace(field_id, i);

            auto iter = cur_field_ids.find(field_id);
            bool was_default_filled =
                fields_filled_with_default_.count(FieldId(field_id)) > 0;
            bool is_new_field =
                iter == cur_field_ids.end() && !was_default_filled;
            bool is_replace_field =
                was_default_filled ||
                (iter != cur_field_ids.end() && iter->second != i);
            if (is_new_field) {
                // Field not in current and not default-filled → new load
                if (field_id < START_USER_FIELDID ||
                    (schema_->ShouldLoadField(FieldId(field_id)) &&
                     field_index_has_raw_data_.find(FieldId(field_id)) ==
                         field_index_has_raw_data_.end())) {
                    fields.emplace_back(field_id);
                } else {
                    lazy_fields.emplace_back(field_id);
                }
            } else if (is_replace_field) {
                // Field was default-filled or moved between groups → replace
                if (field_id < START_USER_FIELDID ||
                    (schema_->ShouldLoadField(FieldId(field_id)) &&
                     field_index_has_raw_data_.find(FieldId(field_id)) ==
                         field_index_has_raw_data_.end())) {
                    replace_fields.emplace_back(field_id);
                } else {
                    lazy_replace_fields.emplace_back(field_id);
                }
            }
        }
        if (!fields.empty()) {
            diff.column_groups_to_load.emplace_back(i, fields);
        }
        if (!replace_fields.empty()) {
            diff.column_groups_to_replace.emplace_back(i, replace_fields);
        }
        if (!lazy_fields.empty()) {
            diff.column_groups_to_lazyload.emplace_back(i, lazy_fields);
        }
        if (!lazy_replace_fields.empty()) {
            diff.column_groups_to_lazyreplace.emplace_back(i,
                                                           lazy_replace_fields);
        }
    }

    // Find field data to drop: fields in current but not in new
    for (const auto& [field_id, cg_index] : cur_field_ids) {
        if (new_field_ids.find(field_id) == new_field_ids.end()) {
            diff.field_data_to_drop.emplace(field_id);
        }
    }
}

void
SegmentLoadInfo::ComputeDiffReloadFields(LoadDiff& diff,
                                         SegmentLoadInfo& new_info) {
    // Find fields that were previously skipped (index had raw data)
    // but now need loading (index no longer has raw data or was dropped)
    for (const auto& field_id : field_index_has_raw_data_) {
        // If new_info doesn't have this field in index_has_raw_data_,
        // we need to reload the field data
        if (new_info.field_index_has_raw_data_.find(field_id) ==
            new_info.field_index_has_raw_data_.end()) {
            diff.fields_to_reload.emplace_back(field_id);
        }
    }
}

void
SegmentLoadInfo::ComputeDiffDefaultFields(LoadDiff& diff,
                                          SegmentLoadInfo& new_info) {
    // Helper lambda to collect all field ids with data source
    auto collect_data_fields = [](SegmentLoadInfo& info) -> std::set<FieldId> {
        std::set<FieldId> fields;

        // From binlog paths
        for (int i = 0; i < info.GetBinlogPathCount(); i++) {
            auto& binlog = info.GetBinlogPath(i);
            std::vector<int64_t> child_fields(binlog.child_fields().begin(),
                                              binlog.child_fields().end());
            if (child_fields.empty()) {
                child_fields.emplace_back(binlog.fieldid());
            }
            for (auto child_id : child_fields) {
                fields.emplace(child_id);
            }
        }

        // From index with raw data
        for (const auto& field_id : info.field_index_has_raw_data_) {
            fields.insert(field_id);
        }

        // From column groups (manifest mode)
        if (info.HasManifestPath()) {
            auto column_groups = info.GetColumnGroups();
            if (column_groups) {
                for (size_t i = 0; i < column_groups->size(); i++) {
                    auto cg = column_groups->at(i);
                    for (const auto& column : cg->columns) {
                        fields.emplace(std::stoll(column));
                    }
                }
            }
        }
        return fields;
    };

    // Collect field ids with data source
    std::set<FieldId> new_info_fields = collect_data_fields(new_info);
    std::set<FieldId> current_fields = collect_data_fields(*this);

    // Build "current handled" set:
    // - Fields with data source in current
    // - Fields already filled with default values
    std::set<FieldId> current_handled = current_fields;
    current_handled.insert(fields_filled_with_default_.begin(),
                           fields_filled_with_default_.end());

    // Compute: schema_fields - new_info_fields - current_handled
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }

        if (new_info_fields.count(field_id)) {
            continue;
        }

        if (current_handled.count(field_id)) {
            new_info.fields_filled_with_default_.insert(field_id);
            continue;
        }

        diff.fields_to_fill_default.push_back(field_id);
        new_info.fields_filled_with_default_.insert(field_id);
    }
}

void
SegmentLoadInfo::ComputeDiffTextIndexes(LoadDiff& diff,
                                        SegmentLoadInfo& new_info) {
    // Build current text indexed fields (fields with loaded text index stats)
    std::set<FieldId> current_text_indexed;
    for (const auto& [field_id, stats] : GetTextStatsLogs()) {
        current_text_indexed.insert(FieldId(field_id));
    }
    // Also include text indexes created from raw data
    for (const auto& field_id : created_text_indexes_) {
        current_text_indexed.insert(field_id);
    }

    // Build new text indexed info (like Go textIndexedInfo)
    // Keep higher version if duplicate field_id
    std::unordered_map<FieldId, const proto::segcore::TextIndexStats*>
        new_text_indexed;
    for (const auto& [field_id, stats] : new_info.GetTextStatsLogs()) {
        auto fid = FieldId(field_id);
        auto it = new_text_indexed.find(fid);
        if (it == new_text_indexed.end() ||
            stats.version() > it->second->version()) {
            new_text_indexed[fid] = &stats;
        }
    }

    // Find text indexes to load: in new_info but not in current
    // Convert TextIndexStats -> LoadTextIndexInfo using new_info's context
    for (const auto& [field_id, stats] : new_text_indexed) {
        if (current_text_indexed.find(field_id) == current_text_indexed.end()) {
            diff.text_indexes_to_load[field_id] =
                new_info.ConvertTextIndexStatsToLoadTextIndexInfo(*stats,
                                                                  field_id);
        }
    }

    // Find text indexes to create: enable_match fields without pre-built index
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (!field_meta.enable_match()) {
            continue;
        }
        // Skip if has pre-built index in new_info
        if (new_text_indexed.find(field_id) != new_text_indexed.end()) {
            continue;
        }
        // Skip if already created from raw data
        if (created_text_indexes_.find(field_id) !=
            created_text_indexes_.end()) {
            continue;
        }
        diff.text_indexes_to_create.insert(field_id);
    }
}

// std::unique_ptr<typename Tp>

LoadDiff
SegmentLoadInfo::ComputeDiff(SegmentLoadInfo& new_info) {
    LoadDiff diff;

    // Handle index changes
    ComputeDiffIndexes(diff, new_info);

    // Compute fields that need to be reloaded due to index raw data changes
    ComputeDiffReloadFields(diff, new_info);

    // Compute text index changes
    ComputeDiffTextIndexes(diff, new_info);

    // Handle field data changes
    // Note: Updates can only happen within the same category:
    // - binlog -> binlog
    // - manifest -> manifest
    // Cross-category changes are not supported.
    if (HasManifestPath()) {
        AssertInfo(new_info.HasManifestPath(),
                   "manifest could only be updated with other manifest");
        if (GetManifestPath() != new_info.GetManifestPath()) {
            diff.manifest_updated = true;
            diff.new_manifest_path = new_info.GetManifestPath();
        }
        ComputeDiffColumnGroups(diff, new_info);
    } else {
        AssertInfo(
            !new_info.HasManifestPath(),
            "field binlogs could only be updated with non-manfest load info");
        ComputeDiffBinlogs(diff, new_info);
    }

    // Compute fields that need default value filling (schema evolution)
    ComputeDiffDefaultFields(diff, new_info);

    return diff;
}

LoadDiff
SegmentLoadInfo::GetLoadDiff() {
    // GetLoadDiff requires non-empty load info (must have data sources)
    AssertInfo(GetBinlogPathCount() > 0 || GetIndexInfoCount() > 0 ||
                   HasManifestPath(),
               "GetLoadDiff called on empty SegmentLoadInfo");

    LoadDiff diff;

    milvus::proto::segcore::SegmentLoadInfo empty_load_info;

    SegmentLoadInfo empty_info(empty_load_info, schema_);

    // Handle index changes
    empty_info.ComputeDiffIndexes(diff, *this);

    // Handle text index changes
    empty_info.ComputeDiffTextIndexes(diff, *this);

    // Handle field data changes
    // Note: Updates can only happen within the same category:
    // - binlog -> binlog
    // - manifest -> manifest
    // Cross-category changes are not supported.
    if (HasManifestPath()) {
        // set mock path for null check
        empty_info.info_.set_manifest_path("mocked manifest path");
        empty_info.column_groups_ =
            std::make_shared<milvus_storage::api::ColumnGroups>();
        empty_info.ComputeDiffColumnGroups(diff, *this);
    } else {
        empty_info.ComputeDiffBinlogs(diff, *this);
    }

    // Compute fields that need default value filling (schema evolution)
    empty_info.ComputeDiffDefaultFields(diff, *this);

    return diff;
}

}  // namespace milvus::segcore
