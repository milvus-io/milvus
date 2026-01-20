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

#include "index/IndexFactory.h"
#include "segcore/SegmentLoadInfo.h"

#include <algorithm>
#include <cctype>
#include <memory>

#include "common/FieldMeta.h"
#include "milvus-storage/column_groups.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/MmapManager.h"

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
    load_index_info.index_store_version =
        field_index_info->index_store_version();
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
        load_index_info.index_params[kv_pair.key()] = kv_pair.value();
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

void
SegmentLoadInfo::ComputeDiffIndexes(LoadDiff& diff, SegmentLoadInfo& new_info) {
    // Get current index IDs from converted cache
    std::set<int64_t> current_index_ids;
    for (const auto& load_index_info : converted_index_infos_) {
        current_index_ids.insert(load_index_info.index_id);
    }

    std::set<int64_t> new_index_ids;
    // Find indexes to load: indexes in new_info but not in current
    // Use converted_field_index_cache_ from new_info
    for (const auto& [field_id, load_index_infos] :
         new_info.converted_field_index_cache_) {
        for (const auto& load_index_info : load_index_infos) {
            new_index_ids.insert(load_index_info.index_id);
            if (current_index_ids.find(load_index_info.index_id) ==
                current_index_ids.end()) {
                diff.indexes_to_load[field_id].push_back(load_index_info);
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
            // Find binlogs to load: fields in new_info match current
            if (iter == current_fields.end() ||
                iter->second != new_field_binlog.fieldid()) {
                ids_to_load.emplace_back(child_id);
            }
        }
        if (!ids_to_load.empty()) {
            diff.binlogs_to_load.emplace_back(ids_to_load, new_field_binlog);
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

    // Build a set of new FieldIds and find column groups to load
    std::map<int64_t, int> new_field_ids;
    for (int i = 0; i < new_column_group->size(); i++) {
        auto cg = new_column_group->at(i);
        std::vector<FieldId> fields;
        std::vector<FieldId> lazy_fields;
        for (const auto& column : cg->columns) {
            auto field_id = std::stoll(column);
            new_field_ids.emplace(field_id, i);

            auto iter = cur_field_ids.find(field_id);
            // If this field doesn't exist in current, mark the column group for loading
            if (iter == cur_field_ids.end() || iter->second != i) {
                if (schema_->ShouldLoadField(FieldId(field_id)) &&
                    field_index_has_raw_data_.find(FieldId(field_id)) ==
                        field_index_has_raw_data_.end()) {
                    fields.emplace_back(field_id);
                } else {
                    // put lazy load & index_has_raw_data field in lazy_fields
                    lazy_fields.emplace_back(field_id);
                }
            }
        }
        if (!fields.empty()) {
            diff.column_groups_to_load.emplace_back(i, fields);
        }
        if (!lazy_fields.empty()) {
            diff.column_groups_to_lazyload.emplace_back(i, lazy_fields);
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

LoadDiff
SegmentLoadInfo::ComputeDiff(SegmentLoadInfo& new_info) {
    LoadDiff diff;

    // Handle index changes
    ComputeDiffIndexes(diff, new_info);

    // Compute fields that need to be reloaded due to index raw data changes
    ComputeDiffReloadFields(diff, new_info);

    // Handle field data changes
    // Note: Updates can only happen within the same category:
    // - binlog -> binlog
    // - manifest -> manifest
    // Cross-category changes are not supported.
    if (HasManifestPath()) {
        AssertInfo(new_info.HasManifestPath(),
                   "manifest could only be updated with other manifest");
        ComputeDiffColumnGroups(diff, new_info);
    } else {
        AssertInfo(
            !new_info.HasManifestPath(),
            "field binlogs could only be updated with non-manfest load info");
        ComputeDiffBinlogs(diff, new_info);
    }

    return diff;
}

LoadDiff
SegmentLoadInfo::GetLoadDiff() {
    LoadDiff diff;

    SegmentLoadInfo empty_info;

    // Handle index changes
    empty_info.ComputeDiffIndexes(diff, *this);

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

    return diff;
}

}  // namespace milvus::segcore
