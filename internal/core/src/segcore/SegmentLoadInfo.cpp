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

#include "cachinglayer/Manager.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "index/Meta.h"
#include "log/Log.h"
#include "common/resource_c.h"
#include "index/IndexFactory.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/manifest.h"
#include "pb/schema.pb.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentLoadInfo.h"
#include "segcore/Utils.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

namespace milvus::segcore {

std::shared_ptr<milvus_storage::api::ColumnGroups>
SegmentLoadInfo::GetColumnGroups() const {
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

// Looks up a storage column in the cached manifest column groups only.
bool
SegmentLoadInfo::HasManifestColumn(const std::string& column_name) const {
    auto column_groups = column_groups_;
    if (column_groups == nullptr) {
        return false;
    }
    for (const auto& group : *column_groups) {
        if (group == nullptr) {
            continue;
        }
        for (const auto& column : group->columns) {
            if (column == column_name) {
                return true;
            }
        }
    }
    return false;
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
    load_index_info.collection_id = GetCollectionID();
    load_index_info.partition_id = GetPartitionID();
    load_index_info.shard = GetInsertChannel();

    // Get field type from schema
    const auto& field_meta = schema_->operator[](field_id);
    load_index_info.field_type = field_meta.get_data_type();
    load_index_info.element_type = field_meta.get_element_type();

    // Set index metadata
    load_index_info.index_id = field_index_info->indexid();
    load_index_info.index_build_id = field_index_info->buildid();
    load_index_info.index_version = field_index_info->index_version();
    load_index_info.index_store_path_version =
        field_index_info->index_store_path_version();
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

    // Propagate base_path for unified (basePath + relativeFiles) model
    if (!text_index_stats.base_path().empty()) {
        info->set_base_path(text_index_stats.base_path());
    }

    return info;
}

std::shared_ptr<proto::indexcgo::LoadJsonKeyIndexInfo>
SegmentLoadInfo::ConvertJsonKeyStatsToLoadJsonKeyIndexInfo(
    const proto::segcore::JsonKeyStats& json_key_stats,
    FieldId field_id) const {
    auto info = std::make_shared<proto::indexcgo::LoadJsonKeyIndexInfo>();

    info->set_fieldid(json_key_stats.fieldid());
    info->set_version(json_key_stats.version());
    info->set_buildid(json_key_stats.buildid());
    for (const auto& f : json_key_stats.files()) {
        info->add_files(f);
    }

    const auto& field_meta = schema_->operator[](field_id);
    *info->mutable_schema() = field_meta.ToProto();

    info->set_collectionid(GetCollectionID());
    info->set_partitionid(GetPartitionID());
    info->set_load_priority(GetPriority());
    info->set_stats_size(json_key_stats.log_size());

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    info->set_enable_mmap(mmap_config.GetJsonStatsEnableMmap());
    info->set_mmap_dir_path(mmap_config.GetJsonStatsMmapPath());

    auto [field_has_warmup, field_warmup_policy] = schema_->WarmupPolicy(
        field_id, /*is_vector=*/false, /*is_index=*/false);
    if (field_has_warmup) {
        info->set_warmup_policy(field_warmup_policy);
    }

    if (!json_key_stats.base_path().empty()) {
        info->set_base_path(json_key_stats.base_path());
    }

    return info;
}

void
SegmentLoadInfo::ComputeDiffIndexes(LoadDiff& diff, SegmentLoadInfo& new_info) {
    // Get current index IDs from the lightweight identity cache.
    std::set<int64_t> current_index_ids;
    // Build a set of field IDs that currently have indexes loaded
    std::set<FieldId> current_indexed_fields;
    for (const auto& [field_id, index_ids] : field_index_id_cache_) {
        current_indexed_fields.insert(field_id);
        for (auto index_id : index_ids) {
            current_index_ids.insert(index_id);
        }
    }

    std::set<int64_t> new_index_ids;
    for (const auto& [field_id, index_ids] : new_info.field_index_id_cache_) {
        if (!new_info.HasFieldInSchema(field_id)) {
            continue;
        }
        for (auto index_id : index_ids) {
            new_index_ids.insert(index_id);
        }
    }
    std::unordered_map<FieldId, std::unordered_set<std::string>>
        new_json_index_paths;
    for (const auto& [field_id, index_paths] :
         new_info.json_index_path_cache_) {
        if (!new_info.HasFieldInSchema(field_id)) {
            continue;
        }
        for (const auto& index_path : index_paths) {
            new_json_index_paths[field_id].insert(index_path.second);
        }
    }
    // Find indexes to load/replace: indexes in new_info but not in current
    // Only consider fields that exist in the current schema (skip dropped fields)
    for (const auto& [field_id, load_index_infos] :
         new_info.converted_field_index_cache_) {
        // Schema-driven: skip indexes for fields not in current schema.
        // Use new_info's schema (the latest) since this->schema_ may still
        // contain dropped fields from a prior load.
        if (!new_info.HasFieldInSchema(field_id)) {
            continue;
        }
        // TODO: Coordinate index invalidation with internal backfill and
        // external refresh when field values change under the same segment ID.
        // An unchanged index ID does not prove that its data is still current;
        // manifest/schema updates alone do not rebuild or replace that index.
        for (const auto& load_index_info : load_index_infos) {
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
    for (const auto& [field_id, index_ids] : field_index_id_cache_) {
        for (auto index_id : index_ids) {
            if (!new_info.HasFieldInSchema(field_id) ||
                new_index_ids.find(index_id) == new_index_ids.end()) {
                auto field_paths = json_index_path_cache_.find(field_id);
                if (field_paths != json_index_path_cache_.end()) {
                    auto path = field_paths->second.find(index_id);
                    if (path != field_paths->second.end()) {
                        if (new_json_index_paths[field_id].find(path->second) ==
                            new_json_index_paths[field_id].end()) {
                            diff.json_indexes_to_drop[field_id].insert(
                                path->second);
                        }
                        continue;
                    }
                }
                diff.indexes_to_drop.insert(field_id);
            }
        }
    }
}

void
SegmentLoadInfo::ComputeDiffBinlogs(LoadDiff& diff, SegmentLoadInfo& new_info) {
    auto prefer_field_data =
        SegcoreConfig::default_config()
            .get_prefer_field_data_when_index_has_raw_data();

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

    // Two FieldBinlogs at the same group id are equivalent only when their
    // underlying log files (log_path sequence) match. Compaction/version
    // bumps can swap files under the same fieldid, and without detecting
    // that here the loader never evicts the stale column cache.
    auto same_binlog_files = [](const proto::segcore::FieldBinlog& a,
                                const proto::segcore::FieldBinlog& b) -> bool {
        if (a.binlogs_size() != b.binlogs_size()) {
            return false;
        }
        for (int j = 0; j < a.binlogs_size(); j++) {
            if (a.binlogs(j).log_path() != b.binlogs(j).log_path()) {
                return false;
            }
        }
        return true;
    };

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

        auto* cur_field_binlog =
            GetFieldBinlog(FieldId(new_field_binlog.fieldid()));
        bool group_files_changed =
            cur_field_binlog != nullptr &&
            !same_binlog_files(*cur_field_binlog, new_field_binlog);

        for (auto child_id : child_fields) {
            auto child_field_id = FieldId(child_id);
            if (!new_info.HasFieldInSchema(child_field_id)) {
                continue;
            }
            new_binlog_fields[child_id] = new_field_binlog.fieldid();
            // current_fields is keyed by child field id, so look up the
            // child — keying by new_field_binlog.fieldid() misses multi-
            // field groups (whose group id is not itself a map key) and
            // spuriously classifies unchanged groups as moved.
            auto iter = current_fields.find(child_id);
            // A binlog entry needs (re)loading when either
            //   (a) the group this child belongs to differs between
            //       current and new, or
            //   (b) the group maps the same way but the underlying log
            //       files changed (e.g. compaction rewrote the segment).
            bool group_mapping_differs =
                iter == current_fields.end() ||
                iter->second != new_field_binlog.fieldid();
            if (!group_mapping_differs && !group_files_changed) {
                continue;
            }
            // Pre-existing children go to replace (to evict stale cached
            // columns); genuinely new children go to load.
            if (current_fields.find(child_id) != current_fields.end() ||
                fields_filled_with_default_.count(child_field_id) > 0) {
                ids_to_replace.emplace_back(child_id);
            } else {
                ids_to_load.emplace_back(child_id);
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
        auto fid = FieldId(field_id);
        if (!new_info.HasFieldInSchema(fid)) {
            diff.field_data_to_drop.emplace(field_id);
            continue;
        }
        if (new_binlog_fields.find(field_id) == new_binlog_fields.end()) {
            if (prefer_field_data &&
                new_info.field_index_has_raw_data_.count(fid) > 0) {
                continue;
            }
            diff.field_data_to_drop.emplace(field_id);
        }
    }
}

bool
SegmentLoadInfo::ShouldEagerLoadManifestField(FieldId fid) const {
    if (schema_->is_external_collection()) {
        if (schema_->get_primary_field_id().value_or(FieldId(-1)) == fid &&
            schema_->IsExternalDataField(fid)) {
            return true;
        }
        const auto& meta = (*schema_)[fid];
        bool is_vector = IsVectorDataType(meta.get_data_type());
        // Without a persistent vector index, raw vectors are the search path
        // and retain the upstream vector-index warmup policy.
        if (is_vector && !HasIndexInfo(fid)) {
            auto [has_index_warmup, index_policy] =
                schema_->CollectionWarmupPolicy(true, true);
            auto resolved =
                has_index_warmup
                    ? getCacheWarmupPolicy(index_policy, true, true, true)
                    : cachinglayer::Manager::GetInstance()
                          .getVectorIndexCacheWarmupPolicy();
            return resolved != CacheWarmupPolicy::CacheWarmupPolicy_Disable;
        }
        auto [has_warmup, policy] =
            schema_->WarmupPolicy(fid, is_vector, /*is_index=*/false);
        return getCacheWarmupPolicy(has_warmup ? policy : "",
                                    is_vector,
                                    /*is_index=*/false,
                                    /*in_load_list=*/true) !=
               CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    }
    return fid.get() < START_USER_FIELDID ||
           (schema_->ShouldLoadField(fid) &&
            (SegcoreConfig::default_config()
                 .get_prefer_field_data_when_index_has_raw_data() ||
             field_index_has_raw_data_.count(fid) == 0));
}

void
SegmentLoadInfo::ComputeDiffColumnGroups(LoadDiff& diff,
                                         SegmentLoadInfo& new_info) {
    auto current_groups = GetColumnGroups();
    auto target_groups = new_info.GetColumnGroups();
    AssertInfo(current_groups && target_groups,
               "manifest column groups must be initialized");

    struct Source {
        std::string column;
        const milvus_storage::api::ColumnGroup* group;
    };
    std::map<FieldId, Source> current_sources;
    for (const auto& group : *current_groups) {
        if (!group) {
            continue;
        }
        for (const auto& column : group->columns) {
            auto fid = schema_->ResolveColumnFieldId(column);
            if (!HasFieldInSchema(fid)) {
                continue;
            }
            auto [it, inserted] =
                current_sources.emplace(fid, Source{column, group.get()});
            AssertInfo(inserted, "duplicate manifest field {}", fid.get());
        }
    }
    auto same_group = [](const auto& a, const auto& b) {
        if (a.format != b.format || a.columns != b.columns ||
            a.files.size() != b.files.size()) {
            return false;
        }
        for (size_t i = 0; i < a.files.size(); ++i) {
            const auto& x = a.files[i];
            const auto& y = b.files[i];
            if (x.path != y.path || x.start_index != y.start_index ||
                x.end_index != y.end_index || x.properties != y.properties) {
                return false;
            }
        }
        return true;
    };
    bool context_changed =
        schema_->get_external_source() !=
            new_info.schema_->get_external_source() ||
        schema_->get_external_spec() != new_info.schema_->get_external_spec();
    std::set<FieldId> target_fields;
    for (size_t i = 0; i < target_groups->size(); ++i) {
        const auto& group = target_groups->at(i);
        if (!group) {
            continue;
        }
        std::vector<FieldId> loads, replacements;
        for (const auto& column : group->columns) {
            auto fid = new_info.schema_->ResolveColumnFieldId(column);
            if (!new_info.HasFieldInSchema(fid)) {
                continue;
            }
            AssertInfo(target_fields.insert(fid).second,
                       "duplicate manifest field {}",
                       fid.get());
            auto old = current_sources.find(fid);
            bool had_physical = old != current_sources.end();
            bool had_default = fields_filled_with_default_.count(fid) > 0;
            // Preserve the internal vector-only raw-index skip. A lazy raw
            // column still consumes disk alongside an index that owns the data.
            if (!new_info.schema_->is_external_collection() &&
                fid.get() >= START_USER_FIELDID &&
                IsVectorDataType((*new_info.schema_)[fid].get_data_type()) &&
                new_info.field_index_has_raw_data_.count(fid) > 0 &&
                !SegcoreConfig::default_config()
                     .get_prefer_field_data_when_index_has_raw_data()) {
                if (had_physical && field_index_has_raw_data_.count(fid) == 0) {
                    diff.field_data_to_drop.insert(fid);
                }
                continue;
            }
            bool eager = new_info.ShouldEagerLoadManifestField(fid);
            bool lost_raw_index =
                field_index_has_raw_data_.count(fid) > 0 &&
                new_info.field_index_has_raw_data_.count(fid) == 0;
            bool replace = had_default ||
                           (had_physical &&
                            (context_changed || old->second.column != column ||
                             !same_group(*old->second.group, *group) ||
                             ShouldEagerLoadManifestField(fid) != eager ||
                             lost_raw_index));
            if (had_physical && !replace) {
                continue;
            }
            if (eager) {
                (replace ? replacements : loads).push_back(fid);
            } else {
                // Each lazy field gets its own projected chunk reader.
                auto& tasks = replace ? diff.column_groups_to_lazyreplace
                                      : diff.column_groups_to_lazyload;
                tasks.emplace_back(i, std::vector<FieldId>{fid});
            }
        }
        if (!loads.empty()) {
            diff.column_groups_to_load.emplace_back(i, std::move(loads));
        }
        if (!replacements.empty()) {
            diff.column_groups_to_replace.emplace_back(i,
                                                       std::move(replacements));
        }
    }
    for (const auto& [fid, source] : current_sources) {
        bool retain_index_raw =
            new_info.field_index_has_raw_data_.count(fid) > 0 &&
            (new_info.schema_->is_external_collection() ||
             SegcoreConfig::default_config()
                 .get_prefer_field_data_when_index_has_raw_data());
        if (!new_info.HasFieldInSchema(fid) ||
            (target_fields.count(fid) == 0 && !retain_index_raw)) {
            diff.field_data_to_drop.insert(fid);
        }
    }
    for (auto fid : fields_filled_with_default_) {
        if (!new_info.HasFieldInSchema(fid)) {
            diff.field_data_to_drop.insert(fid);
        }
    }
}

void
SegmentLoadInfo::ComputeDiffReloadFields(LoadDiff& diff,
                                         SegmentLoadInfo& new_info) {
    // Find fields that were previously skipped (index had raw data)
    // but now need loading (index no longer has raw data or was dropped).
    // These fields need their raw data restored since the index no longer
    // provides it. We put them into load paths (binlogs/column_groups) so
    // that ApplyLoadDiff can rebuild the data from storage.
    // Collect fields that need reload (index no longer has raw data)
    std::set<FieldId> fields_to_reload;
    for (const auto& field_id : field_index_has_raw_data_) {
        if (!new_info.HasFieldInSchema(field_id)) {
            continue;
        }
        if (new_info.field_index_has_raw_data_.find(field_id) ==
            new_info.field_index_has_raw_data_.end()) {
            fields_to_reload.emplace(field_id);
        }
    }

    if (fields_to_reload.empty()) {
        return;
    }

    if (!new_info.HasManifestPath()) {
        // Binlog mode: find each field's binlog and add to binlogs_to_replace
        for (const auto& field_id : fields_to_reload) {
            for (int i = 0; i < new_info.GetBinlogPathCount(); i++) {
                auto& binlog = new_info.GetBinlogPath(i);
                std::vector<int64_t> child_fields(binlog.child_fields().begin(),
                                                  binlog.child_fields().end());
                if (child_fields.empty()) {
                    child_fields.emplace_back(binlog.fieldid());
                }
                bool found = false;
                for (auto child_id : child_fields) {
                    if (FieldId(child_id) == field_id) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    // Use replace since the field may still exist
                    // (e.g. multi-field group where drop was skipped)
                    diff.binlogs_to_replace.emplace_back(
                        std::vector<FieldId>{field_id}, binlog);
                    break;
                }
            }
        }
    }
}

std::set<FieldId>
SegmentLoadInfo::CollectDataFields() const {
    std::set<FieldId> fields;

    for (int i = 0; !HasManifestPath() && i < GetBinlogPathCount(); i++) {
        auto& binlog = GetBinlogPath(i);
        std::vector<int64_t> child_fields(binlog.child_fields().begin(),
                                          binlog.child_fields().end());
        if (child_fields.empty()) {
            child_fields.emplace_back(binlog.fieldid());
        }
        for (auto child_id : child_fields) {
            fields.emplace(child_id);
        }
    }

    for (const auto& field_id : field_index_has_raw_data_) {
        fields.insert(field_id);
    }

    if (HasManifestPath()) {
        auto groups = GetColumnGroups();
        std::unordered_map<std::string, FieldId> schema_columns;
        for (const auto& [fid, meta] : schema_->get_fields()) {
            schema_columns.emplace(schema_->get_storage_column_name(fid), fid);
        }
        for (const auto& group : *groups) {
            if (!group) {
                continue;
            }
            for (const auto& name : group->columns) {
                auto it = schema_columns.find(name);
                if (it != schema_columns.end()) {
                    fields.insert(it->second);
                }
            }
        }
    }
    return fields;
}

std::set<FieldId>
SegmentLoadInfo::GetDefaultFilledFieldsForNewInfo(
    const SegmentLoadInfo& new_info) const {
    auto new_info_fields = new_info.CollectDataFields();
    std::set<FieldId> fields;
    for (const auto& field_id : fields_filled_with_default_) {
        if (!new_info.HasFieldInSchema(field_id)) {
            continue;
        }
        if (new_info_fields.count(field_id) > 0) {
            continue;
        }
        if (!new_info.schema_->is_external_collection() ||
            new_info.schema_->CanFillMissingExternalField(field_id)) {
            fields.insert(field_id);
        }
    }
    return fields;
}

void
SegmentLoadInfo::ComputeDiffDefaultFields(LoadDiff& diff,
                                          SegmentLoadInfo& new_info) {
    std::set<FieldId> new_info_fields = new_info.CollectDataFields();
    std::set<FieldId> current_fields = CollectDataFields();

    if (new_info.HasManifestPath() &&
        new_info.schema_->is_external_collection()) {
        for (const auto& [fid, meta] : new_info.schema_->get_fields()) {
            if (new_info.schema_->IsExternalManifestStoredField(fid) &&
                !new_info.schema_->CanFillMissingExternalField(fid) &&
                !new_info.HasManifestColumn(
                    new_info.schema_->get_storage_column_name(fid))) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "required manifest column {} is missing",
                          new_info.schema_->get_storage_column_name(fid));
            }
        }
    }

    for (const auto& [field_id, field_meta] : new_info.schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID ||
            new_info_fields.count(field_id) ||
            new_info.schema_->is_function_output(field_id)) {
            continue;
        }
        if (new_info.schema_->is_external_collection() &&
            !new_info.schema_->CanFillMissingExternalField(field_id)) {
            continue;
        }
        if (fields_filled_with_default_.count(field_id)) {
            continue;
        }
        // Preserve legacy binlog behavior. In manifest mode the target
        // inventory is authoritative, including physical-to-default changes.
        if (!new_info.HasManifestPath() && current_fields.count(field_id)) {
            continue;
        }
        if (current_fields.count(field_id) && !field_meta.is_nullable() &&
            !field_meta.has_default_value()) {
            continue;
        }
        diff.field_data_to_drop.erase(field_id);
        diff.fields_to_fill_default.push_back(field_id);
    }
}

namespace {

int64_t
CurrentJsonStatsDataFormatVersion() {
    return std::stoll(JSON_STATS_DATA_FORMAT_VERSION);
}

bool
JsonStatsFilesEqual(const proto::segcore::JsonKeyStats& lhs,
                    const proto::segcore::JsonKeyStats& rhs) {
    if (lhs.files_size() != rhs.files_size()) {
        return false;
    }
    for (int i = 0; i < lhs.files_size(); ++i) {
        if (lhs.files(i) != rhs.files(i)) {
            return false;
        }
    }
    return true;
}

bool
JsonStatsLoadIdentityEqual(const proto::segcore::JsonKeyStats& lhs,
                           const proto::segcore::JsonKeyStats& rhs) {
    return lhs.version() == rhs.version() && lhs.buildid() == rhs.buildid() &&
           lhs.json_key_stats_data_format() ==
               rhs.json_key_stats_data_format() &&
           lhs.base_path() == rhs.base_path() &&
           lhs.log_size() == rhs.log_size() &&
           lhs.memory_size() == rhs.memory_size() &&
           JsonStatsFilesEqual(lhs, rhs);
}

}  // namespace

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
        if (!new_info.HasFieldInSchema(fid)) {
            continue;
        }
        auto it = new_text_indexed.find(fid);
        if (it == new_text_indexed.end() ||
            stats.version() > it->second->version()) {
            new_text_indexed[fid] = &stats;
        }
    }

    auto default_transition = [&](FieldId field_id) {
        return new_info.HasManifestPath() &&
               (std::find(diff.fields_to_fill_default.begin(),
                          diff.fields_to_fill_default.end(),
                          field_id) != diff.fields_to_fill_default.end() ||
                (fields_filled_with_default_.count(field_id) > 0 &&
                 new_info.HasManifestColumn(
                     new_info.schema_->get_storage_column_name(field_id))));
    };

    // Find text indexes to load: in new_info but not in current
    // Convert TextIndexStats -> LoadTextIndexInfo using new_info's context
    for (const auto& [field_id, stats] : new_text_indexed) {
        if (current_text_indexed.find(field_id) == current_text_indexed.end() ||
            default_transition(field_id)) {
            diff.text_indexes_to_load[field_id] =
                new_info.ConvertTextIndexStatsToLoadTextIndexInfo(*stats,
                                                                  field_id);
        }
    }

    // Find text indexes to create: enable_match fields without pre-built index
    for (const auto& [field_id, field_meta] : new_info.schema_->get_fields()) {
        if (!field_meta.enable_match()) {
            continue;
        }
        // Skip if has pre-built index in new_info
        if (new_text_indexed.find(field_id) != new_text_indexed.end()) {
            continue;
        }
        // A locally built index must follow a default/physical transition.
        if (created_text_indexes_.count(field_id) > 0 &&
            !default_transition(field_id)) {
            continue;
        }
        diff.text_indexes_to_create.insert(field_id);
    }
}

void
SegmentLoadInfo::ComputeDiffJsonKeyStats(LoadDiff& diff,
                                         SegmentLoadInfo& new_info) {
    if (!JSON_KEY_STATS_ENABLED.load()) {
        auto stats_count = GetJsonKeyStatsLogs().size() +
                           new_info.GetJsonKeyStatsLogs().size();
        if (stats_count > 0) {
            LOG_WARN(
                "skip json key stats diff because json key stats is disabled, "
                "segment:{}, json_stats_count:{}",
                new_info.GetSegmentID(),
                stats_count);
        }
        return;
    }

    const auto expected_format = CurrentJsonStatsDataFormatVersion();
    std::set<FieldId> current_fields;
    for (const auto& [field_id, stats] : GetJsonKeyStatsLogs()) {
        current_fields.insert(FieldId(field_id));
    }

    std::set<FieldId> new_fields;
    for (const auto& [field_id, stats] : new_info.GetJsonKeyStatsLogs()) {
        auto fid = FieldId(field_id);
        if (!new_info.HasFieldInSchema(fid)) {
            continue;
        }
        new_fields.insert(fid);
        auto current_stats = GetJsonKeyStatsLog(field_id);
        if (stats.json_key_stats_data_format() != expected_format) {
            LOG_WARN(
                "skip json key stats diff because data format is invalid, "
                "segment:{}, field:{}, build:{}, version:{}, format:{}, "
                "expected_format:{}, file_count:{}",
                new_info.GetSegmentID(),
                field_id,
                stats.buildid(),
                stats.version(),
                stats.json_key_stats_data_format(),
                expected_format,
                stats.files_size());
            if (current_stats != nullptr) {
                diff.json_stats_to_drop.insert(fid);
                LOG_INFO(
                    "json key stats diff drop invalid format, segment:{}, "
                    "field:{}",
                    new_info.GetSegmentID(),
                    field_id);
            }
            continue;
        }

        if (current_stats == nullptr) {
            diff.json_stats_to_load[fid] =
                new_info.ConvertJsonKeyStatsToLoadJsonKeyIndexInfo(stats, fid);
            LOG_INFO(
                "json key stats diff load, segment:{}, field:{}, build:{}, "
                "version:{}",
                new_info.GetSegmentID(),
                field_id,
                stats.buildid(),
                stats.version());
            continue;
        }

        if (!JsonStatsLoadIdentityEqual(*current_stats, stats)) {
            diff.json_stats_to_replace[fid] =
                new_info.ConvertJsonKeyStatsToLoadJsonKeyIndexInfo(stats, fid);
            LOG_INFO(
                "json key stats diff replace, segment:{}, field:{}, build:{}, "
                "version:{}",
                new_info.GetSegmentID(),
                field_id,
                stats.buildid(),
                stats.version());
        }
    }

    for (const auto& field_id : current_fields) {
        if (new_fields.find(field_id) == new_fields.end()) {
            diff.json_stats_to_drop.insert(field_id);
            LOG_INFO("json key stats diff drop, segment:{}, field:{}",
                     new_info.GetSegmentID(),
                     field_id.get());
        }
    }
}

// std::unique_ptr<typename Tp>

LoadDiff
SegmentLoadInfo::ComputeDiff(SegmentLoadInfo& new_info) {
    LoadDiff diff;

    if (HasManifestPath() && schema_->is_external_collection()) {
        AssertInfo(GetNumOfRows() == new_info.GetNumOfRows(),
                   "external segment row identity cannot change during reopen");
        auto pk = schema_->get_primary_field_id().value_or(FieldId(-1));
        auto next_pk =
            new_info.schema_->get_primary_field_id().value_or(FieldId(-1));
        AssertInfo(
            pk == next_pk &&
                schema_->IsExternalDataField(pk) ==
                    new_info.schema_->IsExternalDataField(next_pk) &&
                schema_->RequiresSourceInsertTimestamps() ==
                    new_info.schema_->RequiresSourceInsertTimestamps(),
            "external segment primary key mode cannot change during reopen");
    }

    // Handle index changes
    ComputeDiffIndexes(diff, new_info);

    // Compute fields that need to be reloaded due to index raw data changes
    ComputeDiffReloadFields(diff, new_info);

    // Compute JSON key stats changes
    ComputeDiffJsonKeyStats(diff, new_info);

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
        diff.rebuild_manifest_reader =
            diff.manifest_updated || schema_ != new_info.schema_;
        ComputeDiffColumnGroups(diff, new_info);
    } else {
        AssertInfo(
            !new_info.HasManifestPath(),
            "field binlogs could only be updated with non-manfest load info");
        ComputeDiffBinlogs(diff, new_info);
    }

    // Compute fields that need default value filling (schema evolution)
    ComputeDiffDefaultFields(diff, new_info);
    ComputeDiffTextIndexes(diff, new_info);

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

    // Handle JSON key stats changes
    empty_info.ComputeDiffJsonKeyStats(diff, *this);

    // Handle field data changes
    // Note: Updates can only happen within the same category:
    // - binlog -> binlog
    // - manifest -> manifest
    // Cross-category changes are not supported.
    if (HasManifestPath()) {
        empty_info.info_.set_manifest_path("empty manifest");
        empty_info.column_groups_ =
            std::make_shared<milvus_storage::api::ColumnGroups>();
        diff.rebuild_manifest_reader = true;
        empty_info.ComputeDiffColumnGroups(diff, *this);
    } else {
        empty_info.ComputeDiffBinlogs(diff, *this);
    }

    empty_info.ComputeDiffDefaultFields(diff, *this);

    return diff;
}

}  // namespace milvus::segcore
