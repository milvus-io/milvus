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

#pragma once

#include <stddef.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "NamedType/underlying_functionalities.hpp"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "milvus-storage/column_groups.h"
#include "pb/common.pb.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/segcore.pb.h"
#include "segcore/Types.h"

namespace milvus::segcore {

/**
 * @brief Structure representing the difference between two SegmentLoadInfos,
 *       used for reopening segments.
 *
 * Note: SegmentLoadInfo can only have updates within the same category:
 * - binlog -> binlog updates
 * - manifest -> manifest updates
 * Cross-category changes (binlog <-> manifest) are not supported.
 */
struct LoadDiff {
    // Indexes that need to be loaded for fields without existing index
    std::unordered_map<FieldId, std::vector<LoadIndexInfo>> indexes_to_load;

    // Indexes that need to replace existing indexes (field already has an index)
    std::unordered_map<FieldId, std::vector<LoadIndexInfo>> indexes_to_replace;

    // Field binlog paths that need to be loaded (new fields)
    // Only populated when both current and new use binlog mode
    std::vector<std::pair<std::vector<FieldId>, proto::segcore::FieldBinlog>>
        binlogs_to_load;

    // Field binlog paths that need to replace existing field data
    std::vector<std::pair<std::vector<FieldId>, proto::segcore::FieldBinlog>>
        binlogs_to_replace;

    // list of column group indices and related field ids to load (new fields)
    // same index could appear multiple times if same group using different setups
    std::vector<std::pair<int, std::vector<FieldId>>> column_groups_to_load;

    // list of column group indices and related field ids to replace
    // (field moved between column groups or column group data changed)
    std::vector<std::pair<int, std::vector<FieldId>>> column_groups_to_replace;

    // list of column group indices and related field ids to lazy load (new fields)
    // used for lazy load fields or fields with index has raw data
    std::vector<std::pair<int, std::vector<FieldId>>> column_groups_to_lazyload;

    // list of column group indices and related field ids to lazy replace
    std::vector<std::pair<int, std::vector<FieldId>>>
        column_groups_to_lazyreplace;

    std::vector<FieldId> fields_to_reload;

    // Indexes that need to be dropped (field_id set)
    std::set<FieldId> indexes_to_drop;

    // Field data that need to be dropped (field_id set)
    // Only populated when both current and new use binlog mode
    std::unordered_set<FieldId> field_data_to_drop;

    // Fields that need to be filled with default values (schema evolution scenario)
    // These fields exist in schema but have no data source (binlog/index/column_group)
    std::vector<FieldId> fields_to_fill_default;

    // Text indexes that need to be loaded from pre-built files
    // (field_id -> converted LoadTextIndexInfo)
    std::unordered_map<FieldId,
                       std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>>
        text_indexes_to_load;

    // Text fields that need text indexes created from raw data
    std::unordered_set<FieldId> text_indexes_to_create;

    // Whether manifest path has changed (only when both use manifest mode)
    bool manifest_updated = false;

    // New manifest path (valid when manifest_updated is true)
    std::string new_manifest_path;

    [[nodiscard]] bool
    HasChanges() const {
        return !indexes_to_load.empty() || !indexes_to_replace.empty() ||
               !binlogs_to_load.empty() || !binlogs_to_replace.empty() ||
               !column_groups_to_load.empty() ||
               !column_groups_to_replace.empty() ||
               !column_groups_to_lazyload.empty() ||
               !column_groups_to_lazyreplace.empty() ||
               !fields_to_reload.empty() || !indexes_to_drop.empty() ||
               !field_data_to_drop.empty() || !fields_to_fill_default.empty() ||
               !text_indexes_to_load.empty() ||
               !text_indexes_to_create.empty() || manifest_updated;
    }

    [[nodiscard]] bool
    HasManifestChange() const {
        return manifest_updated;
    }

    [[nodiscard]] std::string
    ToString() const {
        std::ostringstream oss;
        oss << "LoadDiff{";

        // indexes_to_load
        oss << "indexes_to_load=[";
        bool first = true;
        for (const auto& [field_id, infos] : indexes_to_load) {
            if (!first)
                oss << ", ";
            first = false;
            oss << field_id.get() << ":" << infos.size() << " indexes";
        }
        oss << "], ";

        // indexes_to_replace
        oss << "indexes_to_replace=[";
        first = true;
        for (const auto& [field_id, infos] : indexes_to_replace) {
            if (!first)
                oss << ", ";
            first = false;
            oss << field_id.get() << ":" << infos.size() << " indexes";
        }
        oss << "], ";

        // binlogs_to_load
        oss << "binlogs_to_load=[";
        first = true;
        for (const auto& [field_ids, binlog] : binlogs_to_load) {
            if (!first)
                oss << ", ";
            first = false;
            oss << "[";
            for (size_t i = 0; i < field_ids.size(); ++i) {
                if (i > 0)
                    oss << ",";
                oss << field_ids[i].get();
            }
            oss << "]";
        }
        oss << "], ";

        // binlogs_to_replace
        oss << "binlogs_to_replace=[";
        first = true;
        for (const auto& [field_ids, binlog] : binlogs_to_replace) {
            if (!first)
                oss << ", ";
            first = false;
            oss << "[";
            for (size_t i = 0; i < field_ids.size(); ++i) {
                if (i > 0)
                    oss << ",";
                oss << field_ids[i].get();
            }
            oss << "]";
        }
        oss << "], ";

        // column_groups_to_load
        oss << "column_groups_to_load=[";
        first = true;
        for (const auto& [group_idx, field_ids] : column_groups_to_load) {
            if (!first)
                oss << ", ";
            first = false;
            oss << "group" << group_idx << ":[";
            for (size_t i = 0; i < field_ids.size(); ++i) {
                if (i > 0)
                    oss << ",";
                oss << field_ids[i].get();
            }
            oss << "]";
        }
        oss << "], ";

        // column_groups_to_replace
        oss << "column_groups_to_replace=[";
        first = true;
        for (const auto& [group_idx, field_ids] : column_groups_to_replace) {
            if (!first)
                oss << ", ";
            first = false;
            oss << "group" << group_idx << ":[";
            for (size_t i = 0; i < field_ids.size(); ++i) {
                if (i > 0)
                    oss << ",";
                oss << field_ids[i].get();
            }
            oss << "]";
        }
        oss << "], ";

        // column_groups_to_lazyload
        oss << "column_groups_to_lazyload=[";
        first = true;
        for (const auto& [group_idx, field_ids] : column_groups_to_lazyload) {
            if (!first)
                oss << ", ";
            first = false;
            oss << "group" << group_idx << ":[";
            for (size_t i = 0; i < field_ids.size(); ++i) {
                if (i > 0)
                    oss << ",";
                oss << field_ids[i].get();
            }
            oss << "]";
        }
        oss << "], ";

        // column_groups_to_lazyreplace
        oss << "column_groups_to_lazyreplace=[";
        first = true;
        for (const auto& [group_idx, field_ids] :
             column_groups_to_lazyreplace) {
            if (!first)
                oss << ", ";
            first = false;
            oss << "group" << group_idx << ":[";
            for (size_t i = 0; i < field_ids.size(); ++i) {
                if (i > 0)
                    oss << ",";
                oss << field_ids[i].get();
            }
            oss << "]";
        }
        oss << "], ";

        // indexes_to_drop
        oss << "indexes_to_drop=[";
        first = true;
        for (const auto& field_id : indexes_to_drop) {
            if (!first)
                oss << ", ";
            first = false;
            oss << field_id.get();
        }
        oss << "], ";

        // field_data_to_drop
        oss << "field_data_to_drop=[";
        first = true;
        for (const auto& field_id : field_data_to_drop) {
            if (!first)
                oss << ", ";
            first = false;
            oss << field_id.get();
        }
        oss << "], ";

        // fields_to_fill_default
        oss << "fields_to_fill_default=[";
        first = true;
        for (const auto& field_id : fields_to_fill_default) {
            if (!first)
                oss << ", ";
            first = false;
            oss << field_id.get();
        }
        oss << "], ";

        // text_indexes_to_load
        oss << "text_indexes_to_load=[";
        first = true;
        for (const auto& [field_id, stats] : text_indexes_to_load) {
            if (!first)
                oss << ", ";
            first = false;
            oss << field_id.get();
        }
        oss << "], ";

        // text_indexes_to_create
        oss << "text_indexes_to_create=[";
        first = true;
        for (const auto& field_id : text_indexes_to_create) {
            if (!first)
                oss << ", ";
            first = false;
            oss << field_id.get();
        }
        oss << "], ";

        // manifest_updated and new_manifest_path
        oss << "manifest_updated=" << (manifest_updated ? "true" : "false");
        if (manifest_updated) {
            oss << ", new_manifest_path=" << new_manifest_path;
        }

        oss << "}";
        return oss.str();
    }
};

/**
 * @brief Utility class that wraps milvus::proto::segcore::SegmentLoadInfo
 *        and provides convenient accessor methods.
 *
 * This class simplifies access to SegmentLoadInfo fields and provides
 * utility methods for common operations like field lookups, index queries,
 * and binlog path management.
 */
class SegmentLoadInfo {
 public:
    using ProtoType = milvus::proto::segcore::SegmentLoadInfo;

    SegmentLoadInfo() = delete;

    /**
     * @brief Construct from a protobuf SegmentLoadInfo (copy)
     * @param info The protobuf SegmentLoadInfo to wrap
     */
    explicit SegmentLoadInfo(const ProtoType& info, SchemaPtr schema)
        : info_(info), schema_(std::move(schema)) {
        BuildCache();
    }

    /**
     * @brief Construct from a protobuf SegmentLoadInfo (move)
     * @param info The protobuf SegmentLoadInfo to wrap
     */
    explicit SegmentLoadInfo(ProtoType&& info, SchemaPtr schema)
        : info_(std::move(info)), schema_(std::move(schema)) {
        BuildCache();
    }

    /**
     * @brief Copy constructor
     * @note Rebuilds cache instead of copying (LoadIndexInfo is not copyable)
     */
    SegmentLoadInfo(const SegmentLoadInfo& other)
        : info_(other.info_),
          schema_(other.schema_),
          column_groups_(other.column_groups_),
          created_text_indexes_(other.created_text_indexes_) {
        BuildCache();
    }

    /**
     * @brief Move constructor
     */
    SegmentLoadInfo(SegmentLoadInfo&& other) noexcept
        : info_(std::move(other.info_)),
          schema_(std::move(other.schema_)),
          converted_index_infos_(std::move(other.converted_index_infos_)),
          converted_field_index_cache_(
              std::move(other.converted_field_index_cache_)),
          field_binlog_cache_(std::move(other.field_binlog_cache_)),
          column_groups_(std::move(other.column_groups_)),
          created_text_indexes_(std::move(other.created_text_indexes_)) {
    }

    /**
     * @brief Copy assignment operator
     * @note Rebuilds cache instead of copying (LoadIndexInfo is not copyable)
     */
    SegmentLoadInfo&
    operator=(const SegmentLoadInfo& other) {
        if (this != &other) {
            info_ = other.info_;
            schema_ = other.schema_;
            column_groups_ = other.column_groups_;
            created_text_indexes_ = other.created_text_indexes_;
            BuildCache();
        }
        return *this;
    }

    /**
     * @brief Move assignment operator
     */
    SegmentLoadInfo&
    operator=(SegmentLoadInfo&& other) noexcept {
        if (this != &other) {
            info_ = std::move(other.info_);
            schema_ = std::move(other.schema_);
            converted_index_infos_ = std::move(other.converted_index_infos_);
            converted_field_index_cache_ =
                std::move(other.converted_field_index_cache_);
            field_binlog_cache_ = std::move(other.field_binlog_cache_);
            column_groups_ = std::move(other.column_groups_);
            created_text_indexes_ = std::move(other.created_text_indexes_);
        }
        return *this;
    }

    /**
     * @brief Set from protobuf (copy)
     */
    void
    Set(const ProtoType& info, SchemaPtr schema) {
        info_ = info;
        schema_ = std::move(schema);
        BuildCache();
    }

    /**
     * @brief Set from protobuf (move)
     */
    void
    Set(ProtoType&& info, SchemaPtr schema) {
        info_ = std::move(info);
        schema_ = std::move(schema);
        BuildCache();
    }

    // ==================== Basic Accessors ====================

    [[nodiscard]] int64_t
    GetSegmentID() const {
        return info_.segmentid();
    }

    [[nodiscard]] int64_t
    GetPartitionID() const {
        return info_.partitionid();
    }

    [[nodiscard]] int64_t
    GetCollectionID() const {
        return info_.collectionid();
    }

    [[nodiscard]] int64_t
    GetDbID() const {
        return info_.dbid();
    }

    [[nodiscard]] int64_t
    GetNumOfRows() const {
        return info_.num_of_rows();
    }

    [[nodiscard]] int64_t
    GetFlushTime() const {
        return info_.flush_time();
    }

    [[nodiscard]] int64_t
    GetReadableVersion() const {
        return info_.readableversion();
    }

    [[nodiscard]] int64_t
    GetStorageVersion() const {
        return info_.storageversion();
    }

    [[nodiscard]] bool
    IsSorted() const {
        return info_.is_sorted();
    }

    [[nodiscard]] const std::string&
    GetInsertChannel() const {
        return info_.insert_channel();
    }

    [[nodiscard]] const std::string&
    GetManifestPath() const {
        return info_.manifest_path();
    }

    [[nodiscard]] bool
    HasManifestPath() const {
        return !info_.manifest_path().empty();
    }

    [[nodiscard]] proto::common::LoadPriority
    GetPriority() const {
        return info_.priority();
    }

    // ==================== Compaction Info ====================

    [[nodiscard]] const google::protobuf::RepeatedField<int64_t>&
    GetCompactionFrom() const {
        return info_.compactionfrom();
    }

    [[nodiscard]] bool
    IsCompacted() const {
        return info_.compactionfrom_size() > 0;
    }

    [[nodiscard]] int
    GetCompactionFromCount() const {
        return info_.compactionfrom_size();
    }

    // ==================== Index Info ====================

    [[nodiscard]] int
    GetIndexInfoCount() const {
        return info_.index_infos_size();
    }

    [[nodiscard]] const proto::segcore::FieldIndexInfo&
    GetIndexInfo(int index) const {
        return info_.index_infos(index);
    }

    [[nodiscard]] const google::protobuf::RepeatedPtrField<
        proto::segcore::FieldIndexInfo>&
    GetIndexInfos() const {
        return info_.index_infos();
    }

    /**
     * @brief Check if a field has index info
     * @param field_id The field ID to check
     * @return true if the field has at least one index info
     */
    [[nodiscard]] bool
    HasIndexInfo(FieldId field_id) const {
        return converted_field_index_cache_.find(field_id) !=
               converted_field_index_cache_.end();
    }

    /**
     * @brief Get all index infos for a specific field
     * @param field_id The field ID
     * @return Vector of pointers to FieldIndexInfo, empty if field has no index
     */
    [[nodiscard]] std::vector<LoadIndexInfo>
    GetFieldIndexInfos(FieldId field_id) const {
        auto it = converted_field_index_cache_.find(field_id);
        if (it != converted_field_index_cache_.end()) {
            return it->second;
        }
        return {};
    }

    /**
     * @brief Get all field IDs that have index info
     * @return Set of field IDs with indexes
     */
    [[nodiscard]] std::set<FieldId>
    GetIndexedFieldIds() const {
        std::set<FieldId> result;
        for (const auto& pair : converted_field_index_cache_) {
            result.insert(pair.first);
        }
        return result;
    }

    // ==================== Binlog Info ====================

    [[nodiscard]] int
    GetBinlogPathCount() const {
        return info_.binlog_paths_size();
    }

    [[nodiscard]] const proto::segcore::FieldBinlog&
    GetBinlogPath(int index) const {
        return info_.binlog_paths(index);
    }

    [[nodiscard]] const google::protobuf::RepeatedPtrField<
        proto::segcore::FieldBinlog>&
    GetBinlogPaths() const {
        return info_.binlog_paths();
    }

    /**
     * @brief Check if a field/group has binlog paths
     * @param field_id The field ID or group ID
     * @return true if the field has binlog paths
     */
    [[nodiscard]] bool
    HasBinlogPath(FieldId field_id) const {
        return field_binlog_cache_.find(field_id) != field_binlog_cache_.end();
    }

    /**
     * @brief Get binlog info for a specific field/group
     * @param field_id The field ID or group ID
     * @return Pointer to FieldBinlog, nullptr if not found
     */
    [[nodiscard]] const proto::segcore::FieldBinlog*
    GetFieldBinlog(FieldId field_id) const {
        auto it = field_binlog_cache_.find(field_id);
        if (it != field_binlog_cache_.end()) {
            return it->second;
        }
        return nullptr;
    }

    /**
     * @brief Get all binlog file paths for a specific field/group
     * @param field_id The field ID or group ID
     * @return Vector of binlog file paths, empty if field has no binlogs
     */
    [[nodiscard]] std::vector<std::string>
    GetFieldBinlogPaths(FieldId field_id) const {
        auto binlog = GetFieldBinlog(field_id);
        if (binlog == nullptr) {
            return {};
        }
        std::vector<std::string> paths;
        paths.reserve(binlog->binlogs_size());
        for (const auto& log : binlog->binlogs()) {
            paths.push_back(log.log_path());
        }
        return paths;
    }

    /**
     * @brief Calculate total row count from binlogs for a field
     * @param field_id The field ID or group ID
     * @return Total number of entries across all binlogs
     */
    [[nodiscard]] int64_t
    GetFieldBinlogRowCount(FieldId field_id) const {
        auto binlog = GetFieldBinlog(field_id);
        if (binlog == nullptr) {
            return 0;
        }
        int64_t total = 0;
        for (const auto& log : binlog->binlogs()) {
            total += log.entries_num();
        }
        return total;
    }

    /**
     * @brief Get child field IDs for a column group
     * @param group_id The group/field ID
     * @return Vector of child field IDs, empty if not a group or no children
     */
    [[nodiscard]] std::vector<int64_t>
    GetChildFieldIds(FieldId group_id) const {
        auto binlog = GetFieldBinlog(group_id);
        if (binlog == nullptr || binlog->child_fields_size() == 0) {
            return {};
        }
        std::vector<int64_t> result;
        result.reserve(binlog->child_fields_size());
        for (auto child_id : binlog->child_fields()) {
            result.emplace_back(child_id);
        }
        return result;
    }

    /**
     * @brief Check if a binlog entry represents a column group
     * @param field_id The field ID to check
     * @return true if this is a column group with child fields
     */
    [[nodiscard]] bool
    IsColumnGroup(FieldId field_id) const {
        auto binlog = GetFieldBinlog(field_id);
        return binlog != nullptr && binlog->child_fields_size() > 0;
    }

    // ==================== Column Groups Cache ====================

    /**
     * @brief Get column groups from manifest
     * @return Shared pointer to ColumnGroups, nullptr if manifest is empty
     */
    [[nodiscard]] std::shared_ptr<milvus_storage::api::ColumnGroups>
    GetColumnGroups();

    // ==================== Stats & Delta Logs ====================

    [[nodiscard]] int
    GetStatslogCount() const {
        return info_.statslogs_size();
    }

    [[nodiscard]] const proto::segcore::FieldBinlog&
    GetStatslog(int index) const {
        return info_.statslogs(index);
    }

    [[nodiscard]] const google::protobuf::RepeatedPtrField<
        proto::segcore::FieldBinlog>&
    GetStatslogs() const {
        return info_.statslogs();
    }

    [[nodiscard]] int
    GetDeltalogCount() const {
        return info_.deltalogs_size();
    }

    [[nodiscard]] const proto::segcore::FieldBinlog&
    GetDeltalog(int index) const {
        return info_.deltalogs(index);
    }

    [[nodiscard]] const google::protobuf::RepeatedPtrField<
        proto::segcore::FieldBinlog>&
    GetDeltalogs() const {
        return info_.deltalogs();
    }

    // ==================== Text Index Stats ====================

    [[nodiscard]] bool
    HasTextStatsLog(int64_t field_id) const {
        return info_.textstatslogs().find(field_id) !=
               info_.textstatslogs().end();
    }

    [[nodiscard]] const proto::segcore::TextIndexStats*
    GetTextStatsLog(int64_t field_id) const {
        auto it = info_.textstatslogs().find(field_id);
        if (it != info_.textstatslogs().end()) {
            return &it->second;
        }
        return nullptr;
    }

    [[nodiscard]] const google::protobuf::Map<int64_t,
                                              proto::segcore::TextIndexStats>&
    GetTextStatsLogs() const {
        return info_.textstatslogs();
    }

    // ==================== BM25 Logs ====================

    [[nodiscard]] int
    GetBm25logCount() const {
        return info_.bm25logs_size();
    }

    [[nodiscard]] const proto::segcore::FieldBinlog&
    GetBm25log(int index) const {
        return info_.bm25logs(index);
    }

    [[nodiscard]] const google::protobuf::RepeatedPtrField<
        proto::segcore::FieldBinlog>&
    GetBm25logs() const {
        return info_.bm25logs();
    }

    // ==================== JSON Key Stats ====================

    [[nodiscard]] bool
    HasJsonKeyStatsLog(int64_t field_id) const {
        return info_.jsonkeystatslogs().find(field_id) !=
               info_.jsonkeystatslogs().end();
    }

    [[nodiscard]] const proto::segcore::JsonKeyStats*
    GetJsonKeyStatsLog(int64_t field_id) const {
        auto it = info_.jsonkeystatslogs().find(field_id);
        if (it != info_.jsonkeystatslogs().end()) {
            return &it->second;
        }
        return nullptr;
    }

    [[nodiscard]] const google::protobuf::Map<int64_t,
                                              proto::segcore::JsonKeyStats>&
    GetJsonKeyStatsLogs() const {
        return info_.jsonkeystatslogs();
    }

    // ==================== Created Text Indexes Tracking ====================

    void
    SetTextIndexCreated(FieldId field_id) {
        created_text_indexes_.insert(field_id);
    }

    [[nodiscard]] bool
    HasTextIndexCreated(FieldId field_id) const {
        return created_text_indexes_.find(field_id) !=
               created_text_indexes_.end();
    }

    // ==================== Diff Computation ====================

    /**
     * @brief Compute the difference between this SegmentLoadInfo and a new one
     *
     * This method compares the current SegmentLoadInfo with a new version and
     * produces a LoadDiff that describes what needs to be loaded or dropped
     * when reopening a segment.
     *
     * Note: Updates can only happen within the same category:
     * - binlog -> binlog updates
     * - manifest -> manifest updates
     * Cross-category changes (binlog <-> manifest) are not supported.
     *
     * The diff logic for indexes (always computed):
     * - indexes_to_load: Indexes present in new_info but not in this (by field_id)
     * - indexes_to_drop: Indexes present in this but not in new_info (by field_id)
     *
     * The diff logic for field data:
     *
     * Binlog mode (when current has no manifest):
     * - binlogs_to_load: Binlogs present in new_info but not in this
     * - field_data_to_drop: Fields present in this but not in new_info
     *
     * Manifest mode (when current has manifest):
     * - manifest_updated: true if manifest path changed
     * - new_manifest_path: The new manifest path to load from
     * - Caller should reload all fields via the new manifest at runtime
     *   (since manifest content can only be obtained through runtime APIs)
     *
     * @param new_info The new SegmentLoadInfo to compare against
     * @return LoadDiff containing the differences
     */
    [[nodiscard]] LoadDiff
    ComputeDiff(SegmentLoadInfo& new_info);

    /**
     * @brief Get the LoadDiff from the current SegmentLoadInfo
     *
     * This method produces a LoadDiff that describes what needs to be loaded when 
     * load with current load info.
     *
     * @return LoadDiff containing the differences
     */
    [[nodiscard]] LoadDiff
    GetLoadDiff();

    // ==================== Underlying Proto Access ====================

    /**
     * @brief Get const reference to the underlying protobuf message
     */
    [[nodiscard]] const ProtoType&
    GetProto() const {
        return info_;
    }

    /**
     * @brief Get mutable pointer to the underlying protobuf message
     * @note After modifying the proto, call RebuildCache() to update caches
     */
    ProtoType*
    MutableProto() {
        return &info_;
    }

    /**
     * @brief Rebuild internal caches after direct proto modification
     */
    void
    RebuildCache() {
        BuildCache();
    }

    /**
     * @brief Check if the SegmentLoadInfo is empty/unset
     */
    [[nodiscard]] bool
    IsEmpty() const {
        return info_.segmentid() == 0 && info_.num_of_rows() == 0;
    }

    // ==================== LoadIndexInfo Conversion ====================

    /**
     * @brief Convert a FieldIndexInfo to LoadIndexInfo
     *
     * This method converts the protobuf FieldIndexInfo to the internal
     * LoadIndexInfo structure used for loading indexes.
     *
     * @param field_index_info Pointer to the FieldIndexInfo to convert
     * @param segment_id The segment ID for the LoadIndexInfo
     * @return LoadIndexInfo structure populated with the converted data
     */
    [[nodiscard]] LoadIndexInfo
    ConvertFieldIndexInfoToLoadIndexInfo(
        const proto::segcore::FieldIndexInfo* field_index_info,
        int64_t segment_id) const;

    /**
    * @brief Check if a field's index has raw data
    *
    * Determines whether the index for a given field contains raw data
    * by querying the IndexFactory with the index parameters.
    *
    * @param load_index_info The LoadIndexInfo containing index parameters
    * @return true if the index has raw data, false otherwise
    */
    [[nodiscard]] static bool
    CheckIndexHasRawData(const LoadIndexInfo& load_index_info);

    /**
     * @brief Convert a TextIndexStats to LoadTextIndexInfo
     *
     * This method converts the protobuf TextIndexStats to the
     * LoadTextIndexInfo structure used for loading pre-built text indexes.
     *
     * @param text_index_stats The TextIndexStats to convert
     * @param field_id The field ID for the text index
     * @return shared_ptr to LoadTextIndexInfo populated with the converted data
     */
    [[nodiscard]] std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>
    ConvertTextIndexStatsToLoadTextIndexInfo(
        const proto::segcore::TextIndexStats& text_index_stats,
        FieldId field_id) const;

 private:
    void
    BuildCache() {
        field_binlog_cache_.clear();
        // Build binlog cache
        for (int i = 0; i < info_.binlog_paths_size(); i++) {
            const auto& binlog = info_.binlog_paths(i);
            auto field_id = FieldId(binlog.fieldid());
            field_binlog_cache_[field_id] = &binlog;
        }

        // Convert index infos to LoadIndexInfo and build per-field cache
        converted_index_infos_.clear();
        converted_field_index_cache_.clear();
        field_index_has_raw_data_.clear();
        for (int i = 0; i < info_.index_infos_size(); i++) {
            const auto& index_info = info_.index_infos(i);
            if (index_info.index_file_paths_size() == 0) {
                continue;
            }
            auto load_index_info = ConvertFieldIndexInfoToLoadIndexInfo(
                &index_info, info_.segmentid());
            converted_index_infos_.push_back(load_index_info);
            auto field_id = FieldId(index_info.fieldid());
            // Check if index has raw data before moving
            if (CheckIndexHasRawData(load_index_info)) {
                field_index_has_raw_data_.insert(field_id);
            }
            converted_field_index_cache_[field_id].push_back(
                std::move(load_index_info));
        }
    }

    void
    ComputeDiffIndexes(LoadDiff& diff, SegmentLoadInfo& new_info);

    void
    ComputeDiffBinlogs(LoadDiff& diff, SegmentLoadInfo& new_info);

    void
    ComputeDiffColumnGroups(LoadDiff& diff, SegmentLoadInfo& new_info);

    void
    ComputeDiffReloadFields(LoadDiff& diff, SegmentLoadInfo& new_info);

    void
    ComputeDiffDefaultFields(LoadDiff& diff, SegmentLoadInfo& new_info);

    void
    ComputeDiffTextIndexes(LoadDiff& diff, SegmentLoadInfo& new_info);

    ProtoType info_;

    SchemaPtr schema_;

    std::vector<LoadIndexInfo> converted_index_infos_;

    // Cache for quick field -> converted LoadIndexInfo lookup
    std::unordered_map<FieldId, std::vector<LoadIndexInfo>>
        converted_field_index_cache_;

    // set of field ids that corresponding index has raw data
    std::set<FieldId> field_index_has_raw_data_;

    // set of field ids that have been filled with default values
    std::set<FieldId> fields_filled_with_default_;

    // Cache for quick field -> binlog lookup
    std::map<FieldId, const proto::segcore::FieldBinlog*> field_binlog_cache_;

    // Cache for column groups metadata (used with manifest mode)
    std::shared_ptr<milvus_storage::api::ColumnGroups> column_groups_;

    // Field IDs where text indexes were created from raw data (not loaded from files)
    // These should NOT be re-loaded in diff computation
    std::unordered_set<FieldId> created_text_indexes_;
};

}  // namespace milvus::segcore
