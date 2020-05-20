#pragma once
#include "WrappedTypes.h"
#include <memory>
#include <string>
#include <vector>
#include <assert.h>
#include <iostream>
#include <limits>
#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>


class Snapshot : public ReferenceProxy {
public:
    using Ptr = std::shared_ptr<Snapshot>;
    Snapshot(ID_TYPE id);

    ID_TYPE GetID() const { return collection_commit_->GetID();}
    ID_TYPE GetCollectionId() const { return collection_->GetID(); }
    const std::string& GetName() const { return collection_->GetName(); }
    CollectionCommitPtr GetCollectionCommit() { return collection_commit_.Get(); }
    std::vector<std::string> GetPartitionNames() const {
        std::vector<std::string> names;
        for (auto& kv : partitions_) {
            std::cout << "Partition: " << kv.second->GetName() << std::endl;
            names.push_back(kv.second->GetName());
        }
        return names;
    }

    ID_TYPE GetLatestSchemaCommitId() const {
        return latest_schema_commit_id_;
    }

    PartitionPtr GetPartition(ID_TYPE partition_id) {
        auto it = partitions_.find(partition_id);
        if (it == partitions_.end()) {
            return nullptr;
        }
        return it->second.Get();
    }

    // PXU TODO: add const. Need to change Scopedxxxx::Get
    SegmentCommitPtr GetSegmentCommit(ID_TYPE segment_id) {
        auto it = seg_segc_map_.find(segment_id);
        if (it == seg_segc_map_.end()) return nullptr;
        auto itsc = segment_commits_.find(it->second);
        if (itsc == segment_commits_.end()) {
            return nullptr;
        }
        return itsc->second.Get();
    }

    PartitionCommitPtr GetPartitionCommitByPartitionId(ID_TYPE partition_id) {
        auto it = p_pc_map_.find(partition_id);
        if (it == p_pc_map_.end()) return nullptr;
        auto itpc = partition_commits_.find(it->second);
        if (itpc == partition_commits_.end()) {
            return nullptr;
        }
        return itpc->second.Get();
    }

    IDS_TYPE GetPartitionIds() const {
        IDS_TYPE ids;
        for(auto& kv : partitions_) {
            ids.push_back(kv.first);
        }
        return std::move(ids);
    }

    std::vector<std::string> GetFieldNames() const {
        std::vector<std::string> names;
        for(auto& kv : field_names_map_) {
            names.emplace_back(kv.first);
        }
        return std::move(names);
    }

    bool HasField(const std::string& name) const {
        auto it = field_names_map_.find(name);
        return it != field_names_map_.end();
    }

    bool HasFieldElement(const std::string& field_name, const std::string& field_element_name) const {
        auto id = GetFieldElementId(field_name, field_element_name);
        return id > 0;
    }

    ID_TYPE GetSegmentFileId(const std::string& field_name, const std::string& field_element_name,
            ID_TYPE segment_id) const {
        auto field_element_id = GetFieldElementId(field_name, field_element_name);
        auto it = element_segfiles_map_.find(field_element_id);
        if (it == element_segfiles_map_.end()) {
            return 0;
        }
        auto its = it->second.find(segment_id);
        if (its == it->second.end()) {
            return 0;
        }
        return its->second;
    }

    bool HasSegmentFile(const std::string& field_name, const std::string& field_element_name,
            ID_TYPE segment_id) const {
        auto id = GetSegmentFileId(field_name, field_element_name, segment_id);
        return id > 0;
    }

    ID_TYPE GetFieldElementId(const std::string& field_name, const std::string& field_element_name) const {
        auto itf = field_element_names_map_.find(field_name);
        if (itf == field_element_names_map_.end()) return false;
        auto itfe = itf->second.find(field_element_name);
        if (itfe == itf->second.end()) {
            return 0;
        }

        return itfe->second;
    }

    std::vector<std::string> GetFieldElementNames() const {
        std::vector<std::string> names;
        for(auto& kv : field_elements_) {
            names.emplace_back(kv.second->GetName());
        }

        return std::move(names);
    }

    IDS_TYPE GetSegmentIds() const {
        IDS_TYPE ids;
        for(auto& kv : segments_) {
            ids.push_back(kv.first);
        }
        return std::move(ids);
    }

    IDS_TYPE GetSegmentFileIds() const {
        IDS_TYPE ids;
        for(auto& kv : segment_files_) {
            ids.push_back(kv.first);
        }
        return std::move(ids);
    }

    NUM_TYPE GetMaxSegmentNumByPartition(ID_TYPE partition_id) {
        auto it = p_max_seg_num_.find(partition_id);
        if (it == p_max_seg_num_.end()) return 0;
        return it->second;
    }

    void RefAll();
    void UnRefAll();

    void DumpSegments(const std::string& tag = "");
    void DumpSegmentCommits(const std::string& tag = "");
    void DumpPartitionCommits(const std::string& tag = "");

private:
    // PXU TODO: Re-org below data structures to reduce memory usage
    CollectionScopedT collection_;
    ID_TYPE current_schema_id_;
    SchemaCommitsT schema_commits_;
    FieldsT fields_;
    FieldCommitsT field_commits_;
    FieldElementsT field_elements_;
    CollectionCommitScopedT collection_commit_;
    PartitionsT partitions_;
    PartitionCommitsT partition_commits_;
    SegmentsT segments_;
    SegmentCommitsT segment_commits_;
    SegmentFilesT segment_files_;
    std::map<std::string, ID_TYPE> field_names_map_;
    std::map<std::string, std::map<std::string, ID_TYPE>> field_element_names_map_;
    std::map<ID_TYPE, std::map<ID_TYPE, ID_TYPE>> element_segfiles_map_;
    std::map<ID_TYPE, ID_TYPE> seg_segc_map_;
    std::map<ID_TYPE, ID_TYPE> p_pc_map_;
    ID_TYPE latest_schema_commit_id_;
    std::map<ID_TYPE, NUM_TYPE> p_max_seg_num_;
};

using ScopedSnapshotT = ScopedResource<Snapshot>;
using GCHandler = std::function<void(Snapshot::Ptr)>;
