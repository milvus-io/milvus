// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <assert.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "db/snapshot/Store.h"
#include "db/snapshot/Utils.h"
#include "db/snapshot/WrappedTypes.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

using ScopedResourcesT =
    std::tuple<CollectionCommit::ScopedMapT, Collection::ScopedMapT, SchemaCommit::ScopedMapT, FieldCommit::ScopedMapT,
               Field::ScopedMapT, FieldElement::ScopedMapT, PartitionCommit::ScopedMapT, Partition::ScopedMapT,
               SegmentCommit::ScopedMapT, Segment::ScopedMapT, SegmentFile::ScopedMapT>;

class Snapshot : public ReferenceProxy {
 public:
    using Ptr = std::shared_ptr<Snapshot>;
    Snapshot(StorePtr, ID_TYPE);

    ID_TYPE
    GetID() const {
        return GetCollectionCommit()->GetID();
    }

    ID_TYPE
    GetCollectionId() const {
        auto it = GetResources<Collection>().cbegin();
        return it->first;
    }

    CollectionPtr
    GetCollection() const {
        return GetResources<Collection>().cbegin()->second.Get();
    }

    SchemaCommitPtr
    GetSchemaCommit() const {
        auto id = GetLatestSchemaCommitId();
        return GetResource<SchemaCommit>(id);
    }

    const std::string&
    GetName() const {
        return GetResources<Collection>().cbegin()->second->GetName();
    }

    size_t
    NumberOfPartitions() const {
        return GetResources<Partition>().size();
    }

    const LSN_TYPE&
    GetMaxLsn() const {
        return max_lsn_;
    }

    PartitionPtr
    GetPartition(const std::string& name) const {
        ID_TYPE id = 0;
        auto status = GetPartitionId(name, id);
        if (!status.ok()) {
            return nullptr;
        }
        return GetResource<Partition>(id);
    }

    Status
    GetPartitionId(const std::string& name, ID_TYPE& id) const {
        std::string real_name = name.empty() ? DEFAULT_PARTITON_TAG : name;
        auto it = partition_names_map_.find(real_name);
        if (it == partition_names_map_.end()) {
            return Status(SS_NOT_FOUND_ERROR, "Specified partition name not found");
        }
        id = it->second;
        return Status::OK();
    }

    CollectionCommitPtr
    GetCollectionCommit() const {
        return GetResources<CollectionCommit>().cbegin()->second.Get();
    }

    const std::set<ID_TYPE>&
    GetSegmentFileIds(ID_TYPE segment_id) const {
        auto it = seg_segfiles_map_.find(segment_id);
        if (it == seg_segfiles_map_.end()) {
            return empty_set_;
        }
        return it->second;
    }

    SegmentFilePtr
    GetSegmentFile(ID_TYPE segment_id, ID_TYPE field_element_id) const;

    ID_TYPE
    GetLatestSchemaCommitId() const {
        return latest_schema_commit_id_;
    }

    Status
    GetFieldElement(const std::string& field_name, const std::string& field_element_name,
                    FieldElementPtr& field_element) const;

    SegmentCommitPtr
    GetSegmentCommitBySegmentId(ID_TYPE segment_id) const {
        auto it = seg_segc_map_.find(segment_id);
        if (it == seg_segc_map_.end())
            return nullptr;
        return GetResource<SegmentCommit>(it->second);
    }

    Status
    GetSegmentRowCount(ID_TYPE segment_id, SIZE_TYPE&) const;

    std::vector<std::string>
    GetPartitionNames() const {
        std::vector<std::string> names;
        for (auto& kv : partition_names_map_) {
            names.emplace_back(kv.first);
        }

        return std::move(names);
    }

    PartitionCommitPtr
    GetPartitionCommitByPartitionId(ID_TYPE partition_id) const {
        auto it = p_pc_map_.find(partition_id);
        if (it == p_pc_map_.end())
            return nullptr;
        return GetResource<PartitionCommit>(it->second);
    }

    template <typename HandlerT>
    void
    IterateResources(const typename HandlerT::Ptr& handler) {
        auto& resources = GetResources<typename HandlerT::ResourceT>();
        auto status = handler->PreIterate();
        if (!status.ok()) {
            handler->SetStatus(status);
            return;
        }
        for (auto& kv : resources) {
            status = handler->Handle(kv.second.Get());
            if (!status.ok()) {
                break;
            }
        }
        if (!status.ok()) {
            handler->SetStatus(status);
            return;
        }

        status = handler->PostIterate();
        handler->SetStatus(status);
    }

    std::vector<std::string>
    GetFieldNames() const {
        std::vector<std::string> names;
        for (auto& kv : field_names_map_) {
            names.emplace_back(kv.first);
        }
        return std::move(names);
    }

    bool
    HasField(const std::string& name) const {
        auto it = field_names_map_.find(name);
        return it != field_names_map_.end();
    }

    FieldPtr
    GetField(const std::string& name) const;

    bool
    HasFieldElement(const std::string& field_name, const std::string& field_element_name) const {
        auto id = GetFieldElementId(field_name, field_element_name);
        return id > 0;
    }

    ID_TYPE
    GetSegmentFileId(const std::string& field_name, const std::string& field_element_name, ID_TYPE segment_id) const {
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

    bool
    HasSegmentFile(const std::string& field_name, const std::string& field_element_name, ID_TYPE segment_id) const {
        auto id = GetSegmentFileId(field_name, field_element_name, segment_id);
        return id > 0;
    }

    ID_TYPE
    GetFieldElementId(const std::string& field_name, const std::string& field_element_name) const {
        auto itf = field_element_names_map_.find(field_name);
        if (itf == field_element_names_map_.end())
            return 0;
        auto itfe = itf->second.find(field_element_name);
        if (itfe == itf->second.end()) {
            return 0;
        }

        return itfe->second;
    }

    std::vector<FieldElementPtr>
    GetFieldElementsByField(const std::string& field_name) const {
        auto it = field_element_names_map_.find(field_name);
        if (it == field_element_names_map_.end()) {
            return {};
        }
        std::vector<FieldElementPtr> elements;
        for (auto& kv : it->second) {
            elements.push_back(GetResource<FieldElement>(kv.second));
        }

        return std::move(elements);
    }

    NUM_TYPE
    GetMaxSegmentNumByPartition(ID_TYPE partition_id) const {
        auto it = p_max_seg_num_.find(partition_id);
        if (it == p_max_seg_num_.end())
            return 0;
        return it->second;
    }

    void
    RefAll();
    void
    UnRefAll();

    void
    UnRef() override {
        ReferenceProxy::UnRef();
        if (ref_count_ == 0) {
            UnRefAll();
        }
    }

    template <typename ResourceT>
    void
    DumpResource(const std::string& tag = "") const {
        auto& resources = GetResources<ResourceT>();
        LOG_ENGINE_DEBUG_ << typeid(*this).name() << " Dump " << GetID() << " " << ResourceT::Name << " Start [" << tag
                          << "]:" << resources.size();
        for (auto& kv : resources) {
            LOG_ENGINE_DEBUG_ << "\t" << kv.second->ToString();
        }
        LOG_ENGINE_DEBUG_ << typeid(*this).name() << " Dump " << GetID() << " " << ResourceT::Name << "  End [" << tag
                          << "]:" << resources.size();
    }

    template <typename T>
    void
    DoUnRef(T& resource_map) {
        for (auto& kv : resource_map) {
            kv.second->UnRef();
        }
    }

    template <typename T>
    void
    DoRef(T& resource_map) {
        for (auto& kv : resource_map) {
            kv.second->Ref();
        }
    }

    template <typename ResourceT>
    typename ResourceT::ScopedMapT&
    GetResources() {
        return std::get<Index<typename ResourceT::ScopedMapT, ScopedResourcesT>::value>(resources_);
    }

    template <typename ResourceT>
    const typename ResourceT::ScopedMapT&
    GetResources() const {
        return std::get<Index<typename ResourceT::ScopedMapT, ScopedResourcesT>::value>(resources_);
    }

    template <typename ResourceT>
    typename ResourceT::Ptr
    GetResource(ID_TYPE id) const {
        auto& resources = GetResources<ResourceT>();
        auto it = resources.find(id);
        if (it == resources.end()) {
            return nullptr;
        }
        return it->second.Get();
    }

    template <typename ResourceT>
    void
    AddResource(ScopedResource<ResourceT>& resource) {
        auto& resources = GetResources<ResourceT>();
        resources[resource->GetID()] = resource;
    }

    bool
    IsValid() const {
        return !invalid_;
    }

    const std::string
    ToString() const;

 private:
    Snapshot(const Snapshot&) = delete;
    Snapshot&
    operator=(const Snapshot&) = delete;

    // PXU TODO: Re-org below data structures to reduce memory usage
    ScopedResourcesT resources_;
    ID_TYPE current_schema_id_;
    std::map<std::string, ID_TYPE> field_names_map_;
    std::map<std::string, ID_TYPE> partition_names_map_;
    std::map<std::string, std::map<std::string, ID_TYPE>> field_element_names_map_;
    std::map<ID_TYPE, std::map<ID_TYPE, ID_TYPE>> element_segfiles_map_;
    std::map<ID_TYPE, std::set<ID_TYPE>> seg_segfiles_map_;
    std::map<ID_TYPE, ID_TYPE> seg_segc_map_;
    std::map<ID_TYPE, ID_TYPE> p_pc_map_;
    ID_TYPE latest_schema_commit_id_ = 0;
    std::map<ID_TYPE, NUM_TYPE> p_max_seg_num_;
    LSN_TYPE max_lsn_;
    std::set<ID_TYPE> empty_set_;
    bool invalid_ = true;
};

using GCHandler = std::function<void(Snapshot::Ptr)>;
using ScopedSnapshotT = ScopedResource<Snapshot>;

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
