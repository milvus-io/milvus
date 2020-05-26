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
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include "db/snapshot/Utils.h"
#include "db/snapshot/WrappedTypes.h"

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
    explicit Snapshot(ID_TYPE id);

    ID_TYPE
    GetID() {
        return GetCollectionCommit()->GetID();
    }

    ID_TYPE
    GetCollectionId() const {
        auto it = GetResources<Collection>().begin();
        return it->first;
    }

    const std::string&
    GetName() const {
        return GetResources<Collection>().begin()->second->GetName();
    }

    CollectionCommitPtr
    GetCollectionCommit() {
        return GetResources<CollectionCommit>().begin()->second.Get();
    }

    ID_TYPE
    GetLatestSchemaCommitId() const {
        return latest_schema_commit_id_;
    }

    // PXU TODO: add const. Need to change Scopedxxxx::Get
    SegmentCommitPtr
    GetSegmentCommit(ID_TYPE segment_id) {
        auto it = seg_segc_map_.find(segment_id);
        if (it == seg_segc_map_.end())
            return nullptr;
        return GetResource<SegmentCommit>(it->second);
    }

    PartitionCommitPtr
    GetPartitionCommitByPartitionId(ID_TYPE partition_id) {
        auto it = p_pc_map_.find(partition_id);
        if (it == p_pc_map_.end())
            return nullptr;
        return GetResource<PartitionCommit>(it->second);
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
            return false;
        auto itfe = itf->second.find(field_element_name);
        if (itfe == itf->second.end()) {
            return 0;
        }

        return itfe->second;
    }

    NUM_TYPE
    GetMaxSegmentNumByPartition(ID_TYPE partition_id) {
        auto it = p_max_seg_num_.find(partition_id);
        if (it == p_max_seg_num_.end())
            return 0;
        return it->second;
    }

    void
    RefAll();
    void
    UnRefAll();

    template <typename ResourceT>
    void
    DumpResource(const std::string& tag = "") {
        auto& resources = GetResources<ResourceT>();
        std::cout << typeid(*this).name() << " Dump" << ResourceT::Name << " Start [" << tag << "]:" << resources.size()
                  << std::endl;
        for (auto& kv : resources) {
            std::cout << "\t" << kv.second->ToString() << std::endl;
        }
        std::cout << typeid(*this).name() << " Dump" << ResourceT::Name << "  End [" << tag << "]:" << resources.size()
                  << std::endl;
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
    GetResource(ID_TYPE id) {
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

 private:
    // PXU TODO: Re-org below data structures to reduce memory usage
    ScopedResourcesT resources_;
    ID_TYPE current_schema_id_;
    std::map<std::string, ID_TYPE> field_names_map_;
    std::map<std::string, std::map<std::string, ID_TYPE>> field_element_names_map_;
    std::map<ID_TYPE, std::map<ID_TYPE, ID_TYPE>> element_segfiles_map_;
    std::map<ID_TYPE, ID_TYPE> seg_segc_map_;
    std::map<ID_TYPE, ID_TYPE> p_pc_map_;
    ID_TYPE latest_schema_commit_id_ = 0;
    std::map<ID_TYPE, NUM_TYPE> p_max_seg_num_;
};

using ScopedSnapshotT = ScopedResource<Snapshot>;
using GCHandler = std::function<void(Snapshot::Ptr)>;

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
