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

#include "db/snapshot/Snapshot.h"
#include "db/snapshot/ResourceHolders.h"
#include "db/snapshot/Store.h"

namespace milvus {
namespace engine {
namespace snapshot {

void
Snapshot::RefAll() {
    std::apply([this](auto&... resource) { ((DoRef(resource)), ...); }, resources_);
}

void
Snapshot::UnRefAll() {
    std::apply([this](auto&... resource) { ((DoUnRef(resource)), ...); }, resources_);
}

Snapshot::Snapshot(ID_TYPE id) {
    auto collection_commit = CollectionCommitsHolder::GetInstance().GetResource(id, false);
    AddResource<CollectionCommit>(collection_commit);
    auto& schema_holder = SchemaCommitsHolder::GetInstance();
    auto current_schema = schema_holder.GetResource(collection_commit->GetSchemaId(), false);
    AddResource<SchemaCommit>(current_schema);
    current_schema_id_ = current_schema->GetID();
    auto& field_commits_holder = FieldCommitsHolder::GetInstance();
    auto& fields_holder = FieldsHolder::GetInstance();
    auto& field_elements_holder = FieldElementsHolder::GetInstance();

    auto collection = CollectionsHolder::GetInstance().GetResource(collection_commit->GetCollectionId(), false);
    AddResource<Collection>(collection);
    auto& mappings = collection_commit->GetMappings();
    auto& partition_commits_holder = PartitionCommitsHolder::GetInstance();
    auto& partitions_holder = PartitionsHolder::GetInstance();
    auto& segments_holder = SegmentsHolder::GetInstance();
    auto& segment_commits_holder = SegmentCommitsHolder::GetInstance();
    auto& segment_files_holder = SegmentFilesHolder::GetInstance();

    for (auto& id : mappings) {
        auto partition_commit = partition_commits_holder.GetResource(id, false);
        auto partition = partitions_holder.GetResource(partition_commit->GetPartitionId(), false);
        AddResource<PartitionCommit>(partition_commit);
        p_pc_map_[partition_commit->GetPartitionId()] = partition_commit->GetID();
        AddResource<Partition>(partition);
        p_max_seg_num_[partition->GetID()] = 0;
        auto& s_c_mappings = partition_commit->GetMappings();
        for (auto& s_c_id : s_c_mappings) {
            auto segment_commit = segment_commits_holder.GetResource(s_c_id, false);
            auto segment = segments_holder.GetResource(segment_commit->GetSegmentId(), false);
            auto schema = schema_holder.GetResource(segment_commit->GetSchemaId(), false);
            AddResource<SchemaCommit>(schema);
            AddResource<SegmentCommit>(segment_commit);
            if (segment->GetNum() > p_max_seg_num_[segment->GetPartitionId()]) {
                p_max_seg_num_[segment->GetPartitionId()] = segment->GetNum();
            }
            AddResource<Segment>(segment);
            seg_segc_map_[segment->GetID()] = segment_commit->GetID();
            auto& s_f_mappings = segment_commit->GetMappings();
            for (auto& s_f_id : s_f_mappings) {
                auto segment_file = segment_files_holder.GetResource(s_f_id, false);
                auto field_element = field_elements_holder.GetResource(segment_file->GetFieldElementId(), false);
                AddResource<FieldElement>(field_element);
                AddResource<SegmentFile>(segment_file);
                auto entry = element_segfiles_map_.find(segment_file->GetFieldElementId());
                if (entry == element_segfiles_map_.end()) {
                    element_segfiles_map_[segment_file->GetFieldElementId()] = {
                        {segment_file->GetSegmentId(), segment_file->GetID()}};
                } else {
                    entry->second[segment_file->GetSegmentId()] = segment_file->GetID();
                }
            }
        }
    }

    for (auto& kv : GetResources<SchemaCommit>()) {
        if (kv.first > latest_schema_commit_id_)
            latest_schema_commit_id_ = kv.first;
        auto& schema_commit = kv.second;
        auto& s_c_m = current_schema->GetMappings();
        for (auto field_commit_id : s_c_m) {
            auto field_commit = field_commits_holder.GetResource(field_commit_id, false);
            AddResource<FieldCommit>(field_commit);
            auto field = fields_holder.GetResource(field_commit->GetFieldId(), false);
            AddResource<Field>(field);
            field_names_map_[field->GetName()] = field->GetID();
            auto& f_c_m = field_commit->GetMappings();
            for (auto field_element_id : f_c_m) {
                auto field_element = field_elements_holder.GetResource(field_element_id, false);
                AddResource<FieldElement>(field_element);
                auto entry = field_element_names_map_.find(field->GetName());
                if (entry == field_element_names_map_.end()) {
                    field_element_names_map_[field->GetName()] = {{field_element->GetName(), field_element->GetID()}};
                } else {
                    entry->second[field_element->GetName()] = field_element->GetID();
                }
            }
        }
    }

    /* for(auto kv : partition_commits_) { */
    /*     std::cout << this << " Snapshot " << collection_commit_->GetID() << " PartitionCommit " << */
    /*         kv.first << " Partition " << kv.second->GetPartitionId() << std::endl; */
    /* } */
    /* for(auto kv : p_pc_map_) { */
    /*     std::cout << this << " Snapshot " << collection_commit_->GetID() << " P " << */
    /*         kv.first << " PC " << kv.second << std::endl; */
    /* } */

    RefAll();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
