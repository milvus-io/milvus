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
    /* std::cout << this << " RefAll SS=" << GetID() << " SS RefCnt=" << RefCnt() << std::endl; */
    std::apply([this](auto&... resource) { ((DoRef(resource)), ...); }, resources_);
}

void
Snapshot::UnRefAll() {
    /* std::cout << this << " UnRefAll SS=" << GetID() << " SS RefCnt=" << RefCnt() << std::endl; */
    std::apply([this](auto&... resource) { ((DoUnRef(resource)), ...); }, resources_);
}

Snapshot::Snapshot(ID_TYPE id) {
    auto collection_commit = CollectionCommitsHolder::GetInstance().GetResource(id, false);
    AddResource<CollectionCommit>(collection_commit);
    max_lsn_ = collection_commit->GetLsn();
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

    auto ssid = id;
    for (auto& id : mappings) {
        auto partition_commit = partition_commits_holder.GetResource(id, false);
        auto partition = partitions_holder.GetResource(partition_commit->GetPartitionId(), false);
        AddResource<PartitionCommit>(partition_commit);
        p_pc_map_[partition_commit->GetPartitionId()] = partition_commit->GetID();
        AddResource<Partition>(partition);
        partition_names_map_[partition->GetName()] = partition->GetID();
        p_max_seg_num_[partition->GetID()] = 0;
        auto& s_c_mappings = partition_commit->GetMappings();
        /* std::cout << "SS-" << ssid << "PC_MAP=("; */
        /* for (auto id : s_c_mappings) { */
        /*     std::cout << id << ","; */
        /* } */
        /* std::cout << ")" << std::endl; */
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

    RefAll();
}

const std::string
Snapshot::ToString() {
    auto to_matrix_string = [](const MappingT& mappings, int line_length, size_t ident = 0) -> std::string {
        std::stringstream ss;
        std::string l1_spaces;
        for (auto i = 0; i < ident; ++i) {
            l1_spaces += " ";
        }
        auto l2_spaces = l1_spaces + l1_spaces;
        std::string prefix = "";
        if (mappings.size() > line_length) {
            prefix = "\n" + l1_spaces;
        }
        ss << prefix << "[";
        auto pos = 0;
        for (auto id : mappings) {
            if (pos > line_length) {
                pos = 0;
                ss << "\n" << l2_spaces;
            } else if (pos == 0) {
                if (prefix != "") {
                    ss << "\n" << l2_spaces;
                }
            } else {
                ss << ", ";
            }
            ss << id;
            pos++;
        }
        ss << prefix << "]";
        return ss.str();
    };

    int row_element_size = 8;
    std::stringstream ss;
    ss << "****************************** Snapshot " << GetID() << " ******************************";
    ss << "\nCollection: id=" << GetCollectionId() << ",name=\"" << GetName() << "\"";
    ss << ", CollectionCommit: id=" << GetCollectionCommit()->GetID();
    ss << ",mappings=";
    auto& cc_m = GetCollectionCommit()->GetMappings();
    ss << to_matrix_string(cc_m, row_element_size, 2);
    for (auto& p_c_id : cc_m) {
        auto p_c = GetResource<PartitionCommit>(p_c_id);
        auto p = GetResource<Partition>(p_c->GetPartitionId());
        ss << "\nPartition: id=" << p->GetID() << ",name=\"" << p->GetName() << "\"";
        ss << ", PartitionCommit: id=" << p_c->GetID();
        ss << ",mappings=";
        auto& pc_m = p_c->GetMappings();
        ss << to_matrix_string(pc_m, row_element_size, 2);
        for (auto& sc_id : pc_m) {
            auto sc = GetResource<SegmentCommit>(sc_id);
            auto se = GetResource<Segment>(sc->GetSegmentId());
            ss << "\n  Segment: id=" << se->GetID();
            ss << ", SegmentCommit: id=" << sc->GetID();
            ss << ",mappings=";
            auto& sc_m = sc->GetMappings();
            ss << to_matrix_string(sc_m, row_element_size, 2);
            for (auto& sf_id : sc_m) {
                auto sf = GetResource<SegmentFile>(sf_id);
                ss << "\n\tSegmentFile: id=" << sf_id << ",field_element_id=" << sf->GetFieldElementId();
            }
        }
    }
    ss << "\n----------------------------------------------------------------------------------------";

    return ss.str();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
