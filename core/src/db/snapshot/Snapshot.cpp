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
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/ResourceHolders.h"
#include "db/snapshot/Store.h"

namespace milvus {
namespace engine {
namespace snapshot {

#define CHECK_NOT_NULL_AND_RETURN(target) \
    if (not target) {                     \
        return;                           \
    }

void
Snapshot::RefAll() {
    assert(invalid_ != true);
    std::apply([this](auto&... resource) { ((DoRef(resource)), ...); }, resources_);
}

void
Snapshot::UnRefAll() {
    if (invalid_) {
        return;
    }
    std::apply([this](auto&... resource) { ((DoUnRef(resource)), ...); }, resources_);
}

Snapshot::Snapshot(StorePtr store, ID_TYPE ss_id) {
    auto& collection_commits_holder = CollectionCommitsHolder::GetInstance();
    auto& collections_holder = CollectionsHolder::GetInstance();
    auto& schema_commits_holder = SchemaCommitsHolder::GetInstance();
    auto& field_commits_holder = FieldCommitsHolder::GetInstance();
    auto& fields_holder = FieldsHolder::GetInstance();
    auto& field_elements_holder = FieldElementsHolder::GetInstance();
    auto& partition_commits_holder = PartitionCommitsHolder::GetInstance();
    auto& partitions_holder = PartitionsHolder::GetInstance();
    auto& segment_commits_holder = SegmentCommitsHolder::GetInstance();
    auto& segments_holder = SegmentsHolder::GetInstance();
    auto& segment_files_holder = SegmentFilesHolder::GetInstance();

    auto collection_commit = collection_commits_holder.GetResource(store, ss_id, false);
    CHECK_NOT_NULL_AND_RETURN(collection_commit);
    AddResource<CollectionCommit>(collection_commit);

    max_lsn_ = collection_commit->GetLsn();
    auto schema_commit = schema_commits_holder.GetResource(store, collection_commit->GetSchemaId(), false);
    CHECK_NOT_NULL_AND_RETURN(schema_commit);
    AddResource<SchemaCommit>(schema_commit);

    current_schema_id_ = schema_commit->GetID();
    auto collection = collections_holder.GetResource(store, collection_commit->GetCollectionId(), false);
    CHECK_NOT_NULL_AND_RETURN(collection);
    AddResource<Collection>(collection);

    auto base_path = GetResPath<Collection>(store->GetRootPath(), std::make_shared<Collection>(*collection));
    collection_commit->LoadIds(base_path);
    auto& collection_commit_mappings = collection_commit->GetMappings();
    for (auto p_c_id : collection_commit_mappings) {
        auto partition_commit = partition_commits_holder.GetResource(store, p_c_id, false);
        auto partition_id = partition_commit->GetPartitionId();
        auto partition = partitions_holder.GetResource(store, partition_id, false);
        auto partition_name = partition->GetName();
        CHECK_NOT_NULL_AND_RETURN(partition_commit);
        AddResource<PartitionCommit>(partition_commit);
        base_path = GetResPath<Partition>(store->GetRootPath(), std::make_shared<Partition>(*partition));
        partition_commit->LoadIds(base_path);

        p_pc_map_[partition_id] = partition_commit->GetID();
        CHECK_NOT_NULL_AND_RETURN(partition);
        AddResource<Partition>(partition);
        partition_names_map_[partition_name] = partition_id;
        p_max_seg_num_[partition_id] = 0;
        /* std::cout << "SS-" << ss_id << "PC_MAP=("; */
        /* for (auto id : s_c_mappings) { */
        /*     std::cout << id << ","; */
        /* } */
        /* std::cout << ")" << std::endl; */
        auto& partition_commit_mappings = partition_commit->GetMappings();
        for (auto s_c_id : partition_commit_mappings) {
            auto segment_commit = segment_commits_holder.GetResource(store, s_c_id, false);
            CHECK_NOT_NULL_AND_RETURN(segment_commit);
            auto segment_id = segment_commit->GetSegmentId();
            auto segment = segments_holder.GetResource(store, segment_id, false);
            CHECK_NOT_NULL_AND_RETURN(segment);
            auto segment_schema_id = segment_commit->GetSchemaId();
            auto segment_schema = schema_commits_holder.GetResource(store, segment_schema_id, false);
            CHECK_NOT_NULL_AND_RETURN(segment_schema);
            auto segment_partition_id = segment->GetPartitionId();
            AddResource<SchemaCommit>(segment_schema);
            AddResource<SegmentCommit>(segment_commit);
            if (segment->GetNum() > p_max_seg_num_[segment_partition_id]) {
                p_max_seg_num_[segment_partition_id] = segment->GetNum();
            }
            AddResource<Segment>(segment);

            seg_segc_map_[segment_id] = segment_commit->GetID();
            auto& segment_commit_mappings = segment_commit->GetMappings();
            for (auto s_f_id : segment_commit_mappings) {
                auto segment_file = segment_files_holder.GetResource(store, s_f_id, false);
                CHECK_NOT_NULL_AND_RETURN(segment_file);
                auto segment_file_id = segment_file->GetID();
                auto field_element_id = segment_file->GetFieldElementId();
                auto field_element = field_elements_holder.GetResource(store, field_element_id, false);
                CHECK_NOT_NULL_AND_RETURN(field_element);
                AddResource<FieldElement>(field_element);
                AddResource<SegmentFile>(segment_file);
                element_segfiles_map_[field_element_id][segment_id] = segment_file_id;
                seg_segfiles_map_[segment_id].insert(segment_file_id);
            }
        }
    }

    auto& schema_commit_mappings = schema_commit->GetMappings();
    auto& schema_commits = GetResources<SchemaCommit>();
    for (auto& kv : schema_commits) {
        if (kv.first > latest_schema_commit_id_) {
            latest_schema_commit_id_ = kv.first;
        }
        auto& schema_commit = kv.second;
        for (auto field_commit_id : schema_commit_mappings) {
            auto field_commit = field_commits_holder.GetResource(store, field_commit_id, false);
            CHECK_NOT_NULL_AND_RETURN(field_commit);
            AddResource<FieldCommit>(field_commit);

            auto field_id = field_commit->GetFieldId();
            auto field = fields_holder.GetResource(store, field_id, false);
            CHECK_NOT_NULL_AND_RETURN(field);
            auto field_name = field->GetName();
            AddResource<Field>(field);

            field_names_map_[field_name] = field_id;
            auto& field_commit_mappings = field_commit->GetMappings();
            for (auto field_element_id : field_commit_mappings) {
                auto field_element = field_elements_holder.GetResource(store, field_element_id, false);
                CHECK_NOT_NULL_AND_RETURN(field_element);
                AddResource<FieldElement>(field_element);
                auto field_element_name = field_element->GetName();
                field_element_names_map_[field_name][field_element_name] = field_element_id;
            }
        }
    }

    invalid_ = false;
    RefAll();
}

Status
Snapshot::GetSegmentRowCount(ID_TYPE segment_id, SIZE_TYPE& row_cnt) const {
    auto sc = GetSegmentCommitBySegmentId(segment_id);
    if (!sc) {
        std::stringstream emsg;
        emsg << "Snapshot::GetSegmentRowCount: Specified segment \"" << segment_id;
        emsg << "\" not found";
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }
    row_cnt = sc->GetRowCount();
    return Status::OK();
}

FieldPtr
Snapshot::GetField(const std::string& name) const {
    auto it = field_names_map_.find(name);
    if (it == field_names_map_.end()) {
        return nullptr;
    }

    return GetResource<Field>(it->second);
}

Status
Snapshot::GetFieldElement(const std::string& field_name, const std::string& field_element_name,
                          FieldElementPtr& field_element) const {
    field_element = nullptr;
    auto itf = field_element_names_map_.find(field_name);
    if (itf == field_element_names_map_.end()) {
        std::stringstream emsg;
        emsg << "Snapshot::GetFieldElement: Specified field \"" << field_name;
        emsg << "\" not found";
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }

    auto itfe = itf->second.find(field_element_name);
    if (itfe == itf->second.end()) {
        std::stringstream emsg;
        emsg << "Snapshot::GetFieldElement: Specified field element \"" << field_element_name;
        emsg << "\" not found";
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }

    field_element = GetResource<FieldElement>(itfe->second);
    return Status::OK();
}

SegmentFilePtr
Snapshot::GetSegmentFile(ID_TYPE segment_id, ID_TYPE field_element_id) const {
    auto it = element_segfiles_map_.find(field_element_id);
    if (it == element_segfiles_map_.end()) {
        return nullptr;
    }

    auto its = it->second.find(segment_id);
    if (its == it->second.end()) {
        return nullptr;
    }

    return GetResource<SegmentFile>(its->second);
}

const std::string
Snapshot::ToString() const {
    if (invalid_) {
        return "Invalid Snapshot";
    }
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
    ss << ",size=" << GetCollectionCommit()->GetSize();
    ss << ",rows=" << GetCollectionCommit()->GetRowCount();
    ss << ",lsn=" << GetCollectionCommit()->GetLsn() << ",mappings=";
    auto& cc_m = GetCollectionCommit()->GetMappings();
    ss << to_matrix_string(cc_m, row_element_size, 2);
    auto& cc_fids = GetCollectionCommit()->GetFlushIds();
    ss << ",flushids=";
    ss << to_matrix_string(cc_fids, row_element_size, 2);

    auto& schema_m = GetSchemaCommit()->GetMappings();
    ss << "\nSchemaCommit: id=" << GetSchemaCommit()->GetID() << ",mappings=";
    ss << to_matrix_string(schema_m, row_element_size, 2);
    for (auto& fc_id : schema_m) {
        auto fc = GetResource<FieldCommit>(fc_id);
        auto f = GetResource<Field>(fc->GetFieldId());
        ss << "\n  Field: id=" << f->GetID() << ",name=\"" << f->GetName() << "\"";
        ss << ", FieldCommit: id=" << fc->GetID();
        ss << ",mappings=";
        auto& fc_m = fc->GetMappings();
        ss << to_matrix_string(fc_m, row_element_size, 2);
        for (auto& fe_id : fc_m) {
            auto fe = GetResource<FieldElement>(fe_id);
            ss << "\n\tFieldElement: id=" << fe_id << ",name=" << fe->GetName() << " CID=" << fe->GetCollectionId();
            ss << ",fetype=" << static_cast<int32_t>(fe->GetFEtype()) << ",typename=" << fe->GetTypeName();
        }
    }

    for (auto& p_c_id : cc_m) {
        auto p_c = GetResource<PartitionCommit>(p_c_id);
        auto p = GetResource<Partition>(p_c->GetPartitionId());
        ss << "\nPartition: id=" << p->GetID() << ",name=\"" << p->GetName() << "\"";
        ss << ", PartitionCommit: id=" << p_c->GetID();
        ss << ",size=" << p_c->GetSize();
        ss << ",rows=" << p_c->GetRowCount() << ",mappings=";
        auto& pc_m = p_c->GetMappings();
        ss << to_matrix_string(pc_m, row_element_size, 2);
        for (auto& sc_id : pc_m) {
            auto sc = GetResource<SegmentCommit>(sc_id);
            auto se = GetResource<Segment>(sc->GetSegmentId());
            ss << "\n  Segment: id=" << se->GetID();
            ss << ", SegmentCommit: id=" << sc->GetID();
            ss << ",size=" << sc->GetSize();
            ss << ",rows=" << sc->GetRowCount() << ",mappings=";
            auto& sc_m = sc->GetMappings();
            ss << to_matrix_string(sc_m, row_element_size, 2);
            for (auto& sf_id : sc_m) {
                auto sf = GetResource<SegmentFile>(sf_id);
                ss << "\n\tSegmentFile: id=" << sf_id << ",field_element_id=" << sf->GetFieldElementId();
                ss << ",size=" << sf->GetSize();
                ss << ",fetype=" << static_cast<int32_t>(sf->GetFEtype());
            }
        }
    }
    ss << "\n----------------------------------------------------------------------------------------";

    return ss.str();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
