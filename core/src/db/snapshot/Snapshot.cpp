#include "Snapshot.h"
#include "Store.h"
#include "ResourceHolders.h"


void
Snapshot::DumpSegments(const std::string& tag) {
    std::cout << typeid(*this).name() << " DumpSegments Start [" << tag <<  "]:" << segments_.size() << std::endl;
    for (auto& kv : segments_) {
        /* std::cout << "\t" << kv.first << " RefCnt " << kv.second->RefCnt() << std::endl; */
        std::cout << "\t" << kv.second->ToString() << std::endl;
    }
    std::cout << typeid(*this).name() << " DumpSegments   End [" << tag <<  "]" << std::endl;
}

void
Snapshot::DumpPartitionCommits(const std::string& tag) {
    std::cout << typeid(*this).name() << " DumpPartitionCommits Start [" << tag <<  "]:" << partition_commits_.size() << std::endl;
    for (auto& kv : partition_commits_) {
        std::cout << "\t" << kv.second->ToString() << std::endl;
    }
    std::cout << typeid(*this).name() << " DumpPartitionCommits   End [" << tag <<  "]" << std::endl;
}

void
Snapshot::DumpSegmentCommits(const std::string& tag) {
    std::cout << typeid(*this).name() << " DumpSegmentCommits Start [" << tag <<  "]:" << segment_commits_.size() << std::endl;
    for (auto& kv : segment_commits_) {
        std::cout << "\t" << kv.second->ToString() << std::endl;
    }
    std::cout << typeid(*this).name() << " DumpSegmentCommits   End [" << tag <<  "]" << std::endl;
}

void Snapshot::RefAll() {
    collection_commit_->Ref();
    for (auto& schema : schema_commits_) {
        schema.second->Ref();
    }
    for (auto& element : field_elements_) {
        element.second->Ref();
    }
    for (auto& field : fields_) {
        field.second->Ref();
    }
    for (auto& field_commit : field_commits_) {
        field_commit.second->Ref();
    }
    collection_->Ref();
    for (auto& partition : partitions_) {
        partition.second->Ref();
    }
    for (auto& partition_commit : partition_commits_) {
        partition_commit.second->Ref();
    }
    for (auto& segment : segments_) {
        segment.second->Ref();
    }
    for (auto& segment_commit : segment_commits_) {
        segment_commit.second->Ref();
    }
    for (auto& segment_file : segment_files_) {
        segment_file.second->Ref();
    }
}

void Snapshot::UnRefAll() {
    /* std::cout << this << " UnRefAll " << collection_commit_->GetID() << " RefCnt=" << RefCnt() << std::endl; */
    collection_commit_->UnRef();
    for (auto& schema : schema_commits_) {
        schema.second->UnRef();
    }
    for (auto& element : field_elements_) {
        element.second->UnRef();
    }
    for (auto& field : fields_) {
        field.second->UnRef();
    }
    for (auto& field_commit : field_commits_) {
        field_commit.second->UnRef();
    }
    collection_->UnRef();
    for (auto& partition : partitions_) {
        partition.second->UnRef();
    }
    for (auto& partition_commit : partition_commits_) {
        partition_commit.second->UnRef();
    }
    for (auto& segment : segments_) {
        segment.second->UnRef();
    }
    for (auto& segment_commit : segment_commits_) {
        segment_commit.second->UnRef();
    }
    for (auto& segment_file : segment_files_) {
        segment_file.second->UnRef();
    }
}

Snapshot::Snapshot(ID_TYPE id) {
    collection_commit_ = CollectionCommitsHolder::GetInstance().GetResource(id, false);
    assert(collection_commit_);
    auto& schema_holder =  SchemaCommitsHolder::GetInstance();
    auto current_schema = schema_holder.GetResource(collection_commit_->GetSchemaId(), false);
    schema_commits_[current_schema->GetID()] = current_schema;
    current_schema_id_ = current_schema->GetID();
    auto& field_commits_holder = FieldCommitsHolder::GetInstance();
    auto& fields_holder = FieldsHolder::GetInstance();
    auto& field_elements_holder = FieldElementsHolder::GetInstance();

    collection_ = CollectionsHolder::GetInstance().GetResource(collection_commit_->GetCollectionId(), false);
    auto& mappings =  collection_commit_->GetMappings();
    auto& partition_commits_holder = PartitionCommitsHolder::GetInstance();
    auto& partitions_holder = PartitionsHolder::GetInstance();
    auto& segments_holder = SegmentsHolder::GetInstance();
    auto& segment_commits_holder = SegmentCommitsHolder::GetInstance();
    auto& segment_files_holder = SegmentFilesHolder::GetInstance();

    for (auto& id : mappings) {
        auto partition_commit = partition_commits_holder.GetResource(id, false);
        auto partition = partitions_holder.GetResource(partition_commit->GetPartitionId(), false);
        partition_commits_[partition_commit->GetID()] = partition_commit;
        p_pc_map_[partition_commit->GetPartitionId()] = partition_commit->GetID();
        partitions_[partition_commit->GetPartitionId()] = partition;
        p_max_seg_num_[partition->GetID()] = 0;
        auto& s_c_mappings = partition_commit->GetMappings();
        for (auto& s_c_id : s_c_mappings) {
            auto segment_commit = segment_commits_holder.GetResource(s_c_id, false);
            auto segment = segments_holder.GetResource(segment_commit->GetSegmentId(), false);
            auto schema = schema_holder.GetResource(segment_commit->GetSchemaId(), false);
            schema_commits_[schema->GetID()] = schema;
            segment_commits_[segment_commit->GetID()] = segment_commit;
            if (segment->GetNum() > p_max_seg_num_[segment->GetPartitionId()]) {
                p_max_seg_num_[segment->GetPartitionId()] = segment->GetNum();
            }
            segments_[segment->GetID()] = segment;
            seg_segc_map_[segment->GetID()] = segment_commit->GetID();
            auto& s_f_mappings = segment_commit->GetMappings();
            for (auto& s_f_id : s_f_mappings) {
                auto segment_file = segment_files_holder.GetResource(s_f_id, false);
                auto field_element = field_elements_holder.GetResource(segment_file->GetFieldElementId(), false);
                field_elements_[field_element->GetID()] = field_element;
                segment_files_[s_f_id] = segment_file;
                auto entry = element_segfiles_map_.find(segment_file->GetFieldElementId());
                if (entry == element_segfiles_map_.end()) {
                    element_segfiles_map_[segment_file->GetFieldElementId()] = {
                        {segment_file->GetSegmentId(), segment_file->GetID()}
                    };
                } else {
                    entry->second[segment_file->GetSegmentId()] = segment_file->GetID();
                }
            }
        }
    }

    for (auto& kv : schema_commits_) {
        if (kv.first > latest_schema_commit_id_) latest_schema_commit_id_ = kv.first;
        auto& schema_commit = kv.second;
        auto& s_c_m =  current_schema->GetMappings();
        for (auto field_commit_id : s_c_m) {
            auto field_commit = field_commits_holder.GetResource(field_commit_id, false);
            field_commits_[field_commit_id] = field_commit;
            auto field = fields_holder.GetResource(field_commit->GetFieldId(), false);
            fields_[field->GetID()] = field;
            field_names_map_[field->GetName()] = field->GetID();
            auto& f_c_m = field_commit->GetMappings();
            for (auto field_element_id : f_c_m) {
                auto field_element = field_elements_holder.GetResource(field_element_id, false);
                field_elements_[field_element_id] = field_element;
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
};
