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

#include "db/snapshot/Resources.h"
#include "db/snapshot/Store.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <experimental/filesystem>

namespace milvus::engine::snapshot {

Status
FlushableMappingsField::LoadIds(const std::string& base_path, const std::string& prefix) {
    if (loaded_) {
        return Status::OK();
    }
    loaded_ = true;
    if (ids_.size() == 0) {
        return Status(SS_ERROR, "LoadIds ids_ should not be empty");
    }

    if (!std::experimental::filesystem::exists(base_path)) {
        return Status(SS_NOT_FOUND_ERROR, "FlushIds base_path: " + base_path + " not found");
    }

    auto path = base_path + "/" + prefix + std::to_string(*(ids_.begin())) + ".map";
    if (!std::experimental::filesystem::exists(path)) {
        return Status(SS_NOT_FOUND_ERROR, "FlushIds path: " + path + " not found");
    }

    /* unsigned short* buf = nullptr; */
    try {
        std::ifstream ifs(path, std::ifstream::binary);
        ifs.seekg(0, ifs.end);
        auto size = ifs.tellg();
        std::cout << "READ " << path << " SIZE=" << size << std::endl;
        ifs.seekg(0, ifs.beg);
        if (size > 0) {
            /* buf = new unsigned short[size]; */
            /* ifs.read((char*)buf, size); */
            /* for (auto pos = 0; pos < size; pos += sizeof(ID_TYPE)) { */
            /*     ID_TYPE id = (ID_TYPE)(buf[pos]); */
            /*     std::cout << "READ " << id << std::endl; */
            /*     mappings_.insert(id); */
            /* } */
            std::string str((std::istreambuf_iterator<char>(ifs)),
                 std::istreambuf_iterator<char>());
            std::cout << "READ " << str << std::endl;

            std::stringstream ss(str);

            for (ID_TYPE i; ss >> i;) {
                std::string::size_type sz;
                /* mappings_.insert(std::stoll(i, &sz, 0)); */
                mappings_.insert(i);
                if (ss.peek() == ',')
                    ss.ignore();
            }
        }

        ifs.close();
    } catch (...) {
        /* if (buf) { */
        /*     delete[] buf; */
        /* } */
        Status(SS_ERROR, "Cannot LoadIds from " + path);
    }

    /* if (buf) { */
    /*     delete[] buf; */
    /* } */
    return Status::OK();
}

Status
FlushableMappingsField::FlushIds(const std::string& base_path, const std::string& prefix) {
    if (ids_.size() == 0) {
        return Status(SS_ERROR, "FlushIds ids_ should not be empty");
    }
    if (!std::experimental::filesystem::exists(base_path)) {
        std::experimental::filesystem::create_directories(base_path);
    }
    auto path = base_path + "/" + prefix + std::to_string(*(ids_.begin())) + ".map";
    /* unsigned short* buf = nullptr; */
    auto size = sizeof(ID_TYPE) * mappings_.size();
    /* if (size != 0) { */
    /*     buf = new unsigned short[size]; */
    /* } */
    std::cout << "FLUSH " << path << " SIZE=" << size << std::endl;

    try {
        std::ofstream ofs(path, std::ofstream::binary);
        bool first = true;
        for (auto& id : mappings_) {
            if (!first) {
                ofs << ",";
            }
            ofs << id;
            first = false;
        }
        ofs.close();
        /* std::ofstream ofs(path, std::ofstream::binary); */
        /* auto pos = 0; */
        /* for (auto& id : mappings_) { */
        /*     buf[pos * sizeof(id)] = id; */
        /*     std::cout << "FLUSH " << (ID_TYPE)buf[pos*sizeof(id)] << std::endl; */
        /*     ++pos; */
        /* } */
        /* if (buf) { */
        /*     ofs.write((char const*)buf, size); */
        /* } */
        /* ofs.close(); */
    } catch (...) {
        /* if (buf) { */
        /*     delete[] buf; */
        /* } */
        Status(SS_ERROR, "Cannot FlushIds to " + path);
    }

    /* if (buf) { */
    /*     delete[] buf; */
    /* } */
    return Status::OK();
}

Collection::Collection(const std::string& name, const json& params, ID_TYPE id, LSN_TYPE lsn, State state,
                       TS_TYPE created_on, TS_TYPE updated_on)
    : NameField(name),
      ParamsField(params),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

CollectionCommit::CollectionCommit(ID_TYPE collection_id, ID_TYPE schema_id, const MappingT& mappings,
                                   SIZE_TYPE row_cnt, SIZE_TYPE size, ID_TYPE id, LSN_TYPE lsn, State state,
                                   TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      SchemaIdField(schema_id),
      FlushableMappingsField(mappings),
      RowCountField(row_cnt),
      SizeField(size),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

Partition::Partition(const std::string& name, ID_TYPE collection_id, ID_TYPE id, LSN_TYPE lsn, State state,
                     TS_TYPE created_on, TS_TYPE updated_on)
    : NameField(name),
      CollectionIdField(collection_id),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

PartitionCommit::PartitionCommit(ID_TYPE collection_id, ID_TYPE partition_id, const MappingT& mappings,
                                 SIZE_TYPE row_cnt, SIZE_TYPE size, ID_TYPE id, LSN_TYPE lsn, State state,
                                 TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      PartitionIdField(partition_id),
      MappingsField(mappings),
      RowCountField(row_cnt),
      SizeField(size),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

std::string
PartitionCommit::ToString() const {
    std::stringstream ss;
    ss << "PartitionCommit [" << this << "]: ";
    ss << "id=" << GetID() << ", ";
    ss << "partition_id=" << GetPartitionId() << ", mappings=(";
    for (auto sc_id : GetMappings()) {
        ss << sc_id << ", ";
    }
    ss << ") state=" << GetState() << " ";
    return ss.str();
}

Segment::Segment(ID_TYPE collection_id, ID_TYPE partition_id, ID_TYPE num, ID_TYPE id, LSN_TYPE lsn, State state,
                 TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      PartitionIdField(partition_id),
      NumField(num),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

std::string
Segment::ToString() const {
    std::stringstream ss;
    ss << "Segment [" << this << "]: ";
    ss << "id=" << GetID() << ", ";
    ss << "partition_id=" << GetPartitionId() << ", ";
    ss << "collection_id=" << GetCollectionId() << ", ";
    ss << "num=" << GetNum() << ", ";
    ss << "state=" << GetState() << ", ";
    return ss.str();
}

SegmentCommit::SegmentCommit(ID_TYPE schema_id, ID_TYPE partition_id, ID_TYPE segment_id, const MappingT& mappings,
                             SIZE_TYPE row_cnt, SIZE_TYPE size, ID_TYPE id, LSN_TYPE lsn, State state,
                             TS_TYPE created_on, TS_TYPE updated_on)
    : SchemaIdField(schema_id),
      PartitionIdField(partition_id),
      SegmentIdField(segment_id),
      MappingsField(mappings),
      RowCountField(row_cnt),
      SizeField(size),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

std::string
SegmentCommit::ToString() const {
    std::stringstream ss;
    ss << "SegmentCommit [" << this << "]: ";
    ss << "id=" << GetID() << ", ";
    ss << "partition_id=" << GetPartitionId() << ", ";
    ss << "segment_id=" << GetSegmentId() << ", ";
    ss << "state=" << GetState() << ", ";
    return ss.str();
}

SegmentFile::SegmentFile(ID_TYPE collection_id, ID_TYPE partition_id, ID_TYPE segment_id, ID_TYPE field_element_id,
                         FETYPE_TYPE fetype, SIZE_TYPE row_cnt, SIZE_TYPE size, ID_TYPE id, LSN_TYPE lsn, State state,
                         TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      PartitionIdField(partition_id),
      SegmentIdField(segment_id),
      FieldElementIdField(field_element_id),
      FEtypeField(fetype),
      RowCountField(row_cnt),
      SizeField(size),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

SchemaCommit::SchemaCommit(ID_TYPE collection_id, const MappingT& mappings, ID_TYPE id, LSN_TYPE lsn, State state,
                           TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      MappingsField(mappings),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

Field::Field(const std::string& name, NUM_TYPE num, FTYPE_TYPE ftype, const json& params, ID_TYPE id, LSN_TYPE lsn,
             State state, TS_TYPE created_on, TS_TYPE updated_on)
    : NameField(name),
      NumField(num),
      FtypeField(ftype),
      ParamsField(params),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

FieldCommit::FieldCommit(ID_TYPE collection_id, ID_TYPE field_id, const MappingT& mappings, ID_TYPE id, LSN_TYPE lsn,
                         State state, TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      FieldIdField(field_id),
      MappingsField(mappings),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

FieldElement::FieldElement(ID_TYPE collection_id, ID_TYPE field_id, const std::string& name, FETYPE_TYPE fetype,
                           const std::string& type_name, const json& params, ID_TYPE id, LSN_TYPE lsn, State state,
                           TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      FieldIdField(field_id),
      NameField(name),
      FEtypeField(fetype),
      TypeNameField(type_name),
      ParamsField(params),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

}  // namespace milvus::engine::snapshot
