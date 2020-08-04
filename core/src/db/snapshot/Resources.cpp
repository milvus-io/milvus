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
#include <iostream>
#include <sstream>
#include "db/snapshot/Store.h"

namespace milvus::engine::snapshot {

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
      MappingsField(mappings),
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
    ss << "num=" << (NUM_TYPE)GetNum() << ", ";
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
                         FTYPE_TYPE ftype, SIZE_TYPE row_cnt, SIZE_TYPE size, ID_TYPE id, LSN_TYPE lsn, State state,
                         TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      PartitionIdField(partition_id),
      SegmentIdField(segment_id),
      FieldElementIdField(field_element_id),
      FtypeField(ftype),
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

FieldElement::FieldElement(ID_TYPE collection_id, ID_TYPE field_id, const std::string& name, FTYPE_TYPE ftype,
                           const std::string& type_name, const json& params, ID_TYPE id, LSN_TYPE lsn, State state,
                           TS_TYPE created_on, TS_TYPE updated_on)
    : CollectionIdField(collection_id),
      FieldIdField(field_id),
      NameField(name),
      FtypeField(ftype),
      TypeNameField(type_name),
      ParamsField(params),
      IdField(id),
      LsnField(lsn),
      StateField(state),
      CreatedOnField(created_on),
      UpdatedOnField(updated_on) {
}

}  // namespace milvus::engine::snapshot
