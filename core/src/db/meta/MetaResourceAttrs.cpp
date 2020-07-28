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

#include "db/meta/MetaResourceAttrs.h"

#include "db/meta/MetaNames.h"
#include "utils/Status.h"

namespace milvus::engine::meta {
/////////////////////////////////////////////////////////////////
const char* F_MAPPINGS = snapshot::MappingsField::Name;
const char* F_STATE = snapshot::StateField::Name;
const char* F_LSN = snapshot::LsnField::Name;
const char* F_CREATED_ON = snapshot::CreatedOnField::Name;
const char* F_UPDATED_ON = snapshot::UpdatedOnField::Name;
const char* F_ID = snapshot::IdField::Name;
const char* F_COLLECTON_ID = snapshot::CollectionIdField::Name;
const char* F_SCHEMA_ID = snapshot::SchemaIdField::Name;
const char* F_NUM = snapshot::NumField::Name;
const char* F_FTYPE = snapshot::FtypeField::Name;
const char* F_FIELD_ID = snapshot::FieldIdField::Name;
const char* F_FIELD_ELEMENT_ID = snapshot::FieldElementIdField::Name;
const char* F_PARTITION_ID = snapshot::PartitionIdField::Name;
const char* F_SEGMENT_ID = snapshot::SegmentIdField::Name;
const char* F_NAME = snapshot::NameField::Name;
const char* F_PARAMS = snapshot::ParamsField::Name;
const char* F_SIZE = snapshot::SizeField::Name;
const char* F_ROW_COUNT = snapshot::RowCountField::Name;

const char* TABLE_COLLECTION = snapshot::Collection::Name;
const char* TABLE_COLLECTION_COMMIT = snapshot::CollectionCommit::Name;
const char* TABLE_PARTITION = snapshot::Partition::Name;
const char* TABLE_PARTITION_COMMIT = snapshot::PartitionCommit::Name;
const char* TABLE_SEGMENT = snapshot::Segment::Name;
const char* TABLE_SEGMENT_COMMIT = snapshot::SegmentCommit::Name;
const char* TABLE_SEGMENT_FILE = snapshot::SegmentFile::Name;
const char* TABLE_SCHEMA_COMMIT = snapshot::SchemaCommit::Name;
const char* TABLE_FIELD = snapshot::Field::Name;
const char* TABLE_FIELD_COMMIT = snapshot::FieldCommit::Name;
const char* TABLE_FIELD_ELEMENT = snapshot::FieldElement::Name;

///////////////////////////////////////////////////////////////
Status
ResourceAttrMapOf(const std::string& table, std::vector<std::string>& attrs) {
    static const std::unordered_map<std::string, std::vector<std::string>> ResourceAttrMap = {
        {snapshot::Collection::Name, {F_NAME, F_PARAMS, F_ID, F_LSN, F_STATE, F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::CollectionCommit::Name,
         {F_COLLECTON_ID, F_SCHEMA_ID, F_MAPPINGS, F_ROW_COUNT, F_SIZE, F_ID, F_LSN, F_STATE, F_CREATED_ON,
          F_UPDATED_ON}},
        {snapshot::Partition::Name, {F_NAME, F_COLLECTON_ID, F_ID, F_LSN, F_STATE, F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::PartitionCommit::Name,
         {F_COLLECTON_ID, F_PARTITION_ID, F_MAPPINGS, F_ROW_COUNT, F_SIZE, F_ID, F_LSN, F_STATE, F_CREATED_ON,
          F_UPDATED_ON}},
        {snapshot::Segment::Name,
         {F_COLLECTON_ID, F_PARTITION_ID, F_NUM, F_ID, F_LSN, F_STATE, F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::SegmentCommit::Name,
         {F_SCHEMA_ID, F_PARTITION_ID, F_SEGMENT_ID, F_MAPPINGS, F_ROW_COUNT, F_SIZE, F_ID, F_LSN, F_STATE,
          F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::SegmentFile::Name,
         {F_COLLECTON_ID, F_PARTITION_ID, F_SEGMENT_ID, F_FIELD_ELEMENT_ID, F_FTYPE, F_ROW_COUNT, F_SIZE, F_ID, F_LSN,
          F_STATE, F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::SchemaCommit::Name, {F_COLLECTON_ID, F_MAPPINGS, F_ID, F_LSN, F_STATE, F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::Field::Name, {F_NAME, F_NUM, F_FTYPE, F_PARAMS, F_ID, F_LSN, F_STATE, F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::FieldCommit::Name,
         {F_COLLECTON_ID, F_FIELD_ID, F_MAPPINGS, F_ID, F_LSN, F_STATE, F_CREATED_ON, F_UPDATED_ON}},
        {snapshot::FieldElement::Name,
         {F_COLLECTON_ID, F_FIELD_ID, F_NAME, F_FTYPE, F_PARAMS, F_ID, F_LSN, F_STATE, F_CREATED_ON, F_UPDATED_ON}},
    };

    if (ResourceAttrMap.find(table) == ResourceAttrMap.end()) {
        return Status(SERVER_UNEXPECTED_ERROR, "Cannot not found table " + table + " in ResourceAttrMap");
    }

    attrs = ResourceAttrMap.at(table);

    return Status::OK();
}

}  // namespace milvus::engine::meta
