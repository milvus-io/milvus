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

#include "db/impl/ResourceAttrs.hpp"

#include "db/impl/MetaFields.h"

namespace milvus::engine {

///////////////////////////////////////////////////////////////
const std::unordered_map<std::string, std::vector<std::string>> ResourceAttrMap = {
    {Collection::Name, {F_NAME, F_PARAMS, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
    {CollectionCommit::Name,
     {F_COLLECTON_ID, F_SCHEMA_ID, F_MAPPINGS, F_ROW_COUNT, F_SIZE, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
    {Partition::Name, {F_NAME, F_COLLECTON_ID, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
    {PartitionCommit::Name,
     {F_COLLECTON_ID, F_PARTITION_ID, F_MAPPINGS, F_ROW_COUNT, F_SIZE, F_ID, F_LSN, F_STATUS, F_CREATED_ON,
      F_UPDATED_ON}},
    {Segment::Name, {F_COLLECTON_ID, F_PARTITION_ID, F_NUM, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
    {SegmentCommit::Name,
     {F_SCHEMA_ID, F_PARTITION_ID, F_SEGMENT_ID, F_MAPPINGS, F_ROW_COUNT, F_SIZE, F_ID, F_LSN, F_STATUS, F_CREATED_ON,
      F_UPDATED_ON}},
    {SegmentFile::Name,
     {F_COLLECTON_ID, F_PARTITION_ID, F_SEGMENT_ID, F_FIELD_ELEMENT_ID, F_ROW_COUNT, F_SIZE, F_ID, F_LSN, F_STATUS,
      F_CREATED_ON, F_UPDATED_ON}},
    {SchemaCommit::Name, {F_COLLECTON_ID, F_MAPPINGS, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
    {Field::Name, {F_NAME, F_NUM, F_FTYPE, F_PARAMS, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
    {FieldCommit::Name, {F_COLLECTON_ID, F_FIELD_ID, F_MAPPINGS, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
    {FieldElement::Name, {F_COLLECTON_ID, F_FIELD_ID, F_NAME, F_FTYPE, F_PARAMS, F_ID, F_LSN, F_STATUS, F_CREATED_ON, F_UPDATED_ON}},
};

}