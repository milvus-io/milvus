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

namespace milvus::engine {

using namespace snapshot;

constexpr const char* F_MAPPINGS = MappingsField::Name;
constexpr const char* F_STATE = StateField::Name;
constexpr const char* F_LSN = LsnField::Name;
constexpr const char* F_CREATED_ON = CreatedOnField::Name;
constexpr const char* F_UPDATED_ON = UpdatedOnField::Name;
constexpr const char* F_ID = IdField::Name;
constexpr const char* F_COLLECTON_ID = CollectionIdField::Name;
constexpr const char* F_SCHEMA_ID = SchemaIdField::Name;
constexpr const char* F_NUM = NumField::Name;
constexpr const char* F_FTYPE = FtypeField::Name;
constexpr const char* F_FIELD_ID = FieldIdField::Name;
constexpr const char* F_FIELD_ELEMENT_ID = FieldElementIdField::Name;
constexpr const char* F_PARTITION_ID = PartitionIdField::Name;
constexpr const char* F_SEGMENT_ID = SegmentIdField::Name;
constexpr const char* F_NAME = NameField::Name;
constexpr const char* F_PARAMS = ParamsField::Name;
constexpr const char* F_SIZE = SizeField::Name;
constexpr const char* F_ROW_COUNT = RowCountField::Name;

}