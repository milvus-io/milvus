// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>

#include "Schema.h"
#include "arrow/api.h"

namespace milvus {

milvus::FieldId
GetFieldId(std::shared_ptr<arrow::Field> field);

milvus::DataType
GetDataType(std::shared_ptr<arrow::Field> field);

std::shared_ptr<arrow::Field>
ToArrowField(const milvus::FieldMeta& field_meta);

std::shared_ptr<arrow::Schema>
ToArrowSchema(milvus::SchemaPtr milvus_schema);

milvus::SchemaPtr
FromArrowSchema(std::shared_ptr<arrow::Schema> arrow_schema);

}  // namespace milvus
