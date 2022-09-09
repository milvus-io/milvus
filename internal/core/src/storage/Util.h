// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <string>

#include "storage/PayloadStream.h"
#include "storage/FileManager.h"
#include "knowhere/index/IndexType.h"

namespace milvus::storage {

StorageType
ReadMediumType(PayloadInputStream* input_stream);

void
AddPayloadToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder, const Payload& payload);

void
AddOneStringToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder, const char* str, int str_size);

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type);

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type, int dim);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, int dim);

int64_t
GetPayloadSize(const Payload* payload);

const uint8_t*
GetRawValuesFromArrowArray(std::shared_ptr<arrow::Array> array, DataType data_type);

int
GetDimensionFromArrowArray(std::shared_ptr<arrow::Array> array, DataType data_type);

std::string
GetLocalIndexPathPrefixWithBuildID(int64_t build_id);

std::string
GenLocalIndexPathPrefix(int64_t build_id, int64_t index_version);

std::string
GenRawDataPathPrefix(int64_t segment_id, int64_t field_id);

std::string
GetLocalRawDataPathPrefixWithBuildID(int64_t segment_id);

FileManagerImplPtr
CreateFileManager(knowhere::IndexType index_type, const FieldDataMeta& field_meta, const IndexMeta& index_meta);

}  // namespace milvus::storage
