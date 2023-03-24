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
#include <vector>
#include <utility>
#include <map>

#include "storage/PayloadStream.h"
#include "storage/FileManager.h"
#include "storage/BinlogReader.h"
#include "knowhere/index/IndexType.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"

namespace milvus::storage {

StorageType
ReadMediumType(BinlogReaderPtr reader);

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
GenFieldRawDataPathPrefix(int64_t segment_id, int64_t field_id);

std::string
GetSegmentRawDataPathPrefix(int64_t segment_id);

template <typename T>
inline bool
is_in_list(const T& t, std::function<std::vector<T>()> list_func) {
    auto l = list_func();
    return std::find(l.begin(), l.end(), t) != l.end();
}

bool
is_in_disk_list(const IndexType& index_type);

FileManagerImplPtr
CreateFileManager(IndexType index_type,
                  const FieldDataMeta& field_meta,
                  const IndexMeta& index_meta,
                  const StorageConfig& storage_config);

FileManagerImplPtr
CreateFileManager(IndexType index_type,
                  const FieldDataMeta& field_meta,
                  const IndexMeta& index_meta,
                  RemoteChunkManagerPtr rcm);

std::unique_ptr<DataCodec>
DownloadAndDecodeRemoteFile(RemoteChunkManager* remote_chunk_manager, std::string file);

std::pair<std::string, size_t>
EncodeAndUploadIndexSlice(RemoteChunkManager* remote_chunk_manager,
                          uint8_t* buf,
                          int64_t batch_size,
                          IndexMeta index_meta,
                          FieldDataMeta field_meta,
                          std::string object_key);

std::vector<FieldDataPtr>
GetObjectData(RemoteChunkManager* remote_chunk_manager, std::vector<std::string> remote_files);

std::map<std::string, int64_t>
PutIndexData(RemoteChunkManager* remote_chunk_manager,
             std::vector<const uint8_t*>& data_slices,
             const std::vector<int64_t>& slice_sizes,
             const std::vector<std::string>& slice_names,
             FieldDataMeta& field_meta,
             IndexMeta& index_meta);

int64_t
GetTotalNumRowsForFieldDatas(const std::vector<FieldDataPtr> field_datas);
}  // namespace milvus::storage
