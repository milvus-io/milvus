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
#include <future>

#include "common/FieldData.h"
#include "common/LoadInfo.h"
#include "knowhere/comp/index_param.h"
#include "parquet/schema.h"
#include "storage/PayloadStream.h"
#include "storage/FileManager.h"
#include "storage/BinlogReader.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"
#include "storage/Types.h"

namespace milvus::storage {

StorageType
ReadMediumType(BinlogReaderPtr reader);

void
AddPayloadToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder,
                         const Payload& payload);

void
AddOneStringToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder,
                           const char* str,
                           int str_size);
void
AddOneBinaryToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder,
                           const uint8_t* data,
                           int length);

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type);

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type, int dim);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, bool nullable);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, int dim, bool nullable);

int
GetDimensionFromFileMetaData(const parquet::ColumnDescriptor* schema,
                             DataType data_type);

int
GetDimensionFromArrowArray(std::shared_ptr<arrow::Array> array,
                           DataType data_type);

std::string
GetIndexPathPrefixWithBuildID(ChunkManagerPtr cm, int64_t build_id);

std::string
GenIndexPathIdentifier(int64_t build_id, int64_t index_version);

std::string
GenTextIndexPathIdentifier(int64_t build_id,
                           int64_t index_version,
                           int64_t segment_id,
                           int64_t field_id);

std::string
GenIndexPathPrefix(ChunkManagerPtr cm, int64_t build_id, int64_t index_version);

std::string
GenTextIndexPathPrefix(ChunkManagerPtr cm,
                       int64_t build_id,
                       int64_t index_version,
                       int64_t segment_id,
                       int64_t field_id);

std::string
GenFieldRawDataPathPrefix(ChunkManagerPtr cm,
                          int64_t segment_id,
                          int64_t field_id);

std::string
GetSegmentRawDataPathPrefix(ChunkManagerPtr cm, int64_t segment_id);

std::unique_ptr<DataCodec>
DownloadAndDecodeRemoteFile(ChunkManager* chunk_manager,
                            const std::string& file);

std::pair<std::string, size_t>
EncodeAndUploadIndexSlice(ChunkManager* chunk_manager,
                          uint8_t* buf,
                          int64_t batch_size,
                          IndexMeta index_meta,
                          FieldDataMeta field_meta,
                          std::string object_key);

std::pair<std::string, size_t>
EncodeAndUploadFieldSlice(ChunkManager* chunk_manager,
                          void* buf,
                          int64_t element_count,
                          FieldDataMeta field_data_meta,
                          const FieldMeta& field_meta,
                          std::string object_key);

std::vector<std::future<std::unique_ptr<DataCodec>>>
GetObjectData(ChunkManager* remote_chunk_manager,
              const std::vector<std::string>& remote_files);

std::map<std::string, int64_t>
PutIndexData(ChunkManager* remote_chunk_manager,
             const std::vector<const uint8_t*>& data_slices,
             const std::vector<int64_t>& slice_sizes,
             const std::vector<std::string>& slice_names,
             FieldDataMeta& field_meta,
             IndexMeta& index_meta);

int64_t
GetTotalNumRowsForFieldDatas(const std::vector<FieldDataPtr>& field_datas);

size_t
GetNumRowsForLoadInfo(const LoadFieldDataInfo& load_info);

void
ReleaseArrowUnused();

// size_t
// getCurrentRSS();

ChunkManagerPtr
CreateChunkManager(const StorageConfig& storage_config);

FieldDataPtr
CreateFieldData(const DataType& type,
                bool nullable = false,
                int64_t dim = 1,
                int64_t total_num_rows = 0);

int64_t
GetByteSizeOfFieldDatas(const std::vector<FieldDataPtr>& field_datas);

std::vector<FieldDataPtr>
CollectFieldDataChannel(FieldDataChannelPtr& channel);

FieldDataPtr
MergeFieldData(std::vector<FieldDataPtr>& data_array);

}  // namespace milvus::storage
