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
#include "storage/Event.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/PayloadStream.h"
#include "storage/FileManager.h"
#include "storage/BinlogReader.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"
#include "storage/Types.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/ThreadPools.h"
#include "milvus-storage/common/metadata.h"

namespace milvus::storage {

void
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
CreateArrowBuilder(DataType data_type, DataType element_type, int dim);

/// \brief Utility function to create arrow:Scalar from FieldMeta.default_value
///
/// Construct a arrow::Scalar based on input field meta
/// The data_type_ is checked to determine which `one_of` member of default value shall be used
/// Note that:
/// 1. default_value shall have value
/// 2. the type check shall be guaranteed(current by go side)
///
/// \param[in] field_meta the field meta object to construct arrow::Scalar from.
/// \return an std::shared_ptr of arrow::Scalar
std::shared_ptr<arrow::Scalar>
CreateArrowScalarFromDefaultValue(const FieldMeta& field_meta);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, bool nullable);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, int dim, bool nullable);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, int dim, DataType element_type);

int
GetDimensionFromFileMetaData(const parquet::ColumnDescriptor* schema,
                             DataType data_type);

std::string
GenIndexPathIdentifier(int64_t build_id,
                       int64_t index_version,
                       int64_t segment_id,
                       int64_t field_id);

// is_temp: true for temporary path used during index building,
// false for path to store pre-built index contents downloaded from remote storage
std::string
GenIndexPathPrefixByType(ChunkManagerPtr cm,
                         int64_t build_id,
                         int64_t index_version,
                         int64_t segment_id,
                         int64_t field_id,
                         const std::string& index_type,
                         bool is_temp);

// is_temp: true for temporary path used during index building,
// false for path to store pre-built index contents downloaded from remote storage
std::string
GenIndexPathPrefix(ChunkManagerPtr cm,
                   int64_t build_id,
                   int64_t index_version,
                   int64_t segment_id,
                   int64_t field_id,
                   bool is_temp);

// is_temp: true for temporary path used during index building,
// false for path to store pre-built index contents downloaded from remote storage
std::string
GenTextIndexPathPrefix(ChunkManagerPtr cm,
                       int64_t build_id,
                       int64_t index_version,
                       int64_t segment_id,
                       int64_t field_id,
                       bool is_temp);

std::string
GenJsonStatsPathPrefix(ChunkManagerPtr cm,
                       int64_t build_id,
                       int64_t index_version,
                       int64_t segment_id,
                       int64_t field_id,
                       bool is_temp);

std::string
GenJsonStatsPathIdentifier(int64_t build_id,
                           int64_t index_version,
                           int64_t collection_id,
                           int64_t partition_id,
                           int64_t segment_id,
                           int64_t field_id);

std::string
GenRemoteJsonStatsPathPrefix(ChunkManagerPtr cm,
                             int64_t build_id,
                             int64_t index_version,
                             int64_t collection_id,
                             int64_t partition_id,
                             int64_t segment_id,
                             int64_t field_id);

std::string
GenNgramIndexPrefix(ChunkManagerPtr cm,
                    int64_t build_id,
                    int64_t index_version,
                    int64_t segment_id,
                    int64_t field_id,
                    bool is_temp);

std::string
GenFieldRawDataPathPrefix(ChunkManagerPtr cm,
                          int64_t segment_id,
                          int64_t field_id);

std::string
GetSegmentRawDataPathPrefix(ChunkManagerPtr cm, int64_t segment_id);

std::pair<std::string, size_t>
EncodeAndUploadIndexSlice(ChunkManager* chunk_manager,
                          uint8_t* buf,
                          int64_t batch_size,
                          IndexMeta index_meta,
                          FieldDataMeta field_meta,
                          std::string object_key,
                          std::shared_ptr<CPluginContext> plugin_context);

std::vector<std::future<std::unique_ptr<DataCodec>>>
GetObjectData(
    ChunkManager* remote_chunk_manager,
    const std::vector<std::string>& remote_files,
    milvus::ThreadPoolPriority priority = milvus::ThreadPoolPriority::HIGH,
    bool is_field_data = true);

std::vector<FieldDataPtr>
GetFieldDatasFromStorageV2(std::vector<std::vector<std::string>>& remote_files,
                           int64_t field_id,
                           DataType data_type,
                           DataType element_type,
                           int64_t dim,
                           milvus_storage::ArrowFileSystemPtr fs);

std::map<std::string, int64_t>
PutIndexData(ChunkManager* remote_chunk_manager,
             const std::vector<const uint8_t*>& data_slices,
             const std::vector<int64_t>& slice_sizes,
             const std::vector<std::string>& slice_names,
             FieldDataMeta& field_meta,
             IndexMeta& index_meta,
             std::shared_ptr<CPluginContext> plugin_context);

int64_t
GetTotalNumRowsForFieldDatas(const std::vector<FieldDataPtr>& field_datas);

size_t
GetNumRowsForLoadInfo(const LoadFieldDataInfo& load_info);

void
ReleaseArrowUnused();

ChunkManagerPtr
CreateChunkManager(const StorageConfig& storage_config);

milvus_storage::ArrowFileSystemPtr
InitArrowFileSystem(milvus::storage::StorageConfig storage_config);

FieldDataPtr
CreateFieldData(const DataType& type,
                const DataType& element_type,
                bool nullable = false,
                int64_t dim = 1,
                int64_t total_num_rows = 0);

int64_t
GetByteSizeOfFieldDatas(const std::vector<FieldDataPtr>& field_datas);

std::vector<FieldDataPtr>
CollectFieldDataChannel(FieldDataChannelPtr& channel);

FieldDataPtr
MergeFieldData(std::vector<FieldDataPtr>& data_array);

int64_t
ExtractGroupIdFromPath(const std::string& path);

template <typename T, typename = void>
struct has_native_type : std::false_type {};
template <typename T>
struct has_native_type<T, std::void_t<typename T::NativeType>>
    : std::true_type {};
template <DataType T>
using DataTypeNativeOrVoid =
    typename std::conditional<has_native_type<TypeTraits<T>>::value,
                              typename TypeTraits<T>::NativeType,
                              void>::type;
template <DataType T>
using DataTypeToOffsetMap =
    std::unordered_map<DataTypeNativeOrVoid<T>, int64_t>;

std::vector<FieldDataPtr>
FetchFieldData(ChunkManager* cm, const std::vector<std::string>& batch_files);

inline void
SortByPath(std::vector<std::string>& paths) {
    std::sort(paths.begin(),
              paths.end(),
              [](const std::string& a, const std::string& b) {
                  return std::stol(a.substr(a.find_last_of("/") + 1)) <
                         std::stol(b.substr(b.find_last_of("/") + 1));
              });
}

// same as SortByPath, but with pair of path and entries_nums
inline void
SortByPath(std::vector<std::pair<std::string, int64_t>>& paths) {
    std::sort(
        paths.begin(),
        paths.end(),
        [](const std::pair<std::string, int64_t>& a,
           const std::pair<std::string, int64_t>& b) {
            return std::stol(a.first.substr(a.first.find_last_of("/") + 1)) <
                   std::stol(b.first.substr(b.first.find_last_of("/") + 1));
        });
}

template <typename T>
inline void
SortByPath(std::vector<T>& paths) {
    std::sort(paths.begin(), paths.end(), [](const T& a, const T& b) {
        return std::stol(
                   a.file_path.substr(a.file_path.find_last_of("/") + 1)) <
               std::stol(b.file_path.substr(b.file_path.find_last_of("/") + 1));
    });
}

std::vector<FieldDataPtr>
CacheRawDataAndFillMissing(const MemFileManagerImplPtr& file_manager,
                           const Config& config);

// used only for test
inline std::shared_ptr<ArrowDataWrapper>
ConvertFieldDataToArrowDataWrapper(const FieldDataPtr& field_data) {
    BaseEventData event_data;
    event_data.payload_reader = std::make_shared<PayloadReader>(field_data);
    auto event_data_bytes = event_data.Serialize();

    std::shared_ptr<uint8_t[]> file_data(new uint8_t[event_data_bytes.size()]);
    std::memcpy(
        file_data.get(), event_data_bytes.data(), event_data_bytes.size());

    storage::BinlogReaderPtr reader = std::make_shared<storage::BinlogReader>(
        file_data, event_data_bytes.size());
    event_data = storage::BaseEventData(reader,
                                        event_data_bytes.size(),
                                        field_data->get_data_type(),
                                        field_data->IsNullable(),
                                        false);
    return std::make_shared<ArrowDataWrapper>(
        event_data.payload_reader->get_reader(),
        event_data.payload_reader->get_file_reader(),
        file_data);
}

milvus_storage::FieldIDList
GetFieldIDList(FieldId column_group_id,
               const std::string& filepath,
               const std::shared_ptr<arrow::Schema>& arrow_schema,
               milvus_storage::ArrowFileSystemPtr fs);

}  // namespace milvus::storage
