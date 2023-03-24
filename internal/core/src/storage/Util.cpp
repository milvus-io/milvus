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

#include "storage/Util.h"
#include "exceptions/EasyAssert.h"
#include "common/Consts.h"
#include "config/ConfigChunkManager.h"
#include "storage/FieldDataFactory.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPool.h"

#ifdef BUILD_DISK_ANN
#include "storage/DiskFileManagerImpl.h"
#endif

namespace milvus::storage {

std::map<std::string, ChunkManagerType> ChunkManagerType_Map = {{"local", ChunkManagerType::Local},
                                                                {"minio", ChunkManagerType::Minio}};
StorageType
ReadMediumType(BinlogReaderPtr reader) {
    AssertInfo(reader->Tell() == 0, "medium type must be parsed from stream header");
    int32_t magic_num;
    auto ret = reader->Read(sizeof(magic_num), &magic_num);
    AssertInfo(ret.ok(), "read binlog failed");
    if (magic_num == MAGIC_NUM) {
        return StorageType::Remote;
    }

    return StorageType::LocalDisk;
}

void
add_vector_payload(std::shared_ptr<arrow::ArrayBuilder> builder, uint8_t* values, int length) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto binary_builder = std::dynamic_pointer_cast<arrow::FixedSizeBinaryBuilder>(builder);
    auto ast = binary_builder->AppendValues(values, length);
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

// append values for numeric data
template <typename DT, typename BT>
void
add_numeric_payload(std::shared_ptr<arrow::ArrayBuilder> builder, DT* start, int length) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto numeric_builder = std::dynamic_pointer_cast<BT>(builder);
    auto ast = numeric_builder->AppendValues(start, start + length);
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

void
AddPayloadToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder, const Payload& payload) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto raw_data = const_cast<uint8_t*>(payload.raw_data);
    auto length = payload.rows;
    auto data_type = payload.data_type;

    switch (data_type) {
        case DataType::BOOL: {
            auto bool_data = reinterpret_cast<bool*>(raw_data);
            add_numeric_payload<bool, arrow::BooleanBuilder>(builder, bool_data, length);
            break;
        }
        case DataType::INT8: {
            auto int8_data = reinterpret_cast<int8_t*>(raw_data);
            add_numeric_payload<int8_t, arrow::Int8Builder>(builder, int8_data, length);
            break;
        }
        case DataType::INT16: {
            auto int16_data = reinterpret_cast<int16_t*>(raw_data);
            add_numeric_payload<int16_t, arrow::Int16Builder>(builder, int16_data, length);
            break;
        }
        case DataType::INT32: {
            auto int32_data = reinterpret_cast<int32_t*>(raw_data);
            add_numeric_payload<int32_t, arrow::Int32Builder>(builder, int32_data, length);
            break;
        }
        case DataType::INT64: {
            auto int64_data = reinterpret_cast<int64_t*>(raw_data);
            add_numeric_payload<int64_t, arrow::Int64Builder>(builder, int64_data, length);
            break;
        }
        case DataType::FLOAT: {
            auto float_data = reinterpret_cast<float*>(raw_data);
            add_numeric_payload<float, arrow::FloatBuilder>(builder, float_data, length);
            break;
        }
        case DataType::DOUBLE: {
            auto double_data = reinterpret_cast<double_t*>(raw_data);
            add_numeric_payload<double, arrow::DoubleBuilder>(builder, double_data, length);
            break;
        }
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT: {
            add_vector_payload(builder, const_cast<uint8_t*>(raw_data), length);
            break;
        }
        default: {
            PanicInfo("unsupported data type");
        }
    }
}

void
AddOneStringToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder, const char* str, int str_size) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto string_builder = std::dynamic_pointer_cast<arrow::StringBuilder>(builder);
    arrow::Status ast;
    if (str == nullptr || str_size < 0) {
        ast = string_builder->AppendNull();
    } else {
        ast = string_builder->Append(str, str_size);
    }
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::BOOL: {
            return std::make_shared<arrow::BooleanBuilder>();
        }
        case DataType::INT8: {
            return std::make_shared<arrow::Int8Builder>();
        }
        case DataType::INT16: {
            return std::make_shared<arrow::Int16Builder>();
        }
        case DataType::INT32: {
            return std::make_shared<arrow::Int32Builder>();
        }
        case DataType::INT64: {
            return std::make_shared<arrow::Int64Builder>();
        }
        case DataType::FLOAT: {
            return std::make_shared<arrow::FloatBuilder>();
        }
        case DataType::DOUBLE: {
            return std::make_shared<arrow::DoubleBuilder>();
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            return std::make_shared<arrow::StringBuilder>();
        }
        default: {
            PanicInfo("unsupported numeric data type");
        }
    }
}

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type, int dim) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(dim > 0, "invalid dim value");
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(dim * sizeof(float)));
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0 && dim > 0, "invalid dim value");
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(dim / 8));
        }
        default: {
            PanicInfo("unsupported vector data type");
        }
    }
}

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::BOOL: {
            return arrow::schema({arrow::field("val", arrow::boolean())});
        }
        case DataType::INT8: {
            return arrow::schema({arrow::field("val", arrow::int8())});
        }
        case DataType::INT16: {
            return arrow::schema({arrow::field("val", arrow::int16())});
        }
        case DataType::INT32: {
            return arrow::schema({arrow::field("val", arrow::int32())});
        }
        case DataType::INT64: {
            return arrow::schema({arrow::field("val", arrow::int64())});
        }
        case DataType::FLOAT: {
            return arrow::schema({arrow::field("val", arrow::float32())});
        }
        case DataType::DOUBLE: {
            return arrow::schema({arrow::field("val", arrow::float64())});
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            return arrow::schema({arrow::field("val", arrow::utf8())});
        }
        default: {
            PanicInfo("unsupported numeric data type");
        }
    }
}

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, int dim) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(dim > 0, "invalid dim value");
            return arrow::schema({arrow::field("val", arrow::fixed_size_binary(dim * sizeof(float)))});
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0 && dim > 0, "invalid dim value");
            return arrow::schema({arrow::field("val", arrow::fixed_size_binary(dim / 8))});
        }
        default: {
            PanicInfo("unsupported vector data type");
        }
    }
}

int
GetDimensionFromArrowArray(std::shared_ptr<arrow::Array> data, DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return array->byte_width() / sizeof(float);
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return array->byte_width() * 8;
        }
        default:
            PanicInfo("unsupported data type");
    }
}

std::string
GenLocalIndexPathPrefix(int64_t build_id, int64_t index_version) {
    return milvus::ChunkMangerConfig::GetLocalRootPath() + "/" + std::string(INDEX_ROOT_PATH) + "/" +
           std::to_string(build_id) + "/" + std::to_string(index_version) + "/";
}

std::string
GetLocalIndexPathPrefixWithBuildID(int64_t build_id) {
    return milvus::ChunkMangerConfig::GetLocalRootPath() + "/" + std::string(INDEX_ROOT_PATH) + "/" +
           std::to_string(build_id);
}

std::string
GenFieldRawDataPathPrefix(int64_t segment_id, int64_t field_id) {
    return milvus::ChunkMangerConfig::GetLocalRootPath() + "/" + std::string(RAWDATA_ROOT_PATH) + "/" +
           std::to_string(segment_id) + "/" + std::to_string(field_id) + "/";
}

std::string
GetSegmentRawDataPathPrefix(int64_t segment_id) {
    return milvus::ChunkMangerConfig::GetLocalRootPath() + "/" + std::string(RAWDATA_ROOT_PATH) + "/" +
           std::to_string(segment_id);
}

std::vector<IndexType>
DISK_LIST() {
    static std::vector<IndexType> ret{
        knowhere::IndexEnum::INDEX_DISKANN,
    };
    return ret;
}

bool
is_in_disk_list(const IndexType& index_type) {
    return is_in_list<IndexType>(index_type, DISK_LIST);
}

FileManagerImplPtr
CreateFileManager(IndexType index_type,
                  const FieldDataMeta& field_meta,
                  const IndexMeta& index_meta,
                  const StorageConfig& storage_config) {
#ifdef BUILD_DISK_ANN
    if (is_in_disk_list(index_type)) {
        return std::make_shared<DiskFileManagerImpl>(field_meta, index_meta, storage_config);
    }
#endif

    return std::make_shared<MemFileManagerImpl>(field_meta, index_meta, storage_config);
}

FileManagerImplPtr
CreateFileManager(IndexType index_type,
                  const FieldDataMeta& field_meta,
                  const IndexMeta& index_meta,
                  RemoteChunkManagerPtr rcm) {
#ifdef BUILD_DISK_ANN
    if (is_in_disk_list(index_type)) {
        return std::make_shared<DiskFileManagerImpl>(field_meta, index_meta, rcm);
    }
#endif

    return std::make_shared<MemFileManagerImpl>(field_meta, index_meta, rcm);
}

std::unique_ptr<DataCodec>
DownloadAndDecodeRemoteFile(RemoteChunkManager* remote_chunk_manager, std::string file) {
    auto fileSize = remote_chunk_manager->Size(file);
    auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[fileSize]);
    remote_chunk_manager->Read(file, buf.get(), fileSize);

    return DeserializeFileData(buf, fileSize);
}

std::pair<std::string, size_t>
EncodeAndUploadIndexSlice(RemoteChunkManager* remote_chunk_manager,
                          uint8_t* buf,
                          int64_t batch_size,
                          IndexMeta index_meta,
                          FieldDataMeta field_meta,
                          std::string object_key) {
    auto field_data = milvus::storage::FieldDataFactory::GetInstance().CreateFieldData(DataType::INT8);
    field_data->FillFieldData(buf, batch_size);
    auto indexData = std::make_shared<IndexData>(field_data);
    indexData->set_index_meta(index_meta);
    indexData->SetFieldDataMeta(field_meta);
    auto serialized_index_data = indexData->serialize_to_remote_file();
    auto serialized_index_size = serialized_index_data.size();
    remote_chunk_manager->Write(object_key, serialized_index_data.data(), serialized_index_size);
    return std::pair<std::string, size_t>(object_key, serialized_index_size);
}

std::vector<FieldDataPtr>
GetObjectData(RemoteChunkManager* remote_chunk_manager, std::vector<std::string> remote_files) {
    auto& pool = ThreadPool::GetInstance();
    std::vector<std::future<std::unique_ptr<DataCodec>>> futures;
    for (auto& file : remote_files) {
        futures.emplace_back(pool.Submit(DownloadAndDecodeRemoteFile, remote_chunk_manager, file));
    }

    std::vector<FieldDataPtr> datas;
    for (int i = 0; i < futures.size(); ++i) {
        auto res = futures[i].get();
        datas.emplace_back(res->GetFieldData());
    }

    return datas;
}

std::map<std::string, int64_t>
PutIndexData(RemoteChunkManager* remote_chunk_manager,
             std::vector<const uint8_t*>& data_slices,
             const std::vector<int64_t>& slice_sizes,
             const std::vector<std::string>& slice_names,
             FieldDataMeta& field_meta,
             IndexMeta& index_meta) {
    auto& pool = ThreadPool::GetInstance();
    std::vector<std::future<std::pair<std::string, size_t>>> futures;
    AssertInfo(data_slices.size() == slice_sizes.size(), "inconsistent size of data slices with slice sizes!");
    AssertInfo(data_slices.size() == slice_names.size(), "inconsistent size of data slices with slice names!");

    for (int64_t i = 0; i < data_slices.size(); ++i) {
        futures.push_back(pool.Submit(EncodeAndUploadIndexSlice, remote_chunk_manager,
                                      const_cast<uint8_t*>(data_slices[i]), slice_sizes[i], index_meta, field_meta,
                                      slice_names[i]));
    }

    std::map<std::string, int64_t> remote_paths_to_size;
    for (auto& future : futures) {
        auto res = future.get();
        remote_paths_to_size[res.first] = res.second;
    }

    return remote_paths_to_size;
}

int64_t
GetTotalNumRowsForFieldDatas(const std::vector<FieldDataPtr> field_datas) {
    int64_t count = 0;
    for (auto& field_data : field_datas) {
        count += field_data->get_num_rows();
    }

    return count;
}

}  // namespace milvus::storage
