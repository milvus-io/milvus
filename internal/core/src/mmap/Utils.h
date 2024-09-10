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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldMeta.h"
#include "common/Types.h"
#include "mmap/Types.h"
#include "storage/Util.h"
#include "common/File.h"

namespace milvus {

#define THROW_FILE_WRITE_ERROR                                           \
    PanicInfo(ErrorCode::FileWriteFailed,                                \
              fmt::format("write data to file {} failed, error code {}", \
                          file.Path(),                                   \
                          strerror(errno)));

/*
* If string field's value all empty, need a string padding to avoid
* mmap failing because size_ is zero which causing invalid arguement
* array has the same problem
* TODO: remove it when support NULL value
*/
constexpr size_t FILE_STRING_PADDING = 1;
constexpr size_t FILE_ARRAY_PADDING = 1;

inline size_t
PaddingSize(const DataType& type) {
    switch (type) {
        case DataType::JSON:
            // simdjson requires a padding following the json data
            return simdjson::SIMDJSON_PADDING;
        case DataType::VARCHAR:
        case DataType::STRING:
            return FILE_STRING_PADDING;
            break;
        case DataType::ARRAY:
            return FILE_ARRAY_PADDING;
        default:
            break;
    }
    return 0;
}

inline void
WriteFieldPadding(File& file, DataType data_type, uint64_t& total_written) {
    // write padding 0 in file content directly
    // see also https://github.com/milvus-io/milvus/issues/34442
    auto padding_size = PaddingSize(data_type);
    if (padding_size > 0) {
        std::vector<char> padding(padding_size, 0);
        ssize_t written = file.Write(padding.data(), padding_size);
        if (written < padding_size) {
            THROW_FILE_WRITE_ERROR
        }
        total_written += written;
    }
}

inline void
WriteFieldData(File& file,
               DataType data_type,
               const FieldDataPtr& data,
               uint64_t& total_written,
               std::vector<uint64_t>& indices,
               std::vector<std::vector<uint64_t>>& element_indices,
               FixedVector<bool>& valid_data) {
    if (IsVariableDataType(data_type)) {
        switch (data_type) {
            case DataType::VARCHAR:
            case DataType::STRING: {
                // write as: |size|data|size|data......
                for (auto i = 0; i < data->get_num_rows(); ++i) {
                    indices.push_back(total_written);
                    auto str =
                        static_cast<const std::string*>(data->RawValue(i));
                    ssize_t written_data_size =
                        file.WriteInt<uint32_t>(uint32_t(str->size()));
                    if (written_data_size != sizeof(uint32_t)) {
                        THROW_FILE_WRITE_ERROR
                    }
                    total_written += written_data_size;
                    auto written_data = file.Write(str->data(), str->size());
                    if (written_data < str->size()) {
                        THROW_FILE_WRITE_ERROR
                    }
                    total_written += written_data;
                }
                break;
            }
            case DataType::JSON: {
                // write as: |size|data|size|data......
                for (ssize_t i = 0; i < data->get_num_rows(); ++i) {
                    indices.push_back(total_written);
                    auto padded_string =
                        static_cast<const Json*>(data->RawValue(i))->data();
                    ssize_t written_data_size =
                        file.WriteInt<uint32_t>(uint32_t(padded_string.size()));
                    if (written_data_size != sizeof(uint32_t)) {
                        THROW_FILE_WRITE_ERROR
                    }
                    total_written += written_data_size;
                    ssize_t written_data =
                        file.Write(padded_string.data(), padded_string.size());
                    if (written_data < padded_string.size()) {
                        THROW_FILE_WRITE_ERROR
                    }
                    total_written += written_data;
                }
                break;
            }
            case DataType::ARRAY: {
                // write as: |data|data|data|data|data......
                for (size_t i = 0; i < data->get_num_rows(); ++i) {
                    indices.push_back(total_written);
                    auto array = static_cast<const Array*>(data->RawValue(i));
                    ssize_t written =
                        file.Write(array->data(), array->byte_size());
                    if (written < array->byte_size()) {
                        THROW_FILE_WRITE_ERROR
                    }
                    element_indices.emplace_back(array->get_offsets());
                    total_written += written;
                }
                break;
            }
            case DataType::VECTOR_SPARSE_FLOAT: {
                for (size_t i = 0; i < data->get_num_rows(); ++i) {
                    indices.push_back(total_written);
                    auto vec =
                        static_cast<const knowhere::sparse::SparseRow<float>*>(
                            data->RawValue(i));
                    ssize_t written =
                        file.Write(vec->data(), vec->data_byte_size());
                    if (written < vec->data_byte_size()) {
                        break;
                    }
                    total_written += written;
                }
                break;
            }
            default:
                PanicInfo(DataTypeInvalid,
                          "not supported data type {}",
                          GetDataTypeName(data_type));
        }
    } else {
        // write as: data|data|data|data|data|data......
        size_t written = file.Write(data->Data(), data->DataSize());
        if (written < data->DataSize()) {
            THROW_FILE_WRITE_ERROR
        }
        for (auto i = 0; i < data->get_num_rows(); i++) {
            indices.emplace_back(total_written);
            total_written += data->DataSize(i);
        }
    }
    if (data->IsNullable()) {
        size_t required_rows = valid_data.size() + data->get_num_rows();
        if (required_rows > valid_data.size()) {
            valid_data.reserve(required_rows * 2);
        }
        for (size_t i = 0; i < data->get_num_rows(); i++) {
            valid_data.push_back(data->is_valid(i));
        }
    }
}
}  // namespace milvus
