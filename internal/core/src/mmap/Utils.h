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

inline void
WriteFieldData(File& file,
               DataType data_type,
               const FieldDataPtr& data,
               uint64_t& total_written,
               std::vector<uint64_t>& indices,
               std::vector<std::vector<uint64_t>>& element_indices) {
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
                // TODO(SPARSE): this is for mmap to write data to disk so that
                // the file can be mmaped into memory.
                PanicInfo(
                    ErrorCode::NotImplemented,
                    "WriteFieldData for VECTOR_SPARSE_FLOAT not implemented");
            }
            default:
                PanicInfo(DataTypeInvalid,
                          "not supported data type {}",
                          GetDataTypeName(data_type));
        }
    } else {
        // write as: data|data|data|data|data|data......
        size_t written = file.Write(data->Data(), data->Size());
        if (written < data->Size()) {
            THROW_FILE_WRITE_ERROR
        }
        for (auto i = 0; i < data->get_num_rows(); i++) {
            indices.emplace_back(total_written);
            total_written += data->Size(i);
        }
    }
}
}  // namespace milvus
