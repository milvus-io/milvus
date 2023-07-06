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
#include "mmap/Types.h"
#include "storage/Util.h"

namespace milvus {

inline size_t
GetDataSize(const std::vector<storage::FieldDataPtr>& datas) {
    size_t total_size{0};
    for (auto data : datas) {
        total_size += data->Size();
    }

    return total_size;
}

inline void*
FillField(DataType data_type, const storage::FieldDataPtr data, void* dst) {
    char* dest = reinterpret_cast<char*>(dst);
    if (datatype_is_variable(data_type)) {
        switch (data_type) {
            case DataType::STRING:
            case DataType::VARCHAR: {
                for (ssize_t i = 0; i < data->get_num_rows(); ++i) {
                    auto str =
                        static_cast<const std::string*>(data->RawValue(i));
                    memcpy(dest, str->data(), str->size());
                    dest += str->size();
                }
                break;
            }
            case DataType::JSON: {
                for (ssize_t i = 0; i < data->get_num_rows(); ++i) {
                    auto padded_string =
                        static_cast<const Json*>(data->RawValue(i))->data();
                    memcpy(dest, padded_string.data(), padded_string.size());
                    dest += padded_string.size();
                }
                break;
            }
            default:
                PanicInfo(fmt::format("not supported data type {}",
                                      datatype_name(data_type)));
        }
    } else {
        memcpy(dst, data->Data(), data->Size());
        dest += data->Size();
    }

    return dest;
}

inline size_t
WriteFieldData(int fd, DataType data_type, const storage::FieldDataPtr& data) {
    size_t total_written{0};
    if (datatype_is_variable(data_type)) {
        switch (data_type) {
            case DataType::VARCHAR:
            case DataType::STRING: {
                for (auto i = 0; i < data->get_num_rows(); ++i) {
                    auto str =
                        static_cast<const std::string*>(data->RawValue(i));
                    ssize_t written = write(fd, str->data(), str->size());
                    if (written < str->size()) {
                        break;
                    }
                    total_written += written;
                }
                break;
            }
            case DataType::JSON: {
                for (ssize_t i = 0; i < data->get_num_rows(); ++i) {
                    auto padded_string =
                        static_cast<const Json*>(data->RawValue(i))->data();
                    ssize_t written =
                        write(fd, padded_string.data(), padded_string.size());
                    if (written < padded_string.size()) {
                        break;
                    }
                    total_written += written;
                }
                break;
            }
            default:
                PanicInfo(fmt::format("not supported data type {}",
                                      datatype_name(data_type)));
        }
    } else {
        total_written += write(fd, data->Data(), data->Size());
    }

    return total_written;
}
}  // namespace milvus
