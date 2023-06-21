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

inline ssize_t
WriteFieldData(int fd, DataType data_type, const storage::FieldDataPtr data) {
    ssize_t total_written{0};
    if (datatype_is_variable(data_type)) {
        switch (data_type) {
            case DataType::VARCHAR:
            case DataType::STRING: {
                for (ssize_t i = 0; i < data->get_num_rows(); ++i) {
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

// CreateMap creates a memory mapping,
// if mmap enabled, this writes field data to disk and create a map to the file,
// otherwise this just alloc memory
inline void*
CreateMap(int64_t segment_id,
          const FieldMeta& field_meta,
          const FieldDataInfo& info) {
    static int mmap_flags = MAP_PRIVATE;
#ifdef MAP_POPULATE
    // macOS doesn't support MAP_POPULATE
    mmap_flags |= MAP_POPULATE;
#endif

    // simdjson requires a padding following the json data
    size_t padding = field_meta.get_data_type() == DataType::JSON
                         ? simdjson::SIMDJSON_PADDING
                         : 0;
    auto data_size = GetDataSize(info.datas);
    // Allocate memory
    if (info.mmap_dir_path.empty()) {
        auto data_type = field_meta.get_data_type();
        if (data_size == 0)
            return nullptr;

        // Use anon mapping so we are able to free these memory with munmap only
        void* map = mmap(nullptr,
                         data_size + padding,
                         PROT_READ | PROT_WRITE,
                         mmap_flags | MAP_ANON,
                         -1,
                         0);
        AssertInfo(
            map != MAP_FAILED,
            fmt::format("failed to create anon map, err: {}", strerror(errno)));
        auto dst = map;
        for (auto data : info.datas) {
            dst = FillField(data_type, data, dst);
        }
        return map;
    }

    auto filepath = std::filesystem::path(info.mmap_dir_path) /
                    std::to_string(segment_id) / std::to_string(info.field_id);
    auto dir = filepath.parent_path();
    std::filesystem::create_directories(dir);

    int fd =
        open(filepath.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    AssertInfo(fd != -1,
               fmt::format("failed to create mmap file {}", filepath.c_str()));

    auto data_type = field_meta.get_data_type();
    ssize_t total_written{0};
    for (auto data : info.datas) {
        auto written = WriteFieldData(fd, data_type, data);
        if (written != data->Size()) {
            break;
        }
        total_written += written;
    }
    AssertInfo(
        total_written == data_size ||
            total_written != -1 &&
                datatype_is_variable(field_meta.get_data_type()),
        fmt::format(
            "failed to write data file {}, written {} but total {}, err: {}",
            filepath.c_str(),
            total_written,
            data_size,
            strerror(errno)));
    int ok = fsync(fd);
    AssertInfo(ok == 0,
               fmt::format("failed to fsync mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    // Empty field
    if (total_written == 0) {
        return nullptr;
    }

    auto map =
        mmap(nullptr, total_written + padding, PROT_READ, mmap_flags, fd, 0);
    AssertInfo(map != MAP_FAILED,
               fmt::format("failed to create map for data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

#ifndef MAP_POPULATE
    // Manually access the mapping to populate it
    const size_t page_size = getpagesize();
    char* begin = (char*)map;
    char* end = begin + total_written;
    for (char* page = begin; page < end; page += page_size) {
        char value = page[0];
    }
#endif
    // unlink this data file so
    // then it will be auto removed after we don't need it again
    ok = unlink(filepath.c_str());
    AssertInfo(ok == 0,
               fmt::format("failed to unlink mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));
    ok = close(fd);
    AssertInfo(ok == 0,
               fmt::format("failed to close data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));
    return map;
}
}  // namespace milvus
