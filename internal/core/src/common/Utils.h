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

#include <fcntl.h>
#include <fmt/core.h>
#include <google/protobuf/text_format.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "config/ConfigChunkManager.h"
#include "exceptions/EasyAssert.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "simdjson.h"

namespace milvus {
#define FIELD_DATA(data_array, type) \
    (data_array->scalars().type##_data().data())

#define VEC_FIELD_DATA(data_array, type) \
    (data_array->vectors().type##_vector().data())

inline DatasetPtr
GenDataset(const int64_t nb, const int64_t dim, const void* xb) {
    return knowhere::GenDataSet(nb, dim, xb);
}

inline const float*
GetDatasetDistance(const DatasetPtr& dataset) {
    return dataset->GetDistance();
}

inline const int64_t*
GetDatasetIDs(const DatasetPtr& dataset) {
    return dataset->GetIds();
}

inline int64_t
GetDatasetRows(const DatasetPtr& dataset) {
    return dataset->GetRows();
}

inline const void*
GetDatasetTensor(const DatasetPtr& dataset) {
    return dataset->GetTensor();
}

inline int64_t
GetDatasetDim(const DatasetPtr& dataset) {
    return dataset->GetDim();
}

inline const size_t*
GetDatasetLims(const DatasetPtr& dataset) {
    return dataset->GetLims();
}

inline bool
PrefixMatch(const std::string_view str, const std::string_view prefix) {
    auto ret = strncmp(str.data(), prefix.data(), prefix.length());
    if (ret != 0) {
        return false;
    }

    return true;
}

inline DatasetPtr
GenIdsDataset(const int64_t count, const int64_t* ids) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->SetRows(count);
    ret_ds->SetDim(1);
    ret_ds->SetIds(ids);
    ret_ds->SetIsOwner(false);
    return ret_ds;
}

inline DatasetPtr
GenResultDataset(const int64_t nq,
                 const int64_t topk,
                 const int64_t* ids,
                 const float* distance) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->SetRows(nq);
    ret_ds->SetDim(topk);
    ret_ds->SetIds(ids);
    ret_ds->SetDistance(distance);
    ret_ds->SetIsOwner(true);
    return ret_ds;
}

inline bool
PostfixMatch(const std::string_view str, const std::string_view postfix) {
    if (postfix.length() > str.length()) {
        return false;
    }

    int offset = str.length() - postfix.length();
    auto ret = strncmp(str.data() + offset, postfix.data(), postfix.length());
    if (ret != 0) {
        return false;
    }
    //
    //    int i = postfix.length() - 1;
    //    int j = str.length() - 1;
    //    for (; i >= 0; i--, j--) {
    //        if (postfix[i] != str[j]) {
    //            return false;
    //        }
    //    }
    return true;
}

inline int64_t
upper_align(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups * align;
}

inline int64_t
upper_div(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups;
}

inline bool
IsMetricType(const std::string_view str,
             const knowhere::MetricType& metric_type) {
    return !strcasecmp(str.data(), metric_type.c_str());
}

inline bool
IsFloatMetricType(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::L2) ||
           IsMetricType(metric_type, knowhere::metric::IP) ||
           IsMetricType(metric_type, knowhere::metric::COSINE);
}

inline bool
PositivelyRelated(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::IP) ||
           IsMetricType(metric_type, knowhere::metric::COSINE);
}

inline std::string
MatchKnowhereError(knowhere::Status status) {
    switch (status) {
        case knowhere::Status::invalid_args:
            return "err: invalid args";
        case knowhere::Status::invalid_param_in_json:
            return "err: invalid param in json";
        case knowhere::Status::out_of_range_in_json:
            return "err: out of range in json";
        case knowhere::Status::type_conflict_in_json:
            return "err: type conflict in json";
        case knowhere::Status::invalid_metric_type:
            return "err: invalid metric type";
        case knowhere::Status::empty_index:
            return "err: empty index";
        case knowhere::Status::not_implemented:
            return "err: not implemented";
        case knowhere::Status::index_not_trained:
            return "err: index not trained";
        case knowhere::Status::index_already_trained:
            return "err: index already trained";
        case knowhere::Status::faiss_inner_error:
            return "err: faiss inner error";
        case knowhere::Status::hnsw_inner_error:
            return "err: hnsw inner error";
        case knowhere::Status::malloc_error:
            return "err: malloc error";
        case knowhere::Status::diskann_inner_error:
            return "err: diskann inner error";
        case knowhere::Status::diskann_file_error:
            return "err: diskann file error";
        case knowhere::Status::invalid_value_in_json:
            return "err: invalid value in json";
        case knowhere::Status::arithmetic_overflow:
            return "err: arithmetic overflow";
        case knowhere::Status::raft_inner_error:
            return "err: raft inner error";
        case knowhere::Status::invalid_binary_set:
            return "err: invalid binary set";
        default:
            return "not match the error type in knowhere";
    }
}

inline size_t
GetDataSize(const FieldMeta& field, size_t row_count, const DataArray* data) {
    auto data_type = field.get_data_type();
    if (datatype_is_variable(data_type)) {
        switch (data_type) {
            case DataType::VARCHAR:
            case DataType::STRING: {
                ssize_t size{};
                for (auto& data : FIELD_DATA(data, string)) {
                    size += data.size();
                }
                return size;
            }
            case DataType::JSON: {
                ssize_t size{};
                for (auto& data : FIELD_DATA(data, json)) {
                    size += data.size();
                }
                return size;
            }
            default:
                PanicInfo(fmt::format("not supported data type {}",
                                      datatype_name(data_type)));
        }
    }

    return field.get_sizeof() * row_count;
}

inline void*
FillField(DataType data_type,
          size_t size,
          const LoadFieldDataInfo& info,
          void* dst) {
    auto data = info.field_data;
    switch (data_type) {
        case DataType::BOOL: {
            return memcpy(dst, FIELD_DATA(data, bool).data(), size);
        }
        case DataType::INT8: {
            auto src_data = FIELD_DATA(data, int);
            std::vector<int8_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return memcpy(dst, data_raw.data(), size);
        }
        case DataType::INT16: {
            auto src_data = FIELD_DATA(data, int);
            std::vector<int16_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return memcpy(dst, data_raw.data(), size);
        }
        case DataType::INT32: {
            return memcpy(dst, FIELD_DATA(data, int).data(), size);
        }
        case DataType::INT64: {
            return memcpy(dst, FIELD_DATA(data, long).data(), size);
        }
        case DataType::FLOAT: {
            return memcpy(dst, FIELD_DATA(data, float).data(), size);
        }
        case DataType::DOUBLE: {
            return memcpy(dst, FIELD_DATA(data, double).data(), size);
        }
        case DataType::VARCHAR: {
            char* dest = reinterpret_cast<char*>(dst);
            for (auto& data : FIELD_DATA(data, string)) {
                memcpy(dest, data.data(), data.size());
                dest += data.size();
            }
            return dst;
        }

        case DataType::JSON: {
            char* dest = reinterpret_cast<char*>(dst);
            for (auto& data : FIELD_DATA(data, json)) {
                memcpy(dest, data.data(), data.size());
                dest += data.size();
            }
            return dst;
        }

        case DataType::VECTOR_FLOAT:
            return memcpy(dst, VEC_FIELD_DATA(data, float).data(), size);

        case DataType::VECTOR_BINARY:
            return memcpy(dst, VEC_FIELD_DATA(data, binary), size);

        default: {
            PanicInfo("unsupported");
        }
    }
}

inline ssize_t
WriteFieldData(int fd, DataType data_type, const DataArray* data, size_t size) {
    switch (data_type) {
        case DataType::BOOL: {
            return write(fd, FIELD_DATA(data, bool).data(), size);
        }
        case DataType::INT8: {
            auto src_data = FIELD_DATA(data, int);
            std::vector<int8_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return write(fd, data_raw.data(), size);
        }
        case DataType::INT16: {
            auto src_data = FIELD_DATA(data, int);
            std::vector<int16_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return write(fd, data_raw.data(), size);
        }
        case DataType::INT32: {
            return write(fd, FIELD_DATA(data, int).data(), size);
        }
        case DataType::INT64: {
            return write(fd, FIELD_DATA(data, long).data(), size);
        }
        case DataType::FLOAT: {
            return write(fd, FIELD_DATA(data, float).data(), size);
        }
        case DataType::DOUBLE: {
            return write(fd, FIELD_DATA(data, double).data(), size);
        }
        case DataType::VARCHAR: {
            ssize_t total_written{0};
            for (auto& str : FIELD_DATA(data, string)) {
                ssize_t written = write(fd, str.data(), str.size());
                if (written < str.size()) {
                    break;
                }
                total_written += written;
            }
            return total_written;
        }
        case DataType::JSON: {
            ssize_t total_written{0};
            for (auto& json : FIELD_DATA(data, json)) {
                ssize_t written = write(fd, json.data(), json.size());
                if (written < json.size()) {
                    break;
                }
                total_written += written;
            }
            return total_written;
        }
        case DataType::VECTOR_FLOAT:
            return write(fd, VEC_FIELD_DATA(data, float).data(), size);

        case DataType::VECTOR_BINARY:
            return write(fd, VEC_FIELD_DATA(data, binary), size);

        default: {
            PanicInfo("unsupported");
        }
    }
}

// CreateMap creates a memory mapping,
// if mmap enabled, this writes field data to disk and create a map to the file,
// otherwise this just alloc memory
inline void*
CreateMap(int64_t segment_id,
          const FieldMeta& field_meta,
          const LoadFieldDataInfo& info) {
    static int mmap_flags = MAP_PRIVATE;
#ifdef MAP_POPULATE
    // macOS doesn't support MAP_POPULATE
    mmap_flags |= MAP_POPULATE;
#endif

    // simdjson requires a padding following the json data
    size_t padding = field_meta.get_data_type() == DataType::JSON
                         ? simdjson::SIMDJSON_PADDING
                         : 0;
    // Allocate memory
    if (info.mmap_dir_path == nullptr) {
        auto data_type = field_meta.get_data_type();
        auto data_size =
            GetDataSize(field_meta, info.row_count, info.field_data);
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
        FillField(data_type, data_size, info, map);
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
    size_t size = field_meta.get_sizeof() * info.row_count;
    auto written = WriteFieldData(fd, data_type, info.field_data, size);
    AssertInfo(
        written == size ||
            written != -1 && datatype_is_variable(field_meta.get_data_type()),
        fmt::format(
            "failed to write data file {}, written {} but total {}, err: {}",
            filepath.c_str(),
            written,
            size,
            strerror(errno)));
    int ok = fsync(fd);
    AssertInfo(ok == 0,
               fmt::format("failed to fsync mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    // Empty field
    if (written == 0) {
        return nullptr;
    }

    auto map = mmap(nullptr, written + padding, PROT_READ, mmap_flags, fd, 0);
    AssertInfo(map != MAP_FAILED,
               fmt::format("failed to create map for data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

#ifndef MAP_POPULATE
    // Manually access the mapping to populate it
    const size_t page_size = getpagesize();
    char* begin = (char*)map;
    char* end = begin + written;
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
