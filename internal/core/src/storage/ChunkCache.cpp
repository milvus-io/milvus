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

#include "ChunkCache.h"
#include "common/Types.h"
#include "mmap/Utils.h"

namespace milvus::storage {

std::shared_ptr<ColumnBase>
ChunkCache::Read(const std::string& filepath) {
    auto path = CachePath(filepath);

    {
        std::shared_lock lck(mutex_);
        auto it = columns_.find(path);
        if (it != columns_.end()) {
            AssertInfo(it->second, "unexpected null column, file={}", filepath);
            return it->second;
        }
    }

    auto field_data = DownloadAndDecodeRemoteFile(cm_.get(), filepath);

    std::unique_lock lck(mutex_);
    auto it = columns_.find(path);
    if (it != columns_.end()) {
        return it->second;
    }
    auto column = Mmap(path, field_data->GetFieldData());
    AssertInfo(column, "unexpected null column, file={}", filepath);
    columns_.emplace(path, column);
    return column;
}

void
ChunkCache::Remove(const std::string& filepath) {
    auto path = CachePath(filepath);
    std::unique_lock lck(mutex_);
    columns_.erase(path);
}

void
ChunkCache::Prefetch(const std::string& filepath) {
    auto path = CachePath(filepath);

    std::shared_lock lck(mutex_);
    auto it = columns_.find(path);
    if (it == columns_.end()) {
        return;
    }

    auto column = it->second;
    auto ok = madvise(
        reinterpret_cast<void*>(const_cast<char*>(column->MmappedData())),
        column->ByteSize(),
        read_ahead_policy_);
    AssertInfo(ok == 0,
               "failed to madvise to the data file {}, err: {}",
               path,
               strerror(errno));
}

std::shared_ptr<ColumnBase>
ChunkCache::Mmap(const std::filesystem::path& path,
                 const FieldDataPtr& field_data) {
    auto dir = path.parent_path();
    std::filesystem::create_directories(dir);

    auto dim = field_data->get_dim();
    auto data_type = field_data->get_data_type();

    auto file = File::Open(path.string(), O_CREAT | O_TRUNC | O_RDWR);

    // write the field data to disk
    auto data_size = field_data->Size();
    // unused
    std::vector<std::vector<uint64_t>> element_indices{};
    auto written = WriteFieldData(file, data_type, field_data, element_indices);
    AssertInfo(written == data_size,
               "failed to write data file {}, written "
               "{} but total {}, err: {}",
               path.c_str(),
               written,
               data_size,
               strerror(errno));

    std::shared_ptr<ColumnBase> column{};

    if (IsSparseFloatVectorDataType(data_type)) {
        std::vector<uint64_t> indices{};
        uint64_t offset = 0;
        for (auto i = 0; i < field_data->get_num_rows(); ++i) {
            indices.push_back(offset);
            offset += field_data->Size(i);
        }
        auto sparse_column = std::make_shared<SparseFloatColumn>(
            file, data_size, dim, data_type);
        sparse_column->Seal(std::move(indices));
        column = std::move(sparse_column);
    } else if (IsVariableDataType(data_type)) {
        AssertInfo(
            false, "TODO: unimplemented for variable data type: {}", data_type);
    } else {
        column = std::make_shared<Column>(file, data_size, dim, data_type);
    }

    // unlink
    auto ok = unlink(path.c_str());
    AssertInfo(ok == 0,
               "failed to unlink mmap data file {}, err: {}",
               path.c_str(),
               strerror(errno));

    return column;
}

std::string
ChunkCache::CachePath(const std::string& filepath) {
    auto path = std::filesystem::path(filepath);
    auto prefix = std::filesystem::path(path_prefix_);

    // Cache path shall not use absolute filepath direct, it shall always under path_prefix_
    if (path.is_absolute()) {
        return (prefix /
                filepath.substr(path.root_directory().string().length(),
                                filepath.length()))
            .string();
    }

    return (prefix / filepath).string();
}

}  // namespace milvus::storage
