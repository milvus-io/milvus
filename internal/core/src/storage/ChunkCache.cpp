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

namespace milvus::storage {

std::shared_ptr<ColumnBase>
ChunkCache::Read(const std::string& filepath) {
    auto path = std::filesystem::path(path_prefix_) / filepath;

    ColumnTable::const_accessor ca;
    if (columns_.find(ca, path)) {
        return ca->second;
    }
    ca.release();

    auto field_data = DownloadAndDecodeRemoteFile(cm_.get(), filepath);
    auto column = Mmap(path, field_data->GetFieldData());
    auto ok =
        madvise(reinterpret_cast<void*>(const_cast<char*>(column->Data())),
                column->ByteSize(),
                read_ahead_policy_);
    AssertInfo(ok == 0,
               fmt::format("failed to madvise to the data file {}, err: {}",
                           path.c_str(),
                           strerror(errno)));

    columns_.emplace(path, column);
    return column;
}

void
ChunkCache::Remove(const std::string& filepath) {
    auto path = std::filesystem::path(path_prefix_) / filepath;
    columns_.erase(path);
}

void
ChunkCache::Prefetch(const std::string& filepath) {
    auto path = std::filesystem::path(path_prefix_) / filepath;
    ColumnTable::const_accessor ca;
    if (!columns_.find(ca, path)) {
        return;
    }
    auto column = ca->second;
    auto ok =
        madvise(reinterpret_cast<void*>(const_cast<char*>(column->Data())),
                column->ByteSize(),
                read_ahead_policy_);
    AssertInfo(ok == 0,
               fmt::format("failed to madvise to the data file {}, err: {}",
                           path.c_str(),
                           strerror(errno)));
}

std::shared_ptr<ColumnBase>
ChunkCache::Mmap(const std::filesystem::path& path,
                 const FieldDataPtr& field_data) {
    std::unique_lock lck(mutex_);

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
               fmt::format("failed to write data file {}, written "
                           "{} but total {}, err: {}",
                           path.c_str(),
                           written,
                           data_size,
                           strerror(errno)));

    std::shared_ptr<ColumnBase> column{};

    if (datatype_is_variable(data_type)) {
        AssertInfo(false, "TODO: unimplemented for variable data type");
    } else {
        column = std::make_shared<Column>(file, data_size, dim, data_type);
    }

    // unlink
    auto ok = unlink(path.c_str());
    AssertInfo(ok == 0,
               fmt::format("failed to unlink mmap data file {}, err: {}",
                           path.c_str(),
                           strerror(errno)));

    return column;
}

}  // namespace milvus::storage
