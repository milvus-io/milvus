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
#include <future>
#include <memory>

namespace milvus::storage {

std::shared_ptr<ColumnBase>
ChunkCache::Read(const std::string& filepath) {
    auto path = CachePath(filepath);

    // use rlock to get future
    {
        std::shared_lock lck(mutex_);
        auto it = columns_.find(path);
        if (it != columns_.end()) {
            lck.unlock();
            auto result = it->second.second.get();
            AssertInfo(result, "unexpected null column, file={}", filepath);
            return result;
        }
    }

    // lock for mutation
    std::unique_lock lck(mutex_);
    // double check no-futurn
    auto it = columns_.find(path);
    if (it != columns_.end()) {
        lck.unlock();
        auto result = it->second.second.get();
        AssertInfo(result, "unexpected null column, file={}", filepath);
        return result;
    }

    std::promise<std::shared_ptr<ColumnBase>> p;
    std::shared_future<std::shared_ptr<ColumnBase>> f = p.get_future();
    columns_.emplace(path, std::make_pair(std::move(p), f));
    lck.unlock();

    // release lock and perform download and decode
    // other thread request same path shall get the future.
    auto field_data = DownloadAndDecodeRemoteFile(cm_.get(), filepath);
    auto column = Mmap(path, field_data->GetFieldData());

    // set promise value to notify the future
    lck.lock();
    it = columns_.find(path);
    if (it != columns_.end()) {
        // check pair exists then set value
        it->second.first.set_value(column);
    }
    lck.unlock();
    AssertInfo(column, "unexpected null column, file={}", filepath);
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

    auto column = it->second.second.get();
    auto ok =
        madvise(reinterpret_cast<void*>(const_cast<char*>(column->Data())),
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

    if (datatype_is_variable(data_type)) {
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
