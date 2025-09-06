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

#include "index/PrimaryIndex.h"
#include "index/Utils.h"

namespace milvus::index {
template <typename T>
PrimaryIndex<T>::PrimaryIndex(const storage::FileManagerContext& ctx,
                              bool is_load) {
    mem_file_manager_ = std::make_shared<storage::MemFileManagerImpl>(ctx);
    disk_file_manager_ = std::make_shared<storage::DiskFileManagerImpl>(ctx);

    if (is_load) {
        path_ = disk_file_manager_->GetLocalIndexObjectPrefix();
    } else {
        path_ = disk_file_manager_->GetLocalTempIndexObjectPrefix();
        boost::filesystem::create_directories(path_);
    }

    primary_index_ = std::make_unique<primaryIndex::PrimaryIndex<T>>();
}

struct Segment {
    int64_t segment_id;
    std::vector<std::string> keys;

    Segment(int64_t id) : segment_id(id) {
    }

    void
    add_key(const std::string& key) {
        keys.push_back(key);
    }
};

template <typename T>
void
PrimaryIndex<T>::BuildWithPrimaryKeys(
    const std::vector<SegmentData<T>>& segments) {
    primary_index_->build(segments);
    is_built_ = true;
}

template <typename T>
IndexStatsPtr
PrimaryIndex<T>::Upload(const Config& config) {
    AssertInfo(is_built_, "PrimaryIndex has not been built yet");

    std::string index_file = path_ + "/primary_index.bin";
    primary_index_->save_to_file(index_file);

    AssertInfo(disk_file_manager_->AddFile(index_file),
               "failed to add primary index file: {}",
               index_file);

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();

    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(remote_paths_to_size.size());
    for (auto& file : remote_paths_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    return IndexStats::New(disk_file_manager_->GetAddedTotalFileSize(),
                           std::move(index_files));
}

template <typename T>
void
PrimaryIndex<T>::Load(const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, INDEX_FILES);
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load primary index");

    auto files_value = index_files.value();

    disk_file_manager_->CacheIndexToDisk(files_value,
                                         config[milvus::LOAD_PRIORITY]);

    std::string index_file = path_ + "/primary_index.bin";
    AssertInfo(boost::filesystem::exists(index_file),
               "primary index file not exist: {}",
               index_file);

    bool load_success = primary_index_->load_from_mmap(index_file);
    AssertInfo(load_success, "failed to load primary index from mmap");

    is_built_ = true;
}

template <typename T>
int64_t
PrimaryIndex<T>::query(T primary_key) {
    AssertInfo(is_built_, "PrimaryIndex has not been built yet");
    return primary_index_->lookup(primary_key);
}

template <typename T>
void
PrimaryIndex<T>::reset_segment_id(int64_t to_segment_id,
                                  int64_t from_segment_id) {
    primary_index_->reset_segment_id(to_segment_id, from_segment_id);
}

template <typename T>
std::vector<int64_t>
PrimaryIndex<T>::get_segment_list() {
    return primary_index_->get_segment_list();
}

template class PrimaryIndex<std::string>;
template class PrimaryIndex<int64_t>;

}  // namespace milvus::index
