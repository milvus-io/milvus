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

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <tuple>

#include "common/ScopedTimer.h"
#include "monitor/Monitor.h"
#include "index/json_stats/bson_inverted.h"
#include "storage/LocalChunkManagerSingleton.h"
namespace milvus::index {

BsonInvertedIndex::BsonInvertedIndex(const std::string& path,
                                     int64_t field_id,
                                     bool is_load,
                                     const storage::FileManagerContext& ctx,
                                     int64_t tantivy_index_version)
    : is_load_(is_load),
      field_id_(field_id),
      tantivy_index_version_(tantivy_index_version) {
    disk_file_manager_ =
        std::make_shared<milvus::storage::DiskFileManagerImpl>(ctx);
    if (is_load_) {
        auto prefix = disk_file_manager_->GetLocalJsonStatsSharedIndexPrefix();
        path_ = prefix;
        LOG_INFO("bson inverted index load path:{}", path_);
    } else {
        path_ = path;
        LOG_INFO("bson inverted index build path:{}", path_);
    }
}

BsonInvertedIndex::~BsonInvertedIndex() {
    if (wrapper_) {
        wrapper_->free();
    }
    if (!is_load_) {
        auto local_chunk_manager =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager();
        auto prefix = path_;
        LOG_INFO("bson inverted index remove path:{}", path_);
        local_chunk_manager->RemoveDir(prefix);
    }
}

void
BsonInvertedIndex::AddRecord(const std::string& key,
                             uint32_t row_id,
                             uint32_t offset) {
    if (inverted_index_map_.find(key) == inverted_index_map_.end()) {
        inverted_index_map_[key] = {EncodeInvertedIndexValue(row_id, offset)};
    } else {
        inverted_index_map_[key].push_back(
            EncodeInvertedIndexValue(row_id, offset));
    }
}

void
BsonInvertedIndex::BuildIndex() {
    if (wrapper_ == nullptr) {
        if (tantivy_index_exist(path_.c_str())) {
            ThrowInfo(IndexBuildError,
                      "build inverted index temp dir:{} not empty",
                      path_);
        }
        auto field_name = std::to_string(field_id_) + "_" + "shared";
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field_name.c_str(), path_.c_str(), tantivy_index_version_);
        LOG_INFO("build bson inverted index for field id:{} with dir:{}",
                 field_id_,
                 path_);
    }
    std::vector<const char*> keys;
    std::vector<const int64_t*> json_offsets;
    std::vector<uintptr_t> json_offsets_lens;
    for (const auto& [key, offsets] : inverted_index_map_) {
        keys.push_back(key.c_str());
        json_offsets.push_back(offsets.data());
        json_offsets_lens.push_back(offsets.size());
    }
    wrapper_->add_json_key_stats_data_by_batch(keys.data(),
                                               json_offsets.data(),
                                               json_offsets_lens.data(),
                                               keys.size());
}

void
BsonInvertedIndex::LoadIndex(const std::vector<std::string>& index_files,
                             milvus::proto::common::LoadPriority priority) {
    if (is_load_) {
        // convert shared_key_index/... to remote_prefix/shared_key_index/...
        std::vector<std::string> remote_files;
        for (auto& file : index_files) {
            auto remote_prefix =
                disk_file_manager_->GetRemoteJsonStatsLogPrefix();
            remote_files.emplace_back(remote_prefix + "/" + file);
        }
        // cache shared_key_index/... to disk
        disk_file_manager_->CacheJsonStatsSharedIndexToDisk(remote_files,
                                                            priority);
        AssertInfo(tantivy_index_exist(path_.c_str()),
                   "index dir not exist: {}",
                   path_);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            path_.c_str(), false, milvus::index::SetBitsetUnused);
        LOG_INFO("load json shared key index done for field id:{} with dir:{}",
                 field_id_,
                 path_);
    }
}

IndexStatsPtr
BsonInvertedIndex::UploadIndex() {
    AssertInfo(!is_load_, "upload index is not supported for load index");
    AssertInfo(wrapper_ != nullptr,
               "bson inverted index wrapper is not initialized");
    wrapper_->finish();

    boost::filesystem::path p(path_);
    boost::filesystem::directory_iterator end_iter;

    for (boost::filesystem::directory_iterator iter(p); iter != end_iter;
         iter++) {
        if (boost::filesystem::is_directory(*iter)) {
            LOG_WARN("{} is a directory", iter->path().string());
        } else {
            LOG_INFO("trying to add bson inverted index file: {}",
                     iter->path().string());
            AssertInfo(disk_file_manager_->AddJsonSharedIndexLog(
                           iter->path().string()),
                       "failed to add bson inverted index file: {}",
                       iter->path().string());
            LOG_INFO("bson inverted index file: {} added",
                     iter->path().string());
        }
    }

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();

    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(remote_paths_to_size.size());
    for (auto& file : remote_paths_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    return IndexStats::New(disk_file_manager_->GetAddedTotalFileSize(),
                           std::move(index_files));
}

void
BsonInvertedIndex::TermQuery(
    const std::string& path,
    const std::function<void(const uint32_t* row_id_array,
                             const uint32_t* offset_array,
                             const int64_t array_len)>& visitor) {
    AssertInfo(wrapper_ != nullptr,
               "bson inverted index wrapper is not initialized");
    auto start = std::chrono::steady_clock::now();
    auto array = wrapper_->term_query_i64(path);
    auto end = std::chrono::steady_clock::now();
    LOG_TRACE("term query time:{}",
              std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                  .count());
    auto array_len = array.array_.len;
    LOG_DEBUG("json stats shared column filter size:{} with path:{}",
              array_len,
              path);

    std::vector<uint32_t> row_id_array(array_len);
    std::vector<uint32_t> offset_array(array_len);

    for (int64_t i = 0; i < array_len; i++) {
        auto value = array.array_.array[i];
        auto [row_id, offset] = DecodeInvertedIndexValue(value);
        row_id_array[i] = row_id;
        offset_array[i] = offset;
    }

    visitor(row_id_array.data(), offset_array.data(), array_len);
}

void
BsonInvertedIndex::TermQueryEach(
    const std::string& path,
    const std::function<void(uint32_t row_id, uint32_t offset)>& each) {
    AssertInfo(wrapper_ != nullptr,
               "bson inverted index wrapper is not initialized");
    auto start = std::chrono::steady_clock::now();
    auto array = wrapper_->term_query_i64(path);
    auto end = std::chrono::steady_clock::now();
    LOG_TRACE("term query time:{}",
              std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                  .count());
    auto array_len = array.array_.len;
    LOG_TRACE("json stats shared column filter size:{} with path:{}",
              array_len,
              path);

    for (int64_t i = 0; i < array_len; i++) {
        auto value = array.array_.array[i];
        auto [row_id, offset] = DecodeInvertedIndexValue(value);
        each(row_id, offset);
    }
}

}  // namespace milvus::index