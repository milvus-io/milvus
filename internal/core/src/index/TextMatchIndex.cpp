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

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>

#include "index/TextMatchIndex.h"
#include "index/InvertedIndexUtil.h"
#include "index/Utils.h"

namespace milvus::index {
constexpr const char* TMP_TEXT_LOG_PREFIX = "/tmp/milvus/text-log/";

TextMatchIndex::TextMatchIndex(int64_t commit_interval_in_ms,
                               const char* unique_id,
                               const char* tokenizer_name,
                               const char* analyzer_params)
    : commit_interval_in_ms_(commit_interval_in_ms),
      last_commit_time_(stdclock::now()) {
    d_type_ = TantivyDataType::Text;
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        unique_id, true, "", tokenizer_name, analyzer_params);
}

TextMatchIndex::TextMatchIndex(const std::string& path,
                               const char* unique_id,
                               const char* tokenizer_name,
                               const char* analyzer_params)
    : commit_interval_in_ms_(std::numeric_limits<int64_t>::max()),
      last_commit_time_(stdclock::now()) {
    d_type_ = TantivyDataType::Text;
    boost::filesystem::path prefix = path;
    boost::filesystem::path sub_path = unique_id;
    path_ = (prefix / sub_path).string();
    boost::filesystem::create_directories(path_);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        unique_id, false, path_.c_str(), tokenizer_name, analyzer_params);
}

TextMatchIndex::TextMatchIndex(const storage::FileManagerContext& ctx,
                               const char* tokenizer_name,
                               const char* analyzer_params)
    : commit_interval_in_ms_(std::numeric_limits<int64_t>::max()),
      last_commit_time_(stdclock::now()) {
    schema_ = ctx.fieldDataMeta.field_schema;
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);

    auto prefix = disk_file_manager_->GetTextIndexIdentifier();
    path_ = std::string(TMP_TEXT_LOG_PREFIX) + prefix;

    boost::filesystem::create_directories(path_);
    d_type_ = TantivyDataType::Text;
    std::string field_name =
        std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(field_name.c_str(),
                                                     false,
                                                     path_.c_str(),
                                                     tokenizer_name,
                                                     analyzer_params);
}

TextMatchIndex::TextMatchIndex(const storage::FileManagerContext& ctx)
    : commit_interval_in_ms_(std::numeric_limits<int64_t>::max()),
      last_commit_time_(stdclock::now()) {
    schema_ = ctx.fieldDataMeta.field_schema;
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);
    d_type_ = TantivyDataType::Text;
}

IndexStatsPtr
TextMatchIndex::Upload(const Config& config) {
    finish();

    boost::filesystem::path p(path_);
    boost::filesystem::directory_iterator end_iter;

    for (boost::filesystem::directory_iterator iter(p); iter != end_iter;
         iter++) {
        if (boost::filesystem::is_directory(*iter)) {
            LOG_WARN("{} is a directory", iter->path().string());
        } else {
            LOG_INFO("trying to add text log: {}", iter->path().string());
            AssertInfo(disk_file_manager_->AddTextLog(iter->path().string()),
                       "failed to add text log: {}",
                       iter->path().string());
            LOG_INFO("text log: {} added", iter->path().string());
        }
    }

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();

    auto binary_set = Serialize(config);
    mem_file_manager_->AddFile(binary_set);
    auto remote_mem_path_to_size =
        mem_file_manager_->GetRemotePathsToFileSize();

    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(remote_paths_to_size.size() +
                        remote_mem_path_to_size.size());
    for (auto& file : remote_paths_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    for (auto& file : remote_mem_path_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    return IndexStats::New(mem_file_manager_->GetAddedTotalMemSize() +
                               disk_file_manager_->GetAddedTotalFileSize(),
                           std::move(index_files));
}

void
TextMatchIndex::Load(const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load text log index");
    auto prefix = disk_file_manager_->GetLocalTextIndexPrefix();
    auto files_value = index_files.value();
    auto it = std::find_if(
        files_value.begin(), files_value.end(), [](const std::string& file) {
            return file.substr(file.find_last_of('/') + 1) ==
                   "index_null_offset";
        });
    if (it != files_value.end()) {
        std::vector<std::string> file;
        file.push_back(*it);
        files_value.erase(it);
        auto index_datas = mem_file_manager_->LoadIndexToMemory(file);
        AssembleIndexDatas(index_datas);
        BinarySet binary_set;
        for (auto& [key, data] : index_datas) {
            auto size = data->DataSize();
            auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
            auto buf = std::shared_ptr<uint8_t[]>(
                (uint8_t*)const_cast<void*>(data->Data()), deleter);
            binary_set.Append(key, buf, size);
        }
        auto index_valid_data = binary_set.GetByName("index_null_offset");
        null_offset.resize((size_t)index_valid_data->size / sizeof(size_t));
        memcpy(null_offset.data(),
               index_valid_data->data.get(),
               (size_t)index_valid_data->size);
    }
    disk_file_manager_->CacheTextLogToDisk(files_value);
    AssertInfo(
        tantivy_index_exist(prefix.c_str()), "index not exist: {}", prefix);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(prefix.c_str());
}

void
TextMatchIndex::AddText(const std::string& text,
                        const bool valid,
                        int64_t offset) {
    if (!valid) {
        AddNull(offset);
        if (shouldTriggerCommit()) {
            Commit();
        }
        return;
    }
    wrapper_->add_data(&text, 1, offset);
    if (shouldTriggerCommit()) {
        Commit();
    }
}

void
TextMatchIndex::AddNull(int64_t offset) {
    null_offset.push_back(offset);
    // still need to add null to make offset is correct
    std::string empty = "";
    wrapper_->add_multi_data(&empty, 0, offset);
}

void
TextMatchIndex::AddTexts(size_t n,
                         const std::string* texts,
                         const bool* valids,
                         int64_t offset_begin) {
    if (valids != nullptr) {
        for (int i = 0; i < n; i++) {
            auto offset = i + offset_begin;
            if (!valids[i]) {
                null_offset.push_back(offset);
            }
        }
    }
    wrapper_->add_data(texts, n, offset_begin);
    if (shouldTriggerCommit()) {
        Commit();
    }
}

// schema_ may not be initialized so we need this `nullable` parameter
void
TextMatchIndex::BuildIndexFromFieldData(
    const std::vector<FieldDataPtr>& field_datas, bool nullable) {
    int64_t offset = 0;
    if (nullable) {
        int64_t total = 0;
        for (const auto& data : field_datas) {
            total += data->get_null_count();
        }
        null_offset.reserve(total);
        for (const auto& data : field_datas) {
            auto n = data->get_num_rows();
            for (int i = 0; i < n; i++) {
                if (!data->is_valid(i)) {
                    null_offset.push_back(i);
                }
                wrapper_->add_data(
                    static_cast<const std::string*>(data->RawValue(i)),
                    data->is_valid(i) ? 1 : 0,
                    offset++);
            }
        }
    } else {
        for (const auto& data : field_datas) {
            auto n = data->get_num_rows();
            wrapper_->add_data(
                static_cast<const std::string*>(data->Data()), n, offset);
            offset += n;
        }
    }
}

void
TextMatchIndex::Finish() {
    finish();
}

bool
TextMatchIndex::shouldTriggerCommit() {
    auto span = (std::chrono::duration<double, std::milli>(
                     stdclock::now() - last_commit_time_.load()))
                    .count();
    return span > commit_interval_in_ms_;
}

void
TextMatchIndex::Commit() {
    std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
    if (lck.try_lock()) {
        wrapper_->commit();
        last_commit_time_.store(stdclock::now());
    }
}

void
TextMatchIndex::Reload() {
    std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
    if (lck.try_lock()) {
        wrapper_->reload();
    }
}

void
TextMatchIndex::CreateReader() {
    wrapper_->create_reader();
}

void
TextMatchIndex::RegisterTokenizer(const char* tokenizer_name,
                                  const char* analyzer_params) {
    wrapper_->register_tokenizer(tokenizer_name, analyzer_params);
}

TargetBitmap
TextMatchIndex::MatchQuery(const std::string& query) {
    if (shouldTriggerCommit()) {
        Commit();
        Reload();
    }

    // The count opeartion of tantivy may be get older cnt if the index is committed with new tantivy segment.
    // So we cannot use the count operation to get the total count for bitmap.
    // Just use the maximum offset of hits to get the total count for bitmap here.
    auto hits = wrapper_->match_query(query);
    auto cnt = should_allocate_bitset_size(hits);
    TargetBitmap bitset(cnt);
    if (bitset.empty()) {
        return bitset;
    }
    apply_hits(bitset, hits, true);
    return bitset;
}
}  // namespace milvus::index
