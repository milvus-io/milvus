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

#include "index/TextMatchIndex.h"
#include "index/InvertedIndexUtil.h"
#include "index/Utils.h"

namespace milvus::index {
constexpr const char* TMP_TEXT_LOG_PREFIX = "/tmp/milvus/text-log/";

TextMatchIndex::TextMatchIndex(int64_t commit_interval_in_ms)
    : commit_interval_in_ms_(commit_interval_in_ms),
      last_commit_time_(stdclock::now()) {
    d_type_ = TantivyDataType::Text;
    std::string field_name = "tmp_text_index";
    wrapper_ =
        std::make_shared<TantivyIndexWrapper>(field_name.c_str(), true, "");
}

TextMatchIndex::TextMatchIndex(const std::string& path)
    : commit_interval_in_ms_(std::numeric_limits<int64_t>::max()),
      last_commit_time_(stdclock::now()) {
    path_ = path;
    d_type_ = TantivyDataType::Text;
    std::string field_name = "tmp_text_index";
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        field_name.c_str(), false, path_.c_str());
}

TextMatchIndex::TextMatchIndex(const storage::FileManagerContext& ctx)
    : commit_interval_in_ms_(std::numeric_limits<int64_t>::max()),
      last_commit_time_(stdclock::now()) {
    schema_ = ctx.fieldDataMeta.field_schema;
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);

    auto prefix = disk_file_manager_->GetIndexIdentifier();
    path_ = std::string(TMP_TEXT_LOG_PREFIX) + prefix;

    boost::filesystem::create_directories(path_);
    d_type_ = TantivyDataType::Text;
    if (tantivy_index_exist(path_.c_str())) {
        LOG_INFO(
            "text index {} already exists, which should happen in loading "
            "progress",
            path_);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(path_.c_str());
    } else {
        std::string field_name =
            std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field_name.c_str(), false, path_.c_str());
    }
}

BinarySet
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

    BinarySet ret;

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

void
TextMatchIndex::Load(const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load text log index");
    auto prefix = disk_file_manager_->GetLocalTextIndexPrefix();
    disk_file_manager_->CacheTextLogToDisk(index_files.value());
    wrapper_ = std::make_shared<TantivyIndexWrapper>(prefix.c_str());
}

void
TextMatchIndex::AddText(const std::string& text, int64_t offset) {
    AddTexts(1, &text, offset);
}

void
TextMatchIndex::AddTexts(size_t n,
                         const std::string* texts,
                         int64_t offset_begin) {
    wrapper_->add_data(texts, n, offset_begin);
    if (shouldTriggerCommit()) {
        Commit();
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
    wrapper_->commit();
    last_commit_time_.store(stdclock::now());
}

void
TextMatchIndex::Reload() {
    wrapper_->reload();
}

void
TextMatchIndex::CreateReader() {
    wrapper_->create_reader();
}

TargetBitmap
TextMatchIndex::MatchQuery(const std::string& query) {
    if (shouldTriggerCommit()) {
        Commit();
        Reload();
    }

    auto cnt = wrapper_->count();
    TargetBitmap bitset(cnt);
    if (bitset.empty()) {
        return bitset;
    }
    auto hits = wrapper_->match_query(query);
    apply_hits(bitset, hits, true);
    return bitset;
}
}  // namespace milvus::index
