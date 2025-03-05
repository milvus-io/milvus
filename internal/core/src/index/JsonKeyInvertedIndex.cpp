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

#include "index/JsonKeyInvertedIndex.h"
#include "index/InvertedIndexUtil.h"
#include "index/Utils.h"
#include "storage/MmapManager.h"
namespace milvus::index {
constexpr const char* TMP_JSON_INVERTED_LOG_PREFIX =
    "/tmp/milvus/json-key-inverted-index-log/";

void
JsonKeyInvertedIndex::AddInvertedRecord(const std::vector<std::string>& paths,
                                        uint8_t valid,
                                        uint8_t type,
                                        uint32_t row_id,
                                        uint16_t offset,
                                        uint16_t length,
                                        uint32_t value) {
    std::string key = "";
    if (!paths.empty()) {
        key = std::string("/") + Join(paths, "/");
    }
    LOG_DEBUG(
        "insert inverted key: {}, valid: {}, type: {}, row_id: {}, offset: "
        "{}, length:{}, value:{}",
        key,
        valid,
        type,
        row_id,
        offset,
        length,
        value);
    int64_t combine_id = 0;
    if (valid) {
        combine_id = EncodeValue(valid, type, row_id, value);
    } else {
        combine_id = EncodeOffset(valid, type, row_id, offset, length);
    }

    wrapper_->add_multi_data<std::string>(&key, 1, combine_id);
}

void
JsonKeyInvertedIndex::TravelJson(const char* json,
                                 jsmntok* tokens,
                                 int& index,
                                 std::vector<std::string>& path,
                                 int32_t offset) {
    jsmntok current = tokens[0];
    Assert(current.type != JSMN_UNDEFINED);
    if (current.type == JSMN_OBJECT) {
        if (!path.empty()) {
            AddInvertedRecord(path,
                              0,
                              0,
                              offset,
                              current.start,
                              current.end - current.start,
                              0);
        }
        int j = 1;
        for (int i = 0; i < current.size; i++) {
            Assert(tokens[j].type == JSMN_STRING && tokens[j].size != 0);
            std::string key(json + tokens[j].start,
                            tokens[j].end - tokens[j].start);
            path.push_back(key);
            j++;
            int consumed = 0;
            TravelJson(json, tokens + j, consumed, path, offset);
            path.pop_back();
            j += consumed;
        }
        index = j;
    } else if (current.type == JSMN_PRIMITIVE) {
        std::string value(json + current.start, current.end - current.start);
        auto type = getType(value);

        if (type == JSONType::INT32) {
            AddInvertedRecord(path,
                              1,
                              static_cast<uint8_t>(JSONType::INT32),
                              offset,
                              current.start,
                              current.end - current.start,
                              stoi(value));
        } else if (type == JSONType::INT64) {
            AddInvertedRecord(path,
                              0,
                              static_cast<uint8_t>(JSONType::INT64),
                              offset,
                              current.start,
                              current.end - current.start,
                              0);
        } else if (type == JSONType::FLOAT) {
            auto fvalue = stof(value);
            uint32_t valueBits = *reinterpret_cast<uint32_t*>(&fvalue);
            AddInvertedRecord(path,
                              1,
                              static_cast<uint8_t>(JSONType::FLOAT),
                              offset,
                              current.start,
                              current.end - current.start,
                              valueBits);
        } else if (type == JSONType::DOUBLE) {
            AddInvertedRecord(path,
                              0,
                              static_cast<uint8_t>(JSONType::DOUBLE),
                              offset,
                              current.start,
                              current.end - current.start,
                              0);
        } else if (type == JSONType::BOOL) {
            AddInvertedRecord(path,
                              1,
                              static_cast<uint8_t>(JSONType::BOOL),
                              offset,
                              current.start,
                              current.end - current.start,
                              value == "true" ? 1 : 0);
        }

        index++;
    } else if (current.type == JSMN_ARRAY) {
        AddInvertedRecord(
            path, 0, 0, offset, current.start, current.end - current.start, 0);
        // skip array parse
        int count = current.size;
        int j = 1;
        while (count > 0) {
            count--;
            if (tokens[j].size != 0) {
                count += tokens[j].size;
            }
            j++;
        }
        index = j;
    } else if (current.type == JSMN_STRING) {
        Assert(current.size == 0);
        std::string value(json + current.start, current.end - current.start);
        if (has_escape_sequence(value)) {
            AddInvertedRecord(path,
                              0,
                              static_cast<uint8_t>(JSONType::STRING_ESCAPE),
                              offset,
                              current.start - 1,
                              current.end - current.start + 2,
                              0);
        } else {
            AddInvertedRecord(path,
                              0,
                              static_cast<uint8_t>(JSONType::STRING),
                              offset,
                              current.start,
                              current.end - current.start,
                              0);
        }
        index++;
    }
}

void
JsonKeyInvertedIndex::AddJson(const char* json, int64_t offset) {
    jsmn_parser parser;
    jsmntok_t* tokens = (jsmntok_t*)malloc(16 * sizeof(jsmntok_t));
    if (!tokens) {
        PanicInfo(ErrorCode::UnexpectedError, "alloc jsmn token failed");
        return;
    }
    int num_tokens = 0;
    int token_capacity = 16;

    jsmn_init(&parser);

    while (1) {
        int r = jsmn_parse(&parser, json, strlen(json), tokens, token_capacity);
        if (r < 0) {
            if (r == JSMN_ERROR_NOMEM) {
                // Reallocate tokens array if not enough space
                token_capacity *= 2;
                tokens = (jsmntok_t*)realloc(
                    tokens, token_capacity * sizeof(jsmntok_t));
                if (!tokens) {
                    PanicInfo(ErrorCode::UnexpectedError, "realloc failed");
                }
                continue;
            } else {
                free(tokens);
                PanicInfo(ErrorCode::UnexpectedError,
                          "Failed to parse Json: {}, error: {}",
                          json,
                          int(r));
            }
        }
        num_tokens = r;
        break;
    }

    int index = 0;
    std::vector<std::string> paths;
    TravelJson(json, tokens, index, paths, offset);
    free(tokens);
}

JsonKeyInvertedIndex::JsonKeyInvertedIndex(
    const storage::FileManagerContext& ctx,
    bool is_load,
    int64_t json_stats_tantivy_memory_budget)
    : commit_interval_in_ms_(std::numeric_limits<int64_t>::max()),
      last_commit_time_(stdclock::now()) {
    LOG_INFO("JsonKeyInvertedIndex json_stats_tantivy_memory_budget:{}",
             json_stats_tantivy_memory_budget);
    schema_ = ctx.fieldDataMeta.field_schema;
    field_id_ = ctx.fieldDataMeta.field_id;
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);

    if (is_load) {
        auto prefix = disk_file_manager_->GetLocalJsonKeyIndexPrefix();
        path_ = prefix;
    } else {
        auto prefix = disk_file_manager_->GetJsonKeyIndexIdentifier();
        path_ = std::string(TMP_JSON_INVERTED_LOG_PREFIX) + prefix;
        boost::filesystem::create_directories(path_);
        std::string field_name =
            std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
        d_type_ = TantivyDataType::Keyword;
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field_name.c_str(),
            d_type_,
            path_.c_str(),
            false,
            false,
            1,
            json_stats_tantivy_memory_budget);
    }
}

JsonKeyInvertedIndex::JsonKeyInvertedIndex(int64_t commit_interval_in_ms,
                                           const char* unique_id)
    : commit_interval_in_ms_(commit_interval_in_ms),
      last_commit_time_(stdclock::now()) {
    d_type_ = TantivyDataType::Keyword;
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        unique_id, d_type_, "", false, true);
}

JsonKeyInvertedIndex::JsonKeyInvertedIndex(int64_t commit_interval_in_ms,
                                           const char* unique_id,
                                           const std::string& path)
    : commit_interval_in_ms_(commit_interval_in_ms),
      last_commit_time_(stdclock::now()) {
    boost::filesystem::path prefix = path;
    boost::filesystem::path sub_path = unique_id;
    path_ = (prefix / sub_path).string();
    boost::filesystem::create_directories(path_);
    d_type_ = TantivyDataType::Keyword;
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        unique_id, d_type_, path_.c_str());
}

IndexStatsPtr
JsonKeyInvertedIndex::Upload(const Config& config) {
    finish();
    boost::filesystem::path p(path_);
    boost::filesystem::directory_iterator end_iter;

    for (boost::filesystem::directory_iterator iter(p); iter != end_iter;
         iter++) {
        if (boost::filesystem::is_directory(*iter)) {
            LOG_WARN("{} is a directory", iter->path().string());
        } else {
            LOG_INFO("trying to add json key inverted index log: {}",
                     iter->path().string());
            AssertInfo(
                disk_file_manager_->AddJsonKeyIndexLog(iter->path().string()),
                "failed to add json key inverted index log: {}",
                iter->path().string());
            LOG_INFO("json key inverted index log: {} added",
                     iter->path().string());
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
        index_files.emplace_back(disk_file_manager_->GetFileName(file.first),
                                 file.second);
    }
    for (auto& file : remote_mem_path_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    return IndexStats::New(mem_file_manager_->GetAddedTotalMemSize() +
                               disk_file_manager_->GetAddedTotalFileSize(),
                           std::move(index_files));
}

void
JsonKeyInvertedIndex::Load(milvus::tracer::TraceContext ctx,
                           const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load json key index");
    for (auto& index_file : index_files.value()) {
        boost::filesystem::path p(index_file);
        if (!p.has_parent_path()) {
            auto remote_prefix =
                disk_file_manager_->GetRemoteJsonKeyLogPrefix();
            index_file = remote_prefix + "/" + index_file;
        }
    }
    disk_file_manager_->CacheJsonKeyIndexToDisk(index_files.value());
    AssertInfo(
        tantivy_index_exist(path_.c_str()), "index not exist: {}", path_);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(path_.c_str());
    LOG_INFO("load json key index done for field id:{} with dir:{}",
             field_id_,
             path_);
}

void
JsonKeyInvertedIndex::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    AssertInfo(schema_.data_type() == proto::schema::DataType::JSON,
               "schema data type is {}",
               schema_.data_type());
    BuildWithFieldData(field_datas, schema_.nullable());
}

void
JsonKeyInvertedIndex::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas, bool nullable) {
    int64_t offset = 0;
    if (nullable) {
        for (const auto& data : field_datas) {
            auto n = data->get_num_rows();
            for (int i = 0; i < n; i++) {
                if (!data->is_valid(i)) {
                    continue;
                }
                AddJson(static_cast<const milvus::Json*>(data->RawValue(i))
                            ->data()
                            .data(),
                        offset++);
            }
        }
    } else {
        for (const auto& data : field_datas) {
            auto n = data->get_num_rows();
            for (int i = 0; i < n; i++) {
                AddJson(static_cast<const milvus::Json*>(data->RawValue(i))
                            ->data()
                            .data(),
                        offset++);
            }
        }
    }
    LOG_INFO("build json key index done for field id:{}", field_id_);
}

void
JsonKeyInvertedIndex::AddJSONDatas(size_t n,
                                   const std::string* jsonDatas,
                                   const bool* valids,
                                   int64_t offset_begin) {
    for (int i = 0; i < n; i++) {
        auto offset = i + offset_begin;
        if (valids != nullptr && !valids[i]) {
            continue;
        }
        AddJson(jsonDatas[i].c_str(), offset);
    }
    is_data_uncommitted_ = true;
    LOG_INFO("build json key index done for AddJSONDatas");
    if (shouldTriggerCommit()) {
        Commit();
    }
}

void
JsonKeyInvertedIndex::Finish() {
    finish();
}

bool
JsonKeyInvertedIndex::shouldTriggerCommit() {
    auto span = (std::chrono::duration<double, std::milli>(
                     stdclock::now() - last_commit_time_.load()))
                    .count();
    return span > commit_interval_in_ms_;
}

void
JsonKeyInvertedIndex::Commit() {
    std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
    if (lck.try_lock()) {
        is_data_uncommitted_ = false;
        wrapper_->commit();
        last_commit_time_.store(stdclock::now());
    }
}

void
JsonKeyInvertedIndex::Reload() {
    std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
    if (lck.try_lock()) {
        wrapper_->reload();
    }
}

void
JsonKeyInvertedIndex::CreateReader() {
    wrapper_->create_reader();
}

}  // namespace milvus::index
