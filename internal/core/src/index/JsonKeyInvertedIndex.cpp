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

namespace milvus::index {
constexpr const char* TMP_JSON_INVERTED_LOG_PREFIX =
    "/tmp/milvus/json-key-inverted-index-log/";

void
JsonKeyInvertedIndex::AddInvertedRecord(const std::vector<std::string>& paths,
                                        uint32_t row_id,
                                        uint16_t offset,
                                        uint16_t length) {
    auto key = std::string("/") + Join(paths, "/");
    LOG_DEBUG(
        "insert inverted key: {}, row_id: {}, offset: "
        "{}, length:{}",
        key,
        row_id,
        offset,
        length);
    int64_t combine_id = EncodeOffset(row_id, offset, length);
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
        AddInvertedRecord(
            path, offset, current.start, current.end - current.start);
        index++;
    } else if (current.type == JSMN_ARRAY) {
        AddInvertedRecord(
            path, offset, current.start, current.end - current.start);
        // skip array parse
        int count = current.size;
        int j = 1;
        while (count > 0) {
            if (tokens[j].size == 0) {
                count--;
            } else {
                count += tokens[j].size;
            }
            j++;
        }
        index = j;

    } else if (current.type == JSMN_STRING) {
        Assert(current.size == 0);
        AddInvertedRecord(
            path, offset, current.start, current.end - current.start);
        index++;
    }
}

void
JsonKeyInvertedIndex::AddJson(const char* json, int64_t offset) {
    jsmn_parser parser;
    jsmntok_t* tokens = (jsmntok_t*)malloc(16 * sizeof(jsmntok_t));
    if (!tokens) {
        fprintf(stderr, "Memory allocation failed\n");
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
    const storage::FileManagerContext& ctx, bool is_load) {
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
            field_name.c_str(), d_type_, path_.c_str());
    }
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
JsonKeyInvertedIndex::Load(milvus::tracer::TraceContext ctx,
                           const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load json key index");

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
    if (schema_.nullable()) {
        int64_t total = 0;
        for (const auto& data : field_datas) {
            total += data->get_null_count();
        }
        null_offset.reserve(total);
    }
    int64_t offset = 0;
    if (schema_.nullable()) {
        for (const auto& data : field_datas) {
            auto n = data->get_num_rows();
            for (int i = 0; i < n; i++) {
                if (!data->is_valid(i)) {
                    null_offset.push_back(i);
                    std::string empty = "";
                    wrapper_->add_multi_data(&empty, 0, offset++);
                    return;
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

}  // namespace milvus::index
