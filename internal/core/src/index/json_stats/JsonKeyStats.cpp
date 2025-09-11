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

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <nlohmann/json.hpp>
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/bson_builder.h"
#include "index/InvertedIndexUtil.h"
#include "index/Utils.h"
#include "storage/MmapManager.h"
#include "storage/Util.h"
#include "common/bson_view.h"
#include "mmap/ChunkedColumnGroup.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/common/constants.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev1translator/DefaultValueChunkTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"

namespace milvus::index {

JsonKeyStats::JsonKeyStats(const storage::FileManagerContext& ctx,
                           bool is_load,
                           int64_t json_stats_max_shredding_columns,
                           double json_stats_shredding_ratio_threshold,
                           int64_t json_stats_write_batch_size,
                           uint32_t tantivy_index_version)
    : ScalarIndex<std::string>(JSON_KEY_STATS_INDEX_TYPE) {
    schema_ = ctx.fieldDataMeta.field_schema;
    field_id_ = ctx.fieldDataMeta.field_id;
    segment_id_ = ctx.fieldDataMeta.segment_id;
    rcm_ = ctx.chunkManagerPtr;
    mem_file_manager_ =
        std::make_shared<milvus::storage::MemFileManagerImpl>(ctx);
    disk_file_manager_ =
        std::make_shared<milvus::storage::DiskFileManagerImpl>(ctx);
    write_batch_size_ = json_stats_write_batch_size;
    max_shredding_columns_ = json_stats_max_shredding_columns;
    shredding_ratio_threshold_ = json_stats_shredding_ratio_threshold;
    LOG_INFO(
        "init json key stats with write_batch_size : {}, "
        "max_shredding_columns: {}, shredding_ratio_threshold: {} for segment "
        "{}",
        write_batch_size_,
        max_shredding_columns_,
        shredding_ratio_threshold_,
        segment_id_);

    if (is_load) {
        auto prefix = disk_file_manager_->GetLocalJsonStatsPrefix();
        path_ = prefix;
        bson_inverted_index_ = std::make_shared<BsonInvertedIndex>(
            path_, field_id_, true, ctx, tantivy_index_version);
    } else {
        auto prefix = disk_file_manager_->GetLocalTempJsonStatsPrefix();
        path_ = prefix;
        LOG_INFO("init json key stats with path: {} for segment {}",
                 path_,
                 segment_id_);

        // TODO: add params to modify batch size and max file size
        auto conf = milvus_storage::StorageConfig();
        conf.part_size = DEFAULT_PART_UPLOAD_SIZE;
        auto trueFs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                          .GetArrowFileSystem();
        if (!trueFs) {
            ThrowInfo(ErrorCode::UnexpectedError, "Failed to get filesystem");
        }
        parquet_writer_ = std::make_shared<JsonStatsParquetWriter>(
            trueFs, conf, DEFAULT_BUFFER_SIZE, write_batch_size_);

        // build bson index for shared key
        auto shared_key_index_path = GetSharedKeyIndexDir();
        LOG_INFO("init local shared bson index with path: {} for segment {}",
                 shared_key_index_path,
                 segment_id_);
        boost::filesystem::create_directories(shared_key_index_path);
        bson_inverted_index_ =
            std::make_shared<BsonInvertedIndex>(shared_key_index_path,
                                                field_id_,
                                                false,
                                                ctx,
                                                tantivy_index_version);
    }
}

JsonKeyStats::~JsonKeyStats() {
    boost::filesystem::remove_all(path_);
}

void
JsonKeyStats::AddKeyStatsInfo(const std::vector<std::string>& paths,
                              JSONType type,
                              uint8_t* value,
                              std::map<JsonKey, KeyStatsInfo>& infos) {
    std::string key = "";
    if (!paths.empty()) {
        key = JsonPointer(paths);
    }
    JsonKey json_key;
    json_key.key_ = key;
    json_key.type_ = type;

    if (infos.find(json_key) == infos.end()) {
        infos[json_key] = KeyStatsInfo();
    }
    infos[json_key].hit_row_num_++;
    // TODO: update min and max value
}

void
JsonKeyStats::TraverseJsonForStats(const char* json,
                                   jsmntok* tokens,
                                   int& index,
                                   std::vector<std::string>& path,
                                   std::map<JsonKey, KeyStatsInfo>& infos) {
    jsmntok current = tokens[0];
    Assert(current.type != JSMN_UNDEFINED);
    if (current.type == JSMN_OBJECT) {
        if (!path.empty()) {
            AddKeyStatsInfo(path, JSONType::OBJECT, nullptr, infos);
        }
        int j = 1;
        for (int i = 0; i < current.size; i++) {
            Assert(tokens[j].type == JSMN_STRING && tokens[j].size != 0);
            std::string key(json + tokens[j].start,
                            tokens[j].end - tokens[j].start);
            path.push_back(key);
            j++;
            int consumed = 0;
            TraverseJsonForStats(json, tokens + j, consumed, path, infos);
            path.pop_back();
            j += consumed;
        }
        index = j;
    } else if (current.type == JSMN_PRIMITIVE) {
        std::string value(json + current.start, current.end - current.start);
        auto type = getType(value);

        if (type == JSONType::INT64) {
            AddKeyStatsInfo(path, JSONType::INT64, nullptr, infos);
        } else if (type == JSONType::FLOAT || type == JSONType::DOUBLE) {
            AddKeyStatsInfo(path, JSONType::DOUBLE, nullptr, infos);
        } else if (type == JSONType::BOOL) {
            AddKeyStatsInfo(path, JSONType::BOOL, nullptr, infos);
        } else if (type == JSONType::NONE) {
            AddKeyStatsInfo(path, JSONType::NONE, nullptr, infos);
        } else {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "unsupported json type: {} for build json stats",
                      type);
        }
        index++;
    } else if (current.type == JSMN_ARRAY) {
        AddKeyStatsInfo(path, JSONType::ARRAY, nullptr, infos);
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
        AddKeyStatsInfo(path, JSONType::STRING, nullptr, infos);
        index++;
    }
}

void
JsonKeyStats::CollectSingleJsonStatsInfo(
    const char* json_str, std::map<JsonKey, KeyStatsInfo>& infos) {
    jsmn_parser parser;
    jsmn_init(&parser);

    int num_tokens = 0;
    int token_capacity = 16;
    std::vector<jsmntok_t> tokens(token_capacity);

    while (1) {
        int r = jsmn_parse(
            &parser, json_str, strlen(json_str), tokens.data(), token_capacity);
        if (r < 0) {
            if (r == JSMN_ERROR_NOMEM) {
                // Reallocate tokens array if not enough space
                token_capacity *= 2;
                tokens.resize(token_capacity);
                continue;
            } else {
                ThrowInfo(ErrorCode::UnexpectedError,
                          "Failed to parse Json: {}, error: {}",
                          json_str,
                          int(r));
            }
        }
        num_tokens = r;
        break;
    }

    int index = 0;
    std::vector<std::string> paths;
    TraverseJsonForStats(json_str, tokens.data(), index, paths, infos);
}

std::map<JsonKey, KeyStatsInfo>
JsonKeyStats::CollectKeyInfo(const std::vector<FieldDataPtr>& field_datas,
                             bool nullable) {
    std::map<JsonKey, KeyStatsInfo> infos;
    int64_t num_rows = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int i = 0; i < n; i++) {
            if (nullable && !data->is_valid(i)) {
                continue;
            }
            auto json_str = static_cast<const milvus::Json*>(data->RawValue(i))
                                ->data()
                                .data();
            CollectSingleJsonStatsInfo(json_str, infos);
        }
        num_rows += n;
    }
    num_rows_ = num_rows;
    return infos;
}

std::map<JsonKey, JsonKeyLayoutType>
JsonKeyStats::ClassifyJsonKeyLayoutType(
    const std::map<JsonKey, KeyStatsInfo>& infos) {
    std::map<JsonKey, JsonKeyLayoutType> types;
    std::unordered_map<std::string,
                       std::vector<std::pair<JsonKey, KeyStatsInfo>>>
        grouped;
    std::unordered_map<std::string, int32_t> group_hit_rows;
    for (const auto& [json_key, key_stats_info] : infos) {
        grouped[json_key.key_].emplace_back(json_key, key_stats_info);
        group_hit_rows[json_key.key_] += key_stats_info.hit_row_num_;
    }

    auto ClassifyKey = [&](const JsonKey& key,
                           const KeyStatsInfo& info) -> JsonKeyLayoutType {
        // for null/object, must be classified as shared
        if (key.type_ == JSONType::OBJECT || key.type_ == JSONType::NONE) {
            return JsonKeyLayoutType::SHARED;
        }

        float hit_ratio = float(info.hit_row_num_) / num_rows_;
        if (info.hit_row_num_ == num_rows_) {
            return JsonKeyLayoutType::TYPED;
        } else if (hit_ratio >= shredding_ratio_threshold_) {
            return JsonKeyLayoutType::DYNAMIC;
        } else {
            return JsonKeyLayoutType::SHARED;
        }
    };

    size_t column_path_num = 0;
    for (const auto& [key, infos] : grouped) {
        if (infos.size() == 1) {
            auto stat_type = ClassifyKey(infos[0].first, infos[0].second);
            stat_type =
                // for key with only one type and is primitive type but not all rows hit,
                // can be classified as TYPED_NOT_ALL
                IsPrimitiveJsonType(infos[0].first.type_) &&
                        stat_type == JsonKeyLayoutType::DYNAMIC
                    ? JsonKeyLayoutType::TYPED_NOT_ALL
                    : stat_type;

            if (stat_type == JsonKeyLayoutType::TYPED ||
                stat_type == JsonKeyLayoutType::TYPED_NOT_ALL) {
                column_path_num++;
            }

            types[infos[0].first] = stat_type;
        } else {
            size_t dynamic_path_num = 0;
            for (const auto& [json_key, info] : infos) {
                auto stat_type = ClassifyKey(json_key, info);
                types[json_key] = stat_type;

                if (stat_type == JsonKeyLayoutType::DYNAMIC) {
                    column_path_num++;
                    dynamic_path_num++;
                }
            }

            // if all paths are dynamic, set all paths type to DYNAMIC_ONLY
            if (dynamic_path_num == infos.size()) {
                for (const auto& [json_key, info] : infos) {
                    types[json_key] = JsonKeyLayoutType::DYNAMIC_ONLY;
                }
            }
        }
    }

    if (column_path_num > max_shredding_columns_) {
        // sort by hit rows in descending order to find the least hit rows
        // move them to shared column
        std::vector<std::pair<JsonKey, int32_t>> key_hit_rows;
        for (const auto& [json_key, key_stats_info] : infos) {
            auto it = types.find(json_key);
            if (it != types.end() &&
                (it->second == JsonKeyLayoutType::TYPED ||
                 it->second == JsonKeyLayoutType::TYPED_NOT_ALL ||
                 it->second == JsonKeyLayoutType::DYNAMIC_ONLY ||
                 it->second == JsonKeyLayoutType::DYNAMIC)) {
                key_hit_rows.emplace_back(json_key,
                                          key_stats_info.hit_row_num_);
            }
        }

        std::sort(
            key_hit_rows.begin(),
            key_hit_rows.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });

        size_t idx = 0;
        while (column_path_num > max_shredding_columns_ &&
               idx < key_hit_rows.size()) {
            const auto& [json_key, _] = key_hit_rows[idx++];
            types[json_key] = JsonKeyLayoutType::SHARED;
            column_path_num--;
        }
    }

    return types;
}

void
JsonKeyStats::AddKeyStats(const std::vector<std::string>& path,
                          JSONType type,
                          const std::string& value,
                          std::map<JsonKey, std::string>& values) {
    auto path_str = JsonPointer(path);
    auto key = JsonKey(path_str, type);
    values[key] = value;
}

void
JsonKeyStats::TraverseJsonForBuildStats(
    const char* json,
    jsmntok* tokens,
    int& index,
    std::vector<std::string>& path,
    std::map<JsonKey, std::string>& values) {
    jsmntok current = tokens[0];
    Assert(current.type != JSMN_UNDEFINED);
    if (current.type == JSMN_OBJECT) {
        if (!path.empty() && current.size == 0) {
            AddKeyStats(
                path,
                JSONType::OBJECT,
                std::string(json + current.start, current.end - current.start),
                values);
            return;
        }
        int j = 1;
        for (int i = 0; i < current.size; i++) {
            Assert(tokens[j].type == JSMN_STRING && tokens[j].size != 0);
            std::string key(json + tokens[j].start,
                            tokens[j].end - tokens[j].start);
            path.push_back(key);
            j++;
            int consumed = 0;
            TraverseJsonForBuildStats(json, tokens + j, consumed, path, values);
            path.pop_back();
            j += consumed;
        }
        index = j;
    } else if (current.type == JSMN_PRIMITIVE) {
        std::string value(json + current.start, current.end - current.start);
        JSONType type;
        try {
            type = getType(value);
        } catch (const std::exception& e) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to get json type for value: {} with error: {}",
                      value,
                      e.what());
        }

        if (type == JSONType::INT64) {
            AddKeyStats(path, JSONType::INT64, value, values);
        } else if (type == JSONType::FLOAT || type == JSONType::DOUBLE) {
            AddKeyStats(path, JSONType::DOUBLE, value, values);
        } else if (type == JSONType::BOOL) {
            AddKeyStats(path, JSONType::BOOL, value, values);
        } else if (type == JSONType::NONE) {
            AddKeyStats(path, JSONType::NONE, value, values);
        } else {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "unsupported json type: {} for build json stats",
                      type);
        }
        index++;
    } else if (current.type == JSMN_ARRAY) {
        // Collect array as raw JSON string so it can be shredded into a dedicated column
        AddKeyStats(
            path,
            JSONType::ARRAY,
            std::string(json + current.start, current.end - current.start),
            values);
        // Skip array subtree
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
        AddKeyStats(
            path,
            JSONType::STRING,
            std::string(json + current.start, current.end - current.start),
            values);
        index++;
    }
}

void
JsonKeyStats::BuildKeyStatsForNullRow() {
    // add empty value for column keys that not hit
    for (const auto& key : column_keys_) {
        parquet_writer_->AppendValue(key.ToColumnName(), "");
    }

    // add null bson to shared column
    bsoncxx::builder::basic::document null_doc;
    auto null_bson = null_doc.extract();
    parquet_writer_->AppendSharedRow(null_bson.view().data(),
                                     null_bson.view().length());

    parquet_writer_->AddCurrentRow();
}

void
JsonKeyStats::BuildKeyStatsForRow(const char* json_str, uint32_t row_id) {
    LOG_TRACE("build key stats for row {} with json {} for segment {}",
              row_id,
              json_str,
              segment_id_);
    jsmn_parser parser;
    jsmn_init(&parser);

    int num_tokens = 0;
    int token_capacity = 16;
    std::vector<jsmntok_t> tokens(token_capacity);

    while (1) {
        int r = jsmn_parse(
            &parser, json_str, strlen(json_str), tokens.data(), token_capacity);
        if (r < 0) {
            if (r == JSMN_ERROR_NOMEM) {
                // Reallocate tokens array if not enough space
                token_capacity *= 2;
                tokens.resize(token_capacity);
                continue;
            } else {
                ThrowInfo(ErrorCode::UnexpectedError,
                          "Failed to parse Json: {}, error: {}",
                          json_str,
                          int(r));
            }
        }
        num_tokens = r;
        break;
    }

    int index = 0;
    std::vector<std::string> paths;
    std::map<JsonKey, std::string> values;
    TraverseJsonForBuildStats(json_str, tokens.data(), index, paths, values);
    DomNode root;
    std::set<JsonKey> hit_keys;
    for (const auto& [key, value] : values) {
        AssertInfo(key_types_.find(key) != key_types_.end(),
                   "key {} not found in key types",
                   key.key_);
        if (key_types_[key] == JsonKeyLayoutType::SHARED) {
            auto path_vec = ParseJsonPointerPath(key.key_);
            BsonBuilder::AppendToDom(root, path_vec, value, key.type_);
        } else {
            if (key.type_ == JSONType::ARRAY) {
                auto bson_bytes = BuildBsonArrayBytesFromJsonString(value);
                parquet_writer_->AppendValue(
                    key.ToColumnName(),
                    std::string(
                        reinterpret_cast<const char*>(bson_bytes.data()),
                        bson_bytes.size()));
            } else {
                parquet_writer_->AppendValue(key.ToColumnName(), value);
            }
        }
        hit_keys.insert(key);
    }
    // add empty value for column keys that not hit
    for (const auto& key : column_keys_) {
        if (hit_keys.find(key) == hit_keys.end()) {
            parquet_writer_->AppendValue(key.ToColumnName(), "");
        }
    }

    bsoncxx::builder::basic::document final_doc;
    BsonBuilder::ConvertDomToBson(root, final_doc);
    // build inverted index for shared key
    // cache pairs of (key, row_id/offset) into memory
    // when all rows processed, build it into disk
    auto key_offsets = BsonBuilder::ExtractBsonKeyOffsets(final_doc.view());
    for (const auto& [key, offset] : key_offsets) {
        LOG_TRACE(
            "add record to bson inverted index: {} with row_id: {} and offset: "
            "{} for segment {} for field {}",
            key,
            row_id,
            offset,
            segment_id_,
            field_id_);
        bson_inverted_index_->AddRecord(key, row_id, offset);
    }
    auto bson = final_doc.extract();
    parquet_writer_->AppendSharedRow(bson.view().data(), bson.view().length());

    parquet_writer_->AddCurrentRow();
}

void
JsonKeyStats::BuildKeyStats(const std::vector<FieldDataPtr>& field_datas,
                            bool nullable) {
    uint32_t row_id = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (uint32_t i = 0; i < n; i++) {
            if (nullable && !data->is_valid(i)) {
                BuildKeyStatsForNullRow();
            } else {
                auto json_str =
                    static_cast<const milvus::Json*>(data->RawValue(i))
                        ->data()
                        .data();
                BuildKeyStatsForRow(json_str, row_id);
            }
            row_id++;
        }
    }
}

std::string
JsonKeyStats::GetShreddingDir() {
    std::filesystem::path json_stats_dir = path_;
    std::filesystem::path shredding_path =
        json_stats_dir / JSON_STATS_SHREDDING_DATA_PATH;
    return shredding_path.string();
}

std::string
JsonKeyStats::GetSharedKeyIndexDir() {
    std::filesystem::path json_stats_dir = path_;
    std::filesystem::path shared_key_index_path =
        json_stats_dir / JSON_STATS_SHARED_INDEX_PATH;
    return shared_key_index_path.string();
}

BinarySet
JsonKeyStats::Serialize(const Config& config) {
    return BinarySet();
}

void
JsonKeyStats::Build(const Config& config) {
    if (is_built_)
        return;
    auto start_time = std::chrono::steady_clock::now();
    auto field_datas = mem_file_manager_->CacheRawDataToMemory(config);

    auto lack_binlog_rows =
        GetValueFromConfig<int64_t>(config, "lack_binlog_rows");
    if (lack_binlog_rows.has_value()) {
        auto field_schema = mem_file_manager_->GetFieldDataMeta().field_schema;
        auto default_value = [&]() -> std::optional<DefaultValueType> {
            if (!field_schema.has_default_value()) {
                return std::nullopt;
            }
            return field_schema.default_value();
        }();
        auto field_data = storage::CreateFieldData(
            static_cast<DataType>(field_schema.data_type()),
            DataType::NONE,
            true,
            1,
            lack_binlog_rows.value());
        field_data->FillFieldData(default_value, lack_binlog_rows.value());
        field_datas.insert(field_datas.begin(), field_data);
    }

    BuildWithFieldData(field_datas, schema_.nullable());
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - start_time)
                        .count();
    LOG_INFO(
        "build json stats for segment {} cost {} ms", segment_id_, duration);
    is_built_ = true;
}

std::string
JsonKeyStats::AddBucketName(const std::string& remote_prefix) {
    std::filesystem::path bucket_name = rcm_->GetBucketName();
    std::filesystem::path remote_prefix_path = remote_prefix;
    return (bucket_name / remote_prefix_path).string();
}

void
JsonKeyStats::BuildWithFieldData(const std::vector<FieldDataPtr>& field_datas,
                                 bool nullable) {
    // collect key stats info and classify key type
    auto infos = CollectKeyInfo(field_datas, nullable);
    LOG_INFO("collect key infos: {} for segment {} for field {}",
             PrintKeyInfo(infos),
             segment_id_,
             field_id_);
    key_types_ = ClassifyJsonKeyLayoutType(infos);
    LOG_INFO("key types infos: {} for segment {} for field {}",
             PrintJsonKeyLayoutType(key_types_),
             segment_id_,
             field_id_);
    for (const auto& [json_key, type] : key_types_) {
        if (type == JsonKeyLayoutType::SHARED) {
            shared_keys_.insert(json_key);
        } else {
            column_keys_.insert(json_key);
        }
    }

    // for storage v2, we need to add bucket name to remote prefix
    auto remote_prefix =
        AddBucketName(disk_file_manager_->GetRemoteJsonStatsShreddingPrefix());
    LOG_INFO(
        "init parquet writer with shredding remote prefix: {} for segment {}",
        remote_prefix,
        segment_id_);

    auto writer_context =
        ParquetWriterFactory::CreateContext(key_types_, remote_prefix);
    parquet_writer_->Init(writer_context);
    BuildKeyStats(field_datas, nullable);
    parquet_writer_->Close();
    bson_inverted_index_->BuildIndex();
}

void
JsonKeyStats::GetColumnSchemaFromParquet(int64_t column_group_id,
                                         const std::string& file) {
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
    auto file_reader =
        std::make_shared<milvus_storage::FileRowGroupReader>(fs, file);
    std::shared_ptr<arrow::Schema> file_schema = file_reader->schema();
    LOG_DEBUG("get column schema: [{}] for segment {}",
              file_schema->ToString(true),
              segment_id_);
    column_group_schemas_[column_group_id] = file_schema;

    for (const auto& field : file_schema->fields()) {
        auto field_name = field->name();
        field_names_.emplace_back(field_name);

        const auto& metadata = field->metadata();
        if (metadata == nullptr) {
            LOG_ERROR("metadata is nullptr for field: {} for segment {}",
                      field_name,
                      segment_id_);
            continue;
        }

        auto result = metadata->Get(milvus_storage::ARROW_FIELD_ID_KEY);
        AssertInfo(result.ok(),
                   "failed to get field id from metadata for field {}: {} "
                   "for segment {}",
                   field_name,
                   result.status().ToString(),
                   segment_id_);
        auto field_id_str = result.ValueOrDie();
        auto field_id = std::stoll(field_id_str);
        field_name_to_id_map_[field_name] = field_id;
        field_id_to_name_map_[field_id] = field_name;

        JSONType field_type;
        if (EndWith(field_name, JSON_KEY_STATS_SHARED_FIELD_NAME)) {
            // for shared key, we use string type instead of real binary type
            field_type = JSONType::STRING;
            shared_column_field_name_ = field_name;
        } else if (EndWith(field_name, "_ARRAY")) {
            field_type = JSONType::ARRAY;
        } else {
            field_type = GetPrimitiveJsonType(field->type());
        }
        shred_field_data_type_map_[field_name] = field_type;

        LOG_INFO(
            "parse field_name: {}, field_id: {}, "
            "field_type: {} for segment {}",
            field_name,
            field_id,
            field_type,
            segment_id_);
    }
}

void
JsonKeyStats::GetCommonMetaFromParquet(const std::string& file) {
    LOG_INFO("get common metadata from parquet file: {} for segment {}",
             file,
             segment_id_);

    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
    auto file_reader =
        std::make_shared<milvus_storage::FileRowGroupReader>(fs, file);

    // get key value metadata from parquet file
    std::shared_ptr<milvus_storage::PackedFileMetadata> metadata =
        file_reader->file_metadata();
    auto kv_metadata = metadata->GetParquetMetadata()->key_value_metadata();
    if (kv_metadata == nullptr) {
        LOG_WARN(
            "no key value metadata found in parquet file: {} for segment {} "
            "for field {}",
            file,
            segment_id_,
            field_id_);
        return;
    }

    // Deserialize key field map from metadata
    for (int i = 0; i < kv_metadata->size(); ++i) {
        const auto& key = kv_metadata->key(i);
        const auto& value = kv_metadata->value(i);
        LOG_TRACE("parquet metadata entry: {} = {} for segment {} for field {}",
                  key,
                  value,
                  segment_id_,
                  field_id_);

        if (key == JSON_STATS_META_KEY_LAYOUT_TYPE_MAP) {
            try {
                auto layout_type_json = nlohmann::json::parse(value);
                for (const auto& [k, v] : layout_type_json.items()) {
                    field_layout_type_map_[k] = JsonKeyLayoutTypeFromString(v);
                    key_data_type_map_[k] = GetJsonTypeFromKeyName(k);
                    key_field_map_[GetKeyFromColumnName(k)].insert(k);
                }
            } catch (const std::exception& e) {
                ThrowInfo(
                    ErrorCode::UnexpectedError,
                    "Failed to parse JSON_STATS_META_KEY_LAYOUT_TYPE_MAP from "
                    "metadata: {} in file: {} for segment {}",
                    e.what(),
                    file,
                    segment_id_);
            }
        }
    }
}

void
JsonKeyStats::LoadShreddingMeta(
    std::vector<std::pair<int64_t, std::vector<int64_t>>> sorted_files) {
    if (sorted_files.empty()) {
        return;
    }

    auto remote_prefix =
        AddBucketName(disk_file_manager_->GetRemoteJsonStatsShreddingPrefix());

    // load common meta from parquet, all parquet files have the same meta
    // just need to read one file
    auto file = CreateColumnGroupParquetPath(
        remote_prefix, sorted_files[0].first, sorted_files[0].second[0]);
    GetCommonMetaFromParquet(file);

    // load distinct meta from parquet, distinct meta is different for each parquet file
    // main purpose is to get column schema
    for (const auto& [column_group_id, file_ids] : sorted_files) {
        auto file = CreateColumnGroupParquetPath(
            remote_prefix, column_group_id, file_ids[0]);
        GetColumnSchemaFromParquet(column_group_id, file);
    }
}

void
JsonKeyStats::LoadColumnGroup(int64_t column_group_id,
                              const std::vector<int64_t>& file_ids) {
    if (file_ids.empty()) {
        return;
    }
    int64_t num_rows = 0;

    auto remote_prefix =
        AddBucketName(disk_file_manager_->GetRemoteJsonStatsShreddingPrefix());

    std::vector<std::string> files;
    for (const auto& file_id : file_ids) {
        files.emplace_back(CreateColumnGroupParquetPath(
            remote_prefix, column_group_id, file_id));
    }

    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
    auto file_reader =
        std::make_shared<milvus_storage::FileRowGroupReader>(fs, files[0]);
    std::shared_ptr<milvus_storage::PackedFileMetadata> metadata =
        file_reader->file_metadata();
    milvus_storage::FieldIDList field_id_list =
        metadata->GetGroupFieldIDList().GetFieldIDList(column_group_id);
    std::vector<FieldId> milvus_field_ids;
    for (int i = 0; i < field_id_list.size(); ++i) {
        milvus_field_ids.push_back(FieldId(field_id_list.Get(i)));
    }

    for (const auto& file : files) {
        auto reader =
            std::make_shared<milvus_storage::FileRowGroupReader>(fs, file);
        auto row_group_meta_vector =
            reader->file_metadata()->GetRowGroupMetadataVector();
        num_rows += row_group_meta_vector.row_num();
    }

    if (num_rows_ == 0) {
        num_rows_ = num_rows;
    }
    AssertInfo(num_rows_ == num_rows,
               "num_rows is not equal to num_rows_ for segment {}",
               segment_id_);

    auto enable_mmap = !mmap_filepath_.empty();
    auto column_group_info =
        FieldDataInfo(column_group_id, num_rows, mmap_filepath_);
    LOG_INFO(
        "loads column group {} with num_rows {} for segment "
        "{}",
        column_group_id,
        num_rows,
        segment_id_);

    std::unordered_map<FieldId, FieldMeta> field_meta_map;
    for (const auto& inner_field_id : milvus_field_ids) {
        auto field_name = field_id_to_name_map_[inner_field_id.get()];
        FieldMeta field_meta(
            FieldName(field_name),
            inner_field_id,
            field_id_,
            GetPrimitiveDataType(shred_field_data_type_map_[field_name]),
            true,
            std::nullopt);
        field_meta_map.insert(std::make_pair(FieldId(inner_field_id.get()),
                                             std::move(field_meta)));
    }

    auto translator = std::make_unique<
        milvus::segcore::storagev2translator::GroupChunkTranslator>(
        segment_id_,
        field_meta_map,
        column_group_info,
        files,
        enable_mmap,
        milvus_field_ids.size(),
        load_priority_);

    auto chunked_column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    // Create ProxyChunkColumn for each field in this column group
    for (const auto& inner_field_id : milvus_field_ids) {
        auto field_meta = field_meta_map.at(inner_field_id);
        auto column = std::make_shared<ProxyChunkColumn>(
            chunked_column_group, inner_field_id, field_meta);

        LOG_DEBUG(
            "add shredding column: {}, inner_field_id:{}, for json field {} "
            "segment "
            "{}",
            field_meta.get_name().get(),
            inner_field_id.get(),
            field_id_,
            segment_id_);
        shredding_columns_[field_meta.get_name().get()] = column;
    }
    shared_column_ = shredding_columns_.at(shared_column_field_name_);
}

void
JsonKeyStats::LoadShreddingData(const std::vector<std::string>& index_files) {
    // sort files by column group id and file id
    auto sorted_files = SortByParquetPath(index_files);

    // load shredding meta
    LoadShreddingMeta(sorted_files);

    // load shredding data
    for (const auto& [column_group_id, file_ids] : sorted_files) {
        LoadColumnGroup(column_group_id, file_ids);
    }
}

void
JsonKeyStats::Load(milvus::tracer::TraceContext ctx, const Config& config) {
    if (config.contains(MMAP_FILE_PATH)) {
        mmap_filepath_ = GetValueFromConfig<std::string>(config, MMAP_FILE_PATH)
                             .value_or("");
        LOG_INFO("load json stats for segment {} with mmap local file path: {}",
                 segment_id_,
                 mmap_filepath_);
    }
    load_priority_ = config[milvus::LOAD_PRIORITY];
    LOG_INFO("load json stats for segment {} with load priority: {}",
             segment_id_,
             load_priority_);

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load json stats for segment {}",
               segment_id_);
    // split index_files into shared_key_index and shredding_data
    std::vector<std::string> shared_key_index_files;
    std::vector<std::string> shredding_data_files;
    for (const auto& file : index_files.value()) {
        if (file.find(JSON_STATS_SHARED_INDEX_PATH) != std::string::npos) {
            shared_key_index_files.emplace_back(file);
        } else if (file.find(JSON_STATS_SHREDDING_DATA_PATH) !=
                   std::string::npos) {
            shredding_data_files.emplace_back(file);
        } else {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "unknown file path: {} for segment {}",
                      file,
                      segment_id_);
        }
    }

    // load shredding data
    LoadShreddingData(shredding_data_files);

    // load shared key index
    bson_inverted_index_->LoadIndex(shared_key_index_files, load_priority_);
}

IndexStatsPtr
JsonKeyStats::Upload(const Config& config) {
    // upload inverted index
    auto bson_index_stats = bson_inverted_index_->UploadIndex();

    // upload parquet file, parquet writer has already upload file to remote
    auto shredding_remote_paths_to_size = parquet_writer_->GetPathsToSize();
    auto shared_key_index_remote_paths_to_size =
        bson_index_stats->GetSerializedIndexFileInfo();

    // get all index files for meta
    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(shredding_remote_paths_to_size.size() +
                        shared_key_index_remote_paths_to_size.size());
    // only store shared_key_index/... and shredding_data/... to meta
    // for saving meta space
    for (const auto& file_info : shared_key_index_remote_paths_to_size) {
        auto file_path = file_info.file_name.substr(
            file_info.file_name.find(JSON_STATS_SHARED_INDEX_PATH));
        index_files.emplace_back(file_path, file_info.file_size);
        LOG_INFO("upload shared_key_index file: {} for segment {}",
                 file_path,
                 segment_id_);
    }

    for (auto& file : shredding_remote_paths_to_size) {
        auto file_path =
            file.first.substr(file.first.find(JSON_STATS_SHREDDING_DATA_PATH));
        index_files.emplace_back(file_path, file.second);
        LOG_INFO("upload shredding_data file: {} for segment {}",
                 file_path,
                 segment_id_);
    }

    LOG_INFO(
        "upload json key stats for segment {} with bson mem size: {} "
        "and "
        "shredding data mem size: {} and index files size: {}",
        segment_id_,
        bson_index_stats->GetMemSize(),
        parquet_writer_->GetTotalSize(),
        index_files.size());

    return IndexStats::New(
        bson_index_stats->GetMemSize() + parquet_writer_->GetTotalSize(),
        std::move(index_files));
}

}  // namespace milvus::index