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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <initializer_list>
#include <iosfwd>
#include <limits>
#include <map>
#include <memory>
#include <string.h>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "NamedType/underlying_functionalities.hpp"
#include "arrow/api.h"
#include "boost/filesystem/operations.hpp"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Json.h"
#include "common/Tracer.h"
#include "common/jsmn.h"
#include "fmt/core.h"
#include "folly/ScopeGuard.h"
#include "folly/futures/Future.h"
#include "futures/Executor.h"
#include "index/Utils.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/bson_builder.h"
#include "index/json_stats/parquet_writer.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/format/parquet/parquet_format_reader.h"
#include "milvus-storage/reader.h"
#include "mmap/ChunkedColumnGroup.h"
#include "mmap/Types.h"
#include "nlohmann/json.hpp"
#include "nlohmann/detail/iterators/iteration_proxy.hpp"
#include "nlohmann/json_fwd.hpp"
#include "parquet/metadata.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/storagev2translator/ManifestGroupTranslator.h"
#include "segcore/Utils.h"
#include "segcore/default_fs.h"
#include "segcore/storagev1translator/BsonInvertedIndexTranslator.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileManager.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/MmapManager.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"

namespace milvus::index {

namespace {

constexpr int64_t kJsonStatsRowsPerRange = 16 * 1024;
constexpr size_t kJsonStatsMaterializeMinReservationBytes = 64 * 1024;
constexpr size_t kJsonStatsMaterializeInputExpansionFactor = 2;
constexpr size_t kJsonStatsMaterializeRowOverheadBytes = 32;
constexpr size_t kJsonStatsMaterializeColumnOverheadBytes = 1024;

struct CollectKeyInfoResult {
    uint64_t sequence;
    int64_t row_count;
    std::map<JsonKey, KeyStatsInfo> infos;
};

template <typename Func>
void
ForEachJsonStatsRow(const JsonStatsRowRange& range, Func&& func) {
    int64_t visited_rows = 0;
    for (const auto& slice : range.slices) {
        AssertInfo(slice.data != nullptr,
                   "json stats row range contains null field data");
        AssertInfo(slice.row_count > 0,
                   "json stats row range contains an empty slice");
        AssertInfo(slice.local_begin >= 0,
                   "json stats field data slice has negative local begin: {}",
                   slice.local_begin);
        AssertInfo(
            slice.local_begin <= slice.data->get_num_rows() - slice.row_count,
            "json stats field data slice [{}, {}) exceeds field data rows {}",
            slice.local_begin,
            slice.local_begin + slice.row_count,
            slice.data->get_num_rows());
        AssertInfo(
            slice.global_begin == range.global_begin + visited_rows,
            "json stats field data slice starts at global row {}, expected {}",
            slice.global_begin,
            range.global_begin + visited_rows);

        for (int64_t offset = 0; offset < slice.row_count; ++offset) {
            func(slice.data,
                 slice.local_begin + offset,
                 slice.global_begin + offset);
        }
        visited_rows += slice.row_count;
    }
    AssertInfo(visited_rows == range.row_count,
               "json stats row range contains {} rows, expected {}",
               visited_rows,
               range.row_count);
}

size_t
SaturatingAdd(size_t lhs, size_t rhs) {
    if (rhs > std::numeric_limits<size_t>::max() - lhs) {
        return std::numeric_limits<size_t>::max();
    }
    return lhs + rhs;
}

size_t
SaturatingMultiply(size_t lhs, size_t rhs) {
    if (lhs != 0 && rhs > std::numeric_limits<size_t>::max() / lhs) {
        return std::numeric_limits<size_t>::max();
    }
    return lhs * rhs;
}

size_t
BytesForBits(size_t bit_count) {
    return SaturatingAdd(bit_count / 8, bit_count % 8 == 0 ? 0 : 1);
}

size_t
EstimateArrowFixedBufferBytes(const arrow::Schema& schema, size_t row_count) {
    const auto validity_bytes = BytesForBits(row_count);
    size_t buffer_bytes = 0;
    for (const auto& field : schema.fields()) {
        // Every materialized column can contain nulls. Account for its validity
        // bitmap even when a dense finished array can omit that buffer.
        buffer_bytes = SaturatingAdd(buffer_bytes, validity_bytes);

        const auto& type = *field->type();
        if (type.id() == arrow::Type::STRING ||
            type.id() == arrow::Type::BINARY) {
            // The variable payload is covered by the input-size estimate. This
            // accounts for the offsets that are allocated even for null rows.
            const auto offset_count = SaturatingAdd(row_count, 1);
            buffer_bytes = SaturatingAdd(
                buffer_bytes,
                SaturatingMultiply(offset_count,
                                   sizeof(arrow::BinaryType::offset_type)));
            continue;
        }

        const auto bit_width = type.bit_width();
        AssertInfo(bit_width > 0,
                   "unsupported json stats Arrow field type {}",
                   type.ToString());
        buffer_bytes =
            SaturatingAdd(buffer_bytes,
                          BytesForBits(SaturatingMultiply(
                              row_count, static_cast<size_t>(bit_width))));
    }
    return buffer_bytes;
}

struct JsonStatsParquetMetadata {
    std::shared_ptr<arrow::Schema> schema;
    int64_t num_rows;
};

milvus_storage::api::Properties
GetJsonStatsReadProperties() {
    auto properties =
        storage::LoonFFIPropertiesSingleton::GetInstance().GetProperties();
    if (properties == nullptr) {
        return {};
    }
    return *properties;
}

std::string
NoopParquetKeyRetriever(const std::string&) {
    return {};
}

JsonStatsParquetMetadata
ReadJsonStatsParquetMetadata(const std::string& file) {
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    auto properties = GetJsonStatsReadProperties();
    milvus_storage::parquet::ParquetFormatReader reader(
        fs, file, properties, {}, NoopParquetKeyRetriever);

    auto open_status = reader.open();
    AssertInfo(open_status.ok(),
               "[JsonStats] failed to open parquet metadata reader for {}: {}",
               file,
               open_status.ToString());

    auto row_group_result = reader.get_row_group_infos();
    AssertInfo(row_group_result.ok(),
               "[JsonStats] failed to read parquet row groups for {}: {}",
               file,
               row_group_result.status().ToString());
    auto row_groups = row_group_result.ValueOrDie();

    int64_t num_rows = 0;
    for (const auto& row_group : row_groups) {
        num_rows +=
            static_cast<int64_t>(row_group.end_offset - row_group.start_offset);
    }

    auto schema = reader.get_schema();
    AssertInfo(schema != nullptr,
               "[JsonStats] failed to read parquet schema for {}",
               file);
    return JsonStatsParquetMetadata{std::move(schema), num_rows};
}

FieldId
GetJsonStatsFieldIdFromArrowField(const std::shared_ptr<arrow::Field>& field) {
    const auto& metadata = field->metadata();
    AssertInfo(metadata != nullptr &&
                   metadata->Contains(milvus_storage::ARROW_FIELD_ID_KEY),
               "json stats field id not found in metadata for field {}",
               field->name());
    auto result = metadata->Get(milvus_storage::ARROW_FIELD_ID_KEY);
    AssertInfo(result.ok(),
               "failed to get json stats field id from metadata for field {}: "
               "{}",
               field->name(),
               result.status().ToString());
    return FieldId(std::stoll(result.ValueOrDie()));
}

std::pair<std::vector<FieldId>, std::vector<std::string>>
GetJsonStatsFieldsFromSchema(const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<FieldId> field_ids;
    std::vector<std::string> field_names;
    field_ids.reserve(schema->num_fields());
    field_names.reserve(schema->num_fields());

    for (const auto& field : schema->fields()) {
        field_ids.push_back(GetJsonStatsFieldIdFromArrowField(field));
        field_names.push_back(field->name());
    }
    return {std::move(field_ids), std::move(field_names)};
}

}  // namespace

std::vector<JsonStatsRowRange>
CreateJsonStatsRowRanges(const std::vector<FieldDataPtr>& field_datas,
                         int64_t max_rows_per_range) {
    AssertInfo(max_rows_per_range > 0,
               "json stats max rows per range must be positive, got {}",
               max_rows_per_range);

    std::vector<JsonStatsRowRange> row_ranges;
    JsonStatsRowRange current_range{};
    int64_t global_begin = 0;

    for (const auto& data : field_datas) {
        AssertInfo(data != nullptr,
                   "cannot create json stats row range from null field data");
        const auto data_rows = data->get_num_rows();
        AssertInfo(data_rows >= 0,
                   "json stats field data has negative row count: {}",
                   data_rows);

        int64_t local_begin = 0;
        while (local_begin < data_rows) {
            if (current_range.row_count == 0) {
                current_range.sequence =
                    static_cast<uint64_t>(row_ranges.size());
                current_range.global_begin = global_begin + local_begin;
            }

            const auto remaining_rows =
                max_rows_per_range - current_range.row_count;
            const auto slice_rows =
                std::min(data_rows - local_begin, remaining_rows);
            current_range.slices.push_back(JsonStatsFieldDataSlice{
                data,
                local_begin,
                slice_rows,
                global_begin + local_begin,
            });
            current_range.row_count += slice_rows;
            local_begin += slice_rows;

            if (current_range.row_count == max_rows_per_range) {
                row_ranges.push_back(std::move(current_range));
                current_range = JsonStatsRowRange{};
            }
        }
        global_begin += data_rows;
    }

    if (current_range.row_count > 0) {
        row_ranges.push_back(std::move(current_range));
    }
    return row_ranges;
}

size_t
JsonKeyStats::EstimateJsonStatsMaterializeReservationBytes(
    const JsonStatsRowRange& range, const arrow::Schema& schema) {
    size_t input_bytes = 0;
    ForEachJsonStatsRow(
        range,
        [&](const FieldDataPtr& data,
            int64_t local_row,
            int64_t /* global_row */) {
            if (data->IsNullable() && !data->is_valid(local_row)) {
                return;
            }
            auto raw_value = data->RawValue(local_row);
            if (raw_value == nullptr) {
                return;
            }
            const auto* json = static_cast<const milvus::Json*>(raw_value);
            input_bytes = SaturatingAdd(input_bytes, json->data().size());
        });

    const auto row_count = static_cast<size_t>(range.row_count);
    auto reservation_bytes = SaturatingMultiply(
        input_bytes, kJsonStatsMaterializeInputExpansionFactor);
    reservation_bytes = SaturatingAdd(
        reservation_bytes,
        SaturatingMultiply(row_count, kJsonStatsMaterializeRowOverheadBytes));
    reservation_bytes = SaturatingAdd(
        reservation_bytes,
        SaturatingMultiply(static_cast<size_t>(schema.num_fields()),
                           kJsonStatsMaterializeColumnOverheadBytes));
    reservation_bytes = SaturatingAdd(
        reservation_bytes, EstimateArrowFixedBufferBytes(schema, row_count));
    return std::max(kJsonStatsMaterializeMinReservationBytes,
                    reservation_bytes);
}

JsonKeyStats::JsonKeyStats(const storage::FileManagerContext& ctx,
                           bool is_load,
                           int64_t json_stats_max_shredding_columns,
                           double json_stats_shredding_ratio_threshold,
                           int64_t json_stats_write_batch_size,
                           uint32_t tantivy_index_version)
    : ScalarIndex<std::string>(JSON_KEY_STATS_INDEX_TYPE),
      file_manager_context_(ctx) {
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
        LOG_INFO("load json key stats from local path: {} for segment {}",
                 path_,
                 segment_id_);
    } else {
        auto prefix = disk_file_manager_->GetLocalTempJsonStatsPrefix();
        path_ = prefix;
        LOG_INFO("init json key stats with path: {} for segment {}",
                 path_,
                 segment_id_);

        // TODO: add params to modify batch size and max file size
        auto conf = milvus_storage::StorageConfig();
        conf.part_size = DEFAULT_PART_UPLOAD_SIZE;
        auto trueFs = ctx.fs;
        // try singleton if possible
        if (!trueFs) {
            trueFs = milvus::segcore::GetDefaultArrowFileSystem();
        }
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
        bson_inverted_index_ = std::make_shared<BsonInvertedIndex>(
            shared_key_index_path, field_id_, ctx, tantivy_index_version);
    }
}

JsonKeyStats::~JsonKeyStats() {
    bson_inverted_index_.reset();
    bson_index_cache_slot_.reset();
    boost::filesystem::remove_all(path_);
    LOG_INFO("remove json key stats with path: {}", path_);
}

void
JsonKeyStats::AddKeyStatsInfo(const std::vector<std::string>& paths,
                              JSONType type,
                              uint8_t* value,
                              std::map<JsonKey, KeyStatsInfo>& infos) {
    std::string key;
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
    AssertInfo(current.type != JSMN_UNDEFINED,
               "current token type is undefined for json: {}.",
               json);
    if (current.type == JSMN_OBJECT) {
        if (!path.empty()) {
            AddKeyStatsInfo(path, JSONType::OBJECT, nullptr, infos);
        }
        int j = 1;
        for (int i = 0; i < current.size; i++) {
            AssertInfo(tokens[j].type == JSMN_STRING && tokens[j].size != 0,
                       "current token type is not string for json: {} at "
                       "type: {}, size: {}, value: {}",
                       json,
                       int(tokens[j].type),
                       tokens[j].size,
                       std::string(json + tokens[j].start,
                                   tokens[j].end - tokens[j].start));
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
    std::string_view json_str, std::map<JsonKey, KeyStatsInfo>& infos) {
    jsmn_parser parser;
    jsmn_init(&parser);

    int num_tokens = 0;
    int token_capacity = 16;
    std::vector<jsmntok_t> tokens(token_capacity);

    while (1) {
        int r = jsmn_parse(&parser,
                           json_str.data(),
                           json_str.size(),
                           tokens.data(),
                           token_capacity);
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

    if (num_tokens == 0) {
        return;
    }

    int index = 0;
    std::vector<std::string> paths;
    TraverseJsonForStats(json_str.data(), tokens.data(), index, paths, infos);
}

std::map<JsonKey, KeyStatsInfo>
JsonKeyStats::CollectKeyInfo(const std::vector<FieldDataPtr>& field_datas,
                             bool nullable) {
    std::map<JsonKey, KeyStatsInfo> infos;
    int64_t num_rows = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int i = 0; i < n; i++) {
            if ((nullable || data->IsNullable()) && !data->is_valid(i)) {
                continue;
            }
            auto json_str =
                static_cast<const milvus::Json*>(data->RawValue(i))->data();
            CollectSingleJsonStatsInfo(json_str, infos);
        }
        num_rows += n;
    }
    num_rows_ = num_rows;
    return infos;
}

std::map<JsonKey, KeyStatsInfo>
JsonKeyStats::CollectKeyInfo(const std::vector<JsonStatsRowRange>& row_ranges,
                             bool nullable) {
    std::map<JsonKey, KeyStatsInfo> infos;
    int64_t expected_global_begin = 0;
    uint64_t expected_sequence = 0;
    for (const auto& range : row_ranges) {
        AssertInfo(range.sequence == expected_sequence,
                   "json stats row range sequence is {}, expected {}",
                   range.sequence,
                   expected_sequence);
        AssertInfo(range.global_begin == expected_global_begin,
                   "json stats row range starts at global row {}, expected {}",
                   range.global_begin,
                   expected_global_begin);
        expected_global_begin += range.row_count;
        ++expected_sequence;
    }

    if (row_ranges.empty()) {
        num_rows_ = 0;
        return infos;
    }

    auto* executor = milvus::futures::getJsonStatsBuildExecutor();
    const auto max_inflight = std::min(
        row_ranges.size(), std::max<size_t>(1, executor->numThreads()));

    // Futures are kept in sequence order. Only max_inflight tasks are active
    // at a time, so completed local maps cannot accumulate for every range.
    std::vector<folly::Future<CollectKeyInfoResult>> futures;
    futures.reserve(row_ranges.size());
    size_t next_submit = 0;
    size_t next_consume = 0;
    std::exception_ptr first_exception;

    auto submit_next = [&]() {
        try {
            auto range = row_ranges[next_submit];
            auto future = folly::via(
                executor, [this, range = std::move(range), nullable]() {
                    std::map<JsonKey, KeyStatsInfo> local_infos;
                    ForEachJsonStatsRow(
                        range,
                        [&](const FieldDataPtr& data,
                            int64_t local_row,
                            int64_t /* global_row */) {
                            if ((nullable || data->IsNullable()) &&
                                !data->is_valid(local_row)) {
                                return;
                            }
                            auto json_str = static_cast<const milvus::Json*>(
                                                data->RawValue(local_row))
                                                ->data();
                            CollectSingleJsonStatsInfo(json_str, local_infos);
                        });
                    return CollectKeyInfoResult{
                        range.sequence,
                        range.row_count,
                        std::move(local_infos),
                    };
                });
            futures.emplace_back(std::move(future));
            ++next_submit;
        } catch (...) {
            if (first_exception == nullptr) {
                first_exception = std::current_exception();
            }
        }
    };

    while (next_submit < max_inflight && first_exception == nullptr) {
        submit_next();
    }

    int64_t num_rows = 0;
    uint64_t next_result_sequence = 0;
    while (next_consume < futures.size()) {
        try {
            auto result = std::move(futures[next_consume]).get();
            if (first_exception == nullptr) {
                AssertInfo(result.sequence == next_result_sequence,
                           "json stats collect result sequence is {}, expected "
                           "{}",
                           result.sequence,
                           next_result_sequence);
                for (const auto& [key, local_info] : result.infos) {
                    infos[key].hit_row_num_ += local_info.hit_row_num_;
                }
                num_rows += result.row_count;
                ++next_result_sequence;
            }
        } catch (...) {
            if (first_exception == nullptr) {
                first_exception = std::current_exception();
            }
        }
        ++next_consume;

        if (first_exception == nullptr && next_submit < row_ranges.size()) {
            submit_next();
        }
    }

    // Every submitted future has been consumed before an error escapes. This
    // keeps `this` and the FieldData referenced by worker lambdas alive.
    if (first_exception != nullptr) {
        std::rethrow_exception(first_exception);
    }

    AssertInfo(num_rows == expected_global_begin,
               "collected {} json stats rows, expected {}",
               num_rows,
               expected_global_begin);
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
                          std::map<JsonKey, std::string>& values) const {
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
    std::map<JsonKey, std::string>& values) const {
    jsmntok current = tokens[0];
    AssertInfo(current.type != JSMN_UNDEFINED,
               "current token type is undefined for json: {}",
               json);
    if (current.type == JSMN_OBJECT) {
        if (!path.empty() && current.size == 0) {
            AddKeyStats(
                path,
                JSONType::OBJECT,
                std::string(json + current.start, current.end - current.start),
                values);
            index++;
            return;
        }
        int j = 1;
        for (int i = 0; i < current.size; i++) {
            AssertInfo(tokens[j].type == JSMN_STRING && tokens[j].size != 0,
                       "current token type is not string for json: {} at "
                       "type: {}, size: {}, value: {}",
                       json,
                       int(tokens[j].type),
                       tokens[j].size,
                       std::string(json + tokens[j].start,
                                   tokens[j].end - tokens[j].start));

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
        auto value =
            std::string(json + current.start, current.end - current.start);
        auto unescaped = UnescapeJsonString(value);
        Assert(current.size == 0);
        AddKeyStats(path, JSONType::STRING, unescaped, values);
        index++;
    }
}

bool
JsonKeyStats::ParseJsonForBuildStats(
    std::string_view json_str, std::map<JsonKey, std::string>& values) const {
    jsmn_parser parser;
    jsmn_init(&parser);

    int num_tokens = 0;
    int token_capacity = 16;
    std::vector<jsmntok_t> tokens(token_capacity);

    while (1) {
        int r = jsmn_parse(&parser,
                           json_str.data(),
                           json_str.size(),
                           tokens.data(),
                           token_capacity);
        if (r < 0) {
            if (r == JSMN_ERROR_NOMEM) {
                token_capacity *= 2;
                tokens.resize(token_capacity);
                continue;
            }
            ThrowInfo(ErrorCode::UnexpectedError,
                      "Failed to parse Json: {}, error: {}",
                      json_str,
                      int(r));
        }
        num_tokens = r;
        break;
    }

    if (num_tokens == 0) {
        return false;
    }

    int index = 0;
    std::vector<std::string> paths;
    TraverseJsonForBuildStats(
        json_str.data(), tokens.data(), index, paths, values);
    return true;
}

void
JsonKeyStats::BuildKeyStatsForNullRow() {
    // add empty value for column keys that not hit
    for (const auto& key : column_keys_) {
        parquet_writer_->AppendValue(key.ToColumnName(), "");
    }

    // add an empty BSON document to the shared column
    BsonDocument null_doc;
    parquet_writer_->AppendSharedRow(null_doc.data(), null_doc.length());

    parquet_writer_->AddCurrentRow();
}

void
JsonKeyStats::BuildKeyStatsForRow(std::string_view json_str, uint32_t row_id) {
    LOG_TRACE("build key stats for row {} with json {} for segment {}",
              row_id,
              json_str,
              segment_id_);
    std::map<JsonKey, std::string> values;
    if (!ParseJsonForBuildStats(json_str, values)) {
        BuildKeyStatsForNullRow();
        return;
    }

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

    BsonDocument final_doc;
    BsonBuilder::ConvertDomToBson(root, final_doc.get());
    // build inverted index for shared key
    // cache pairs of (key, row_id/offset) into memory
    // when all rows processed, build it into disk
    auto key_offsets = BsonBuilder::ExtractBsonKeyOffsets(final_doc.data(),
                                                          final_doc.length());
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
    parquet_writer_->AppendSharedRow(final_doc.data(), final_doc.length());

    parquet_writer_->AddCurrentRow();
}

void
JsonKeyStats::BuildKeyStats(const std::vector<JsonStatsRowRange>& row_ranges,
                            bool nullable) {
    int64_t processed_rows = 0;
    uint64_t expected_sequence = 0;
    for (const auto& range : row_ranges) {
        AssertInfo(range.sequence == expected_sequence,
                   "json stats row range sequence is {}, expected {}",
                   range.sequence,
                   expected_sequence);
        AssertInfo(range.global_begin == processed_rows,
                   "json stats row range starts at global row {}, expected {}",
                   range.global_begin,
                   processed_rows);
        ForEachJsonStatsRow(
            range,
            [&](const FieldDataPtr& data,
                int64_t local_row,
                int64_t global_row) {
                if ((nullable || data->IsNullable()) &&
                    !data->is_valid(local_row)) {
                    BuildKeyStatsForNullRow();
                } else {
                    auto json_str = static_cast<const milvus::Json*>(
                                        data->RawValue(local_row))
                                        ->data();

                    // some situations, such as empty json string,
                    // should be handled as null row
                    if (json_str.empty()) {
                        BuildKeyStatsForNullRow();
                    } else {
                        BuildKeyStatsForRow(json_str,
                                            static_cast<uint32_t>(global_row));
                    }
                }
            });
        processed_rows += range.row_count;
        ++expected_sequence;
    }
    AssertInfo(processed_rows == num_rows_,
               "materialized {} json stats rows, expected {}",
               processed_rows,
               num_rows_);
}

JsonKeyStats::MaterializedChunk
JsonKeyStats::MaterializeKeyStatsRange(
    const JsonStatsRowRange& range,
    bool nullable,
    const std::shared_ptr<arrow::Schema>& schema) const {
    AssertInfo(schema != nullptr,
               "json stats materialize schema must not be null");

    auto [builders, builders_map] = CreateArrowBuilders(key_types_);
    AssertInfo(
        builders.size() == static_cast<size_t>(schema->num_fields()),
        "json stats materialize builder count {} does not match schema field "
        "count {}",
        builders.size(),
        schema->num_fields());
    // Match the pre-dispatch fixed-buffer estimate and avoid geometric
    // over-allocation while appending a partially filled final range.
    for (const auto& builder : builders) {
        auto status = builder->Reserve(range.row_count);
        AssertInfo(status.ok(),
                   "failed to reserve json stats builder for {} rows: {}",
                   range.row_count,
                   status.ToString());
    }

    std::map<std::string, std::vector<int64_t>> bson_postings;
    auto append_null_row = [&]() {
        for (size_t i = 0; i + 1 < builders.size(); ++i) {
            auto status = builders[i]->AppendNull();
            AssertInfo(status.ok(),
                       "failed to append null json stats value: {}",
                       status.ToString());
        }

        BsonDocument null_doc;
        auto shared_builder =
            std::static_pointer_cast<arrow::BinaryBuilder>(builders.back());
        auto status =
            shared_builder->Append(null_doc.data(), null_doc.length());
        AssertInfo(status.ok(),
                   "failed to append empty shared json stats value: {}",
                   status.ToString());
    };

    ForEachJsonStatsRow(
        range,
        [&](const FieldDataPtr& data, int64_t local_row, int64_t global_row) {
            if ((nullable || data->IsNullable()) &&
                !data->is_valid(local_row)) {
                append_null_row();
                return;
            }

            auto json_str =
                static_cast<const milvus::Json*>(data->RawValue(local_row))
                    ->data();
            if (json_str.empty()) {
                append_null_row();
                return;
            }

            LOG_TRACE(
                "materialize key stats for row {} with json {} for "
                "segment {}",
                global_row,
                json_str,
                segment_id_);
            std::map<JsonKey, std::string> values;
            if (!ParseJsonForBuildStats(json_str, values)) {
                append_null_row();
                return;
            }

            DomNode root;
            std::set<JsonKey> hit_keys;
            for (const auto& [key, value] : values) {
                auto key_type = key_types_.find(key);
                AssertInfo(key_type != key_types_.end(),
                           "key {} not found in key types",
                           key.key_);
                if (key_type->second == JsonKeyLayoutType::SHARED) {
                    auto path_vec = ParseJsonPointerPath(key.key_);
                    BsonBuilder::AppendToDom(root, path_vec, value, key.type_);
                } else {
                    auto builder = builders_map.find(key.ToColumnName());
                    AssertInfo(builder != builders_map.end(),
                               "builder for key {} not found",
                               key.ToColumnName());

                    arrow::Status status;
                    if (key.type_ == JSONType::ARRAY) {
                        auto bson_bytes =
                            BuildBsonArrayBytesFromJsonString(value);
                        status = AppendJsonStatsValueToBuilder(
                            std::string(reinterpret_cast<const char*>(
                                            bson_bytes.data()),
                                        bson_bytes.size()),
                            builder->second);
                    } else {
                        status = AppendJsonStatsValueToBuilder(value,
                                                               builder->second);
                    }
                    AssertInfo(status.ok(),
                               "failed to append json stats value for key {}: "
                               "{}",
                               key.ToColumnName(),
                               status.ToString());
                }
                hit_keys.insert(key);
            }

            for (const auto& key : column_keys_) {
                if (hit_keys.find(key) != hit_keys.end()) {
                    continue;
                }
                auto builder = builders_map.find(key.ToColumnName());
                AssertInfo(builder != builders_map.end(),
                           "builder for key {} not found",
                           key.ToColumnName());
                auto status = builder->second->AppendNull();
                AssertInfo(status.ok(),
                           "failed to append null json stats value for key "
                           "{}: {}",
                           key.ToColumnName(),
                           status.ToString());
            }

            BsonDocument final_doc;
            BsonBuilder::ConvertDomToBson(root, final_doc.get());
            auto key_offsets = BsonBuilder::ExtractBsonKeyOffsets(
                final_doc.data(), final_doc.length());
            for (const auto& [key, offset] : key_offsets) {
                bson_postings[key].push_back(EncodeInvertedIndexValue(
                    static_cast<uint32_t>(global_row), offset));
            }

            auto shared_builder =
                std::static_pointer_cast<arrow::BinaryBuilder>(builders.back());
            auto status =
                shared_builder->Append(final_doc.data(), final_doc.length());
            AssertInfo(status.ok(),
                       "failed to append shared json stats value: {}",
                       status.ToString());
        });

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders.size());
    for (const auto& builder : builders) {
        AssertInfo(builder->length() == range.row_count,
                   "json stats materialize builder contains {} rows, "
                   "expected {}",
                   builder->length(),
                   range.row_count);
        std::shared_ptr<arrow::Array> array;
        auto status = builder->Finish(&array);
        AssertInfo(status.ok(),
                   "failed to finish json stats materialize builder: {}",
                   status.ToString());
        arrays.push_back(std::move(array));
    }

    auto record_batch =
        arrow::RecordBatch::Make(schema, range.row_count, std::move(arrays));
    auto validate_status = record_batch->Validate();
    AssertInfo(validate_status.ok(),
               "invalid materialized json stats record batch: {}",
               validate_status.ToString());

    size_t materialized_bytes = sizeof(MaterializedChunk);
    for (const auto& array : record_batch->columns()) {
        materialized_bytes =
            SaturatingAdd(materialized_bytes, GetArrowArrayMemorySize(array));
        materialized_bytes = SaturatingAdd(
            materialized_bytes, sizeof(std::shared_ptr<arrow::Array>));
    }
    for (const auto& [key, postings] : bson_postings) {
        materialized_bytes = SaturatingAdd(
            materialized_bytes,
            sizeof(std::pair<const std::string, std::vector<int64_t>>) +
                3 * sizeof(void*));
        materialized_bytes =
            SaturatingAdd(materialized_bytes, key.capacity() + 1);
        materialized_bytes = SaturatingAdd(
            materialized_bytes,
            SaturatingMultiply(postings.capacity(), sizeof(int64_t)));
    }
    return MaterializedChunk{
        range.sequence,
        range.global_begin,
        range.row_count,
        std::move(record_batch),
        std::move(bson_postings),
        materialized_bytes,
    };
}

void
JsonKeyStats::BuildKeyStatsParallel(
    const std::vector<JsonStatsRowRange>& row_ranges, bool nullable) {
    int64_t expected_global_begin = 0;
    uint64_t expected_sequence = 0;
    for (const auto& range : row_ranges) {
        AssertInfo(range.sequence == expected_sequence,
                   "json stats row range sequence is {}, expected {}",
                   range.sequence,
                   expected_sequence);
        AssertInfo(range.global_begin == expected_global_begin,
                   "json stats row range starts at global row {}, expected {}",
                   range.global_begin,
                   expected_global_begin);
        expected_global_begin += range.row_count;
        ++expected_sequence;
    }

    if (row_ranges.empty()) {
        AssertInfo(num_rows_ == 0,
                   "materialized 0 json stats rows, expected {}",
                   num_rows_);
        return;
    }

    AssertInfo(parquet_writer_ != nullptr,
               "json stats parquet writer must be initialized before "
               "materialize");
    AssertInfo(bson_inverted_index_ != nullptr,
               "json stats bson index must be initialized before materialize");
    auto schema = parquet_writer_->GetSchema();
    AssertInfo(schema != nullptr,
               "json stats parquet writer schema must be initialized before "
               "materialize");

    auto* executor = milvus::futures::getJsonStatsBuildExecutor();
    const auto max_active_tasks = std::min(
        row_ranges.size(), std::max<size_t>(1, executor->numThreads()));
    auto& memory_budget =
        storage::TransientMemoryBudget::GetJsonStatsBuildBudget();

    struct ActiveMaterializeTask {
        std::shared_ptr<std::atomic<size_t>> accounted_bytes;
        folly::Future<MaterializedChunk> future;
    };

    std::vector<ActiveMaterializeTask> tasks;
    tasks.reserve(row_ranges.size());
    size_t next_submit = 0;
    size_t next_consume = 0;
    std::exception_ptr first_exception;

    auto remember_exception = [&](std::exception_ptr exception) {
        if (first_exception == nullptr) {
            first_exception = std::move(exception);
        }
    };

    auto submit_next = [&](bool block_for_budget) -> bool {
        size_t reservation_bytes = 0;
        JsonStatsRowRange range;
        try {
            range = row_ranges[next_submit];
            reservation_bytes =
                EstimateJsonStatsMaterializeReservationBytes(range, *schema);
        } catch (...) {
            remember_exception(std::current_exception());
            return false;
        }

        std::shared_ptr<std::atomic<size_t>> accounted_bytes;
        try {
            accounted_bytes =
                std::make_shared<std::atomic<size_t>>(reservation_bytes);
            if (block_for_budget) {
                memory_budget.Acquire(reservation_bytes);
            } else if (!memory_budget.TryAcquire(reservation_bytes)) {
                return false;
            }
        } catch (...) {
            remember_exception(std::current_exception());
            return false;
        }

        try {
            auto future = folly::via(
                executor,
                [this,
                 range = std::move(range),
                 nullable,
                 schema,
                 reservation_bytes,
                 accounted_bytes,
                 &memory_budget]() {
                    auto result =
                        MaterializeKeyStatsRange(range, nullable, schema);
                    memory_budget.ReconcileReservation(reservation_bytes,
                                                       result.memory_bytes);
                    accounted_bytes->store(result.memory_bytes,
                                           std::memory_order_release);
                    return result;
                });
            tasks.push_back(
                ActiveMaterializeTask{accounted_bytes, std::move(future)});
            ++next_submit;
        } catch (...) {
            memory_budget.Release(
                accounted_bytes->load(std::memory_order_acquire));
            remember_exception(std::current_exception());
            return false;
        }
        return true;
    };

    auto refill = [&]() {
        while (first_exception == nullptr && next_submit < row_ranges.size() &&
               tasks.size() - next_consume < max_active_tasks) {
            const bool block_for_budget = tasks.size() == next_consume;
            if (!submit_next(block_for_budget)) {
                break;
            }
        }
    };

    auto release_task_budget = [&](const ActiveMaterializeTask& task) {
        memory_budget.Release(
            task.accounted_bytes->load(std::memory_order_acquire));
    };

    auto drain_submitted_tasks = [&]() {
        while (next_consume < tasks.size()) {
            auto& task = tasks[next_consume];
            try {
                std::move(task.future).get();
            } catch (...) {
                remember_exception(std::current_exception());
            }
            release_task_budget(task);
            ++next_consume;
        }
    };

    refill();

    int64_t processed_rows = 0;
    uint64_t next_result_sequence = 0;
    while (next_consume < tasks.size()) {
        auto& task = tasks[next_consume];
        try {
            auto result = std::move(task.future).get();
            if (first_exception == nullptr) {
                AssertInfo(result.sequence == next_result_sequence,
                           "json stats materialize result sequence is {}, "
                           "expected {}",
                           result.sequence,
                           next_result_sequence);
                AssertInfo(result.global_begin == processed_rows,
                           "json stats materialize result starts at global "
                           "row {}, expected {}",
                           result.global_begin,
                           processed_rows);

                auto status =
                    parquet_writer_->AppendRecordBatch(result.record_batch);
                AssertInfo(status.ok(),
                           "failed to append materialized json stats record "
                           "batch: {}",
                           status.ToString());
                for (auto& [key, postings] : result.bson_postings) {
                    bson_inverted_index_->AddRecords(key, std::move(postings));
                }

                processed_rows += result.row_count;
                ++next_result_sequence;
            }
        } catch (...) {
            remember_exception(std::current_exception());
        }
        release_task_budget(task);
        ++next_consume;

        if (first_exception != nullptr) {
            drain_submitted_tasks();
            break;
        }

        refill();
    }

    // Consume every submitted future before propagating an error so no worker
    // can retain references to this JsonKeyStats or its FieldData inputs.
    if (first_exception != nullptr) {
        std::rethrow_exception(first_exception);
    }

    AssertInfo(processed_rows == expected_global_begin,
               "materialized {} json stats rows, expected {}",
               processed_rows,
               expected_global_begin);
    AssertInfo(processed_rows == num_rows_,
               "materialized {} json stats rows, expected {}",
               processed_rows,
               num_rows_);
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

std::string
JsonKeyStats::GetMetaFilePath() {
    std::filesystem::path json_stats_dir = path_;
    std::filesystem::path meta_file_path =
        json_stats_dir / JSON_STATS_META_FILE_NAME;
    return meta_file_path.string();
}

void
JsonKeyStats::WriteMetaFile() {
    json_stats_meta_.SetLayoutTypeMap(key_types_);
    json_stats_meta_.SetInt64(META_KEY_NUM_ROWS, num_rows_);
    json_stats_meta_.SetInt64(META_KEY_NUM_SHREDDING_COLUMNS,
                              column_keys_.size());

    auto meta_content = json_stats_meta_.Serialize();
    auto meta_file_path = GetMetaFilePath();

    auto local_chunk_manager =
        milvus::storage::LocalChunkManagerSingleton::GetInstance()
            .GetChunkManager();
    local_chunk_manager->Write(
        meta_file_path, meta_content.data(), meta_content.size());

    meta_file_size_ = meta_content.size();
    LOG_INFO("write meta file: {} with size {} for segment {} for field {}",
             meta_file_path,
             meta_file_size_,
             segment_id_,
             field_id_);
}

void
JsonKeyStats::LoadMetaFile(const std::string& local_meta_file_path) {
    LOG_INFO("load meta file: {} for segment {} for field {}",
             local_meta_file_path,
             segment_id_,
             field_id_);

    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();

    auto file_size = local_chunk_manager->Size(local_meta_file_path);
    std::string meta_content;
    meta_content.resize(file_size);
    local_chunk_manager->Read(
        local_meta_file_path, meta_content.data(), file_size);

    key_field_map_ = JsonStatsMeta::DeserializeToKeyFieldMap(meta_content);

    LOG_INFO(
        "loaded meta file with {} key field entries for segment {} for field "
        "{}",
        key_field_map_.size(),
        segment_id_,
        field_id_);
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
    auto field_datas =
        storage::CacheRawDataAndFillMissing(mem_file_manager_, config);

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
    const auto row_ranges =
        CreateJsonStatsRowRanges(field_datas, kJsonStatsRowsPerRange);
    auto* executor = milvus::futures::getJsonStatsBuildExecutor();
    const auto executor_threads = std::max<size_t>(1, executor->numThreads());
    const auto effective_concurrency =
        std::min(row_ranges.size(), executor_threads);
    const bool use_parallel_pipeline = effective_concurrency > 1;
    LOG_INFO(
        "json stats build selects {} pipeline for segment {} field {} with "
        "{} row ranges, {} executor threads, and effective concurrency {}",
        use_parallel_pipeline ? "parallel" : "serial",
        segment_id_,
        field_id_,
        row_ranges.size(),
        executor_threads,
        effective_concurrency);

    // collect key stats info and classify key type
    auto infos = use_parallel_pipeline ? CollectKeyInfo(row_ranges, nullable)
                                       : CollectKeyInfo(field_datas, nullable);
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
        disk_file_manager_->GetRemoteJsonStatsShreddingPrefix();
    LOG_INFO(
        "init parquet writer with shredding remote prefix: {} for segment {}",
        remote_prefix,
        segment_id_);

    auto writer_context =
        ParquetWriterFactory::CreateContext(key_types_, remote_prefix);
    parquet_writer_->Init(std::move(writer_context));
    if (use_parallel_pipeline) {
        BuildKeyStatsParallel(row_ranges, nullable);
    } else {
        BuildKeyStats(row_ranges, nullable);
    }
    auto close_status = parquet_writer_->Close();
    AssertInfo(close_status.ok(),
               "failed to close json stats parquet writer: {}",
               close_status.ToString());
    bson_inverted_index_->BuildIndex();

    // write meta file with layout type map and other metadata
    WriteMetaFile();
}

void
JsonKeyStats::GetColumnSchemaFromParquet(int64_t column_group_id,
                                         const std::string& file) {
    auto parquet_metadata = ReadJsonStatsParquetMetadata(file);
    std::shared_ptr<arrow::Schema> file_schema = parquet_metadata.schema;
    LOG_DEBUG("get column schema: [{}] for segment {}",
              file_schema->ToString(true),
              segment_id_);

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

    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    auto result = milvus_storage::FileRowGroupReader::Make(fs, file);
    AssertInfo(result.ok(),
               "[StorageV2] Failed to create file row group reader: {}",
               result.status().ToString());
    auto file_reader = result.ValueOrDie();
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
                    auto layout_type = JsonKeyLayoutTypeFromString(v);
                    // Only store metadata for shredding columns (TYPED/DYNAMIC),
                    // skip SHARED keys to save memory
                    if (layout_type == JsonKeyLayoutType::SHARED) {
                        continue;
                    }
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
    std::vector<std::pair<int64_t, std::vector<int64_t>>> sorted_files,
    const std::string& override_prefix) {
    if (sorted_files.empty()) {
        return;
    }

    AssertInfo(!override_prefix.empty(),
               "shredding prefix is required for loading json stats");
    const auto& remote_prefix = override_prefix;

    // load common meta from parquet only if key_field_map_ is not already populated
    // (for backward compatibility with old data that doesn't have separate meta file)
    if (key_field_map_.empty()) {
        auto file = CreateColumnGroupParquetPath(
            remote_prefix, sorted_files[0].first, sorted_files[0].second[0]);
        GetCommonMetaFromParquet(file);
    } else {
        LOG_INFO(
            "skip loading common meta from parquet, already loaded from meta "
            "file for segment {}",
            segment_id_);
    }

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
                              const std::vector<int64_t>& file_ids,
                              const std::string& warmup_policy,
                              const std::string& override_prefix) {
    if (file_ids.empty()) {
        return;
    }
    int64_t num_rows = 0;

    const auto& remote_prefix = override_prefix;

    std::vector<std::string> files;
    for (const auto& file_id : file_ids) {
        files.emplace_back(CreateColumnGroupParquetPath(
            remote_prefix, column_group_id, file_id));
    }

    auto first_file_metadata = ReadJsonStatsParquetMetadata(files[0]);
    auto [milvus_field_ids, column_names] =
        GetJsonStatsFieldsFromSchema(first_file_metadata.schema);

    std::vector<int64_t> file_num_rows;
    file_num_rows.reserve(files.size());
    file_num_rows.push_back(first_file_metadata.num_rows);
    num_rows += first_file_metadata.num_rows;

    // Fetch row group metadata from remaining files in parallel using HIGH POOL
    // to avoid blocking the caller thread with serial S3 I/O.
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    std::vector<std::future<int64_t>> futures;
    futures.reserve(files.size() - 1);
    for (size_t i = 1; i < files.size(); ++i) {
        const auto& file = files[i];
        futures.push_back(pool.Submit(
            [file]() { return ReadJsonStatsParquetMetadata(file).num_rows; }));
    }
    // Ensure all futures are awaited even if one throws, to prevent
    // use-after-free on captured references in background tasks.
    auto futures_guard = folly::makeGuard([&futures]() {
        for (auto& f : futures) {
            if (f.valid()) {
                try {
                    f.get();
                } catch (...) {
                }
            }
        }
    });
    for (auto& f : futures) {
        auto file_rows = f.get();
        file_num_rows.push_back(file_rows);
        num_rows += file_rows;
    }

    if (num_rows_ == 0) {
        num_rows_ = num_rows;
    }
    AssertInfo(num_rows_ == num_rows,
               "num_rows is not equal to num_rows_ for segment {}",
               segment_id_);

    auto enable_mmap = !mmap_filepath_.empty();
    LOG_INFO(
        "loads column group {} with num_rows {} for segment "
        "{}",
        column_group_id,
        num_rows,
        segment_id_);

    std::unordered_map<FieldId, FieldMeta> field_meta_map;
    for (size_t i = 0; i < milvus_field_ids.size(); ++i) {
        const auto& inner_field_id = milvus_field_ids[i];
        auto field_name_it = field_id_to_name_map_.find(inner_field_id.get());
        AssertInfo(field_name_it != field_id_to_name_map_.end(),
                   "field id {} not found in json stats field map for "
                   "segment {}",
                   inner_field_id.get(),
                   segment_id_);
        auto field_name = field_name_it->second;
        FieldMeta field_meta(
            FieldName(field_name),
            inner_field_id,
            field_id_,
            GetPrimitiveDataType(shred_field_data_type_map_[field_name]),
            true,
            std::nullopt,
            column_names[i]);
        field_meta_map.insert(std::make_pair(FieldId(inner_field_id.get()),
                                             std::move(field_meta)));
    }

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();

    auto column_group = std::make_shared<milvus_storage::api::ColumnGroup>();
    column_group->columns = column_names;
    column_group->format = LOON_FORMAT_PARQUET;
    column_group->files.reserve(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
        column_group->files.push_back(milvus_storage::api::ColumnGroupFile{
            .path = files[i],
            .start_index = 0,
            .end_index = file_num_rows[i],
            .properties = {},
        });
    }
    auto column_groups = std::make_shared<milvus_storage::api::ColumnGroups>();
    column_groups->push_back(std::move(column_group));
    auto properties = GetJsonStatsReadProperties();
    auto resolved_warmup_policy =
        milvus::segcore::getCacheWarmupPolicy(warmup_policy,
                                              /*is_vector=*/false,
                                              /*is_index=*/false,
                                              /*in_load_list=*/true);
    auto eager_load =
        resolved_warmup_policy != CacheWarmupPolicy::CacheWarmupPolicy_Disable;

    if (eager_load) {
        auto needed_columns =
            std::make_shared<std::vector<std::string>>(column_names);
        auto reader = milvus_storage::api::Reader::create(
            column_groups, nullptr, needed_columns, properties);
        auto chunk_reader_result = reader->get_chunk_reader(0, needed_columns);
        AssertInfo(chunk_reader_result.ok(),
                   "[JsonStats] failed to create chunk reader for column group "
                   "{} segment {}: {}",
                   column_group_id,
                   segment_id_,
                   chunk_reader_result.status().ToString());
        auto chunk_reader_unique = std::move(chunk_reader_result).ValueOrDie();
        std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader(
            std::move(chunk_reader_unique));

        auto translator = std::make_unique<
            milvus::segcore::storagev2translator::ManifestGroupTranslator>(
            segment_id_,
            GroupChunkType::JSON_KEY_STATS,
            column_group_id,
            std::move(chunk_reader),
            field_meta_map,
            column_names,
            column_names,
            enable_mmap,
            mmap_config.GetMmapPopulate(),
            mmap_filepath_,
            milvus_field_ids.size(),
            load_priority_,
            /*eager_load=*/true,
            warmup_policy,
            fmt::format("jks_{}", field_id_),
            /*fallback_bytes_per_row=*/0,
            shard_);

        auto chunked_column_group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));

        for (const auto& inner_field_id : milvus_field_ids) {
            auto field_meta = field_meta_map.at(inner_field_id);
            auto column = std::make_shared<ProxyChunkColumn>(
                chunked_column_group, inner_field_id, field_meta);

            LOG_DEBUG(
                "add shredding column: {}, inner_field_id:{}, for json field "
                "{} segment "
                "{}",
                field_meta.get_name().get(),
                inner_field_id.get(),
                field_id_,
                segment_id_);
            shredding_columns_[field_meta.get_name().get()] = column;
        }
        shared_column_ = shredding_columns_.at(shared_column_field_name_);
        return;
    }

    // Lazy JSON stats columns are loaded through per-column projected readers,
    // same as lazy storage-v2 column-group entries. Fetch the complete estimate
    // matrix once and pass that temporary result to each projected translator.
    auto all_columns = std::make_shared<std::vector<std::string>>(column_names);
    auto reader = milvus_storage::api::Reader::create(
        column_groups, nullptr, all_columns, properties);
    auto estimate_reader_result = reader->get_chunk_reader(0, all_columns);
    AssertInfo(estimate_reader_result.ok(),
               "[JsonStats] failed to create estimate chunk reader for column "
               "group {} segment {}: {}",
               column_group_id,
               segment_id_,
               estimate_reader_result.status().ToString());
    auto estimate_chunk_reader = std::move(estimate_reader_result).ValueOrDie();
    auto size_estimate =
        milvus::segcore::storagev2translator::FetchColumnSizeEstimates(
            *estimate_chunk_reader);
    for (size_t i = 0; i < milvus_field_ids.size(); ++i) {
        const auto& inner_field_id = milvus_field_ids[i];
        const auto& column_name = column_names[i];
        auto needed_columns =
            std::make_shared<std::vector<std::string>>(std::vector<std::string>{
                column_name,
            });
        auto chunk_reader_result = reader->get_chunk_reader(0, needed_columns);
        AssertInfo(chunk_reader_result.ok(),
                   "[JsonStats] failed to create projected chunk reader for "
                   "column group {} column {} segment {}: {}",
                   column_group_id,
                   column_name,
                   segment_id_,
                   chunk_reader_result.status().ToString());
        auto chunk_reader_unique = std::move(chunk_reader_result).ValueOrDie();
        std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader(
            std::move(chunk_reader_unique));

        auto field_meta = field_meta_map.at(inner_field_id);
        std::unordered_map<FieldId, FieldMeta> projected_field_meta_map;
        projected_field_meta_map.emplace(inner_field_id, field_meta);
        auto translator = std::make_unique<
            milvus::segcore::storagev2translator::ManifestGroupTranslator>(
            segment_id_,
            GroupChunkType::JSON_KEY_STATS,
            column_group_id,
            std::move(chunk_reader),
            projected_field_meta_map,
            column_names,
            *needed_columns,
            enable_mmap,
            mmap_config.GetMmapPopulate(),
            mmap_filepath_,
            /*num_fields=*/1,
            load_priority_,
            eager_load,
            warmup_policy,
            fmt::format("jks_{}_{}", field_id_, inner_field_id.get()),
            /*fallback_bytes_per_row=*/0,
            shard_,
            size_estimate);

        auto chunked_column_group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));
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
JsonKeyStats::LoadShreddingData(const std::vector<std::string>& index_files,
                                const std::string& warmup_policy) {
    // sort files by column group id and file id
    auto sorted_files = SortByParquetPath(index_files);

    // Extract the shredding prefix from the first file path.
    // Files are absolute paths like: basePath/shredding_data/0/0
    // The prefix is everything up to and including "shredding_data".
    std::string shredding_prefix;
    if (!index_files.empty()) {
        auto pos = index_files[0].find(JSON_STATS_SHREDDING_DATA_PATH);
        if (pos != std::string::npos) {
            shredding_prefix = index_files[0].substr(
                0, pos + strlen(JSON_STATS_SHREDDING_DATA_PATH));
        }
    }

    // load shredding meta
    LoadShreddingMeta(sorted_files, shredding_prefix);

    // load shredding data
    for (const auto& [column_group_id, file_ids] : sorted_files) {
        LoadColumnGroup(
            column_group_id, file_ids, warmup_policy, shredding_prefix);
    }
}

void
JsonKeyStats::LoadSharedKeyIndex(
    const std::vector<std::string>& shared_key_index_files,
    bool enable_mmap,
    int64_t index_size,
    const std::string& warmup_policy) {
    // shared_key_index_files are absolute remote paths (basePath already prepended)
    segcore::storagev1translator::BsonInvertedIndexLoadInfo load_info;
    load_info.enable_mmap = enable_mmap;
    load_info.segment_id = segment_id_;
    load_info.field_id = field_id_;
    load_info.index_files = shared_key_index_files;
    load_info.index_size = index_size;
    load_info.load_priority = load_priority_;
    load_info.warmup_policy = warmup_policy;
    load_info.shard = shard_;
    std::unique_ptr<cachinglayer::Translator<index::BsonInvertedIndex>>
        translator = std::make_unique<
            segcore::storagev1translator::BsonInvertedIndexTranslator>(
            load_info, file_manager_context_);

    bson_index_cache_slot_ =
        cachinglayer::Manager::GetInstance().CreateCacheSlot(
            std::move(translator));

    LOG_INFO(
        "loaded bson inverted index using translator for field:{} of "
        "segment:{}, enable_mmap:{}",
        field_id_,
        segment_id_,
        enable_mmap);
}

void
JsonKeyStats::Load(milvus::tracer::TraceContext ctx, const Config& config) {
    auto enable_mmap =
        GetValueFromConfig<bool>(config, ENABLE_MMAP).value_or(false);
    if (enable_mmap) {
        mmap_filepath_ =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager()
                ->GetRootPath();
        LOG_INFO("load json stats for segment {} with mmap local file path: {}",
                 segment_id_,
                 mmap_filepath_);
    }
    load_priority_ = config[milvus::LOAD_PRIORITY];
    LOG_INFO("load json stats for segment {} with load priority: {}",
             segment_id_,
             static_cast<int>(load_priority_));
    shard_ = GetValueFromConfig<std::string>(config, JSON_STATS_CACHE_SHARD_KEY)
                 .value_or("");
    auto warmup_policy =
        GetValueFromConfig<std::string>(config, WARMUP).value_or("");

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load json stats for segment {}",
               segment_id_);

    auto base_path =
        GetValueFromConfig<std::string>(config, STATS_BASE_PATH_KEY)
            .value_or("");
    AssertInfo(!base_path.empty(),
               "stats_base_path is required for loading json stats, segment {}",
               segment_id_);

    // Split index_files into meta, shared_key_index, and shredding_data.
    // Files are relative paths; prepend base_path to get absolute remote paths.
    // Note: Check directory paths (shared_key_index, shredding_data) BEFORE meta.json,
    // because shared_key_index/meta.json_0 contains "meta.json" but is not the meta file.
    std::vector<std::string> meta_files;
    std::vector<std::string> shared_key_index_files;
    std::vector<std::string> shredding_data_files;
    for (const auto& file : index_files.value()) {
        auto abs_path = base_path + "/" + file;
        if (file.find(JSON_STATS_SHARED_INDEX_PATH) != std::string::npos) {
            shared_key_index_files.emplace_back(abs_path);
        } else if (file.find(JSON_STATS_SHREDDING_DATA_PATH) !=
                   std::string::npos) {
            shredding_data_files.emplace_back(abs_path);
        } else if (file.find(JSON_STATS_META_FILE_NAME) != std::string::npos) {
            meta_files.emplace_back(abs_path);
        } else {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "unknown file path: {} for segment {}",
                      file,
                      segment_id_);
        }
    }

    // load meta file first (contains layout type map)
    if (!meta_files.empty()) {
        AssertInfo(
            meta_files.size() == 1,
            "expected exactly one meta file, got {} for segment {}, field {}",
            meta_files.size(),
            segment_id_,
            field_id_);
        auto local_meta_file = disk_file_manager_->CacheJsonStatsMetaToDisk(
            meta_files[0], load_priority_);
        LoadMetaFile(local_meta_file);
    }

    // load shredding data (files are already absolute paths)
    LoadShreddingData(shredding_data_files, warmup_policy);

    auto index_size =
        GetValueFromConfig<int64_t>(config, milvus::index::INDEX_SIZE)
            .value_or(0);
    // load shared key index (files are already absolute paths)
    LoadSharedKeyIndex(
        shared_key_index_files, enable_mmap, index_size, warmup_policy);
}

IndexStatsPtr
JsonKeyStats::Upload(const Config& config) {
    // upload inverted index
    auto bson_index_stats = bson_inverted_index_->UploadIndex();

    // upload meta file
    auto meta_file_path = GetMetaFilePath();
    AssertInfo(disk_file_manager_->AddJsonStatsMetaLog(meta_file_path),
               "failed to upload meta file: {} for segment {}",
               meta_file_path,
               segment_id_);

    // upload parquet file, parquet writer has already upload file to remote
    auto shredding_remote_paths_to_size = parquet_writer_->GetPathsToSize();
    auto shared_key_index_remote_paths_to_size =
        bson_index_stats->GetSerializedIndexFileInfo();
    auto meta_remote_paths_to_size =
        disk_file_manager_->GetRemotePathsToFileSize();

    // get all index files for meta
    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(shredding_remote_paths_to_size.size() +
                        shared_key_index_remote_paths_to_size.size() + 1);

    // add meta file
    for (const auto& [path, size] : meta_remote_paths_to_size) {
        if (path.find(JSON_STATS_META_FILE_NAME) != std::string::npos) {
            auto file_path = path.substr(path.find(JSON_STATS_META_FILE_NAME));
            index_files.emplace_back(file_path, size);
            LOG_INFO(
                "upload meta file: {} for segment {}", file_path, segment_id_);
        }
    }

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
        "and shredding data mem size: {} and meta file size: {} "
        "and index files size: {}",
        segment_id_,
        bson_index_stats->GetMemSize(),
        parquet_writer_->GetTotalSize(),
        meta_file_size_,
        index_files.size());

    return IndexStats::New(bson_index_stats->GetMemSize() +
                               parquet_writer_->GetTotalSize() +
                               meta_file_size_,
                           std::move(index_files));
}

}  // namespace milvus::index
