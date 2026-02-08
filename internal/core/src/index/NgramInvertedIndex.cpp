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

#include "index/NgramInvertedIndex.h"

#include <simdjson.h>
#include <string.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <map>
#include <string_view>
#include <utility>

#include "bitset/bitset.h"
#include "boost/filesystem/operations.hpp"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "common/RegexQuery.h"
#include "exec/expression/Expr.h"
#include "glog/logging.h"
#include "index/JsonIndexBuilder.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/binaryset.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "opentelemetry/trace/span.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "simdjson/error.h"
#include "storage/DataCodec.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"

namespace milvus::index {

const std::string NGRAM_AVG_ROW_SIZE_FILE_NAME = "ngram_avg_row_size";

const JsonCastType JSON_CAST_TYPE = JsonCastType::FromString("VARCHAR");
// ngram index doesn't need cast function
const JsonCastFunction JSON_CAST_FUNCTION =
    JsonCastFunction::FromString("unknown");

constexpr size_t kLargeRowThreshold = 5000;
constexpr size_t kMediumRowThreshold = 1000;
constexpr size_t kSmallRowThreshold = 100;

constexpr double kPreFilterHitRateThreshold = 0.20;  // 20%
// Default avg_row_size for compatibility (older indexes without metadata)
constexpr size_t kDefaultAvgRowSize = kLargeRowThreshold;
// Iterative strategy parameters
constexpr size_t kMaxIterations = 5;
constexpr double kBreakThreshold = 0.002;  // 0.2%

constexpr size_t kMaxIterationsForMediumRow = 3;

constexpr double kBreakThresholdForSmallRow = 0.01;  // 1%
constexpr size_t kMaxIterationsForSmallRow = 2;

// for string/varchar type
NgramInvertedIndex::NgramInvertedIndex(const storage::FileManagerContext& ctx,
                                       const NgramParams& params)
    : min_gram_(params.min_gram), max_gram_(params.max_gram) {
    schema_ = ctx.fieldDataMeta.field_schema;
    field_id_ = ctx.fieldDataMeta.field_id;
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);

    if (params.loading_index) {
        path_ = disk_file_manager_->GetLocalNgramIndexPrefix();
    } else {
        path_ = disk_file_manager_->GetLocalTempNgramIndexPrefix();
        boost::filesystem::create_directories(path_);
        d_type_ = TantivyDataType::Keyword;
        std::string field_name =
            std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field_name.c_str(), path_.c_str(), min_gram_, max_gram_);
    }
}

// for json type
NgramInvertedIndex::NgramInvertedIndex(const storage::FileManagerContext& ctx,
                                       const NgramParams& params,
                                       const std::string& nested_path)
    : NgramInvertedIndex(ctx, params) {
    nested_path_ = nested_path;
}

void
NgramInvertedIndex::BuildWithFieldData(const std::vector<FieldDataPtr>& datas) {
    AssertInfo(schema_.data_type() == proto::schema::DataType::String ||
                   schema_.data_type() == proto::schema::DataType::VarChar ||
                   schema_.data_type() == proto::schema::DataType::JSON,
               "schema data type is {}",
               schema_.data_type());
    LOG_INFO("Start to build ngram index, data type {}, field id: {}",
             schema_.data_type(),
             field_id_);

    index_build_begin_ = std::chrono::system_clock::now();
    if (schema_.data_type() == proto::schema::DataType::JSON) {
        BuildWithJsonFieldData(datas);
    } else {
        // Calculate avg_row_size for String/VarChar types
        size_t total_bytes = 0;
        size_t total_rows = 0;
        for (const auto& data : datas) {
            auto n = data->get_num_rows();
            for (size_t i = 0; i < n; i++) {
                if (schema_.nullable() && !data->is_valid(i)) {
                    continue;
                }
                auto* str = static_cast<const std::string*>(data->RawValue(i));
                if (str) {
                    total_bytes += str->size();
                    total_rows += 1;
                }
            }
        }
        avg_row_size_ = total_rows > 0 ? total_bytes / total_rows : 0;
        LOG_INFO("Ngram index avg_row_size: {} bytes", avg_row_size_);

        InvertedIndexTantivy<std::string>::BuildWithFieldData(datas);
    }
}

void
NgramInvertedIndex::BuildWithJsonFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    AssertInfo(schema_.data_type() == proto::schema::DataType::JSON,
               "schema data should be json, but is {}",
               schema_.data_type());
    LOG_INFO("Start to build ngram index for json, field_id: {}, field: {}",
             field_id_,
             nested_path_);

    index_build_begin_ = std::chrono::system_clock::now();

    // Track total bytes and rows for avg_row_size calculation
    size_t total_bytes = 0;
    size_t total_rows = 0;

    ProcessJsonFieldData<std::string>(
        field_datas,
        this->schema_,
        nested_path_,
        JSON_CAST_TYPE,
        JSON_CAST_FUNCTION,
        // add data
        [this, &total_bytes, &total_rows](
            const std::string* data, int64_t size, int64_t offset) {
            this->wrapper_->template add_array_data<std::string>(
                data, size, offset);
            // Track row size
            if (data && size > 0) {
                total_bytes += data->size();
                total_rows++;
            }
        },
        // handle null
        [this](int64_t offset) { this->null_offset_.push_back(offset); },
        // handle non exist
        [this](int64_t offset) {},
        // handle error
        [this](const Json& json,
               const std::string& nested_path,
               simdjson::error_code error) {
            this->error_recorder_.Record(json, nested_path, error);
        });

    avg_row_size_ = total_rows > 0 ? total_bytes / total_rows : 0;
    LOG_INFO("Ngram index (JSON) avg_row_size: {} bytes", avg_row_size_);

    error_recorder_.PrintErrStats();
}

BinarySet
NgramInvertedIndex::Serialize(const Config& config) {
    auto res_set = InvertedIndexTantivy<std::string>::Serialize(config);

    // Serialize avg_row_size
    std::shared_ptr<uint8_t[]> avg_row_size_data(new uint8_t[sizeof(size_t)]);
    memcpy(avg_row_size_data.get(), &avg_row_size_, sizeof(size_t));
    res_set.Append(
        NGRAM_AVG_ROW_SIZE_FILE_NAME, avg_row_size_data, sizeof(size_t));

    return res_set;
}

IndexStatsPtr
NgramInvertedIndex::Upload(const Config& config) {
    finish();
    auto index_build_end = std::chrono::system_clock::now();
    auto index_build_duration =
        std::chrono::duration<double>(index_build_end - index_build_begin_)
            .count();
    LOG_INFO(
        "index build done for ngram index, data type {}, field id: {}, "
        "duration: {}s, avg_row_size: {} bytes",
        schema_.data_type(),
        field_id_,
        index_build_duration,
        avg_row_size_);

    return InvertedIndexTantivy<std::string>::Upload(config);
}

void
NgramInvertedIndex::LoadIndexMetas(const std::vector<std::string>& index_files,
                                   const Config& config) {
    // Call parent to load null_offset
    InvertedIndexTantivy<std::string>::LoadIndexMetas(index_files, config);

    // Load avg_row_size
    auto avg_row_size_it = std::find_if(
        index_files.begin(), index_files.end(), [](const std::string& file) {
            return file.find(NGRAM_AVG_ROW_SIZE_FILE_NAME) != std::string::npos;
        });

    if (avg_row_size_it != index_files.end()) {
        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);
        // avg_row_size is only 8 bytes, never sliced
        auto index_datas = mem_file_manager_->LoadIndexToMemory(
            {*avg_row_size_it}, load_priority);
        auto avg_row_size_data =
            std::move(index_datas.at(NGRAM_AVG_ROW_SIZE_FILE_NAME));
        memcpy(
            &avg_row_size_, avg_row_size_data->PayloadData(), sizeof(size_t));
        LOG_INFO("Loaded ngram index avg_row_size: {} bytes", avg_row_size_);
    } else {
        avg_row_size_ = kDefaultAvgRowSize;
        LOG_INFO("No avg_row_size metadata found, using default: {}",
                 kDefaultAvgRowSize);
    }
}

void
NgramInvertedIndex::RetainTantivyIndexFiles(
    std::vector<std::string>& index_files) {
    // Call parent to filter null_offset
    InvertedIndexTantivy<std::string>::RetainTantivyIndexFiles(index_files);

    // Also filter avg_row_size
    index_files.erase(
        std::remove_if(index_files.begin(),
                       index_files.end(),
                       [](const std::string& file) {
                           return file.find(NGRAM_AVG_ROW_SIZE_FILE_NAME) !=
                                  std::string::npos;
                       }),
        index_files.end());
}

void
NgramInvertedIndex::Load(milvus::tracer::TraceContext ctx,
                         const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, INDEX_FILES);
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load ngram index");
    auto files_value = index_files.value();

    LoadIndexMetas(files_value, config);
    RetainTantivyIndexFiles(files_value);

    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    disk_file_manager_->CacheNgramIndexToDisk(files_value, load_priority);
    AssertInfo(
        tantivy_index_exist(path_.c_str()), "index not exist: {}", path_);

    auto load_in_mmap =
        GetValueFromConfig<bool>(config, ENABLE_MMAP).value_or(true);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        path_.c_str(), load_in_mmap, milvus::index::SetBitsetSealed);

    if (!load_in_mmap) {
        // the index is loaded in ram, so we can remove files in advance
        disk_file_manager_->RemoveNgramIndexFiles();
    }

    LOG_INFO(
        "load ngram index done for field id:{} with dir:{}", field_id_, path_);
}

std::vector<std::string>
split_by_wildcard(const std::string& literal) {
    std::vector<std::string> result;
    std::string r;
    r.reserve(literal.size());
    bool escape_mode = false;
    for (char c : literal) {
        if (escape_mode) {
            r += c;
            escape_mode = false;
        } else {
            if (c == '\\') {
                // consider case "\\%", we should reserve %
                escape_mode = true;
            } else if (c == '%' || c == '_') {
                if (r.length() > 0) {
                    result.push_back(r);
                    r.clear();
                }
            } else {
                r += c;
            }
        }
    }
    if (r.length() > 0) {
        result.push_back(r);
    }
    return result;
}

bool
NgramInvertedIndex::CanHandleLiteral(const std::string& literal,
                                     proto::plan::OpType op_type) const {
    switch (op_type) {
        case proto::plan::OpType::Match: {
            // For Match (LIKE pattern), check all parts after splitting by wildcard
            auto literals = split_by_wildcard(literal);
            if (literals.empty()) {
                return false;
            }
            for (const auto& l : literals) {
                if (l.length() < min_gram_) {
                    return false;
                }
            }
            return true;
        }
        case proto::plan::OpType::InnerMatch:
        case proto::plan::OpType::PrefixMatch:
        case proto::plan::OpType::PostfixMatch:
            return literal.length() >= min_gram_;
        default:
            return false;
    }
}

template <typename T, typename Predicate>
inline void
apply_predicate_on_batch(const T* data,
                         const int64_t size,
                         TargetBitmapView res,
                         Predicate&& predicate) {
    auto next_off_option = res.find_first();
    while (next_off_option.has_value()) {
        auto next_off = next_off_option.value();
        if (next_off >= static_cast<size_t>(size)) {
            return;
        }
        if (!predicate(data[next_off])) {
            res[next_off] = false;
        }
        next_off_option = res.find_next(next_off);
    }
}

void
NgramInvertedIndex::ApplyIterativeNgramFilter(
    const std::vector<std::string>& sorted_terms,
    size_t total_count,
    TargetBitmap& bitset) {
    auto max_iterations = kMaxIterations;
    if (avg_row_size_ < kSmallRowThreshold) {
        max_iterations = kMaxIterationsForSmallRow;
    } else if (avg_row_size_ < kMediumRowThreshold) {
        max_iterations = kMaxIterationsForMediumRow;
    }

    for (size_t i = 0; i < std::min(sorted_terms.size(), max_iterations); i++) {
        TargetBitmap term_bitset{total_count};
        wrapper_->ngram_term_posting_list(sorted_terms[i], &term_bitset);
        bitset &= term_bitset;

        double current_hit_rate = 1.0 * bitset.count() / total_count;
        if (current_hit_rate < kBreakThreshold) {
            break;
        }
        if (avg_row_size_ < kSmallRowThreshold &&
            current_hit_rate < kBreakThresholdForSmallRow) {
            break;
        }
    }
}

// Strategy selection based on avg_row_size and pre_filter_hit_rate:
// - Batch strategy: query all ngram terms at once via ngram_match_query.
//   Used for large rows (>= 5KB) where full matching is efficient, or for
//   medium rows (>= 1KB) with high pre_filter_hit_rate (> 20%).
// - Iterative strategy: query terms one by one, sorted by doc_freq (rarest first).
//   Used for small/medium rows with low pre_filter_hit_rate, allowing early
//   termination when hit_rate drops below threshold.
bool
NgramInvertedIndex::ShouldUseBatchStrategy(double pre_filter_hit_rate) const {
    return avg_row_size_ >= kLargeRowThreshold ||
           (avg_row_size_ >= kMediumRowThreshold &&
            pre_filter_hit_rate > kPreFilterHitRateThreshold);
}

void
NgramInvertedIndex::ExecutePhase1(const std::string& literal,
                                  proto::plan::OpType op_type,
                                  TargetBitmap& candidates) {
    tracer::AutoSpan span(
        "NgramInvertedIndex::ExecutePhase1", tracer::GetRootSpan(), true);

    auto total_count = static_cast<size_t>(Count());
    AssertInfo(total_count > 0, "ExecutePhase1: total_count must be > 0");
    AssertInfo(!candidates.empty(),
               "ExecutePhase1: candidates must be non-empty");
    AssertInfo(candidates.size() == total_count,
               "ExecutePhase1: candidates size {} != total_count {}",
               candidates.size(),
               total_count);

    // If candidates has no bits set, return immediately
    if (candidates.none()) {
        return;
    }

    // Use candidates for strategy selection
    size_t pre_count = candidates.count();
    double candidates_hit_rate = 1.0 * pre_count / total_count;

    // Get literals to query
    std::vector<std::string> literals_vec;
    if (op_type == proto::plan::OpType::Match) {
        literals_vec = split_by_wildcard(literal);
        AssertInfo(!literals_vec.empty(),
                   "ExecutePhase1: Match pattern must have non-empty parts");
        for (const auto& l : literals_vec) {
            AssertInfo(l.length() >= min_gram_,
                       "ExecutePhase1: part length {} < min_gram {}",
                       l.length(),
                       min_gram_);
        }
    } else {
        AssertInfo(literal.length() >= min_gram_,
                   "ExecutePhase1: literal length {} < min_gram {}",
                   literal.length(),
                   min_gram_);
        literals_vec.push_back(literal);
    }

    bool use_batch_strategy = ShouldUseBatchStrategy(candidates_hit_rate);

    // Choose strategy and execute, AND results into candidates
    if (use_batch_strategy) {
        // Batch strategy: query all ngram terms at once
        for (const auto& l : literals_vec) {
            TargetBitmap ngram_bitset{total_count};
            wrapper_->ngram_match_query(l, min_gram_, max_gram_, &ngram_bitset);
            candidates &= ngram_bitset;
        }
    } else {
        // Iterative strategy: query terms one by one, sorted by doc_freq
        auto sorted_terms =
            wrapper_->ngram_tokenize(literals_vec, min_gram_, max_gram_);
        AssertInfo(!sorted_terms.empty(),
                   "ngram_tokenize should not return empty for valid literal");
        ApplyIterativeNgramFilter(sorted_terms, total_count, candidates);
    }

    // Set tracing attributes
    if (auto root_span = tracer::GetRootSpan()) {
        size_t post_count = candidates.count();
        double pre_hit_rate = 1.0 * pre_count / total_count;
        double post_hit_rate = 1.0 * post_count / total_count;
        root_span->SetAttribute("phase1_op_type", static_cast<int>(op_type));
        root_span->SetAttribute("phase1_literal_length",
                                static_cast<int>(literal.length()));
        root_span->SetAttribute("phase1_total_count",
                                static_cast<int>(total_count));
        root_span->SetAttribute("phase1_pre_hit_rate", pre_hit_rate);
        root_span->SetAttribute("phase1_post_hit_rate", post_hit_rate);
        root_span->SetAttribute("phase1_use_batch_strategy",
                                use_batch_strategy);
    }
}

void
NgramInvertedIndex::ExecutePhase2(const std::string& literal,
                                  proto::plan::OpType op_type,
                                  exec::SegmentExpr* segment,
                                  TargetBitmap& candidates,
                                  int64_t segment_offset,
                                  int64_t batch_size) {
    // InnerMatch with short literal doesn't need post-filter
    if (op_type == proto::plan::OpType::InnerMatch &&
        literal.length() <= max_gram_) {
        return;
    }

    if (candidates.none()) {
        return;
    }

    AssertInfo(static_cast<int64_t>(candidates.size()) == batch_size,
               "candidates size {} != batch_size {}",
               candidates.size(),
               batch_size);

    size_t pre_count = candidates.count();

    TargetBitmapView res(candidates);

    if (schema_.data_type() == proto::schema::DataType::JSON) {
        // JSON type handling
        auto apply_predicate = [&](auto&& predicate) {
            auto execute_batch = [&predicate](const milvus::Json* data,
                                              const int64_t size,
                                              TargetBitmapView res) {
                apply_predicate_on_batch<milvus::Json>(
                    data, size, res, predicate);
            };
            segment->template ProcessDataChunkForRange<milvus::Json>(
                execute_batch, res, segment_offset, batch_size);
        };

        switch (op_type) {
            case proto::plan::OpType::InnerMatch: {
                apply_predicate([&literal, this](const milvus::Json& data) {
                    auto x =
                        data.template at<std::string_view>(this->nested_path_);
                    if (x.error()) {
                        return false;
                    }
                    return x.value().find(literal) != std::string::npos;
                });
                break;
            }
            case proto::plan::OpType::PrefixMatch: {
                apply_predicate([&literal, this](const milvus::Json& data) {
                    auto x =
                        data.template at<std::string_view>(this->nested_path_);
                    if (x.error()) {
                        return false;
                    }
                    auto data_val = x.value();
                    return data_val.length() >= literal.length() &&
                           std::equal(literal.begin(),
                                      literal.end(),
                                      data_val.begin());
                });
                break;
            }
            case proto::plan::OpType::PostfixMatch: {
                apply_predicate([&literal, this](const milvus::Json& data) {
                    auto x =
                        data.template at<std::string_view>(this->nested_path_);
                    if (x.error()) {
                        return false;
                    }
                    auto data_val = x.value();
                    return data_val.length() >= literal.length() &&
                           std::equal(literal.rbegin(),
                                      literal.rend(),
                                      data_val.rbegin());
                });
                break;
            }
            case proto::plan::OpType::Match: {
                // Use LikePatternMatcher optimized for LIKE patterns
                LikePatternMatcher matcher(literal);
                apply_predicate([&matcher, this](const milvus::Json& data) {
                    auto x =
                        data.template at<std::string_view>(this->nested_path_);
                    if (x.error()) {
                        return false;
                    }
                    return matcher(x.value());
                });
                break;
            }
            default:
                break;
        }
    } else {
        // String/Varchar type handling
        auto apply_predicate = [&](auto&& predicate) {
            auto execute_batch = [&predicate](const std::string_view* data,
                                              const int64_t size,
                                              TargetBitmapView res) {
                apply_predicate_on_batch<std::string_view>(
                    data, size, res, predicate);
            };
            segment->template ProcessDataChunkForRange<std::string_view>(
                execute_batch, res, segment_offset, batch_size);
        };

        switch (op_type) {
            case proto::plan::OpType::InnerMatch: {
                apply_predicate([&literal](const std::string_view& data) {
                    return data.find(literal) != std::string::npos;
                });
                break;
            }
            case proto::plan::OpType::PrefixMatch: {
                apply_predicate([&literal](const std::string_view& data) {
                    return data.length() >= literal.length() &&
                           std::equal(
                               literal.begin(), literal.end(), data.begin());
                });
                break;
            }
            case proto::plan::OpType::PostfixMatch: {
                apply_predicate([&literal](const std::string_view& data) {
                    return data.length() >= literal.length() &&
                           std::equal(
                               literal.rbegin(), literal.rend(), data.rbegin());
                });
                break;
            }
            case proto::plan::OpType::Match: {
                // Use LikePatternMatcher optimized for LIKE patterns
                LikePatternMatcher matcher(literal);
                apply_predicate([&matcher](const std::string_view& data) {
                    return matcher(data);
                });
                break;
            }
            default:
                break;
        }
    }
}

std::optional<TargetBitmap>
NgramInvertedIndex::ExecuteQueryForUT(const std::string& literal,
                                      proto::plan::OpType op_type,
                                      exec::SegmentExpr* segment,
                                      const TargetBitmap* pre_filter) {
    if (!CanHandleLiteral(literal, op_type)) {
        return std::nullopt;
    }

    auto total_count = static_cast<size_t>(Count());
    if (total_count == 0) {
        return TargetBitmap{};
    }

    // Initialize candidates: start with all-true, then AND with pre_filter
    TargetBitmap candidates(total_count, true);
    if (pre_filter != nullptr) {
        candidates &= *pre_filter;
        if (candidates.none()) {
            return std::move(candidates);
        }
    }

    // Phase 1: ngram index query
    ExecutePhase1(literal, op_type, candidates);

    if (candidates.none()) {
        return std::move(candidates);
    }

    // Phase 2: post-filter verification (full segment)
    ExecutePhase2(literal, op_type, segment, candidates, 0, total_count);

    return std::optional<TargetBitmap>(std::move(candidates));
}

}  // namespace milvus::index
