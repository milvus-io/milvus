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

#include <chrono>

#include "exec/expression/Expr.h"
#include "index/JsonIndexBuilder.h"

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

std::optional<TargetBitmap>
NgramInvertedIndex::ExecuteQuery(const std::string& literal,
                                 proto::plan::OpType op_type,
                                 exec::SegmentExpr* segment,
                                 const TargetBitmap* pre_filter) {
    tracer::AutoSpan span(
        "NgramInvertedIndex::ExecuteQuery", tracer::GetRootSpan(), true);
    if (pre_filter != nullptr && pre_filter->none()) {
        return TargetBitmap(pre_filter->size(), false);
    }
    if (literal.length() < min_gram_) {
        return std::nullopt;
    }
    if (Count() == 0) {
        return TargetBitmap{};
    }

    switch (op_type) {
        case proto::plan::OpType::Match:
            return MatchQuery(literal, segment, pre_filter);
        case proto::plan::OpType::InnerMatch: {
            span.GetSpan()->SetAttribute("op_type", "InnerMatch");
            span.GetSpan()->SetAttribute("query_literal_length",
                                         static_cast<int>(literal.length()));
            span.GetSpan()->SetAttribute("min_gram", min_gram_);
            span.GetSpan()->SetAttribute("max_gram", max_gram_);
            bool need_post_filter = literal.length() > max_gram_;

            if (schema_.data_type() == proto::schema::DataType::JSON) {
                auto predicate = [&literal, this](const milvus::Json& data) {
                    auto x =
                        data.template at<std::string_view>(this->nested_path_);
                    if (x.error()) {
                        return false;
                    }
                    auto data_val = x.value();
                    return data_val.find(literal) != std::string::npos;
                };

                return ExecuteQueryWithPredicate<milvus::Json>(
                    literal, segment, predicate, need_post_filter, pre_filter);
            } else {
                auto predicate = [&literal](const std::string_view& data) {
                    return data.find(literal) != std::string::npos;
                };

                return ExecuteQueryWithPredicate<std::string_view>(
                    literal, segment, predicate, need_post_filter, pre_filter);
            }
        }
        case proto::plan::OpType::PrefixMatch: {
            span.GetSpan()->SetAttribute("op_type", "PrefixMatch");
            span.GetSpan()->SetAttribute("query_literal_length",
                                         static_cast<int>(literal.length()));
            span.GetSpan()->SetAttribute("min_gram", min_gram_);
            span.GetSpan()->SetAttribute("max_gram", max_gram_);
            if (schema_.data_type() == proto::schema::DataType::JSON) {
                auto predicate = [&literal, this](const milvus::Json& data) {
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
                };

                return ExecuteQueryWithPredicate<milvus::Json>(
                    literal, segment, predicate, true, pre_filter);
            } else {
                auto predicate = [&literal](const std::string_view& data) {
                    return data.length() >= literal.length() &&
                           std::equal(
                               literal.begin(), literal.end(), data.begin());
                };

                return ExecuteQueryWithPredicate<std::string_view>(
                    literal, segment, predicate, true, pre_filter);
            }
        }
        case proto::plan::OpType::PostfixMatch: {
            span.GetSpan()->SetAttribute("op_type", "PostfixMatch");
            span.GetSpan()->SetAttribute("query_literal_length",
                                         static_cast<int>(literal.length()));
            span.GetSpan()->SetAttribute("min_gram", min_gram_);
            span.GetSpan()->SetAttribute("max_gram", max_gram_);
            if (schema_.data_type() == proto::schema::DataType::JSON) {
                auto predicate = [&literal, this](const milvus::Json& data) {
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
                };

                return ExecuteQueryWithPredicate<milvus::Json>(
                    literal, segment, predicate, true, pre_filter);
            } else {
                auto predicate = [&literal](const std::string_view& data) {
                    return data.length() >= literal.length() &&
                           std::equal(
                               literal.rbegin(), literal.rend(), data.rbegin());
                };

                return ExecuteQueryWithPredicate<std::string_view>(
                    literal, segment, predicate, true, pre_filter);
            }
        }
        default:
            LOG_WARN("unsupported op type for ngram index: {}", op_type);
            return std::nullopt;
    }
}

template <typename T, typename Predicate>
inline void
handle_batch(const T* data,
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

template <typename T, typename Predicate>
std::optional<TargetBitmap>
NgramInvertedIndex::ExecuteQueryWithPredicate(const std::string& literal,
                                              exec::SegmentExpr* segment,
                                              Predicate&& predicate,
                                              bool need_post_filter,
                                              const TargetBitmap* pre_filter) {
    auto total_count = static_cast<size_t>(Count());

    AssertInfo(total_count > 0, "total_count should be greater than 0");

    // Calculate pre_filter stats for strategy selection
    size_t candidate_count = (pre_filter != nullptr && !pre_filter->empty())
                                 ? pre_filter->count()
                                 : total_count;
    double pre_filter_hit_rate = 1.0 * candidate_count / total_count;

    // Phase 1: ngram index query with adaptive strategy
    TargetBitmap bitset(total_count, true);
    if (pre_filter != nullptr && !pre_filter->empty()) {
        bitset &= *pre_filter;
    }

    if (ShouldUseBatchStrategy(pre_filter_hit_rate)) {
        TargetBitmap ngram_bitset{total_count};
        wrapper_->ngram_match_query(
            literal, min_gram_, max_gram_, &ngram_bitset);
        bitset &= ngram_bitset;
    } else {
        std::vector<std::string> literals_vec = {literal};
        auto sorted_terms =
            wrapper_->ngram_tokenize(literals_vec, min_gram_, max_gram_);
        AssertInfo(!sorted_terms.empty(),
                   "ngram_tokenize should not return empty for valid literal");
        ApplyIterativeNgramFilter(sorted_terms, total_count, bitset);
    }

    if (bitset.none()) {
        return std::move(bitset);
    }

    auto after_pre_filter_count = bitset.count();
    auto final_result_count = after_pre_filter_count;

    // Phase 2: post-filter
    if (need_post_filter) {
        TargetBitmapView res(bitset);

        auto execute_batch = [&predicate](const T* data,
                                          const int64_t size,
                                          TargetBitmapView res) {
            handle_batch(data, size, res, predicate);
        };

        segment->template ProcessAllDataChunkBatched<T>(execute_batch, res);

        final_result_count = bitset.count();
    }
    if (auto root_span = tracer::GetRootSpan()) {
        double hit_rate_before_phase2 =
            1.0 * after_pre_filter_count / total_count;
        double final_hit_rate = 1.0 * final_result_count / total_count;

        root_span->SetAttribute("need_post_filter", need_post_filter);
        root_span->SetAttribute("pre_filter_hit_rate", pre_filter_hit_rate);
        root_span->SetAttribute("hit_rate_before_phase2",
                                hit_rate_before_phase2);
        root_span->SetAttribute("final_hit_rate", final_hit_rate);
        root_span->SetAttribute("total_count", static_cast<int>(total_count));
    }

    return std::optional<TargetBitmap>(std::move(bitset));
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

std::optional<TargetBitmap>
NgramInvertedIndex::MatchQuery(const std::string& literal,
                               exec::SegmentExpr* segment,
                               const TargetBitmap* pre_filter) {
    if (auto root_span = tracer::GetRootSpan()) {
        root_span->SetAttribute("match_query_literal_length",
                                static_cast<int>(literal.length()));
        root_span->SetAttribute("match_query_min_gram", min_gram_);
        root_span->SetAttribute("match_query_max_gram", max_gram_);
    }

    auto total_count = static_cast<size_t>(Count());
    AssertInfo(total_count > 0, "total_count should be greater than 0");

    auto literals = split_by_wildcard(literal);
    for (const auto& l : literals) {
        if (l.length() < min_gram_) {
            return std::nullopt;
        }
    }

    // Calculate pre_filter stats for strategy selection
    size_t candidate_count = (pre_filter != nullptr && !pre_filter->empty())
                                 ? pre_filter->count()
                                 : total_count;
    double pre_filter_hit_rate = 1.0 * candidate_count / total_count;

    TargetBitmap bitset(total_count, true);
    if (pre_filter != nullptr && !pre_filter->empty()) {
        bitset &= *pre_filter;
    }

    if (ShouldUseBatchStrategy(pre_filter_hit_rate)) {
        for (const auto& l : literals) {
            TargetBitmap tmp_bitset{total_count};
            wrapper_->ngram_match_query(l, min_gram_, max_gram_, &tmp_bitset);
            bitset &= tmp_bitset;
        }
    } else {
        auto sorted_terms =
            wrapper_->ngram_tokenize(literals, min_gram_, max_gram_);
        AssertInfo(!sorted_terms.empty(),
                   "ngram_tokenize returned empty sorted_terms for iterative "
                   "strategy");
        ApplyIterativeNgramFilter(sorted_terms, total_count, bitset);
    }

    if (bitset.none()) {
        return std::move(bitset);
    }

    auto after_pre_filter_count = bitset.count();

    TargetBitmapView res(bitset);

    PatternMatchTranslator translator;
    auto regex_pattern = translator(literal);
    RegexMatcher matcher(regex_pattern);

    if (schema_.data_type() == proto::schema::DataType::JSON) {
        auto predicate = [&matcher, this](const milvus::Json& data) {
            auto x = data.template at<std::string_view>(this->nested_path_);
            if (x.error()) {
                return false;
            }
            return matcher(x.value());
        };

        auto execute_batch = [&predicate](const milvus::Json* data,
                                          const int64_t size,
                                          TargetBitmapView res) {
            handle_batch<milvus::Json>(data, size, res, predicate);
        };

        segment->template ProcessAllDataChunkBatched<milvus::Json>(
            execute_batch, res);
    } else {
        auto predicate = [&matcher](const std::string_view& data) {
            return matcher(data);
        };

        auto execute_batch = [&predicate](const std::string_view* data,
                                          const int64_t size,
                                          TargetBitmapView res) {
            handle_batch<std::string_view>(data, size, res, predicate);
        };

        segment->template ProcessAllDataChunkBatched<std::string_view>(
            execute_batch, res);
    }

    auto final_result_count = bitset.count();

    if (auto root_span = tracer::GetRootSpan()) {
        double hit_rate_before_phase2 =
            1.0 * after_pre_filter_count / total_count;
        double final_hit_rate = 1.0 * final_result_count / total_count;

        root_span->SetAttribute("match_pre_filter_hit_rate",
                                pre_filter_hit_rate);
        root_span->SetAttribute("match_hit_rate_before_phase2",
                                hit_rate_before_phase2);
        root_span->SetAttribute("match_final_hit_rate", final_hit_rate);
        root_span->SetAttribute("match_total_count",
                                static_cast<int>(total_count));
    }

    return std::optional<TargetBitmap>(std::move(bitset));
}

}  // namespace milvus::index
