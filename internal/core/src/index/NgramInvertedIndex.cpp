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
#include "exec/expression/Expr.h"
#include "index/JsonIndexBuilder.h"

namespace milvus::index {

const JsonCastType JSON_CAST_TYPE = JsonCastType::FromString("VARCHAR");
// ngram index doesn't need cast function
const JsonCastFunction JSON_CAST_FUNCTION =
    JsonCastFunction::FromString("unknown");

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
    ProcessJsonFieldData<std::string>(
        field_datas,
        this->schema_,
        nested_path_,
        JSON_CAST_TYPE,
        JSON_CAST_FUNCTION,
        // add data
        [this](const std::string* data, int64_t size, int64_t offset) {
            this->wrapper_->template add_array_data<std::string>(
                data, size, offset);
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

    error_recorder_.PrintErrStats();
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
        "duration: {}s",
        schema_.data_type(),
        field_id_,
        index_build_duration);
    return InvertedIndexTantivy<std::string>::Upload(config);
}

void
NgramInvertedIndex::Load(milvus::tracer::TraceContext ctx,
                         const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, INDEX_FILES);
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load ngram index");

    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    auto files_value = index_files.value();
    auto it = std::find_if(
        files_value.begin(), files_value.end(), [](const std::string& file) {
            constexpr std::string_view suffix{"/index_null_offset"};
            return file.size() >= suffix.size() &&
                   std::equal(suffix.rbegin(), suffix.rend(), file.rbegin());
        });
    if (it != files_value.end()) {
        std::vector<std::string> file;
        file.push_back(*it);
        files_value.erase(it);
        auto index_datas =
            mem_file_manager_->LoadIndexToMemory(file, load_priority);
        BinarySet binary_set;
        AssembleIndexDatas(index_datas, binary_set);
        // clear index_datas to free memory early
        index_datas.clear();
        auto index_valid_data = binary_set.GetByName("index_null_offset");
        null_offset_.resize((size_t)index_valid_data->size / sizeof(size_t));
        memcpy(null_offset_.data(),
               index_valid_data->data.get(),
               (size_t)index_valid_data->size);
    }

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
                                 exec::SegmentExpr* segment) {
    if (literal.length() < min_gram_) {
        return std::nullopt;
    }

    switch (op_type) {
        case proto::plan::OpType::Match:
            return MatchQuery(literal, segment);
        case proto::plan::OpType::InnerMatch: {
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
                    literal, segment, predicate, need_post_filter);
            } else {
                auto predicate = [&literal](const std::string_view& data) {
                    return data.find(literal) != std::string::npos;
                };

                return ExecuteQueryWithPredicate<std::string_view>(
                    literal, segment, predicate, need_post_filter);
            }
        }
        case proto::plan::OpType::PrefixMatch: {
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
                    literal, segment, predicate, true);
            } else {
                auto predicate = [&literal](const std::string_view& data) {
                    return data.length() >= literal.length() &&
                           std::equal(
                               literal.begin(), literal.end(), data.begin());
                };

                return ExecuteQueryWithPredicate<std::string_view>(
                    literal, segment, predicate, true);
            }
        }
        case proto::plan::OpType::PostfixMatch: {
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
                    literal, segment, predicate, true);
            } else {
                auto predicate = [&literal](const std::string_view& data) {
                    return data.length() >= literal.length() &&
                           std::equal(
                               literal.rbegin(), literal.rend(), data.rbegin());
                };

                return ExecuteQueryWithPredicate<std::string_view>(
                    literal, segment, predicate, true);
            }
        }
        default:
            LOG_WARN("unsupported op type for ngram index: {}", op_type);
            return std::nullopt;
    }
}

template <typename T>
inline void
handle_batch(const T* data,
             const int size,
             TargetBitmapView res,
             std::function<bool(const T&)> predicate) {
    auto next_off_option = res.find_first();
    while (next_off_option.has_value()) {
        auto next_off = next_off_option.value();
        if (next_off >= size) {
            return;
        }
        if (!predicate(data[next_off])) {
            res[next_off] = false;
        }
        next_off_option = res.find_next(next_off);
    }
}

template <typename T>
std::optional<TargetBitmap>
NgramInvertedIndex::ExecuteQueryWithPredicate(
    const std::string& literal,
    exec::SegmentExpr* segment,
    std::function<bool(const T&)> predicate,
    bool need_post_filter) {
    TargetBitmap bitset{static_cast<size_t>(Count())};
    wrapper_->ngram_match_query(literal, min_gram_, max_gram_, &bitset);

    if (need_post_filter) {
        TargetBitmapView res(bitset);
        TargetBitmap valid(res.size(), true);
        TargetBitmapView valid_res(valid.data(), valid.size());

        auto execute_batch =
            [&predicate](
                const T* data,
                // `valid_data` is not used as the results returned  by ngram_match_query are all valid
                const bool* _valid_data,
                const int32_t* _offsets,
                const int size,
                TargetBitmapView res,
                // the same with `valid_data`
                TargetBitmapView _valid_res) {
                handle_batch(data, size, res, predicate);
            };

        segment->ProcessAllDataChunk<T>(
            execute_batch, std::nullptr_t{}, res, valid_res);
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
                               exec::SegmentExpr* segment) {
    TargetBitmap bitset{static_cast<size_t>(Count())};
    auto literals = split_by_wildcard(literal);
    for (const auto& l : literals) {
        if (l.length() < min_gram_) {
            return std::nullopt;
        }
        wrapper_->ngram_match_query(l, min_gram_, max_gram_, &bitset);
    }

    TargetBitmapView res(bitset);
    TargetBitmap valid(res.size(), true);
    TargetBitmapView valid_res(valid.data(), valid.size());

    PatternMatchTranslator translator;
    auto regex_pattern = translator(literal);
    RegexMatcher matcher(regex_pattern);

    if (schema_.data_type() == proto::schema::DataType::JSON) {
        auto predicate = [&literal, &matcher, this](const milvus::Json& data) {
            auto x = data.template at<std::string_view>(this->nested_path_);
            if (x.error()) {
                return false;
            }
            auto data_val = x.value();
            return matcher(data_val);
        };

        auto execute_batch_json =
            [&predicate](
                const milvus::Json* data,
                // `valid_data` is not used as the results returned  by ngram_match_query are all valid
                const bool* _valid_data,
                const int32_t* _offsets,
                const int size,
                TargetBitmapView res,
                // the same with `valid_data`
                TargetBitmapView _valid_res,
                std::string val) {
                handle_batch<milvus::Json>(data, size, res, predicate);
            };

        segment->ProcessAllDataChunk<milvus::Json>(
            execute_batch_json, std::nullptr_t{}, res, valid_res, literal);
    } else {
        auto predicate = [&matcher](const std::string_view& data) {
            return matcher(data);
        };

        auto execute_batch =
            [&predicate](
                const std::string_view* data,
                // `valid_data` is not used as the results returned  by ngram_match_query are all valid
                const bool* _valid_data,
                const int32_t* _offsets,
                const int size,
                TargetBitmapView res,
                // the same with `valid_data`
                TargetBitmapView _valid_res) {
                handle_batch<std::string_view>(data, size, res, predicate);
            };
        segment->ProcessAllDataChunk<std::string_view>(
            execute_batch, std::nullptr_t{}, res, valid_res);
    }

    return std::optional<TargetBitmap>(std::move(bitset));
}

}  // namespace milvus::index
