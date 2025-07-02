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

namespace milvus::index {
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

void
NgramInvertedIndex::BuildWithFieldData(const std::vector<FieldDataPtr>& datas) {
    AssertInfo(schema_.data_type() == proto::schema::DataType::String ||
                   schema_.data_type() == proto::schema::DataType::VarChar,
               "schema data type is {}",
               schema_.data_type());
    index_build_begin_ = std::chrono::system_clock::now();
    InvertedIndexTantivy<std::string>::BuildWithFieldData(datas);
}

IndexStatsPtr
NgramInvertedIndex::Upload(const Config& config) {
    finish();
    auto index_build_end = std::chrono::system_clock::now();
    auto index_build_duration =
        std::chrono::duration<double>(index_build_end - index_build_begin_)
            .count();
    LOG_INFO("index build done for ngram index, field id: {}, duration: {}s",
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
        auto index_datas = mem_file_manager_->LoadIndexToMemory(
            file, config[milvus::LOAD_PRIORITY]);
        BinarySet binary_set;
        AssembleIndexDatas(index_datas, binary_set);
        auto index_valid_data = binary_set.GetByName("index_null_offset");
        folly::SharedMutex::WriteHolder lock(mutex_);
        null_offset_.resize((size_t)index_valid_data->size / sizeof(size_t));
        memcpy(null_offset_.data(),
               index_valid_data->data.get(),
               (size_t)index_valid_data->size);
    }

    disk_file_manager_->CacheNgramIndexToDisk(files_value,
                                              config[milvus::LOAD_PRIORITY]);
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
        case proto::plan::OpType::InnerMatch:
            return InnerMatchQuery(literal, segment);
        case proto::plan::OpType::Match:
            return MatchQuery(literal, segment);
        case proto::plan::OpType::PrefixMatch:
            return PrefixMatchQuery(literal, segment);
        case proto::plan::OpType::PostfixMatch:
            return PostfixMatchQuery(literal, segment);
        default:
            LOG_WARN("unsupported op type for ngram index: {}", op_type);
            return std::nullopt;
    }
}

inline void
handle_sub_batch(const std::string_view* data,
                 const bool* valid_data,
                 const int32_t* offsets,
                 const int size,
                 TargetBitmapView res,
                 TargetBitmapView valid_res,
                 std::function<bool(const std::string_view&)> predicate) {
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

std::optional<TargetBitmap>
NgramInvertedIndex::PrefixMatchQuery(const std::string& literal,
                                     exec::SegmentExpr* segment) {
    TargetBitmap bitset{static_cast<size_t>(Count())};
    wrapper_->ngram_match_query(literal, min_gram_, max_gram_, &bitset);

    TargetBitmapView res(bitset);
    TargetBitmap valid(res.size(), true);
    TargetBitmapView valid_res(valid.data(), valid.size());

    auto predicate = [&literal](const std::string_view& data) {
        return data.length() >= literal.length() &&
               std::equal(literal.begin(), literal.end(), data.begin());
    };

    auto execute_sub_batch = [&predicate](const std::string_view* data,
                                          const bool* valid_data,
                                          const int32_t* offsets,
                                          const int size,
                                          TargetBitmapView res,
                                          TargetBitmapView valid_res) {
        handle_sub_batch(
            data, valid_data, offsets, size, res, valid_res, predicate);
    };

    segment->ProcessAllDataChunk<std::string_view>(
        execute_sub_batch, std::nullptr_t{}, res, valid_res);

    return std::optional<TargetBitmap>(std::move(bitset));
}

std::optional<TargetBitmap>
NgramInvertedIndex::PostfixMatchQuery(const std::string& literal,
                                      exec::SegmentExpr* segment) {
    TargetBitmap bitset{static_cast<size_t>(Count())};
    wrapper_->ngram_match_query(literal, min_gram_, max_gram_, &bitset);

    TargetBitmapView res(bitset);
    TargetBitmap valid(res.size(), true);
    TargetBitmapView valid_res(valid.data(), valid.size());

    auto predicate = [&literal](const std::string_view& data) {
        return data.length() >= literal.length() &&
               std::equal(literal.rbegin(), literal.rend(), data.rbegin());
    };

    auto execute_sub_batch = [&predicate](const std::string_view* data,
                                          const bool* valid_data,
                                          const int32_t* offsets,
                                          const int size,
                                          TargetBitmapView res,
                                          TargetBitmapView valid_res) {
        handle_sub_batch(
            data, valid_data, offsets, size, res, valid_res, predicate);
    };

    segment->ProcessAllDataChunk<std::string_view>(
        execute_sub_batch, std::nullptr_t{}, res, valid_res);

    return std::optional<TargetBitmap>(std::move(bitset));
}

std::optional<TargetBitmap>
NgramInvertedIndex::InnerMatchQuery(const std::string& literal,
                                    exec::SegmentExpr* segment) {
    TargetBitmap bitset{static_cast<size_t>(Count())};
    wrapper_->ngram_match_query(literal, min_gram_, max_gram_, &bitset);

    // Post filtering: if the literal length is larger than the max_gram
    // we need to filter out the bitset
    if (literal.length() > max_gram_) {
        TargetBitmapView res(bitset);
        TargetBitmap valid(res.size(), true);
        TargetBitmapView valid_res(valid.data(), valid.size());

        auto predicate = [&literal](const std::string_view& data) {
            return data.find(literal) != std::string::npos;
        };

        auto execute_sub_batch = [&predicate](const std::string_view* data,
                                              const bool* valid_data,
                                              const int32_t* offsets,
                                              const int size,
                                              TargetBitmapView res,
                                              TargetBitmapView valid_res) {
            handle_sub_batch(
                data, valid_data, offsets, size, res, valid_res, predicate);
        };

        segment->ProcessAllDataChunk<std::string_view>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res);
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

    auto predicate = [&matcher](const std::string_view& data) {
        return matcher(data);
    };

    auto execute_sub_batch = [&predicate](const std::string_view* data,
                                          const bool* valid_data,
                                          const int32_t* offsets,
                                          const int size,
                                          TargetBitmapView res,
                                          TargetBitmapView valid_res) {
        handle_sub_batch(
            data, valid_data, offsets, size, res, valid_res, predicate);
    };
    segment->ProcessAllDataChunk<std::string_view>(
        execute_sub_batch, std::nullptr_t{}, res, valid_res);

    return std::optional<TargetBitmap>(std::move(bitset));
}

}  // namespace milvus::index
