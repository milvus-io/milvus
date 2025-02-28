#include <assert.h>
#include <sstream>
#include <fmt/format.h>
#include <set>
#include <iostream>
#include <map>
#include <vector>
#include <type_traits>

#include "common/EasyAssert.h"
#include "tantivy-binding.h"
#include "rust-binding.h"
#include "rust-array.h"
#include "rust-hashmap.h"

namespace milvus::tantivy {
using Map = std::map<std::string, std::string>;

static constexpr const char* DEFAULT_TOKENIZER_NAME = "milvus_tokenizer";
static const char* DEFAULT_analyzer_params = "{}";
static constexpr uintptr_t DEFAULT_NUM_THREADS =
    1;  // Every field with index writer will generate a thread, make huge thread amount, wait for refactoring.
static constexpr uintptr_t DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES =
    DEFAULT_NUM_THREADS * 15 * 1024 * 1024;

template <typename T>
inline TantivyDataType
guess_data_type() {
    if constexpr (std::is_same_v<T, bool>) {
        return TantivyDataType::Bool;
    }

    if constexpr (std::is_integral_v<T>) {
        return TantivyDataType::I64;
    }

    if constexpr (std::is_floating_point_v<T>) {
        return TantivyDataType::F64;
    }

    throw fmt::format("guess_data_type: unsupported data type: {}",
                      typeid(T).name());
}

// TODO: should split this into IndexWriter & IndexReader.
struct TantivyIndexWrapper {
    using IndexWriter = void*;
    using IndexReader = void*;

    NO_COPY_OR_ASSIGN(TantivyIndexWrapper);

    TantivyIndexWrapper() = default;

    TantivyIndexWrapper(TantivyIndexWrapper&& other) noexcept {
        writer_ = other.writer_;
        reader_ = other.reader_;
        finished_ = other.finished_;
        path_ = other.path_;
        other.writer_ = nullptr;
        other.reader_ = nullptr;
        other.finished_ = false;
        other.path_ = "";
    }

    TantivyIndexWrapper&
    operator=(TantivyIndexWrapper&& other) noexcept {
        if (this != &other) {
            free();
            writer_ = other.writer_;
            reader_ = other.reader_;
            path_ = other.path_;
            finished_ = other.finished_;
            other.writer_ = nullptr;
            other.reader_ = nullptr;
            other.finished_ = false;
            other.path_ = "";
        }
        return *this;
    }

    // create index writer for non-text type.
    TantivyIndexWrapper(const char* field_name,
                        TantivyDataType data_type,
                        const char* path,
                        bool inverted_single_semgnent = false,
                        uintptr_t num_threads = DEFAULT_NUM_THREADS,
                        uintptr_t overall_memory_budget_in_bytes =
                            DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES) {
        RustResultWrapper res;
        if (inverted_single_semgnent) {
            res = RustResultWrapper(tantivy_create_index_with_single_segment(
                field_name, data_type, path));
        } else {
            res = RustResultWrapper(
                tantivy_create_index(field_name,
                                     data_type,
                                     path,
                                     num_threads,
                                     overall_memory_budget_in_bytes));
        }
        AssertInfo(res.result_->success,
                   "failed to create index: {}",
                   res.result_->error);
        writer_ = res.result_->value.ptr._0;
        path_ = std::string(path);
    }

    // load index. create index reader.
    explicit TantivyIndexWrapper(const char* path) {
        assert(tantivy_index_exist(path));
        auto res = RustResultWrapper(tantivy_load_index(path));
        AssertInfo(res.result_->success,
                   "failed to load index: {}",
                   res.result_->error);
        reader_ = res.result_->value.ptr._0;
        path_ = std::string(path);
    }

    // create index writer for text type with tokenizer.
    TantivyIndexWrapper(const char* field_name,
                        bool in_ram,
                        const char* path,
                        const char* tokenizer_name = DEFAULT_TOKENIZER_NAME,
                        const char* analyzer_params = DEFAULT_analyzer_params,
                        uintptr_t num_threads = DEFAULT_NUM_THREADS,
                        uintptr_t overall_memory_budget_in_bytes =
                            DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES) {
        auto res = RustResultWrapper(
            tantivy_create_text_writer(field_name,
                                       path,
                                       tokenizer_name,
                                       analyzer_params,
                                       num_threads,
                                       overall_memory_budget_in_bytes,
                                       in_ram));
        AssertInfo(res.result_->success,
                   "failed to create text writer: {}",
                   res.result_->error);
        writer_ = res.result_->value.ptr._0;
        path_ = std::string(path);
    }

    // create reader.
    void
    create_reader() {
        if (writer_ != nullptr) {
            auto res =
                RustResultWrapper(tantivy_create_reader_from_writer(writer_));
            AssertInfo(res.result_->success,
                       "failed to create reader from writer: {}",
                       res.result_->error);
            reader_ = res.result_->value.ptr._0;
        } else if (!path_.empty()) {
            assert(tantivy_index_exist(path_.c_str()));
            auto res = RustResultWrapper(tantivy_load_index(path_.c_str()));
            AssertInfo(res.result_->success,
                       "failed to load index: {}",
                       res.result_->error);
            reader_ = res.result_->value.ptr._0;
        }
    }

    ~TantivyIndexWrapper() {
        free();
    }

    void
    register_tokenizer(const char* tokenizer_name,
                       const char* analyzer_params) {
        if (reader_ != nullptr) {
            auto res = RustResultWrapper(tantivy_register_tokenizer(
                reader_, tokenizer_name, analyzer_params));
            AssertInfo(res.result_->success,
                       "failed to register tokenizer: {}",
                       res.result_->error);
        }
    }

    template <typename T>
    void
    add_data(const T* array, uintptr_t len, int64_t offset_begin) {
        assert(!finished_);

        if constexpr (std::is_same_v<T, bool>) {
            auto res = RustResultWrapper(
                tantivy_index_add_bools(writer_, array, len, offset_begin));
            AssertInfo(res.result_->success,
                       "failed to add bools: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int8s(writer_, array, len, offset_begin));
            AssertInfo(res.result_->success,
                       "failed to add int8s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int16s(writer_, array, len, offset_begin));
            AssertInfo(res.result_->success,
                       "failed to add int16s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int32s(writer_, array, len, offset_begin));
            AssertInfo(res.result_->success,
                       "failed to add int32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int64s(writer_, array, len, offset_begin));
            AssertInfo(res.result_->success,
                       "failed to add int64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            auto res = RustResultWrapper(
                tantivy_index_add_f32s(writer_, array, len, offset_begin));
            AssertInfo(res.result_->success,
                       "failed to add f32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            auto res = RustResultWrapper(
                tantivy_index_add_f64s(writer_, array, len, offset_begin));
            AssertInfo(res.result_->success,
                       "failed to add f64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            // TODO: not very efficient, a lot of overhead due to rust-ffi call.
            for (uintptr_t i = 0; i < len; i++) {
                auto res = RustResultWrapper(tantivy_index_add_string(
                    writer_,
                    static_cast<const std::string*>(array)[i].c_str(),
                    offset_begin + i));
                AssertInfo(res.result_->success,
                           "failed to add string: {}",
                           res.result_->error);
            }
            return;
        }

        throw fmt::format("InvertedIndex.add_data: unsupported data type: {}",
                          typeid(T).name());
    }

    template <typename T>
    void
    add_array_data(const T* array, uintptr_t len, int64_t offset) {
        assert(!finished_);

        if constexpr (std::is_same_v<T, bool>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_bools(writer_, array, len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi bools: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int8s(writer_, array, len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi int8s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int16s(writer_, array, len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi int16s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int32s(writer_, array, len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi int32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int64s(writer_, array, len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi int64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_f32s(writer_, array, len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi f32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_f64s(writer_, array, len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi f64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            std::vector<const char*> views;
            for (uintptr_t i = 0; i < len; i++) {
                views.push_back(array[i].c_str());
            }
            auto res = RustResultWrapper(tantivy_index_add_array_keywords(
                writer_, views.data(), len, offset));
            AssertInfo(res.result_->success,
                       "failed to add multi keywords: {}",
                       res.result_->error);
            return;
        }

        throw fmt::format(
            "InvertedIndex.add_array_data: unsupported data type: {}",
            typeid(T).name());
    }

    template <typename T>
    void
    add_data_by_single_segment_writer(const T* array, uintptr_t len) {
        assert(!finished_);

        if constexpr (std::is_same_v<T, bool>) {
            auto res = RustResultWrapper(
                tantivy_index_add_bools_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add bools: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int8s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int8s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int16s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int16s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int32s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int64s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            auto res = RustResultWrapper(
                tantivy_index_add_f32s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add f32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            auto res = RustResultWrapper(
                tantivy_index_add_f64s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add f64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            // TODO: not very efficient, a lot of overhead due to rust-ffi call.
            for (uintptr_t i = 0; i < len; i++) {
                auto res = RustResultWrapper(
                    tantivy_index_add_string_by_single_segment_writer(
                        writer_,
                        static_cast<const std::string*>(array)[i].c_str()));
                AssertInfo(res.result_->success,
                           "failed to add string: {}",
                           res.result_->error);
            }
            return;
        }

        throw fmt::format("InvertedIndex.add_data: unsupported data type: {}",
                          typeid(T).name());
    }

    template <typename T>
    void
    add_array_data_by_single_segment_writer(const T* array, uintptr_t len) {
        assert(!finished_);

        if constexpr (std::is_same_v<T, bool>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_bools_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi bools: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int8s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int8s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int16s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int16s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int32s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_int64s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_f32s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi f32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            auto res = RustResultWrapper(
                tantivy_index_add_array_f64s_by_single_segment_writer(
                    writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi f64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            std::vector<const char*> views;
            for (uintptr_t i = 0; i < len; i++) {
                views.push_back(array[i].c_str());
            }
            auto res = RustResultWrapper(
                tantivy_index_add_array_keywords_by_single_segment_writer(
                    writer_, views.data(), len));
            AssertInfo(res.result_->success,
                       "failed to add multi keywords: {}",
                       res.result_->error);
            return;
        }

        throw fmt::format(
            "InvertedIndex.add_array_data: unsupported data type: {}",
            typeid(T).name());
    }

    inline void
    finish() {
        if (finished_) {
            return;
        }

        auto res = RustResultWrapper(tantivy_finish_index(writer_));
        AssertInfo(res.result_->success,
                   "failed to finish index: {}",
                   res.result_->error);
        writer_ = nullptr;
        finished_ = true;
    }

    inline void
    commit() {
        if (writer_ != nullptr) {
            auto res = RustResultWrapper(tantivy_commit_index(writer_));
            AssertInfo(res.result_->success,
                       "failed to commit index: {}",
                       res.result_->error);
        }
    }

    inline void
    reload() {
        if (reader_ != nullptr) {
            auto res = RustResultWrapper(tantivy_reload_index(reader_));
            AssertInfo(res.result_->success,
                       "failed to reload index: {}",
                       res.result_->error);
        }
    }

    inline uint32_t
    count() {
        auto res = RustResultWrapper(tantivy_index_count(reader_));
        AssertInfo(res.result_->success,
                   "failed to get count: {}",
                   res.result_->error);
        return res.result_->value.u32._0;
    }

 public:
    template <typename T>
    RustArrayWrapper
    term_query(T term) {
        auto array = [&]() {
            if constexpr (std::is_same_v<T, bool>) {
                return tantivy_term_query_bool(reader_, term);
            }

            if constexpr (std::is_integral_v<T>) {
                return tantivy_term_query_i64(reader_,
                                              static_cast<int64_t>(term));
            }

            if constexpr (std::is_floating_point_v<T>) {
                return tantivy_term_query_f64(reader_,
                                              static_cast<double>(term));
            }

            if constexpr (std::is_same_v<T, std::string>) {
                return tantivy_term_query_keyword(
                    reader_, static_cast<std::string>(term).c_str());
            }

            throw fmt::format(
                "InvertedIndex.term_query: unsupported data type: {}",
                typeid(T).name());
        }();

        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.term_query: {}",
                   res.result_->error);
        AssertInfo(res.result_->value.tag == Value::Tag::RustArray,
                   "TantivyIndexWrapper.term_query: invalid result type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

    template <typename T>
    RustArrayWrapper
    lower_bound_range_query(T lower_bound, bool inclusive) {
        auto array = [&]() {
            if constexpr (std::is_same_v<T, bool>) {
                return tantivy_lower_bound_range_query_bool(
                    reader_, static_cast<bool>(lower_bound), inclusive);
            }

            if constexpr (std::is_integral_v<T>) {
                return tantivy_lower_bound_range_query_i64(
                    reader_, static_cast<int64_t>(lower_bound), inclusive);
            }

            if constexpr (std::is_floating_point_v<T>) {
                return tantivy_lower_bound_range_query_f64(
                    reader_, static_cast<double>(lower_bound), inclusive);
            }

            if constexpr (std::is_same_v<T, std::string>) {
                return tantivy_lower_bound_range_query_keyword(
                    reader_,
                    static_cast<std::string>(lower_bound).c_str(),
                    inclusive);
            }

            throw fmt::format(
                "InvertedIndex.lower_bound_range_query: unsupported data type: "
                "{}",
                typeid(T).name());
        }();
        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.lower_bound_range_query: {}",
                   res.result_->error);
        AssertInfo(
            res.result_->value.tag == Value::Tag::RustArray,
            "TantivyIndexWrapper.lower_bound_range_query: invalid result "
            "type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

    template <typename T>
    RustArrayWrapper
    upper_bound_range_query(T upper_bound, bool inclusive) {
        auto array = [&]() {
            if constexpr (std::is_same_v<T, bool>) {
                return tantivy_upper_bound_range_query_bool(
                    reader_, static_cast<bool>(upper_bound), inclusive);
            }

            if constexpr (std::is_integral_v<T>) {
                return tantivy_upper_bound_range_query_i64(
                    reader_, static_cast<int64_t>(upper_bound), inclusive);
            }

            if constexpr (std::is_floating_point_v<T>) {
                return tantivy_upper_bound_range_query_f64(
                    reader_, static_cast<double>(upper_bound), inclusive);
            }

            if constexpr (std::is_same_v<T, std::string>) {
                return tantivy_upper_bound_range_query_keyword(
                    reader_,
                    static_cast<std::string>(upper_bound).c_str(),
                    inclusive);
            }

            throw fmt::format(
                "InvertedIndex.upper_bound_range_query: unsupported data type: "
                "{}",
                typeid(T).name());
        }();
        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.upper_bound_range_query: {}",
                   res.result_->error);
        AssertInfo(
            res.result_->value.tag == Value::Tag::RustArray,
            "TantivyIndexWrapper.upper_bound_range_query: invalid result "
            "type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

    template <typename T>
    RustArrayWrapper
    range_query(T lower_bound,
                T upper_bound,
                bool lb_inclusive,
                bool ub_inclusive) {
        auto array = [&]() {
            if constexpr (std::is_same_v<T, bool>) {
                return tantivy_range_query_bool(reader_,
                                                static_cast<bool>(lower_bound),
                                                static_cast<bool>(upper_bound),
                                                lb_inclusive,
                                                ub_inclusive);
            }

            if constexpr (std::is_integral_v<T>) {
                return tantivy_range_query_i64(
                    reader_,
                    static_cast<int64_t>(lower_bound),
                    static_cast<int64_t>(upper_bound),
                    lb_inclusive,
                    ub_inclusive);
            }

            if constexpr (std::is_floating_point_v<T>) {
                return tantivy_range_query_f64(reader_,
                                               static_cast<double>(lower_bound),
                                               static_cast<double>(upper_bound),
                                               lb_inclusive,
                                               ub_inclusive);
            }

            if constexpr (std::is_same_v<T, std::string>) {
                return tantivy_range_query_keyword(
                    reader_,
                    static_cast<std::string>(lower_bound).c_str(),
                    static_cast<std::string>(upper_bound).c_str(),
                    lb_inclusive,
                    ub_inclusive);
            }

            throw fmt::format(
                "InvertedIndex.range_query: unsupported data type: {}",
                typeid(T).name());
        }();
        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.range_query: {}",
                   res.result_->error);
        AssertInfo(res.result_->value.tag == Value::Tag::RustArray,
                   "TantivyIndexWrapper.range_query: invalid result type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

    RustArrayWrapper
    prefix_query(const std::string& prefix) {
        auto array = tantivy_prefix_query_keyword(reader_, prefix.c_str());
        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.prefix_query: {}",
                   res.result_->error);
        AssertInfo(res.result_->value.tag == Value::Tag::RustArray,
                   "TantivyIndexWrapper.prefix_query: invalid result type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

    RustArrayWrapper
    regex_query(const std::string& pattern) {
        auto array = tantivy_regex_query(reader_, pattern.c_str());
        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.regex_query: {}",
                   res.result_->error);
        AssertInfo(res.result_->value.tag == Value::Tag::RustArray,
                   "TantivyIndexWrapper.regex_query: invalid result type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

    RustArrayWrapper
    match_query(const std::string& query) {
        auto array = tantivy_match_query(reader_, query.c_str());
        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.match_query: {}",
                   res.result_->error);
        AssertInfo(res.result_->value.tag == Value::Tag::RustArray,
                   "TantivyIndexWrapper.match_query: invalid result type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

    RustArrayWrapper
    phrase_match_query(const std::string& query, uint32_t slop) {
        auto array = tantivy_phrase_match_query(reader_, query.c_str(), slop);
        auto res = RustResultWrapper(array);
        AssertInfo(res.result_->success,
                   "TantivyIndexWrapper.phrase_match_query: {}",
                   res.result_->error);
        AssertInfo(
            res.result_->value.tag == Value::Tag::RustArray,
            "TantivyIndexWrapper.phrase_match_query: invalid result type");
        return RustArrayWrapper(std::move(res.result_->value.rust_array._0));
    }

 public:
    inline IndexWriter
    get_writer() {
        return writer_;
    }

    inline IndexReader
    get_reader() {
        return reader_;
    }

    void
    free() {
        if (writer_ != nullptr) {
            tantivy_free_index_writer(writer_);
            writer_ = nullptr;
        }

        if (reader_ != nullptr) {
            tantivy_free_index_reader(reader_);
            reader_ = nullptr;
        }
    }

 private:
    void
    check_search() {
        // TODO
    }

 private:
    bool finished_ = false;
    IndexWriter writer_ = nullptr;
    IndexReader reader_ = nullptr;
    std::string path_;
};
}  // namespace milvus::tantivy
