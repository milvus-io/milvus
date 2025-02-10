#include <sstream>
#include <fmt/format.h>
#include <set>
#include <iostream>
#include <map>
#include <vector>

#include "common/EasyAssert.h"
#include "rust-array.h"
#include "tantivy-binding.h"

namespace milvus::tantivy {

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

struct TantivyIndexWrapper {
    using IndexWriter = void*;
    using IndexReader = void*;

    TantivyIndexWrapper() = default;

    TantivyIndexWrapper(TantivyIndexWrapper&) = delete;
    TantivyIndexWrapper&
    operator=(TantivyIndexWrapper&) = delete;

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

    TantivyIndexWrapper(const char* field_name,
                        TantivyDataType data_type,
                        const char* path) {
        auto res = RustResultWrapper(
            tantivy_create_index(field_name, data_type, path));
        AssertInfo(res.result_->success,
                   "failed to create index: {}",
                   res.result_->error);
        writer_ = res.result_->value.ptr._0;
        path_ = std::string(path);
    }

    explicit TantivyIndexWrapper(const char* path) {
        assert(tantivy_index_exist(path));
        auto res = RustResultWrapper(tantivy_load_index(path));
        AssertInfo(res.result_->success,
                   "failed to load index: {}",
                   res.result_->error);
        reader_ = res.result_->value.ptr._0;
        path_ = std::string(path);
    }

    ~TantivyIndexWrapper() {
        free();
    }

    template <typename T>
    void
    add_data(const T* array, uintptr_t len) {
        assert(!finished_);

        if constexpr (std::is_same_v<T, bool>) {
            auto res =
                RustResultWrapper(tantivy_index_add_bools(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add bools: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            auto res =
                RustResultWrapper(tantivy_index_add_int8s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int8s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int16s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int16s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int32s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_int64s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add int64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            auto res =
                RustResultWrapper(tantivy_index_add_f32s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add f32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            auto res =
                RustResultWrapper(tantivy_index_add_f64s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add f64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            // TODO: not very efficient, a lot of overhead due to rust-ffi call.
            for (uintptr_t i = 0; i < len; i++) {
                auto res = RustResultWrapper(tantivy_index_add_keyword(
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
    add_multi_data(const T* array, uintptr_t len) {
        assert(!finished_);

        if constexpr (std::is_same_v<T, bool>) {
            auto res = RustResultWrapper(
                tantivy_index_add_multi_bools(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi bools: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_multi_int8s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int8s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_multi_int16s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int16s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_multi_int32s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            auto res = RustResultWrapper(
                tantivy_index_add_multi_int64s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi int64s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            auto res = RustResultWrapper(
                tantivy_index_add_multi_f32s(writer_, array, len));
            AssertInfo(res.result_->success,
                       "failed to add multi f32s: {}",
                       res.result_->error);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            auto res = RustResultWrapper(
                tantivy_index_add_multi_f64s(writer_, array, len));
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
                tantivy_index_add_multi_keywords(writer_, views.data(), len));
            AssertInfo(res.result_->success,
                       "failed to add multi keywords: {}",
                       res.result_->error);
            return;
        }

        throw fmt::format(
            "InvertedIndex.add_multi_data: unsupported data type: {}",
            typeid(T).name());
    }

    inline void
    finish() {
        if (!finished_) {
            auto res = RustResultWrapper(tantivy_finish_index(writer_));
            AssertInfo(res.result_->success,
                       "failed to finish index: {}",
                       res.result_->error);
            writer_ = nullptr;
            auto load_res =
                RustResultWrapper(tantivy_load_index(path_.c_str()));
            AssertInfo(load_res.result_->success,
                       "failed to load index: {}",
                       load_res.result_->error);
            reader_ = load_res.result_->value.ptr._0;
            finished_ = true;
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
