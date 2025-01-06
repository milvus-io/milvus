#include <sstream>
#include <fmt/format.h>
#include <set>
#include <iostream>
#include "tantivy-binding.h"

namespace milvus::tantivy {
struct RustArrayWrapper {
    explicit RustArrayWrapper(RustArray array) : array_(array) {
    }

    RustArrayWrapper(RustArrayWrapper&) = delete;
    RustArrayWrapper&
    operator=(RustArrayWrapper&) = delete;

    RustArrayWrapper(RustArrayWrapper&& other) noexcept {
        array_.array = other.array_.array;
        array_.len = other.array_.len;
        array_.cap = other.array_.cap;
        other.array_.array = nullptr;
        other.array_.len = 0;
        other.array_.cap = 0;
    }

    RustArrayWrapper&
    operator=(RustArrayWrapper&& other) noexcept {
        if (this != &other) {
            free();
            array_.array = other.array_.array;
            array_.len = other.array_.len;
            array_.cap = other.array_.cap;
            other.array_.array = nullptr;
            other.array_.len = 0;
            other.array_.cap = 0;
        }
        return *this;
    }

    ~RustArrayWrapper() {
        free();
    }

    void
    debug() {
        std::stringstream ss;
        ss << "[ ";
        for (int i = 0; i < array_.len; i++) {
            ss << array_.array[i] << " ";
        }
        ss << "]";
        std::cout << ss.str() << std::endl;
    }

    RustArray array_;

 private:
    void
    free() {
        if (array_.array != nullptr) {
            free_rust_array(array_);
        }
    }
};

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
        writer_ = tantivy_create_index(field_name, data_type, path);
        path_ = std::string(path);
    }

    explicit TantivyIndexWrapper(const char* path) {
        assert(tantivy_index_exist(path));
        reader_ = tantivy_load_index(path);
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
            tantivy_index_add_bools(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            tantivy_index_add_int8s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            tantivy_index_add_int16s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            tantivy_index_add_int32s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            tantivy_index_add_int64s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            tantivy_index_add_f32s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            tantivy_index_add_f64s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            // TODO: not very efficient, a lot of overhead due to rust-ffi call.
            for (uintptr_t i = 0; i < len; i++) {
                tantivy_index_add_keyword(
                    writer_, static_cast<const std::string*>(array)[i].c_str());
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
            tantivy_index_add_multi_bools(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int8_t>) {
            tantivy_index_add_multi_int8s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int16_t>) {
            tantivy_index_add_multi_int16s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int32_t>) {
            tantivy_index_add_multi_int32s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, int64_t>) {
            tantivy_index_add_multi_int64s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, float>) {
            tantivy_index_add_multi_f32s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, double>) {
            tantivy_index_add_multi_f64s(writer_, array, len);
            return;
        }

        if constexpr (std::is_same_v<T, std::string>) {
            std::vector<const char*> views;
            for (uintptr_t i = 0; i < len; i++) {
                views.push_back(array[i].c_str());
            }
            tantivy_index_add_multi_keywords(writer_, views.data(), len);
            return;
        }

        throw fmt::format(
            "InvertedIndex.add_multi_data: unsupported data type: {}",
            typeid(T).name());
    }

    inline void
    finish() {
        if (!finished_) {
            tantivy_finish_index(writer_);
            writer_ = nullptr;
            reader_ = tantivy_load_index(path_.c_str());
            finished_ = true;
        }
    }

    inline uint32_t
    count() {
        return tantivy_index_count(reader_);
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
        return RustArrayWrapper(array);
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
        return RustArrayWrapper(array);
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
        return RustArrayWrapper(array);
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
        return RustArrayWrapper(array);
    }

    RustArrayWrapper
    prefix_query(const std::string& prefix) {
        auto array = tantivy_prefix_query_keyword(reader_, prefix.c_str());
        return RustArrayWrapper(array);
    }

    RustArrayWrapper
    regex_query(const std::string& pattern) {
        auto array = tantivy_regex_query(reader_, pattern.c_str());
        return RustArrayWrapper(array);
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
