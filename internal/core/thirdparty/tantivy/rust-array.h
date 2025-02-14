#pragma once

#include <iostream>
#include <memory>
#include <sstream>

#include "tantivy-binding.h"
#include "rust-binding.h"

namespace milvus::tantivy {

struct RustArrayWrapper {
    NO_COPY_OR_ASSIGN(RustArrayWrapper);

    explicit RustArrayWrapper(RustArray&& array) {
        array_.array = array.array;
        array_.len = array.len;
        array_.cap = array.cap;
        array.array = nullptr;
        array.len = 0;
        array.cap = 0;
    }

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

struct RustArrayI64Wrapper {
    NO_COPY_OR_ASSIGN(RustArrayI64Wrapper);

    explicit RustArrayI64Wrapper(RustArrayI64&& array) {
        array_.array = array.array;
        array_.len = array.len;
        array_.cap = array.cap;
        array.array = nullptr;
        array.len = 0;
        array.cap = 0;
    }

    RustArrayI64Wrapper(RustArrayI64Wrapper&& other) noexcept {
        array_.array = other.array_.array;
        array_.len = other.array_.len;
        array_.cap = other.array_.cap;
        other.array_.array = nullptr;
        other.array_.len = 0;
        other.array_.cap = 0;
    }

    RustArrayI64Wrapper&
    operator=(RustArrayI64Wrapper&& other) noexcept {
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

    ~RustArrayI64Wrapper() {
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

    RustArrayI64 array_;

 private:
    void
    free() {
        if (array_.array != nullptr) {
            free_rust_array_i64(array_);
        }
    }
};
struct RustResultWrapper {
    NO_COPY_OR_ASSIGN(RustResultWrapper);

    RustResultWrapper() = default;
    explicit RustResultWrapper(RustResult result)
        : result_(std::make_unique<RustResult>(result)) {
    }

    RustResultWrapper(RustResultWrapper&& other) noexcept {
        result_ = std::move(other.result_);
    }

    RustResultWrapper&
    operator=(RustResultWrapper&& other) noexcept {
        if (this != &other) {
            free();
            result_ = std::move(other.result_);
        }

        return *this;
    }

    ~RustResultWrapper() {
        free();
    }

    std::unique_ptr<RustResult> result_;

 private:
    void
    free() {
        if (result_) {
            free_rust_result(*result_);
            result_.reset();
        }
    }
};

}  // namespace milvus::tantivy
