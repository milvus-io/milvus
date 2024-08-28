#pragma once

#include <iostream>
#include <sstream>

#include "tantivy-binding.h"
#include "rust-binding.h"

namespace milvus::tantivy {

struct RustArrayWrapper {
    NO_COPY_OR_ASSIGN(RustArrayWrapper);

    explicit RustArrayWrapper(RustArray array) : array_(array) {
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
}  // namespace milvus::tantivy
