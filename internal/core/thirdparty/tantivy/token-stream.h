#pragma once

#include <memory>
#include <string>

#include "tantivy-binding.h"
#include "rust-binding.h"

namespace milvus::tantivy {
struct TokenStream {
 public:
    NO_COPY_OR_ASSIGN(TokenStream);

    TokenStream(void* ptr, std::shared_ptr<std::string> text)
        : ptr_(ptr), text_(text) {
    }

    ~TokenStream() {
        if (ptr_ != nullptr) {
            tantivy_free_token_stream(ptr_);
        }
    }

 public:
    bool
    advance() {
        return tantivy_token_stream_advance(ptr_);
    }

    std::string
    get_token() {
        auto token = tantivy_token_stream_get_token(ptr_);
        std::string s(token);
        free_rust_string(token);
        return s;
    }

 public:
    void* ptr_;
    std::shared_ptr<std::string> text_;
};
}  // namespace milvus::tantivy
