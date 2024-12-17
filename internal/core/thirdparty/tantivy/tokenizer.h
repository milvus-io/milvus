#pragma once

#include "tantivy-binding.h"
#include "rust-binding.h"
#include "rust-hashmap.h"
#include "tantivy/rust-array.h"
#include "token-stream.h"

namespace milvus::tantivy {

struct Tokenizer {
 public:
    NO_COPY_OR_ASSIGN(Tokenizer);

    explicit Tokenizer(std::string&& params) {
        auto shared_params = std::make_shared<std::string>(std::move(params));
        auto res =
            RustResultWrapper(tantivy_create_tokenizer(shared_params->c_str()));
        AssertInfo(res.result_->success,
                   "Tokenizer creation failed: {}",
                   res.result_->error);
        ptr_ = res.result_->value.ptr._0;
    }

    explicit Tokenizer(void* _ptr) : ptr_(_ptr) {
    }

    ~Tokenizer() {
        if (ptr_ != nullptr) {
            tantivy_free_tokenizer(ptr_);
        }
    }

    std::unique_ptr<TokenStream>
    CreateTokenStream(std::string&& text) {
        auto shared_text = std::make_shared<std::string>(std::move(text));
        auto token_stream =
            tantivy_create_token_stream(ptr_, shared_text->c_str());
        return std::make_unique<TokenStream>(token_stream, shared_text);
    }

    std::unique_ptr<Tokenizer>
    Clone() {
        auto newptr = tantivy_clone_tokenizer(ptr_);
        return std::make_unique<milvus::tantivy::Tokenizer>(newptr);
    }

    // CreateTokenStreamCopyText will copy the text and then create token stream based on the text.
    std::unique_ptr<TokenStream>
    CreateTokenStreamCopyText(const std::string& text) {
        auto shared_text = std::make_shared<std::string>(text);
        auto token_stream =
            tantivy_create_token_stream(ptr_, shared_text->c_str());
        return std::make_unique<TokenStream>(token_stream, shared_text);
    }

 private:
    void* ptr_;
};

}  // namespace milvus::tantivy
