#pragma once

#include "tantivy-binding.h"
#include "rust-binding.h"
#include "rust-hashmap.h"
#include "token-stream.h"

namespace milvus::tantivy {

struct Tokenizer {
 public:
    NO_COPY_OR_ASSIGN(Tokenizer);

    explicit Tokenizer(const std::map<std::string, std::string>& params) {
        RustHashMap m;
        m.from(params);
        ptr_ = tantivy_create_tokenizer(m.get_pointer());
        if (ptr_ == nullptr) {
            throw "invalid tokenizer parameters";
        }
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
