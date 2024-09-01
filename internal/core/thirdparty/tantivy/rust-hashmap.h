#pragma once

#include <map>

#include "tantivy-binding.h"
#include "rust-binding.h"

namespace milvus::tantivy {

struct RustHashMap {
 public:
    NO_COPY_OR_ASSIGN(RustHashMap);

    RustHashMap() {
        ptr_ = create_hashmap();
    }

    ~RustHashMap() {
        if (ptr_ != nullptr) {
            free_hashmap(ptr_);
        }
    }

    void
    from(const std::map<std::string, std::string>& m) {
        for (const auto& [k, v] : m) {
            set(k, v);
        }
    }

    void*
    get_pointer() {
        return ptr_;
    }

    void
    set(const std::string& k, const std::string& v) {
        hashmap_set_value(ptr_, k.c_str(), v.c_str());
    }

 private:
    void* ptr_ = nullptr;
};
}  // namespace milvus::tantivy
