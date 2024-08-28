#pragma once

namespace milvus::tantivy {
#define NO_COPY_OR_ASSIGN(ClassName)      \
    ClassName(const ClassName&) = delete; \
    ClassName& operator=(const ClassName&) = delete;
}  // namespace milvus::tantivy
