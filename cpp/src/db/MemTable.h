#pragma once

#include "Status.h"
#include "MemTableFile.h"
#include "VectorSource.h"

#include <stack>

namespace zilliz {
namespace milvus {
namespace engine {

class MemTable {

public:

    using Ptr = std::shared_ptr<MemTable>;
    using MemTableFileStack = std::stack<MemTableFile::Ptr>;
    using MetaPtr = meta::Meta::Ptr;

    MemTable(const std::string& table_id, const std::shared_ptr<meta::Meta>& meta);

    Status Add(VectorSource::Ptr& source);

    void GetCurrentMemTableFile(MemTableFile::Ptr& mem_table_file);

    size_t GetStackSize();

private:
    const std::string table_id_;

    MemTableFileStack mem_table_file_stack_;

    MetaPtr meta_;

}; //MemTable

} // namespace engine
} // namespace milvus
} // namespace zilliz