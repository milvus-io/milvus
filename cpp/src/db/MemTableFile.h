#pragma once

#include "Status.h"
#include "Meta.h"
#include "VectorSource.h"

namespace zilliz {
namespace milvus {
namespace engine {

class MemTableFile {

public:

    using Ptr = std::shared_ptr<MemTableFile>;
    using MetaPtr = meta::Meta::Ptr;

    MemTableFile(const std::string& table_id, const std::shared_ptr<meta::Meta>& meta);

    Status Add(const VectorSource::Ptr& source);

    size_t GetCurrentMem();

    size_t GetMemLeft();

    bool isFull();

private:

    Status CreateTableFile();

    const std::string table_id_;

    meta::TableFileSchema table_file_schema_;

    MetaPtr meta_;

    size_t current_mem_;

}; //MemTableFile

} // namespace engine
} // namespace milvus
} // namespace zilliz