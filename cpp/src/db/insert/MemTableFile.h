#pragma once

#include "db/Status.h"
#include "db/meta/Meta.h"
#include "VectorSource.h"
#include "db/engine/ExecutionEngine.h"


namespace zilliz {
namespace milvus {
namespace engine {

class MemTableFile {

 public:

    using Ptr = std::shared_ptr<MemTableFile>;
    using MetaPtr = meta::Meta::Ptr;

    MemTableFile(const std::string &table_id, const std::shared_ptr<meta::Meta> &meta, const Options &options);

    Status Add(const VectorSource::Ptr &source);

    size_t GetCurrentMem();

    size_t GetMemLeft();

    bool IsFull();

    Status Serialize();

 private:

    Status CreateTableFile();

    const std::string table_id_;

    meta::TableFileSchema table_file_schema_;

    MetaPtr meta_;

    Options options_;

    size_t current_mem_;

    ExecutionEnginePtr execution_engine_;

}; //MemTableFile

} // namespace engine
} // namespace milvus
} // namespace zilliz