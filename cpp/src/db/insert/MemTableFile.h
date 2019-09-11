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
    MemTableFile(const std::string &table_id, const meta::MetaPtr &meta, const Options &options);

    Status Add(const VectorSourcePtr &source, IDNumbers& vector_ids);

    size_t GetCurrentMem();

    size_t GetMemLeft();

    bool IsFull();

    Status Serialize();

 private:

    Status CreateTableFile();

    const std::string table_id_;

    meta::TableFileSchema table_file_schema_;

    meta::MetaPtr meta_;

    Options options_;

    size_t current_mem_;

    ExecutionEnginePtr execution_engine_;

}; //MemTableFile

using MemTableFilePtr = std::shared_ptr<MemTableFile>;

} // namespace engine
} // namespace milvus
} // namespace zilliz