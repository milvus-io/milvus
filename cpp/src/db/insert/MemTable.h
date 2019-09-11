#pragma once

#include "db/Status.h"
#include "MemTableFile.h"
#include "VectorSource.h"

#include <mutex>


namespace zilliz {
namespace milvus {
namespace engine {

class MemTable {
 public:
    using MemTableFileList = std::vector<MemTableFilePtr>;

    MemTable(const std::string &table_id, const meta::MetaPtr &meta, const Options &options);

    Status Add(VectorSourcePtr &source, IDNumbers &vector_ids);

    void GetCurrentMemTableFile(MemTableFilePtr &mem_table_file);

    size_t GetTableFileCount();

    Status Serialize();

    bool Empty();

    const std::string &GetTableId() const;

    size_t GetCurrentMem();

 private:
    const std::string table_id_;

    MemTableFileList mem_table_file_list_;

    meta::MetaPtr meta_;

    Options options_;

    std::mutex mutex_;

}; //MemTable

using MemTablePtr = std::shared_ptr<MemTable>;

} // namespace engine
} // namespace milvus
} // namespace zilliz