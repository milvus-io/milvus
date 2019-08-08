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

    using Ptr = std::shared_ptr<MemTable>;
    using MemTableFileList = std::vector<MemTableFile::Ptr>;
    using MetaPtr = meta::Meta::Ptr;

    MemTable(const std::string &table_id, const std::shared_ptr<meta::Meta> &meta, const Options &options);

    Status Add(VectorSource::Ptr &source);

    void GetCurrentMemTableFile(MemTableFile::Ptr &mem_table_file);

    size_t GetTableFileCount();

    Status Serialize();

    bool Empty();

    const std::string &GetTableId() const;

    size_t GetCurrentMem();

 private:
    const std::string table_id_;

    MemTableFileList mem_table_file_list_;

    MetaPtr meta_;

    Options options_;

    std::mutex mutex_;

}; //MemTable

} // namespace engine
} // namespace milvus
} // namespace zilliz