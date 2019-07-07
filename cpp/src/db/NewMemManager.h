#pragma once

#include "Meta.h"
#include "MemTable.h"
#include "Status.h"
#include "MemManagerAbstract.h"

#include <map>
#include <string>
#include <ctime>
#include <memory>
#include <mutex>

namespace zilliz {
namespace milvus {
namespace engine {

class NewMemManager : public MemManagerAbstract {
 public:
    using MetaPtr = meta::Meta::Ptr;
    using Ptr = std::shared_ptr<NewMemManager>;
    using MemTablePtr = typename MemTable::Ptr;

    NewMemManager(const std::shared_ptr<meta::Meta> &meta, const Options &options)
        : meta_(meta), options_(options) {}

    Status InsertVectors(const std::string &table_id,
                         size_t n, const float *vectors, IDNumbers &vector_ids) override;

    Status Serialize(std::set<std::string> &table_ids) override;

    Status EraseMemVector(const std::string &table_id) override;

    size_t GetCurrentMutableMem() override;

    size_t GetCurrentImmutableMem() override;

    size_t GetCurrentMem() override;

 private:
    MemTablePtr GetMemByTable(const std::string &table_id);

    Status InsertVectorsNoLock(const std::string &table_id,
                               size_t n, const float *vectors, IDNumbers &vector_ids);
    Status ToImmutable();

    using MemIdMap = std::map<std::string, MemTablePtr>;
    using MemList = std::vector<MemTablePtr>;
    MemIdMap mem_id_map_;
    MemList immu_mem_list_;
    MetaPtr meta_;
    Options options_;
    std::mutex mutex_;
    std::mutex serialization_mtx_;
}; // NewMemManager


} // namespace engine
} // namespace milvus
} // namespace zilliz