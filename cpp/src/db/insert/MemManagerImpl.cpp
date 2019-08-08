#include "MemManagerImpl.h"
#include "VectorSource.h"
#include "db/Log.h"
#include "db/Constants.h"

#include <thread>


namespace zilliz {
namespace milvus {
namespace engine {

MemManagerImpl::MemTablePtr MemManagerImpl::GetMemByTable(const std::string &table_id) {
    auto memIt = mem_id_map_.find(table_id);
    if (memIt != mem_id_map_.end()) {
        return memIt->second;
    }

    mem_id_map_[table_id] = std::make_shared<MemTable>(table_id, meta_, options_);
    return mem_id_map_[table_id];
}

Status MemManagerImpl::InsertVectors(const std::string &table_id_,
                                    size_t n_,
                                    const float *vectors_,
                                    IDNumbers &vector_ids_) {

    while (GetCurrentMem() > options_.insert_buffer_size) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertVectorsNoLock(table_id_, n_, vectors_, vector_ids_);
}

Status MemManagerImpl::InsertVectorsNoLock(const std::string &table_id,
                                          size_t n,
                                          const float *vectors,
                                          IDNumbers &vector_ids) {

    MemTablePtr mem = GetMemByTable(table_id);
    VectorSource::Ptr source = std::make_shared<VectorSource>(n, vectors);

    auto status = mem->Add(source);
    if (status.ok()) {
        vector_ids = source->GetVectorIds();
    }
    return status;
}

Status MemManagerImpl::ToImmutable() {
    std::unique_lock<std::mutex> lock(mutex_);
    MemIdMap temp_map;
    for (auto &kv: mem_id_map_) {
        if (kv.second->Empty()) {
            //empty table, no need to serialize
            temp_map.insert(kv);
        } else {
            immu_mem_list_.push_back(kv.second);
        }
    }

    mem_id_map_.swap(temp_map);
    return Status::OK();
}

Status MemManagerImpl::Serialize(std::set<std::string> &table_ids) {
    ToImmutable();
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    table_ids.clear();
    for (auto &mem : immu_mem_list_) {
        mem->Serialize();
        table_ids.insert(mem->GetTableId());
    }
    immu_mem_list_.clear();
    return Status::OK();
}

Status MemManagerImpl::EraseMemVector(const std::string &table_id) {
    {//erase MemVector from rapid-insert cache
        std::unique_lock<std::mutex> lock(mutex_);
        mem_id_map_.erase(table_id);
    }

    {//erase MemVector from serialize cache
        std::unique_lock<std::mutex> lock(serialization_mtx_);
        MemList temp_list;
        for (auto &mem : immu_mem_list_) {
            if (mem->GetTableId() != table_id) {
                temp_list.push_back(mem);
            }
        }
        immu_mem_list_.swap(temp_list);
    }

    return Status::OK();
}

size_t MemManagerImpl::GetCurrentMutableMem() {
    size_t total_mem = 0;
    for (auto &kv : mem_id_map_) {
        auto memTable = kv.second;
        total_mem += memTable->GetCurrentMem();
    }
    return total_mem;
}

size_t MemManagerImpl::GetCurrentImmutableMem() {
    size_t total_mem = 0;
    for (auto &mem_table : immu_mem_list_) {
        total_mem += mem_table->GetCurrentMem();
    }
    return total_mem;
}

size_t MemManagerImpl::GetCurrentMem() {
    return GetCurrentMutableMem() + GetCurrentImmutableMem();
}

} // namespace engine
} // namespace milvus
} // namespace zilliz