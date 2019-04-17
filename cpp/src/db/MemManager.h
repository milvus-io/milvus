#ifndef STORAGE_VECENGINE_MEMVECTORS_H_
#define STORAGE_VECENGINE_MEMVECTORS_H_

#include <map>
#include <string>
#include <ctime>
#include <memory>
#include <mutex>
#include "IDGenerator.h"
#include "Status.h"

namespace faiss {
    class Index;
}


namespace zilliz {
namespace vecwise {
namespace engine {

namespace meta {
    class Meta;
}

class MemVectors {
public:
    explicit MemVectors(const std::string& group_id,
            size_t dimension,
            const std::string& file_location);

    void add(size_t n_, const float* vectors_, IDNumbers& vector_ids_);

    size_t total() const;

    size_t approximate_size() const;

    Status serialize(std::string& group_id);

    ~MemVectors();

    const std::string& location() const { return _file_location; }

private:
    MemVectors() = delete;
    MemVectors(const MemVectors&) = delete;
    MemVectors& operator=(const MemVectors&) = delete;

    std::string group_id_;
    const std::string _file_location;
    IDGenerator* _pIdGenerator;
    size_t _dimension;
    faiss::Index* pIndex_;

}; // MemVectors


typedef std::shared_ptr<MemVectors> VectorsPtr;

class MemManager {
public:
    MemManager(const std::shared_ptr<meta::Meta>& meta_)
        : _pMeta(meta_) /*_last_compact_time(std::time(nullptr))*/ {}

    VectorsPtr get_mem_by_group(const std::string& group_id_);

    Status add_vectors(const std::string& group_id_,
            size_t n_, const float* vectors_, IDNumbers& vector_ids_);

    Status serialize(std::vector<std::string>& group_ids);

private:
    Status add_vectors_no_lock(const std::string& group_id_,
            size_t n_, const float* vectors_, IDNumbers& vector_ids_);
    Status mark_memory_as_immutable();

    typedef std::map<std::string, VectorsPtr> MemMap;
    typedef std::vector<VectorsPtr> ImmMemPool;
    MemMap _memMap;
    ImmMemPool _immMems;
    std::shared_ptr<meta::Meta> _pMeta;
    /* std::time_t _last_compact_time; */
    std::mutex _mutex;
}; // MemManager


} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif
