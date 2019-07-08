#pragma once

#include <set>


namespace zilliz {
namespace milvus {
namespace engine {

class MemManagerAbstract {
 public:

    virtual Status InsertVectors(const std::string &table_id,
                                 size_t n, const float *vectors, IDNumbers &vector_ids) = 0;

    virtual Status Serialize(std::set<std::string> &table_ids) = 0;

    virtual Status EraseMemVector(const std::string &table_id) = 0;

    virtual size_t GetCurrentMutableMem() = 0;

    virtual size_t GetCurrentImmutableMem() = 0;

    virtual size_t GetCurrentMem() = 0;

}; // MemManagerAbstract

using MemManagerAbstractPtr = std::shared_ptr<MemManagerAbstract>;

} // namespace engine
} // namespace milvus
} // namespace zilliz