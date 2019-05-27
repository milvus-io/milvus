/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <map>
#include <string>
#include <ctime>
#include <memory>
#include <mutex>
#include "IDGenerator.h"
#include "Status.h"
#include "Meta.h"


namespace zilliz {
namespace vecwise {
namespace engine {

namespace meta {
    class Meta;
}

template <typename EngineT>
class MemVectors {
public:
    typedef typename EngineT::Ptr EnginePtr;
    typedef typename meta::Meta::Ptr MetaPtr;
    typedef std::shared_ptr<MemVectors<EngineT>> Ptr;

    explicit MemVectors(const std::shared_ptr<meta::Meta>&,
            const meta::TableFileSchema&, const Options&);

    void add(size_t n_, const float* vectors_, IDNumbers& vector_ids_);

    size_t total() const;

    size_t approximate_size() const;

    Status serialize(std::string& table_id);

    ~MemVectors();

    const std::string& location() const { return schema_.location; }

private:
    MemVectors() = delete;
    MemVectors(const MemVectors&) = delete;
    MemVectors& operator=(const MemVectors&) = delete;

    MetaPtr pMeta_;
    Options options_;
    meta::TableFileSchema schema_;
    IDGenerator* _pIdGenerator;
    EnginePtr pEE_;

}; // MemVectors



template<typename EngineT>
class MemManager {
public:
    typedef typename meta::Meta::Ptr MetaPtr;
    typedef typename MemVectors<EngineT>::Ptr MemVectorsPtr;
    typedef std::shared_ptr<MemManager<EngineT>> Ptr;

    MemManager(const std::shared_ptr<meta::Meta>& meta_, const Options& options)
        : _pMeta(meta_), options_(options) {}

    MemVectorsPtr get_mem_by_group(const std::string& table_id_);

    Status add_vectors(const std::string& table_id_,
            size_t n_, const float* vectors_, IDNumbers& vector_ids_);

    Status serialize(std::vector<std::string>& table_ids);

private:
    Status add_vectors_no_lock(const std::string& table_id_,
            size_t n_, const float* vectors_, IDNumbers& vector_ids_);
    Status mark_memory_as_immutable();

    typedef std::map<std::string, MemVectorsPtr> MemMap;
    typedef std::vector<MemVectorsPtr> ImmMemPool;
    MemMap _memMap;
    ImmMemPool _immMems;
    MetaPtr _pMeta;
    Options options_;
    std::mutex _mutex;
    std::mutex serialization_mtx_;
}; // MemManager


} // namespace engine
} // namespace vecwise
} // namespace zilliz
#include "MemManager.cpp"
