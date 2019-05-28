/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IDGenerator.h"
#include "Status.h"
#include "Meta.h"

#include <map>
#include <string>
#include <ctime>
#include <memory>
#include <mutex>

namespace zilliz {
namespace vecwise {
namespace engine {

namespace meta {
    class Meta;
}

template <typename EngineT>
class MemVectors {
public:
    using EnginePtr = typename EngineT::Ptr;
    using MetaPtr = meta::Meta::Ptr;
    using Ptr = std::shared_ptr<MemVectors<EngineT>>;

    explicit MemVectors(const std::shared_ptr<meta::Meta>&,
            const meta::TableFileSchema&, const Options&);

    void Add(size_t n_, const float* vectors_, IDNumbers& vector_ids_);

    size_t Total() const;

    size_t ApproximateSize() const;

    Status Serialize(std::string& table_id);

    ~MemVectors();

    const std::string& Location() const { return schema_.location; }

private:
    MemVectors() = delete;
    MemVectors(const MemVectors&) = delete;
    MemVectors& operator=(const MemVectors&) = delete;

    MetaPtr pMeta_;
    Options options_;
    meta::TableFileSchema schema_;
    IDGenerator* pIdGenerator_;
    EnginePtr pEE_;

}; // MemVectors



template<typename EngineT>
class MemManager {
public:
    using MetaPtr = meta::Meta::Ptr;
    using MemVectorsPtr = typename MemVectors<EngineT>::Ptr;
    using Ptr = std::shared_ptr<MemManager<EngineT>>;

    MemManager(const std::shared_ptr<meta::Meta>& meta, const Options& options)
        : pMeta_(meta), options_(options) {}

    MemVectorsPtr GetMemByTable(const std::string& table_id);

    Status InsertVectors(const std::string& table_id,
            size_t n, const float* vectors, IDNumbers& vector_ids);

    Status Serialize(std::vector<std::string>& table_ids);

private:
    Status InsertVectorsNoLock(const std::string& table_id,
            size_t n, const float* vectors, IDNumbers& vector_ids);
    Status ToImmutable();

    using MemMap = std::map<std::string, MemVectorsPtr>;
    using ImmMemPool = std::vector<MemVectorsPtr>;
    MemMap memMap_;
    ImmMemPool immMems_;
    MetaPtr pMeta_;
    Options options_;
    std::mutex mutex_;
    std::mutex serialization_mtx_;
}; // MemManager


} // namespace engine
} // namespace vecwise
} // namespace zilliz
#include "MemManager.inl"
