/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "ExecutionEngine.h"
#include "IDGenerator.h"
#include "Status.h"
#include "Meta.h"
#include "MemManagerAbstract.h"

#include <map>
#include <string>
#include <ctime>
#include <memory>
#include <mutex>
#include <set>


namespace zilliz {
namespace milvus {
namespace engine {

namespace meta {
class Meta;
}

class MemVectors {
 public:
    using MetaPtr = meta::Meta::Ptr;
    using Ptr = std::shared_ptr<MemVectors>;

    explicit MemVectors(const std::shared_ptr<meta::Meta> &,
                        const meta::TableFileSchema &, const Options &);

    Status Add(size_t n_, const float *vectors_, IDNumbers &vector_ids_);

    size_t RowCount() const;

    size_t Size() const;

    Status Serialize(std::string &table_id);

    ~MemVectors();

    const std::string &Location() const { return schema_.location_; }

    std::string TableId() const { return schema_.table_id_; }

 private:
    MemVectors() = delete;
    MemVectors(const MemVectors &) = delete;
    MemVectors &operator=(const MemVectors &) = delete;

    MetaPtr meta_;
    Options options_;
    meta::TableFileSchema schema_;
    IDGenerator *id_generator_;
    ExecutionEnginePtr active_engine_;

}; // MemVectors



class MemManager : public MemManagerAbstract {
 public:
    using MetaPtr = meta::Meta::Ptr;
    using MemVectorsPtr = typename MemVectors::Ptr;
    using Ptr = std::shared_ptr<MemManager>;

    MemManager(const std::shared_ptr<meta::Meta> &meta, const Options &options)
        : meta_(meta), options_(options) {}

    Status InsertVectors(const std::string &table_id,
                         size_t n, const float *vectors, IDNumbers &vector_ids) override;

    Status Serialize(std::set<std::string> &table_ids) override;

    Status EraseMemVector(const std::string &table_id) override;

    size_t GetCurrentMutableMem() override;

    size_t GetCurrentImmutableMem() override;

    size_t GetCurrentMem() override;

 private:
    MemVectorsPtr GetMemByTable(const std::string &table_id);

    Status InsertVectorsNoLock(const std::string &table_id,
                               size_t n, const float *vectors, IDNumbers &vector_ids);
    Status ToImmutable();

    using MemIdMap = std::map<std::string, MemVectorsPtr>;
    using MemList = std::vector<MemVectorsPtr>;
    MemIdMap mem_id_map_;
    MemList immu_mem_list_;
    MetaPtr meta_;
    Options options_;
    std::mutex mutex_;
    std::mutex serialization_mtx_;
}; // MemManager


} // namespace engine
} // namespace milvus
} // namespace zilliz
