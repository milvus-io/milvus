/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "ExecutionEngine.h"
#include "faiss/Index.h"

#include <memory>
#include <string>


namespace zilliz {
namespace milvus {
namespace engine {

const static std::string BUILD_INDEX_TYPE_IDMAP = "IDMap";
const static std::string BUILD_INDEX_TYPE_IVF = "IVF";
const static std::string BUILD_INDEX_TYPE_IVFSQ8 = "IVFSQ8";

class IndexStatsHelper {

 public:
    using Ptr = std::shared_ptr<IndexStatsHelper>;
    virtual std::string ToString(const std::string &prefix = "") const;
    virtual void Reset() const;
    virtual ~IndexStatsHelper() {}
};

class FaissIndexIVFStatsHelper : public IndexStatsHelper {
 public:
    std::string ToString(const std::string &prefix = "") const override;

 private:
    const std::string identifier_ = BUILD_INDEX_TYPE_IVF;
};

class FaissExecutionEngine : public ExecutionEngine {
 public:

    FaissExecutionEngine(uint16_t dimension,
                         const std::string &location,
                         const std::string &build_index_type,
                         const std::string &raw_index_type);

    FaissExecutionEngine(std::shared_ptr<faiss::Index> index,
                         const std::string &location,
                         const std::string &build_index_type,
                         const std::string &raw_index_type);

    Status AddWithIds(long n, const float *xdata, const long *xids) override;

    size_t Count() const override;

    size_t Size() const override;

    size_t Dimension() const override;

    size_t PhysicalSize() const override;

    Status Serialize() override;

    Status Load(bool to_cache) override;

    Status Merge(const std::string &location) override;

    Status Search(long n,
                  const float *data,
                  long k,
                  float *distances,
                  long *labels) const override;

    ExecutionEnginePtr BuildIndex(const std::string &) override;

    Status Cache() override;

    Status Init() override;

 protected:
    FaissIndexIVFStatsHelper ivf_stats_helper_;
    std::shared_ptr<faiss::Index> pIndex_;
    std::string location_;

    std::string build_index_type_;
    std::string raw_index_type_;

    size_t nprobe_ = 0;
    size_t nlist_ = 0;
};


} // namespace engine
} // namespace milvus
} // namespace zilliz
