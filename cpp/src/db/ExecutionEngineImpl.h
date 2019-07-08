/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "ExecutionEngine.h"
#include "wrapper/knowhere/vec_index.h"

#include <memory>
#include <string>


namespace zilliz {
namespace milvus {
namespace engine {


class ExecutionEngineImpl : public ExecutionEngine {
 public:

    ExecutionEngineImpl(uint16_t dimension,
                        const std::string &location,
                        EngineType type);

    ExecutionEngineImpl(VecIndexPtr index,
                        const std::string &location,
                        EngineType type);

    Status AddWithIds(long n, const float *xdata, const long *xids) override;

    size_t Count() const override;

    size_t Size() const override;

    size_t Dimension() const override;

    size_t PhysicalSize() const override;

    Status Serialize() override;

    Status Load() override;

    Status Merge(const std::string &location) override;

    Status Search(long n,
                  const float *data,
                  long k,
                  float *distances,
                  long *labels) const override;

    ExecutionEnginePtr BuildIndex(const std::string &) override;

    Status Cache() override;

    Status Init() override;

 private:
    VecIndexPtr CreatetVecIndex(EngineType type);

    VecIndexPtr Load(const std::string &location);

 protected:
    VecIndexPtr index_ = nullptr;
    EngineType build_type;

    int64_t dim;
    std::string location_;

    size_t nprobe_ = 0;
    int64_t gpu_num = 0;
};


} // namespace engine
} // namespace milvus
} // namespace zilliz
