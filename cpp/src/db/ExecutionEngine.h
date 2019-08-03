/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Status.h"

#include <vector>
#include <memory>

namespace zilliz {
namespace milvus {
namespace engine {

enum class EngineType {
    INVALID = 0,
    FAISS_IDMAP = 1,
    FAISS_IVFFLAT,
    FAISS_IVFSQ8,
    NSG_MIX,
    MAX_VALUE = FAISS_IVFSQ8,
};

class ExecutionEngine {
public:

    virtual Status AddWithIdArray(const std::vector<float>& vectors, const std::vector<long>& vector_ids);

    virtual Status AddWithIds(long n, const float *xdata, const long *xids) = 0;

    virtual size_t Count() const = 0;

    virtual size_t Size() const = 0;

    virtual size_t Dimension() const = 0;

    virtual size_t PhysicalSize() const = 0;

    virtual Status Serialize() = 0;

    virtual Status Load(bool to_cache = true) = 0;

    virtual Status Merge(const std::string& location) = 0;

    virtual Status Search(long n,
                  const float *data,
                  long k,
                  float *distances,
                  long *labels) const = 0;

    virtual std::shared_ptr<ExecutionEngine> BuildIndex(const std::string&) = 0;

    virtual Status Cache() = 0;

    virtual Status Init() = 0;
};

using ExecutionEnginePtr = std::shared_ptr<ExecutionEngine>;


} // namespace engine
} // namespace milvus
} // namespace zilliz
