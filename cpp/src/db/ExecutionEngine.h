#pragma once

#include <vector>
#include <memory>

#include "Status.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class ExecutionEngine;

class ExecutionEngine {
public:

    Status AddWithIds(const std::vector<float>& vectors,
                              const std::vector<long>& vector_ids);

    virtual Status AddWithIds(long n, const float *xdata, const long *xids) = 0;

    virtual size_t Count() const = 0;

    virtual size_t Size() const = 0;

    virtual Status Serialize() = 0;

    virtual Status Load() = 0;

    virtual Status Merge(const std::string& location) = 0;

    virtual std::shared_ptr<ExecutionEngine> BuildIndex(const std::string&) = 0;

    virtual Status Cache() = 0;

    virtual ~ExecutionEngine() {}
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
