#pragma once

#include <memory>
#include <string>

#include "ExecutionEngine.h"

namespace faiss {
    class Index;
}

namespace zilliz {
namespace vecwise {
namespace engine {

class FaissExecutionEngine : public ExecutionEngine {
public:
    FaissExecutionEngine(uint16_t dimension, const std::string& location);
    FaissExecutionEngine(std::shared_ptr<faiss::Index> index, const std::string& location);

    virtual Status AddWithIds(long n, const float *xdata, const long *xids) override;

    virtual size_t Count() const override;

    virtual size_t Size() const override;

    virtual Status Merge(const std::string& location) override;

    virtual Status Serialize() override;
    virtual Status Load() override;

    virtual Status Cache() override;

    virtual std::shared_ptr<ExecutionEngine> BuildIndex(const std::string&) override;

protected:
    std::shared_ptr<faiss::Index> pIndex_;
    std::string location_;
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
