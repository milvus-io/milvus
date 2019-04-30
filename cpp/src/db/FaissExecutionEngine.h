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

    virtual size_t PhysicalSize() const override;

    virtual Status Merge(const std::string& location) override;

    virtual Status Serialize() override;
    virtual Status Load() override;

    virtual Status Cache() override;

    virtual Status Search(long n,
                          const float *data,
                          long k,
                          float *distances,
                          long *labels) const override;

    virtual std::shared_ptr<ExecutionEngine> BuildIndex(const std::string&) override;

protected:
    std::shared_ptr<faiss::Index> pIndex_;
    std::string location_;
};

class FaissExecutionEngineBase : public ExecutionEngineBase<FaissExecutionEngineBase> {
public:
    FaissExecutionEngineBase(uint16_t dimension, const std::string& location);
    FaissExecutionEngineBase(std::shared_ptr<faiss::Index> index, const std::string& location);

    Status AddWithIds(const std::vector<float>& vectors,
                              const std::vector<long>& vector_ids);

    Status AddWithIds(long n, const float *xdata, const long *xids);

    size_t Count() const;

    size_t Size() const;

    size_t PhysicalSize() const;

    Status Serialize();

    Status Load();

    Status Merge(const std::string& location);

    Status Search(long n,
                  const float *data,
                  long k,
                  float *distances,
                  long *labels) const;

    std::shared_ptr<FaissExecutionEngineBase> BuildIndex(const std::string&);

    Status Cache();
protected:
    std::shared_ptr<faiss::Index> pIndex_;
    std::string location_;
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
