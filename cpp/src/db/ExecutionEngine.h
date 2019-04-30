#pragma once

#include <vector>
#include <memory>

#include "Status.h"

namespace zilliz {
namespace vecwise {
namespace engine {

template <typename Derived>
class ExecutionEngine {
public:

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

    std::shared_ptr<Derived> BuildIndex(const std::string&);

    Status Cache();
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
