#pragma once

#include <vector>

namespace zilliz {
namespace vecwise {
namespace engine {

class Serializer {
public:

    bool AddWithIds(const std::vector<float>& vectors,
                              const std::vector<long>& vector_ids);

    virtual bool AddWithIds(long n, const float *xdata, const long *xids) = 0;

    virtual ~Serializer() {}
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
