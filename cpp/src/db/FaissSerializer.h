#pragma once

#include <memory>
#include <string>

#include "Serializer.h"

namespace faiss {
    class Index;
}

namespace zilliz {
namespace vecwise {
namespace engine {

class FaissSerializer : public Serializer {
public:
    FaissSerializer(uint16_t dimension);
    virtual bool AddWithIds(long n, const float *xdata, const long *xids) override;

protected:
    std::shared_ptr<faiss::Index> pIndex_;
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
