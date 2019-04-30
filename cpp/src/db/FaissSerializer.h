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
    FaissSerializer(uint16_t dimension, const std::string& location);
    virtual Status AddWithIds(long n, const float *xdata, const long *xids) override;

    virtual size_t Count() const override;

    virtual size_t Size() const override;

    virtual Status Serialize() override;

    virtual Status Cache() override;

protected:
    std::shared_ptr<faiss::Index> pIndex_;
    std::string location_;
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
