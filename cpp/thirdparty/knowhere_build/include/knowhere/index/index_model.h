#pragma once

#include <memory>
#include "knowhere/common/binary_set.h"

namespace zilliz {
namespace knowhere {


class IndexModel {
 public:
    virtual BinarySet
    Serialize() = 0;

    virtual void
    Load(const BinarySet &binary) = 0;
};

using IndexModelPtr = std::shared_ptr<IndexModel>;



} // namespace knowhere
} // namespace zilliz
