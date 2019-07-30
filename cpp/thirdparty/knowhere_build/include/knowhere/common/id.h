#pragma once

//#include "zcommon/id/id.h"
//using ID = zilliz::common::ID;

#include <stdint.h>
#include <string>

namespace zilliz {
namespace knowhere {



class ID {
 public:
    constexpr static int64_t kIDSize = 20;

 public:
    const int32_t *
    data() const { return content_; }

    int32_t *
    mutable_data() { return content_; }

    bool
    IsValid() const;

    std::string
    ToString() const;

    bool
    operator==(const ID &that) const;

    bool
    operator<(const ID &that) const;

 protected:
    int32_t content_[5] = {};
};

} // namespace knowhere
} // namespace zilliz
