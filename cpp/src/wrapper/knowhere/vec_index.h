////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include <memory>

#include "knowhere/common/config.h"
#include "knowhere/common/binary_set.h"


namespace zilliz {
namespace vecwise {
namespace engine {

// TODO(linxj): jsoncons => rapidjson or other.
using Config = zilliz::knowhere::Config;

class VecIndex {
 public:
    virtual void BuildAll(const long &nb,
                          const float *xb,
                          const long *ids,
                          const Config &cfg,
                          const long &nt = 0,
                          const float *xt = nullptr) = 0;

    virtual void Add(const long &nb,
                     const float *xb,
                     const long *ids,
                     const Config &cfg = Config()) = 0;

    virtual void Search(const long &nq,
                        const float *xq,
                        float *dist,
                        long *ids,
                        const Config &cfg = Config()) = 0;

    virtual zilliz::knowhere::BinarySet Serialize() = 0;

    virtual void Load(const zilliz::knowhere::BinarySet &index_binary) = 0;
};

using VecIndexPtr = std::shared_ptr<VecIndex>;

extern VecIndexPtr GetVecIndexFactory(const std::string &index_type);

extern VecIndexPtr LoadVecIndex(const std::string &index_type, const zilliz::knowhere::BinarySet &index_binary);

}
}
}
