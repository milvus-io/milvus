////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include <memory>

#include "utils/Error.h"

#include "knowhere/common/config.h"
#include "knowhere/common/binary_set.h"


namespace zilliz {
namespace milvus {
namespace engine {

// TODO(linxj): jsoncons => rapidjson or other.
using Config = zilliz::knowhere::Config;

enum class IndexType {
    INVALID = 0,
    FAISS_IDMAP = 1,
    FAISS_IVFFLAT_CPU,
    FAISS_IVFFLAT_GPU,
    FAISS_IVFFLAT_MIX, // build on gpu and search on cpu
    FAISS_IVFPQ_CPU,
    FAISS_IVFPQ_GPU,
    SPTAG_KDT_RNT_CPU,
    //NSG,
};

class VecIndex {
 public:
    virtual server::KnowhereError BuildAll(const long &nb,
                                           const float *xb,
                                           const long *ids,
                                           const Config &cfg,
                                           const long &nt = 0,
                                           const float *xt = nullptr) = 0;

    virtual server::KnowhereError Add(const long &nb,
                                      const float *xb,
                                      const long *ids,
                                      const Config &cfg = Config()) = 0;

    virtual server::KnowhereError Search(const long &nq,
                                         const float *xq,
                                         float *dist,
                                         long *ids,
                                         const Config &cfg = Config()) = 0;

    virtual IndexType GetType() = 0;

    virtual int64_t Dimension() = 0;

    virtual int64_t Count() = 0;

    virtual zilliz::knowhere::BinarySet Serialize() = 0;

    virtual server::KnowhereError Load(const zilliz::knowhere::BinarySet &index_binary) = 0;
};

using VecIndexPtr = std::shared_ptr<VecIndex>;

extern server::KnowhereError write_index(VecIndexPtr index, const std::string &location);

extern VecIndexPtr read_index(const std::string &location);

extern VecIndexPtr GetVecIndexFactory(const IndexType &type);

extern VecIndexPtr LoadVecIndex(const IndexType &index_type, const zilliz::knowhere::BinarySet &index_binary);

}
}
}
