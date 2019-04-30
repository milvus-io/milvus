#include <easylogging++.h>
#include <faiss/AutoTune.h>

#include "FaissSerializer.h"

namespace zilliz {
namespace vecwise {
namespace engine {

const std::string IndexType = "IDMap,Flat";

FaissSerializer::FaissSerializer(uint16_t dimension)
    : pIndex_(faiss::index_factory(dimension, IndexType.c_str())) {
}

bool FaissSerializer::AddWithIds(long n, const float *xdata, const long *xids) {
    pIndex_->add_with_ids(n, xdata, xids);
    return true;
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz
