#include <easylogging++.h>
#include <faiss/AutoTune.h>
#include <wrapper/Index.h>
#include <cache/CpuCacheMgr.h>

#include "FaissSerializer.h"

namespace zilliz {
namespace vecwise {
namespace engine {

const std::string IndexType = "IDMap,Flat";

FaissSerializer::FaissSerializer(uint16_t dimension, const std::string& location)
    : pIndex_(faiss::index_factory(dimension, IndexType.c_str())),
      location_(location) {
}

Status FaissSerializer::AddWithIds(long n, const float *xdata, const long *xids) {
    pIndex_->add_with_ids(n, xdata, xids);
    return Status::OK();
}

size_t FaissSerializer::Count() const {
    return (size_t)(pIndex_->ntotal);
}

size_t FaissSerializer::Size() const {
    return (size_t)(Count() * pIndex_->d);
}

Status FaissSerializer::Serialize() {
    write_index(pIndex_.get(), location_.c_str());
    return Status::OK();
}

Status FaissSerializer::Cache() {
    zilliz::vecwise::cache::CpuCacheMgr::GetInstance(
            )->InsertItem(location_, std::make_shared<Index>(pIndex_));

    return Status::OK();
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
