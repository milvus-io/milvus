#include <easylogging++.h>
#include <faiss/AutoTune.h>
#include <wrapper/Index.h>
#include <cache/CpuCacheMgr.h>

#include "FaissExecutionEngine.h"

namespace zilliz {
namespace vecwise {
namespace engine {

const std::string IndexType = "IDMap,Flat";

FaissExecutionEngine::FaissExecutionEngine(uint16_t dimension, const std::string& location)
    : pIndex_(faiss::index_factory(dimension, IndexType.c_str())),
      location_(location) {
}

Status FaissExecutionEngine::AddWithIds(long n, const float *xdata, const long *xids) {
    pIndex_->add_with_ids(n, xdata, xids);
    return Status::OK();
}

size_t FaissExecutionEngine::Count() const {
    return (size_t)(pIndex_->ntotal);
}

size_t FaissExecutionEngine::Size() const {
    return (size_t)(Count() * pIndex_->d);
}

Status FaissExecutionEngine::Serialize() {
    write_index(pIndex_.get(), location_.c_str());
    return Status::OK();
}

Status FaissExecutionEngine::Cache() {
    zilliz::vecwise::cache::CpuCacheMgr::GetInstance(
            )->InsertItem(location_, std::make_shared<Index>(pIndex_));

    return Status::OK();
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
