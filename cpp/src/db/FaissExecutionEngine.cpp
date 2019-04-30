#include <easylogging++.h>
#include <faiss/AutoTune.h>
#include <faiss/MetaIndexes.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>
#include <wrapper/Index.h>
#include <wrapper/IndexBuilder.h>
#include <cache/CpuCacheMgr.h>

#include "FaissExecutionEngine.h"

namespace zilliz {
namespace vecwise {
namespace engine {

const std::string RawIndexType = "IDMap,Flat";
const std::string BuildIndexType = "IDMap,Flat";

FaissExecutionEngine::FaissExecutionEngine(uint16_t dimension, const std::string& location)
    : pIndex_(faiss::index_factory(dimension, RawIndexType.c_str())),
      location_(location) {
}

FaissExecutionEngine::FaissExecutionEngine(std::shared_ptr<faiss::Index> index, const std::string& location)
    : pIndex_(index),
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

size_t FaissExecutionEngine::PhysicalSize() const {
    return (size_t)(Size()*sizeof(float));
}

Status FaissExecutionEngine::Serialize() {
    write_index(pIndex_.get(), location_.c_str());
    return Status::OK();
}

Status FaissExecutionEngine::Load() {
    auto index  = zilliz::vecwise::cache::CpuCacheMgr::GetInstance()->GetIndex(location_);
    if (!index) {
        index = read_index(location_);
        Cache();
        LOG(DEBUG) << "Disk io from: " << location_;
    }

    pIndex_ = index->data();
    return Status::OK();
}

Status FaissExecutionEngine::Merge(const std::string& location) {
    if (location == location_) {
        return Status::Error("Cannot Merge Self");
    }
    auto to_merge = zilliz::vecwise::cache::CpuCacheMgr::GetInstance()->GetIndex(location);
    if (!to_merge) {
        to_merge = read_index(location);
    }
    auto file_index = dynamic_cast<faiss::IndexIDMap*>(to_merge->data().get());
    pIndex_->add_with_ids(file_index->ntotal, dynamic_cast<faiss::IndexFlat*>(file_index->index)->xb.data(),
            file_index->id_map.data());
    return Status::OK();
}

std::shared_ptr<ExecutionEngine> FaissExecutionEngine::BuildIndex(const std::string& location) {
    auto opd = std::make_shared<Operand>();
    opd->d = pIndex_->d;
    opd->index_type = BuildIndexType;
    IndexBuilderPtr pBuilder = GetIndexBuilder(opd);

    auto from_index = dynamic_cast<faiss::IndexIDMap*>(pIndex_.get());

    auto index = pBuilder->build_all(from_index->ntotal,
            dynamic_cast<faiss::IndexFlat*>(from_index->index)->xb.data(),
            from_index->id_map.data());

    std::shared_ptr<ExecutionEngine> new_ee(new FaissExecutionEngine(index->data(), location));
    new_ee->Serialize();
    return new_ee;
}

Status FaissExecutionEngine::Search(long n,
                                    const float *data,
                                    long k,
                                    float *distances,
                                    long *labels) const {

    pIndex_->search(n, data, k, distances, labels);
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
