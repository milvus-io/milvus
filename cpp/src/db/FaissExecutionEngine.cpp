/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#if 0
#include "FaissExecutionEngine.h"
#include "Log.h"

#include <faiss/AutoTune.h>
#include <faiss/MetaIndexes.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>
#include <wrapper/Index.h>
#include <wrapper/IndexBuilder.h>
#include <cache/CpuCacheMgr.h>
#include "faiss/IndexIVF.h"
#include "metrics/Metrics.h"


namespace zilliz {
namespace milvus {
namespace engine {


FaissExecutionEngine::FaissExecutionEngine(uint16_t dimension,
        const std::string& location,
        const std::string& build_index_type,
        const std::string& raw_index_type)
    : pIndex_(faiss::index_factory(dimension, raw_index_type.c_str())),
      location_(location),
      build_index_type_(build_index_type),
      raw_index_type_(raw_index_type) {
}

FaissExecutionEngine::FaissExecutionEngine(std::shared_ptr<faiss::Index> index,
        const std::string& location,
        const std::string& build_index_type,
        const std::string& raw_index_type)
    : pIndex_(index),
      location_(location),
      build_index_type_(build_index_type),
      raw_index_type_(raw_index_type) {
}

Status FaissExecutionEngine::AddWithIds(long n, const float *xdata, const long *xids) {
    pIndex_->add_with_ids(n, xdata, xids);
    return Status::OK();
}

size_t FaissExecutionEngine::Count() const {
    return (size_t)(pIndex_->ntotal);
}

size_t FaissExecutionEngine::Size() const {
    return (size_t)(Count() * pIndex_->d)*sizeof(float);
}

size_t FaissExecutionEngine::Dimension() const {
    return pIndex_->d;
}

size_t FaissExecutionEngine::PhysicalSize() const {
    return (size_t)(Count() * pIndex_->d)*sizeof(float);
}

Status FaissExecutionEngine::Serialize() {
    write_index(pIndex_.get(), location_.c_str());
    return Status::OK();
}

Status FaissExecutionEngine::Load() {
    auto index  = zilliz::milvus::cache::CpuCacheMgr::GetInstance()->GetIndex(location_);
    bool to_cache = false;
    auto start_time = METRICS_NOW_TIME;
    if (!index) {
        index = read_index(location_);
        to_cache = true;
        ENGINE_LOG_DEBUG << "Disk io from: " << location_;
    }

    pIndex_ = index->data();
    if (to_cache) {
        Cache();
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time, end_time);

        server::Metrics::GetInstance().FaissDiskLoadDurationSecondsHistogramObserve(total_time);
        double total_size = (pIndex_->d) * (pIndex_->ntotal) * 4;


        server::Metrics::GetInstance().FaissDiskLoadSizeBytesHistogramObserve(total_size);
//        server::Metrics::GetInstance().FaissDiskLoadIOSpeedHistogramObserve(total_size/double(total_time));
        server::Metrics::GetInstance().FaissDiskLoadIOSpeedGaugeSet(total_size/double(total_time));
    }
    return Status::OK();
}

Status FaissExecutionEngine::Merge(const std::string& location) {
    if (location == location_) {
        return Status::Error("Cannot Merge Self");
    }
    ENGINE_LOG_DEBUG << "Merge index file: " << location << " to: " << location_;

    auto to_merge = zilliz::milvus::cache::CpuCacheMgr::GetInstance()->GetIndex(location);
    if (!to_merge) {
        to_merge = read_index(location);
    }
    auto file_index = dynamic_cast<faiss::IndexIDMap*>(to_merge->data().get());
    pIndex_->add_with_ids(file_index->ntotal, dynamic_cast<faiss::IndexFlat*>(file_index->index)->xb.data(),
            file_index->id_map.data());
    return Status::OK();
}

ExecutionEnginePtr
FaissExecutionEngine::BuildIndex(const std::string& location) {
    ENGINE_LOG_DEBUG << "Build index file: " << location << " from: " << location_;

    auto opd = std::make_shared<Operand>();
    opd->d = pIndex_->d;
    opd->index_type = build_index_type_;
    IndexBuilderPtr pBuilder = GetIndexBuilder(opd);

    auto from_index = dynamic_cast<faiss::IndexIDMap*>(pIndex_.get());

    auto index = pBuilder->build_all(from_index->ntotal,
            dynamic_cast<faiss::IndexFlat*>(from_index->index)->xb.data(),
            from_index->id_map.data());

    ExecutionEnginePtr new_ee(new FaissExecutionEngine(index->data(), location, build_index_type_, raw_index_type_));
    return new_ee;
}

Status FaissExecutionEngine::Search(long n,
                                    const float *data,
                                    long k,
                                    float *distances,
                                    long *labels) const {
    auto start_time = METRICS_NOW_TIME;

    std::shared_ptr<faiss::IndexIVF> ivf_index = std::dynamic_pointer_cast<faiss::IndexIVF>(pIndex_);
    //ENGINE_LOG_DEBUG << "Index nlist: " << ivf_index->nlist << ", ntotal: "<< ivf_index->ntotal;
    if(ivf_index) {
        ENGINE_LOG_DEBUG << "Index type: IVFFLAT nProbe: " << nprobe_;
        ivf_index->nprobe = nprobe_;
        ivf_index->search(n, data, k, distances, labels);
    } else {
        pIndex_->search(n, data, k, distances, labels);
    }

    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time,end_time);
    server::Metrics::GetInstance().QueryIndexTypePerSecondSet(build_index_type_, double(n)/double(total_time));
    return Status::OK();
}

Status FaissExecutionEngine::Cache() {
    zilliz::milvus::cache::CpuCacheMgr::GetInstance(
            )->InsertItem(location_, std::make_shared<Index>(pIndex_));

    return Status::OK();
}

Status FaissExecutionEngine::Init() {

    if(build_index_type_ == "IVF") {

        using namespace zilliz::milvus::server;
        ServerConfig &config = ServerConfig::GetInstance();
        ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
        nprobe_ = engine_config.GetInt32Value(CONFIG_NPROBE, 1000);

    } else if(build_index_type_ == "IDMap") {
        ;
    } else {
        return Status::Error("Wrong index type: ", build_index_type_);
    }

    return Status::OK();
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
#endif
