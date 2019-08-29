////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////


#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVF.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/index_io.h>


#include "knowhere/common/exception.h"
#include "knowhere/index/vector_index/cloner.h"
#include "knowhere/adapter/faiss_adopt.h"
#include "knowhere/index/vector_index/gpu_ivf.h"


namespace zilliz {
namespace knowhere {

IndexModelPtr GPUIVF::Train(const DatasetPtr &dataset, const Config &config) {
    auto nlist = config["nlist"].as<size_t>();
    auto gpu_device = config.get_with_default("gpu_id", gpu_id_);
    auto metric_type = config["metric_type"].as_string() == "L2" ?
                       faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;

    GETTENSOR(dataset)

    // TODO(linxj): use device_id
    auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_device);
    ResScope rs(gpu_device, res);
    faiss::gpu::GpuIndexIVFFlat device_index(res.get(), dim, nlist, metric_type);
    device_index.train(rows, (float *) p_data);

    std::shared_ptr<faiss::Index> host_index = nullptr;
    host_index.reset(faiss::gpu::index_gpu_to_cpu(&device_index));

    return std::make_shared<IVFIndexModel>(host_index);
}

void GPUIVF::set_index_model(IndexModelPtr model) {
    std::lock_guard<std::mutex> lk(mutex_);

    auto host_index = std::static_pointer_cast<IVFIndexModel>(model);
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_)) {
        ResScope rs(gpu_id_, res);
        auto device_index = faiss::gpu::index_cpu_to_gpu(res.get(), gpu_id_, host_index->index_.get());
        index_.reset(device_index);
    } else {
        KNOWHERE_THROW_MSG("load index model error, can't get gpu_resource");
    }
}

BinarySet GPUIVF::SerializeImpl() {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        MemoryIOWriter writer;
        {
            faiss::Index *index = index_.get();
            faiss::Index *host_index = faiss::gpu::index_gpu_to_cpu(index);

            SealImpl();

            faiss::write_index(host_index, &writer);
            delete host_index;
        }
        auto data = std::make_shared<uint8_t>();
        data.reset(writer.data_);

        BinarySet res_set;
        res_set.Append("IVF", data, writer.rp);

        return res_set;
    } catch (std::exception &e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void GPUIVF::LoadImpl(const BinarySet &index_binary) {
    auto binary = index_binary.GetByName("IVF");
    MemoryIOReader reader;
    {
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        faiss::Index *index = faiss::read_index(&reader);

        if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_)) {
            ResScope rs(gpu_id_, res);
            auto device_index = faiss::gpu::index_cpu_to_gpu(res.get(), gpu_id_, index);
            index_.reset(device_index);
        } else {
            KNOWHERE_THROW_MSG("Load error, can't get gpu resource");
        }

        delete index;
    }
}

IVFIndexPtr GPUIVF::Copy_index_gpu_to_cpu() {
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index *device_index = index_.get();
    faiss::Index *host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVF>(new_index);
}

void GPUIVF::search_impl(int64_t n,
                         const float *data,
                         int64_t k,
                         float *distances,
                         int64_t *labels,
                         const Config &cfg) {
    if (auto device_index = std::static_pointer_cast<faiss::gpu::GpuIndexIVF>(index_)) {
        auto nprobe = cfg.get_with_default("nprobe", size_t(1));

        std::lock_guard<std::mutex> lk(mutex_);
        device_index->setNumProbes(nprobe);
        device_index->search(n, (float *) data, k, distances, labels);
    }
}

VectorIndexPtr GPUIVF::CopyGpuToCpu(const Config &config) {
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index *device_index = index_.get();
    faiss::Index *host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVF>(new_index);
}

VectorIndexPtr GPUIVF::Clone() {
    auto cpu_idx = CopyGpuToCpu(Config());
    return ::zilliz::knowhere::CopyCpuToGpu(cpu_idx, gpu_id_, Config());
}

VectorIndexPtr GPUIVF::CopyGpuToGpu(const int64_t &device_id, const Config &config) {
    auto host_index = CopyGpuToCpu(config);
    return std::static_pointer_cast<IVF>(host_index)->CopyCpuToGpu(device_id, config);
}

IndexModelPtr GPUIVFPQ::Train(const DatasetPtr &dataset, const Config &config) {
    auto nlist = config["nlist"].as<size_t>();
    auto M = config["M"].as<size_t>();        // number of subquantizers(subvectors)
    auto nbits = config["nbits"].as<size_t>();// number of bit per subvector index
    auto gpu_num = config.get_with_default("gpu_id", gpu_id_);
    auto metric_type = config["metric_type"].as_string() == "L2" ?
                       faiss::METRIC_L2 : faiss::METRIC_L2; // IP not support.

    GETTENSOR(dataset)

    // TODO(linxj): set device here.
    faiss::gpu::StandardGpuResources res;
    faiss::gpu::GpuIndexIVFPQ device_index(&res, dim, nlist, M, nbits, metric_type);
    device_index.train(rows, (float *) p_data);

    std::shared_ptr<faiss::Index> host_index = nullptr;
    host_index.reset(faiss::gpu::index_gpu_to_cpu(&device_index));

    return std::make_shared<IVFIndexModel>(host_index);
}

std::shared_ptr<faiss::IVFSearchParameters> GPUIVFPQ::GenParams(const Config &config) {
    auto params = std::make_shared<faiss::IVFPQSearchParameters>();
    params->nprobe = config.get_with_default("nprobe", size_t(1));
    //params->scan_table_threshold = 0;
    //params->polysemous_ht = 0;
    //params->max_codes = 0;

    return params;
}

VectorIndexPtr GPUIVFPQ::CopyGpuToCpu(const Config &config) {
    KNOWHERE_THROW_MSG("not support yet");
}

IndexModelPtr GPUIVFSQ::Train(const DatasetPtr &dataset, const Config &config) {
    auto nlist = config["nlist"].as<size_t>();
    auto nbits = config["nbits"].as<size_t>(); // TODO(linxj):  gpu only support SQ4 SQ8 SQ16
    auto gpu_num = config.get_with_default("gpu_id", gpu_id_);
    auto metric_type = config["metric_type"].as_string() == "L2" ?
                       faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;

    GETTENSOR(dataset)

    std::stringstream index_type;
    index_type << "IVF" << nlist << "," << "SQ" << nbits;
    auto build_index = faiss::index_factory(dim, index_type.str().c_str(), metric_type);

    faiss::gpu::StandardGpuResources res;
    auto device_index = faiss::gpu::index_cpu_to_gpu(&res, gpu_num, build_index);
    device_index->train(rows, (float *) p_data);

    std::shared_ptr<faiss::Index> host_index = nullptr;
    host_index.reset(faiss::gpu::index_gpu_to_cpu(device_index));

    delete device_index;
    delete build_index;

    return std::make_shared<IVFIndexModel>(host_index);
}

VectorIndexPtr GPUIVFSQ::CopyGpuToCpu(const Config &config) {
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index *device_index = index_.get();
    faiss::Index *host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVFSQ>(new_index);
}

FaissGpuResourceMgr &FaissGpuResourceMgr::GetInstance() {
    static FaissGpuResourceMgr instance;
    return instance;
}

void FaissGpuResourceMgr::AllocateTempMem(std::shared_ptr<faiss::gpu::StandardGpuResources> &res,
                                          const int64_t &device_id,
                                          const int64_t &size) {
    if (size) {
        res->setTempMemory(size);
    }
    else {
        auto search = devices_params_.find(device_id);
        if (search != devices_params_.end()) {
            res->setTempMemory(search->second.temp_mem_size);
        }
        // else do nothing. allocate when use.
    }
}

void FaissGpuResourceMgr::InitDevice(int64_t device_id,
                                     int64_t pin_mem_size,
                                     int64_t temp_mem_size,
                                     int64_t res_num) {
    DeviceParams params;
    params.pinned_mem_size = pin_mem_size;
    params.temp_mem_size = temp_mem_size;
    params.resource_num = res_num;

    devices_params_.emplace(device_id, params);
}

void FaissGpuResourceMgr::InitResource() {
    for(auto& device : devices_params_) {
        auto& resource_vec = idle_[device.first];

        for (int i = 0; i < device.second.resource_num; ++i) {
            auto res = std::make_shared<faiss::gpu::StandardGpuResources>();
            res->noTempMemory();
            resource_vec.push_back(res);
        }
    }
}

std::shared_ptr<faiss::gpu::StandardGpuResources> FaissGpuResourceMgr::GetRes(const int64_t &device_id,
                                                                              const int64_t &alloc_size) {
    std::lock_guard<std::mutex> lk(mutex_);

    if (!is_init) {
        InitResource();
        is_init = true;
    }

    auto search = idle_.find(device_id);
    if (search != idle_.end()) {
        auto res = search->second.back();
        AllocateTempMem(res, device_id, alloc_size);

        search->second.pop_back();
        return res;
    }
}

void FaissGpuResourceMgr::MoveToInuse(const int64_t &device_id, const std::shared_ptr<faiss::gpu::StandardGpuResources> &res) {
    std::lock_guard<std::mutex> lk(mutex_);
    in_use_[device_id].push_back(res);
}

void FaissGpuResourceMgr::MoveToIdle(const int64_t &device_id, const std::shared_ptr<faiss::gpu::StandardGpuResources> &res) {
    std::lock_guard<std::mutex> lk(mutex_);
    idle_[device_id].push_back(res);
}

void GPUIndex::SetGpuDevice(const int &gpu_id) {
    gpu_id_ = gpu_id;
}

const int64_t &GPUIndex::GetGpuDevice() {
    return gpu_id_;
}

}
}
