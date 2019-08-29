#pragma once

#include <faiss/gpu/StandardGpuResources.h>

#include "ivf.h"


namespace zilliz {
namespace knowhere {

class FaissGpuResourceMgr {
 public:
    struct DeviceParams {
        int64_t temp_mem_size = 0;
        int64_t pinned_mem_size = 0;
        int64_t resource_num = 2;
    };

 public:
    using ResPtr = std::shared_ptr<faiss::gpu::StandardGpuResources>;
    using ResWPtr = std::weak_ptr<faiss::gpu::StandardGpuResources>;

    static FaissGpuResourceMgr &
    GetInstance();

    void
    AllocateTempMem(ResPtr &res, const int64_t& device_id, const int64_t& size);

    void
    InitDevice(int64_t device_id,
               int64_t pin_mem_size = 0,
               int64_t temp_mem_size = 0,
               int64_t res_num = 2);

    void InitResource();

    ResPtr GetRes(const int64_t &device_id, const int64_t& alloc_size = 0);

    void MoveToInuse(const int64_t &device_id, const ResPtr& res);
    void MoveToIdle(const int64_t &device_id, const ResPtr& res);

 protected:
    bool is_init = false;

    std::mutex mutex_;
    std::map<int64_t, DeviceParams> devices_params_;
    std::map<int64_t, std::vector<ResPtr>> in_use_;
    std::map<int64_t, std::vector<ResPtr>> idle_;
};

class ResScope {
 public:
    ResScope(const int64_t device_id,std::shared_ptr<faiss::gpu::StandardGpuResources> &res) : resource(res), device_id(device_id) {
        FaissGpuResourceMgr::GetInstance().MoveToInuse(device_id, resource);
    }

    ~ResScope() {
        resource->noTempMemory();
        FaissGpuResourceMgr::GetInstance().MoveToIdle(device_id, resource);
    }

 private:
    std::shared_ptr<faiss::gpu::StandardGpuResources> resource;
    int64_t device_id;
};

class GPUIndex {
 public:
    explicit GPUIndex(const int &device_id) : gpu_id_(device_id) {};

    virtual VectorIndexPtr CopyGpuToCpu(const Config &config) = 0;
    virtual VectorIndexPtr CopyGpuToGpu(const int64_t &device_id, const Config &config) = 0;

    void SetGpuDevice(const int &gpu_id);
    const int64_t &GetGpuDevice();

 protected:
    int64_t gpu_id_;
};

class GPUIVF : public IVF, public GPUIndex {
 public:
    explicit GPUIVF(const int &device_id) : IVF(), GPUIndex(device_id) {}
    explicit GPUIVF(std::shared_ptr<faiss::Index> index, const int64_t &device_id)
        : IVF(std::move(index)), GPUIndex(device_id) {};
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
    void set_index_model(IndexModelPtr model) override;
    //DatasetPtr Search(const DatasetPtr &dataset, const Config &config) override;
    VectorIndexPtr CopyGpuToCpu(const Config &config) override;
    VectorIndexPtr CopyGpuToGpu(const int64_t &device_id, const Config &config) override;
    VectorIndexPtr Clone() final;

    // TODO(linxj): Deprecated
    virtual IVFIndexPtr Copy_index_gpu_to_cpu();

 protected:
    void search_impl(int64_t n,
                     const float *data,
                     int64_t k,
                     float *distances,
                     int64_t *labels,
                     const Config &cfg) override;
    BinarySet SerializeImpl() override;
    void LoadImpl(const BinarySet &index_binary) override;
};

class GPUIVFSQ : public GPUIVF {
 public:
    explicit GPUIVFSQ(const int &device_id) : GPUIVF(device_id) {}
    explicit GPUIVFSQ(std::shared_ptr<faiss::Index> index, const int64_t& device_id) : GPUIVF(std::move(index),device_id) {};
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;

 public:
    VectorIndexPtr CopyGpuToCpu(const Config &config) override;
};

class GPUIVFPQ : public GPUIVF {
 public:
    explicit GPUIVFPQ(const int &device_id) : GPUIVF(device_id) {}
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;

 public:
    VectorIndexPtr CopyGpuToCpu(const Config &config) override;

 protected:
    // TODO(linxj): remove GenParams.
    std::shared_ptr<faiss::IVFSearchParameters> GenParams(const Config &config) override;
};


} // namespace knowhere
} // namespace zilliz
