#pragma once

#include <faiss/gpu/StandardGpuResources.h>

#include "ivf.h"
#include "src/utils/BlockingQueue.h"


namespace zilliz {
namespace knowhere {

struct Resource {
    explicit Resource(std::shared_ptr<faiss::gpu::StandardGpuResources> &r): faiss_res(r) {
        static int64_t global_id = 0;
        id = global_id++;
    }

    std::shared_ptr<faiss::gpu::StandardGpuResources> faiss_res;
    int64_t id;
    std::mutex mutex;
};
using ResPtr = std::shared_ptr<Resource>;
using ResWPtr = std::weak_ptr<Resource>;

class FaissGpuResourceMgr {
 public:
    friend class ResScope;

 public:
    using ResBQ = zilliz::milvus::server::BlockingQueue<ResPtr>;

    struct DeviceParams {
        int64_t temp_mem_size = 0;
        int64_t pinned_mem_size = 0;
        int64_t resource_num = 2;
    };

 public:
    static FaissGpuResourceMgr &
    GetInstance();

    // Free gpu resource, avoid cudaGetDevice error when deallocate.
    // this func should be invoke before main return
    void
    Free();

    void
    AllocateTempMem(ResPtr &resource, const int64_t& device_id, const int64_t& size);

    void
    InitDevice(int64_t device_id,
               int64_t pin_mem_size = 0,
               int64_t temp_mem_size = 0,
               int64_t res_num = 2);

    void
    InitResource();

    // allocate gpu memory invoke by build or copy_to_gpu
    ResPtr
    GetRes(const int64_t &device_id, const int64_t& alloc_size = 0);

    // allocate gpu memory before search
    // this func will return True if the device is idle and exists an idle resource.
    //bool
    //GetRes(const int64_t& device_id, ResPtr &res, const int64_t& alloc_size = 0);

    void
    MoveToIdle(const int64_t &device_id, const ResPtr& res);

    void
    Dump();

 protected:
    bool is_init = false;

    std::map<int64_t ,std::unique_ptr<std::mutex>> mutex_cache_;
    std::map<int64_t, DeviceParams> devices_params_;
    std::map<int64_t, ResBQ> idle_map_;
};

class ResScope {
 public:
    ResScope(ResPtr &res, const int64_t& device_id, const bool& isown)
        : resource(res), device_id(device_id), move(true), own(isown) {
        if (isown) FaissGpuResourceMgr::GetInstance().mutex_cache_[device_id]->lock();
        res->mutex.lock();
    }

    // specif for search
    // get the ownership of gpuresource and gpu
    ResScope(ResPtr &res, const int64_t &device_id)
        : resource(res), device_id(device_id), move(false), own(true) {
        FaissGpuResourceMgr::GetInstance().mutex_cache_[device_id]->lock();
        res->mutex.lock();
    }

    ~ResScope() {
        if (own) FaissGpuResourceMgr::GetInstance().mutex_cache_[device_id]->unlock();
        if (move) FaissGpuResourceMgr::GetInstance().MoveToIdle(device_id, resource);
        resource->mutex.unlock();
    }

 private:
    ResPtr resource;
    int64_t device_id;
    bool move = true;
    bool own = false;
};

class GPUIndex {
 public:
    explicit GPUIndex(const int &device_id) : gpu_id_(device_id) {}
    GPUIndex(const int& device_id, ResPtr resource): gpu_id_(device_id), res_(std::move(resource)){}

    virtual VectorIndexPtr CopyGpuToCpu(const Config &config) = 0;
    virtual VectorIndexPtr CopyGpuToGpu(const int64_t &device_id, const Config &config) = 0;

    void SetGpuDevice(const int &gpu_id);
    const int64_t &GetGpuDevice();

 protected:
    int64_t gpu_id_;
    ResPtr res_ = nullptr;
};

class GPUIVF : public IVF, public GPUIndex {
 public:
    explicit GPUIVF(const int &device_id) : IVF(), GPUIndex(device_id) {}
    explicit GPUIVF(std::shared_ptr<faiss::Index> index, const int64_t &device_id, ResPtr &resource)
        : IVF(std::move(index)), GPUIndex(device_id, resource) {};
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
    void Add(const DatasetPtr &dataset, const Config &config) override;
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
    explicit GPUIVFSQ(std::shared_ptr<faiss::Index> index, const int64_t &device_id, ResPtr &resource)
        : GPUIVF(std::move(index), device_id, resource) {};
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
