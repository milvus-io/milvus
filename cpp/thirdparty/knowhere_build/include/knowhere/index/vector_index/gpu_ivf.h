#pragma once

#include <faiss/gpu/StandardGpuResources.h>

#include "ivf.h"


namespace zilliz {
namespace knowhere {


class GPUIVF : public IVF {
 public:
    explicit GPUIVF(const int &device_id) : IVF(), gpu_id_(device_id) {}
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
    void set_index_model(IndexModelPtr model) override;
    DatasetPtr Search(const DatasetPtr &dataset, const Config &config) override;
    IVFIndexPtr Copy_index_gpu_to_cpu();
    void SetGpuDevice(const int &gpu_id);

 protected:
    BinarySet SerializeImpl() override;
    void LoadImpl(const BinarySet &index_binary) override;

 protected:
    int64_t gpu_id_;
    faiss::gpu::StandardGpuResources res_;
};

class GPUIVFSQ : public GPUIVF {
 public:
    explicit GPUIVFSQ(const int &device_id) : GPUIVF(device_id) {}
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
};

class GPUIVFPQ : public GPUIVF {
 public:
    explicit GPUIVFPQ(const int &device_id) : GPUIVF(device_id) {}
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;

 protected:
    // TODO(linxj): remove GenParams.
    std::shared_ptr<faiss::IVFSearchParameters> GenParams(const Config &config) override;
};


} // namespace knowhere
} // namespace zilliz
