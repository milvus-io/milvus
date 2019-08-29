////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "ivf.h"
#include "gpu_ivf.h"


namespace zilliz {
namespace knowhere {

class IDMAP : public VectorIndex, public BasicIndex {
 public:
    IDMAP() : BasicIndex(nullptr) {};
    explicit IDMAP(std::shared_ptr<faiss::Index> index) : BasicIndex(std::move(index)) {};
    BinarySet Serialize() override;
    void Load(const BinarySet &index_binary) override;
    void Train(const Config &config);
    DatasetPtr Search(const DatasetPtr &dataset, const Config &config) override;
    int64_t Count() override;
    VectorIndexPtr Clone() override;
    int64_t Dimension() override;
    void Add(const DatasetPtr &dataset, const Config &config) override;
    VectorIndexPtr CopyCpuToGpu(const int64_t &device_id, const Config &config);
    void Seal() override;

    virtual float *GetRawVectors();
    virtual int64_t *GetRawIds();

 protected:
    std::mutex mutex_;
};

using IDMAPPtr = std::shared_ptr<IDMAP>;

class GPUIDMAP : public IDMAP, public GPUIndex {
 public:
    explicit GPUIDMAP(std::shared_ptr<faiss::Index> index, const int64_t &device_id)
        : IDMAP(std::move(index)), GPUIndex(device_id) {}

    VectorIndexPtr CopyGpuToCpu(const Config &config) override;
    float *GetRawVectors() override;
    int64_t *GetRawIds() override;
    VectorIndexPtr Clone() override;
    VectorIndexPtr CopyGpuToGpu(const int64_t &device_id, const Config &config) override;

 protected:
    BinarySet SerializeImpl() override;
    void LoadImpl(const BinarySet &index_binary) override;
};

using GPUIDMAPPtr = std::shared_ptr<GPUIDMAP>;

}
}
