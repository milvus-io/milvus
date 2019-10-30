#pragma once

#include "IndexGPUIVF.h"
#include "IndexIVF.h"
#include "IndexIDMAP.h"

#include <memory>
#include <utility>

namespace knowhere {

    class GPUIDMAP : public IDMAP, public GPUIndex {
    public:
        explicit GPUIDMAP(std::shared_ptr<faiss::Index> index, const int64_t &device_id, ResPtr &res)
                : IDMAP(std::move(index)), GPUIndex(device_id, res) {
        }

        VectorIndexPtr
        CopyGpuToCpu(const Config &config) override;

        float *
        GetRawVectors() override;

        int64_t *
        GetRawIds() override;

        VectorIndexPtr
        Clone() override;

        VectorIndexPtr
        CopyGpuToGpu(const int64_t &device_id, const Config &config) override;

    protected:
        void
        search_impl(int64_t n, const float *data, int64_t k, float *distances, int64_t *labels,
                    const Config &cfg) override;

        BinarySet
        SerializeImpl() override;

        void
        LoadImpl(const BinarySet &index_binary) override;
    };

    using GPUIDMAPPtr = std::shared_ptr<GPUIDMAP>;

} // knowhere