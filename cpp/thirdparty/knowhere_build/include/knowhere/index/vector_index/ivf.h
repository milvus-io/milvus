////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <mutex>

#include <faiss/IndexIVF.h>
#include <faiss/AuxIndexStructures.h>
#include <faiss/Index.h>

#include "knowhere/index/vector_index/vector_index.h"


namespace zilliz {
namespace knowhere {

class BasicIndex {
 protected:
    explicit BasicIndex(std::shared_ptr<faiss::Index> index);
    virtual BinarySet SerializeImpl();
    virtual void LoadImpl(const BinarySet &index_binary);

 protected:
    std::shared_ptr<faiss::Index> index_ = nullptr;
};

using Graph = std::vector<std::vector<int64_t>>;

class IVF : public VectorIndex, public BasicIndex {
 public:
    IVF() : BasicIndex(nullptr) {};
    explicit IVF(std::shared_ptr<faiss::Index> index) : BasicIndex(std::move(index)) {};
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
    void set_index_model(IndexModelPtr model) override;
    void Add(const DatasetPtr &dataset, const Config &config) override;
    void AddWithoutIds(const DatasetPtr &dataset, const Config &config);
    DatasetPtr Search(const DatasetPtr &dataset, const Config &config) override;
    void GenGraph(const int64_t &k, Graph &graph, const DatasetPtr &dataset, const Config &config);
    BinarySet Serialize() override;
    void Load(const BinarySet &index_binary) override;
    int64_t Count() override;
    int64_t Dimension() override;

 protected:
    virtual std::shared_ptr<faiss::IVFSearchParameters> GenParams(const Config &config);

    virtual void search_impl(int64_t n,
                             const float *data,
                             int64_t k,
                             float *distances,
                             int64_t *labels,
                             const Config &cfg);

 protected:
    std::mutex mutex_;
};

using IVFIndexPtr = std::shared_ptr<IVF>;

class IVFSQ : public IVF {
 public:
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
};

class IVFPQ : public IVF {
 public:
    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
 protected:
    std::shared_ptr<faiss::IVFSearchParameters> GenParams(const Config &config) override;
};


//class OPQIVFPQ : public IVFPQ {
// public:
//    PreprocessorPtr BuildPreprocessor(const Dataset &dataset, const Config &config) override;
//};


class GPUIVF;


struct MemoryIOWriter : public faiss::IOWriter {
    uint8_t *data_ = nullptr;
    size_t total = 0;
    size_t rp = 0;

    size_t operator()(const void *ptr, size_t size, size_t nitems) override;
};


struct MemoryIOReader : public faiss::IOReader {
    uint8_t *data_;
    size_t rp = 0;
    size_t total = 0;

    size_t operator()(void *ptr, size_t size, size_t nitems) override;

};


class IVFIndexModel : public IndexModel, public BasicIndex {
    friend IVF;
    friend GPUIVF;

 public:
    explicit IVFIndexModel(std::shared_ptr<faiss::Index> index);
    IVFIndexModel() : BasicIndex(nullptr) {};
    BinarySet Serialize() override;
    void Load(const BinarySet &binary) override;

 protected:
    std::mutex mutex_;
};

using IVFIndexModelPtr = std::shared_ptr<IVFIndexModel>;

}
}