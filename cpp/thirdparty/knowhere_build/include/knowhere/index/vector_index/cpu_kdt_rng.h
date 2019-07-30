#pragma once

#include <cstdint>
#include <memory>
#include "knowhere/index/vector_index/vector_index.h"
#include "knowhere/index/index_model.h"
#include <SPTAG/AnnService/inc/Core/VectorIndex.h>


namespace zilliz {
namespace knowhere {


class CPUKDTRNG : public VectorIndex {
 public:
    CPUKDTRNG() {
        index_ptr_ = SPTAG::VectorIndex::CreateInstance(SPTAG::IndexAlgoType::KDT,
                                                        SPTAG::VectorValueType::Float);
        index_ptr_->SetParameter("DistCalcMethod", "L2");
    }

 public:
    BinarySet
    Serialize() override;

    void
    Load(const BinarySet &index_array) override;

 public:
    PreprocessorPtr
    BuildPreprocessor(const DatasetPtr &dataset, const Config &config) override;
    int64_t Count() override;
    int64_t Dimension() override;

    IndexModelPtr
    Train(const DatasetPtr &dataset, const Config &config) override;

    void
    Add(const DatasetPtr &dataset, const Config &config) override;

    DatasetPtr
    Search(const DatasetPtr &dataset, const Config &config) override;

 private:
    void
    SetParameters(const Config &config);

 private:
    PreprocessorPtr preprocessor_;
    std::shared_ptr<SPTAG::VectorIndex> index_ptr_;
};

using CPUKDTRNGPtr = std::shared_ptr<CPUKDTRNG>;

class CPUKDTRNGIndexModel : public IndexModel {
 public:
    BinarySet
    Serialize() override;

    void
    Load(const BinarySet &binary) override;

 private:
    std::shared_ptr<SPTAG::VectorIndex> index_;
};

using CPUKDTRNGIndexModelPtr = std::shared_ptr<CPUKDTRNGIndexModel>;

} // namespace knowhere
} // namespace zilliz
