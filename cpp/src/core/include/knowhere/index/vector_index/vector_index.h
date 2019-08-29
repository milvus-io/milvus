#pragma once


#include <memory>
#include "knowhere/common/config.h"
#include "knowhere/common/dataset.h"
#include "knowhere/index/index.h"
#include "knowhere/index/preprocessor/preprocessor.h"


namespace zilliz {
namespace knowhere {

class VectorIndex;
using VectorIndexPtr = std::shared_ptr<VectorIndex>;


class VectorIndex : public Index {
 public:
    virtual PreprocessorPtr
    BuildPreprocessor(const DatasetPtr &dataset, const Config &config) { return nullptr; }

    virtual IndexModelPtr
    Train(const DatasetPtr &dataset, const Config &config) { return nullptr; }

    virtual void
    Add(const DatasetPtr &dataset, const Config &config) = 0;

    virtual void
    Seal() = 0;

    virtual VectorIndexPtr
    Clone() = 0;

    virtual int64_t
    Count() = 0;

    virtual int64_t
    Dimension() = 0;
};



} // namespace knowhere
} // namespace zilliz
