#pragma once

#include <memory>
#include "preprocessor.h"


namespace zilliz {
namespace knowhere {

class NormalizePreprocessor : public Preprocessor {
 public:
    DatasetPtr
    Preprocess(const DatasetPtr &input) override;

 private:

    void
    Normalize(float *arr, int64_t dimension);
};


using NormalizePreprocessorPtr = std::shared_ptr<NormalizePreprocessor>;


} // namespace knowhere
} // namespace zilliz
