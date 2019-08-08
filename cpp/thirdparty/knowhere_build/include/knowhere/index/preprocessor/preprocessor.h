#pragma once

#include <memory>
#include "knowhere/common/dataset.h"


namespace zilliz {
namespace knowhere {


class Preprocessor {
 public:
    virtual DatasetPtr
    Preprocess(const DatasetPtr &input) = 0;
};


using PreprocessorPtr = std::shared_ptr<Preprocessor>;


} // namespace knowhere
} // namespace zilliz
