#pragma once

#include <memory>
#include "arrow/tensor.h"


namespace zilliz {
namespace knowhere {


using Tensor = arrow::Tensor;
using TensorPtr = std::shared_ptr<Tensor>;


} // namespace knowhere
} // namespace zilliz
