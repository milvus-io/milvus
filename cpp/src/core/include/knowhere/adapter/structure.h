////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <knowhere/common/dataset.h>


namespace zilliz {
namespace knowhere {

extern ArrayPtr
ConstructInt64ArraySmart(uint8_t *data, int64_t size);

extern ArrayPtr
ConstructFloatArraySmart(uint8_t *data, int64_t size);

extern TensorPtr
ConstructFloatTensorSmart(uint8_t *data, int64_t size, std::vector<int64_t> shape);

extern ArrayPtr
ConstructInt64Array(uint8_t *data, int64_t size);

extern ArrayPtr
ConstructFloatArray(uint8_t *data, int64_t size);

extern TensorPtr
ConstructFloatTensor(uint8_t *data, int64_t size, std::vector<int64_t> shape);

extern FieldPtr
ConstructInt64Field(const std::string &name);

extern FieldPtr
ConstructFloatField(const std::string &name);


}
}
