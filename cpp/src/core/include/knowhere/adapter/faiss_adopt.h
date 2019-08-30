////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

namespace zilliz {
namespace knowhere {

#define GETTENSOR(dataset)                  \
    auto tensor = dataset->tensor()[0];     \
    auto p_data = tensor->raw_data();       \
    auto dim = tensor->shape()[1];          \
    auto rows = tensor->shape()[0];         \


}
}