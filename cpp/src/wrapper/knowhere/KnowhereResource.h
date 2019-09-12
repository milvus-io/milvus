////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "utils/Error.h"
#include "knowhere/index/vector_index/gpu_ivf.h"

namespace zilliz {
namespace milvus {
namespace engine {

class KnowhereResource {
public:
    static ErrorCode Initialize();
    static ErrorCode Finalize();
};


}
}
}
