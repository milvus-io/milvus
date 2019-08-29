/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#pragma once

#include "vector_index.h"


namespace zilliz {
namespace knowhere {

// TODO(linxj): rename CopyToGpu
extern VectorIndexPtr
CopyCpuToGpu(const VectorIndexPtr &index, const int64_t &device_id, const Config &config);

extern VectorIndexPtr
CopyGpuToCpu(const VectorIndexPtr &index, const Config &config);

}
}