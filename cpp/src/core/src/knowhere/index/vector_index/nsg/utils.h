////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <x86intrin.h>
#include <iostream>

#include <faiss/AutoTune.h>

#include "knowhere/index/vector_index/nsg/nsg.h"
#include "knowhere/common/config.h"


namespace zilliz {
namespace knowhere {
namespace algo {

extern int InsertIntoPool(Neighbor *addr, unsigned K, Neighbor nn);
extern float calculate(const float *a, const float *b, unsigned size);

}
}
}
