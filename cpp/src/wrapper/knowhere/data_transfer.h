////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "knowhere/adapter/structure.h"


namespace zilliz {
namespace milvus {
namespace engine {

extern zilliz::knowhere::DatasetPtr
GenDatasetWithIds(const int64_t &nb, const int64_t &dim, const float *xb, const long *ids);

extern zilliz::knowhere::DatasetPtr
GenDataset(const int64_t &nb, const int64_t &dim, const float *xb);

}
}
}
