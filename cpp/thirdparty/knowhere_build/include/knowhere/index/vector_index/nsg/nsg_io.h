////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "nsg.h"
#include "knowhere/index/vector_index/ivf.h"


namespace zilliz {
namespace knowhere {
namespace algo {

extern void write_index(NsgIndex* index, MemoryIOWriter& writer);
extern NsgIndex* read_index(MemoryIOReader& reader);

}
}
}
