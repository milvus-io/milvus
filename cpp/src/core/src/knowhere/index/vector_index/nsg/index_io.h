////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "knowhere/index/vector_index/nsg/nsg.h"

namespace zilliz {
namespace knowhere {
namespace algo {

void read_from_file(NsgIndex* index, const char *filename);
void write_to_file(NsgIndex* index, const char *filename);

}
}
}
