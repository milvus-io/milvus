/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MegaSearch.h"

namespace megasearch {

class ConvertUtil {
public:
    static std::string IndexType2Str(megasearch::IndexType index);
    static megasearch::IndexType Str2IndexType(const std::string& type);
};

}
