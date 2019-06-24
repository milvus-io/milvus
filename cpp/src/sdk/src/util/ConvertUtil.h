/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MilvusApi.h"

namespace milvus {

class ConvertUtil {
public:
    static std::string IndexType2Str(IndexType index);
    static IndexType Str2IndexType(const std::string& type);
};

}
