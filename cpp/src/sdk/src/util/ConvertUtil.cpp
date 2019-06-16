/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ConvertUtil.h"
#include "Exception.h"

#include <map>

namespace milvus {

static const std::string INDEX_RAW = "raw";
static const std::string INDEX_IVFFLAT = "ivfflat";

std::string ConvertUtil::IndexType2Str(IndexType index) {
    static const std::map<IndexType, std::string> s_index2str = {
            {IndexType::cpu_idmap, INDEX_RAW},
            {IndexType::gpu_ivfflat, INDEX_IVFFLAT}
    };

    const auto& iter = s_index2str.find(index);
    if(iter == s_index2str.end()) {
        throw Exception(StatusCode::InvalidAgument, "Invalid index type");
    }

    return iter->second;
}

IndexType ConvertUtil::Str2IndexType(const std::string& type) {
    static const std::map<std::string, IndexType> s_str2index = {
            {INDEX_RAW, IndexType::cpu_idmap},
            {INDEX_IVFFLAT, IndexType::gpu_ivfflat}
    };

    const auto& iter = s_str2index.find(type);
    if(iter == s_str2index.end()) {
        throw Exception(StatusCode::InvalidAgument, "Invalid index type");
    }

    return iter->second;
}

}