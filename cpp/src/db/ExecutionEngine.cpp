/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ExecutionEngine.h"

#include <easylogging++.h>

namespace zilliz {
namespace milvus {
namespace engine {

Status ExecutionEngine::AddWithIdArray(const std::vector<float>& vectors, const std::vector<long>& vector_ids) {
    long n = (long)vector_ids.size();
    return AddWithIds(n, vectors.data(), vector_ids.data());
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
