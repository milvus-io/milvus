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

Status ExecutionEngine::AddWithIds(const std::vector<float>& vectors, const std::vector<long>& vector_ids) {
    long n1 = (long)vectors.size();
    long n2 = (long)vector_ids.size();
    if (n1 != n2) {
        LOG(ERROR) << "vectors size is not equal to the size of vector_ids: " << n1 << "!=" << n2;
        return Status::Error("Error: AddWithIds");
    }
    return AddWithIds(n1, vectors.data(), vector_ids.data());
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
