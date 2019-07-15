/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

namespace zilliz {
namespace milvus {
namespace engine {

const size_t K = 1024UL;
const size_t M = K * K;
const size_t G = K * M;
const size_t T = K * G;

const size_t MAX_TABLE_FILE_MEM = 128 * M;

const int VECTOR_TYPE_SIZE = sizeof(float);

} // namespace engine
} // namespace milvus
} // namespace zilliz
