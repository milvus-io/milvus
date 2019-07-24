/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

namespace zilliz {
namespace milvus {
namespace engine {

constexpr size_t K = 1024UL;
constexpr size_t M = K * K;
constexpr size_t G = K * M;
constexpr size_t T = K * G;

constexpr size_t MAX_TABLE_FILE_MEM = 128 * M;

constexpr int VECTOR_TYPE_SIZE = sizeof(float);

} // namespace engine
} // namespace milvus
} // namespace zilliz
