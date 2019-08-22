/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <stdint.h>

namespace zilliz {
namespace milvus {
namespace engine {

constexpr uint64_t K = 1024UL;
constexpr uint64_t M = K * K;
constexpr uint64_t G = K * M;
constexpr uint64_t T = K * G;

constexpr uint64_t MAX_TABLE_FILE_MEM = 128 * M;

constexpr int VECTOR_TYPE_SIZE = sizeof(float);

static constexpr uint64_t ONE_KB = K;
static constexpr uint64_t ONE_MB = ONE_KB*ONE_KB;
static constexpr uint64_t ONE_GB = ONE_KB*ONE_MB;

} // namespace engine
} // namespace milvus
} // namespace zilliz
