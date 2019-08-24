/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "db/engine/ExecutionEngine.h"

#include <vector>
#include <stdint.h>

namespace zilliz {
namespace milvus {
namespace engine {

typedef long IDNumber;
typedef IDNumber* IDNumberPtr;
typedef std::vector<IDNumber> IDNumbers;

typedef std::vector<std::pair<IDNumber, double>> QueryResult;
typedef std::vector<QueryResult> QueryResults;

struct TableIndex {
    int32_t engine_type_ = (int)EngineType::FAISS_IDMAP;
    int32_t nlist_ = 16384;
    int32_t metric_type_ = (int)MetricType::L2;
};

} // namespace engine
} // namespace milvus
} // namespace zilliz
