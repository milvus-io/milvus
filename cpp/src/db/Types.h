/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>

namespace zilliz {
namespace vecwise {
namespace engine {

typedef long IDNumber;
typedef IDNumber* IDNumberPtr;
typedef std::vector<IDNumber> IDNumbers;

typedef std::vector<IDNumber> QueryResult;
typedef std::vector<QueryResult> QueryResults;


} // namespace engine
} // namespace vecwise
} // namespace zilliz
