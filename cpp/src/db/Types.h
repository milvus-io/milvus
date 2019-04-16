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
