/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <ctime>
#include "Meta.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

DateT Meta::GetDate(const std::time_t& t) {
    tm *ltm = std::localtime(&t);
    return ltm->tm_year*10000 + ltm->tm_mon*100 + ltm->tm_mday;
}

DateT Meta::GetDate() {
    return GetDate(std::time(nullptr));
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
