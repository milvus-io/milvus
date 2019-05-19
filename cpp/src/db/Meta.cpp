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

DateT Meta::GetDate(const std::time_t& t, int day_delta) {
    tm *ltm = std::localtime(&t);
    if (day_delta > 0) {
        do {
            ++ltm->tm_mday;
            --day_delta;
        } while(day_delta > 0);
        mktime(ltm);
    } else if (day_delta < 0) {
        do {
            --ltm->tm_mday;
            ++day_delta;
        } while(day_delta < 0);
        mktime(ltm);
    } else {
        ltm->tm_mday;
    }
    return ltm->tm_year*10000 + ltm->tm_mon*100 + ltm->tm_mday;
}

DateT Meta::GetDate(int day_delta) {
    return GetDate(std::time(nullptr), day_delta);
}

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
