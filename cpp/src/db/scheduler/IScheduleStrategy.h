/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IndexLoaderQueue.h"
#include "SearchContext.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class IScheduleStrategy {
public:
    virtual ~IScheduleStrategy() {}

    virtual bool Schedule(const SearchContextPtr &search_context, IndexLoaderQueue::LoaderQueue& loader_list) = 0;
};

using ScheduleStrategyPtr = std::shared_ptr<IScheduleStrategy>;

}
}
}