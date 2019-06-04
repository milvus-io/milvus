/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "SearchContext.h"
#include "utils/ThreadPool.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class SearchScheduler {
private:
    SearchScheduler();
    virtual ~SearchScheduler();

public:
    static SearchScheduler& GetInstance();

    bool ScheduleSearchTask(SearchContextPtr& search_context);

private:
    bool Start();
    bool Stop();

    bool IndexLoadWorker();
    bool SearchWorker();

private:
    server::ThreadPool thread_pool_;
    bool stopped_ = true;
};


}
}
}
