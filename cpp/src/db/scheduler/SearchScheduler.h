/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "SearchContext.h"
#include "IndexLoaderQueue.h"
#include "SearchTaskQueue.h"

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
    std::shared_ptr<std::thread> index_load_thread_;
    std::shared_ptr<std::thread> search_thread_;

    IndexLoaderQueue index_load_queue_;
    SearchTaskQueue search_queue_;

    bool stopped_ = true;
};


}
}
}
