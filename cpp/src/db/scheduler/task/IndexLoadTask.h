/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IScheduleTask.h"
#include "db/scheduler/context/SearchContext.h"

namespace zilliz {
namespace milvus {
namespace engine {

class IndexLoadTask : public IScheduleTask {
public:
    IndexLoadTask();

    virtual std::shared_ptr<IScheduleTask> Execute() override;

public:
    TableFileSchemaPtr file_;
    std::vector<SearchContextPtr> search_contexts_;
};

using IndexLoadTaskPtr = std::shared_ptr<IndexLoadTask>;

}
}
}
