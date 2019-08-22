/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <memory>
#include <src/db/scheduler/context/SearchContext.h>
#include "src/db/scheduler/task/IScheduleTask.h"


namespace zilliz {
namespace milvus {
namespace engine {

enum class LoadType {
    DISK2CPU,
    CPU2GPU,
    GPU2CPU,
};

class Task;

using TaskPtr = std::shared_ptr<Task>;

class Task {
public:
    Task() = default;

    virtual void
    Load(LoadType type, uint8_t device_id) = 0;

    virtual void
    Execute() = 0;

    virtual TaskPtr
    Clone() = 0;

public:
    std::vector<SearchContextPtr> search_contexts_;
    ScheduleTaskPtr task_;
};


}
}
}
