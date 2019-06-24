/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "context/IScheduleContext.h"
#include "task/IScheduleTask.h"

#include <condition_variable>
#include <iostream>
#include <queue>
#include <list>


namespace zilliz {
namespace milvus {
namespace engine {

class TaskDispatchQueue {
public:
    TaskDispatchQueue() : mtx(), full_(), empty_() {}

    TaskDispatchQueue(const TaskDispatchQueue &rhs) = delete;

    TaskDispatchQueue &operator=(const TaskDispatchQueue &rhs) = delete;

    using TaskList = std::list<ScheduleTaskPtr>;

    void Put(const ScheduleContextPtr &context);

    ScheduleTaskPtr Take();

    ScheduleTaskPtr Front();

    ScheduleTaskPtr Back();

    size_t Size();

    bool Empty();

    void SetCapacity(const size_t capacity);

private:
    mutable std::mutex mtx;
    std::condition_variable full_;
    std::condition_variable empty_;

    TaskList queue_;
    size_t capacity_ = 1000000;
};

}
}
}
