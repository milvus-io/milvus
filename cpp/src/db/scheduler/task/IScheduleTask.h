/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <memory>

namespace zilliz {
namespace milvus {
namespace engine {

enum class ScheduleTaskType {
    kUnknown = 0,
    kIndexLoad,
    kSearch,
    kDelete,
};

class IScheduleTask {
public:
    IScheduleTask(ScheduleTaskType type)
    : type_(type) {
    }

    virtual ~IScheduleTask() = default;

    ScheduleTaskType type() const { return type_; }

    virtual std::shared_ptr<IScheduleTask> Execute() = 0;

protected:
    ScheduleTaskType type_;
};

using ScheduleTaskPtr = std::shared_ptr<IScheduleTask>;

}
}
}
