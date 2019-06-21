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

enum class ScheduleContextType {
    kUnknown = 0,
    kSearch,
    kDelete,
};

class IScheduleContext {
public:
    IScheduleContext(ScheduleContextType type)
    : type_(type) {
    }

    virtual ~IScheduleContext() = default;

    ScheduleContextType type() const { return type_; }

protected:
    ScheduleContextType type_;
};

using ScheduleContextPtr = std::shared_ptr<IScheduleContext>;

}
}
}
