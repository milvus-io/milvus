/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <src/db/scheduler/context/DeleteContext.h>
#include "Task.h"


namespace zilliz {
namespace milvus {
namespace engine {

class XDeleteTask : public Task {
public:
    explicit
    XDeleteTask(DeleteContextPtr &delete_context);

    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

    TaskPtr
    Clone() override;

public:
    DeleteContextPtr delete_context_ptr_;
};

}
}
}
