/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Task.h"


namespace zilliz {
namespace milvus {
namespace engine {

class XDeleteTask : public Task {
public:
    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;
};

}
}
}
