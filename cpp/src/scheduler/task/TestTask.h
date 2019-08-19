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

class TestTask : public Task {
public:
    TestTask() = default;

public:
    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

public:
    uint64_t load_count_;
    uint64_t exec_count_;
};


}
}
}
