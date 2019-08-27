/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "SearchTask.h"


namespace zilliz {
namespace milvus {
namespace engine {

class TestTask : public XSearchTask {
public:
    TestTask(TableFileSchemaPtr& file);

public:
    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

    TaskPtr
    Clone() override;

    void
    Wait();

public:
    uint64_t load_count_ = 0;
    uint64_t exec_count_ = 0;

    bool done_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
};


}
}
}
