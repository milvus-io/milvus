/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <src/cache/GpuCacheMgr.h>
#include "TestTask.h"


namespace zilliz {
namespace milvus {
namespace engine {


TestTask::TestTask(TableFileSchemaPtr& file) : XSearchTask(file) {}

void
TestTask::Load(LoadType type, uint8_t device_id) {
    load_count_++;
}

void
TestTask::Execute() {
    std::lock_guard<std::mutex> lock(mutex_);
    exec_count_++;
    done_ = true;
}

TaskPtr
TestTask::Clone() {
    TableFileSchemaPtr dummy = nullptr;
    auto ret = std::make_shared<TestTask>(dummy);
    ret->load_count_ = load_count_;
    ret->exec_count_ = exec_count_;
    return ret;
}

void
TestTask::Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return done_; });
}

}
}
}

