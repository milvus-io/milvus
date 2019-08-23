/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "ResourceMgr.h"
#include "Scheduler.h"

#include <mutex>
#include <memory>


namespace zilliz {
namespace milvus {
namespace engine {

class ResMgrInst {
public:
    static ResourceMgrPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<ResourceMgr>();
            }
        }
        return instance;
    }

private:
    static ResourceMgrPtr instance;
    static std::mutex mutex_;
};

class SchedInst {
public:
    static SchedulerPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<Scheduler>(ResMgrInst::GetInstance());
            }
        }
        return instance;
    }

private:
    static SchedulerPtr instance;
    static std::mutex mutex_;
};

void
SchedServInit();

}
}
}
