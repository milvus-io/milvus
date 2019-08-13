/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "Scheduler.h"


namespace zilliz {
namespace milvus {
namespace engine {

void
StartUpEvent::Process() {

}

void
FinishTaskEvent::Process() {
//        for (nei : res->neighbours) {
//            tasks = cost(nei->task_table(), nei->connection, limit = 3)
//            res->task_table()->PutTasks(tasks);
//        }
//        res->WakeUpExec();
}

void
CopyCompletedEvent::Process() {

}

void
TaskTableUpdatedEvent::Process() {

}


void
Scheduler::Start() {
    worker_thread_ = std::thread(&Scheduler::worker_thread_, this);
}

std::string
Scheduler::Dump() {
    return std::string();
}

void
Scheduler::worker_function() {
    while (running_) {
        auto event = event_queue_.front();
        event->Process();
    }
}

}
}
}
