/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "TaskDispatchQueue.h"
#include "TaskDispatchStrategy.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

void
TaskDispatchQueue::Put(const ScheduleContextPtr &context) {
    std::unique_lock <std::mutex> lock(mtx);
    full_.wait(lock, [this] { return (queue_.size() < capacity_); });

    if(context == nullptr) {
        queue_.push_front(nullptr);
        empty_.notify_all();
        return;
    }

    TaskDispatchStrategy::Schedule(context, queue_);

    empty_.notify_all();
}

ScheduleTaskPtr
TaskDispatchQueue::Take() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    ScheduleTaskPtr front(queue_.front());
    queue_.pop_front();
    full_.notify_all();
    return front;
}

size_t
TaskDispatchQueue::Size() {
    std::lock_guard <std::mutex> lock(mtx);
    return queue_.size();
}

ScheduleTaskPtr
TaskDispatchQueue::Front() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });
    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(server::SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }
    ScheduleTaskPtr front(queue_.front());
    return front;
}

ScheduleTaskPtr
TaskDispatchQueue::Back() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(server::SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    ScheduleTaskPtr back(queue_.back());
    return back;
}

bool
TaskDispatchQueue::Empty() {
    std::unique_lock <std::mutex> lock(mtx);
    return queue_.empty();
}

void
TaskDispatchQueue::SetCapacity(const size_t capacity) {
    capacity_ = (capacity > 0 ? capacity : capacity_);
}

}
}
}