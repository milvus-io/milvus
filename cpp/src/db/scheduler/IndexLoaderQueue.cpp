/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "IndexLoaderQueue.h"
#include "ScheduleStrategy.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

IndexLoaderQueue&
IndexLoaderQueue::GetInstance() {
    static IndexLoaderQueue instance;
    return instance;
}

void
IndexLoaderQueue::Put(const SearchContextPtr &search_context) {
    std::unique_lock <std::mutex> lock(mtx);
    full_.wait(lock, [this] { return (queue_.size() < capacity_); });

    if(search_context == nullptr) {
        queue_.push_back(nullptr);
        return;
    }

    if (queue_.size() >= capacity_) {
        std::string error_msg =
                "blocking queue is full, capacity: " + std::to_string(capacity_) + " queue_size: " +
                std::to_string(queue_.size());
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(server::SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    ScheduleStrategyPtr strategy = StrategyFactory::CreateMemStrategy();
    strategy->Schedule(search_context, queue_);

    empty_.notify_all();
}

IndexLoaderContextPtr
IndexLoaderQueue::Take() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(server::SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    IndexLoaderContextPtr front(queue_.front());
    queue_.pop_front();
    full_.notify_all();
    return front;
}

size_t
IndexLoaderQueue::Size() {
    std::lock_guard <std::mutex> lock(mtx);
    return queue_.size();
}

IndexLoaderContextPtr
IndexLoaderQueue::Front() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });
    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(server::SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }
    IndexLoaderContextPtr front(queue_.front());
    return front;
}

IndexLoaderContextPtr
IndexLoaderQueue::Back() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(server::SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    IndexLoaderContextPtr back(queue_.back());
    return back;
}

bool
IndexLoaderQueue::Empty() {
    std::unique_lock <std::mutex> lock(mtx);
    return queue_.empty();
}

void
IndexLoaderQueue::SetCapacity(const size_t capacity) {
    capacity_ = (capacity > 0 ? capacity : capacity_);
}

}
}
}