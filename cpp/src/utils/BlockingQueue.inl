#pragma once

#include "Log.h"
#include "Error.h"

namespace zilliz {
namespace vecwise {
namespace server {

template<typename T>
void
BlockingQueue<T>::Put(const T &task) {
    std::unique_lock <std::mutex> lock(mtx);
    full_.wait(lock, [this] { return (queue_.size() < capacity_); });

    if (queue_.size() >= capacity_) {
        std::string error_msg =
                "blocking queue is full, capacity: " + std::to_string(capacity_) + " queue_size: " +
                std::to_string(queue_.size());
        SERVER_LOG_ERROR << error_msg;
        throw ServerException(SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    queue_.push(task);
    empty_.notify_all();
}

template<typename T>
T
BlockingQueue<T>::Take() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw ServerException(SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    T front(queue_.front());
    queue_.pop();
    full_.notify_all();
    return front;
}

template<typename T>
size_t
BlockingQueue<T>::Size() {
    std::lock_guard <std::mutex> lock(mtx);
    return queue_.size();
}

template<typename T>
T
BlockingQueue<T>::Front() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });
    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw ServerException(SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }
    T front(queue_.front());
    return front;
}

template<typename T>
T
BlockingQueue<T>::Back() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw ServerException(SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    T back(queue_.back());
    return back;
}

template<typename T>
bool
BlockingQueue<T>::Empty() {
    std::unique_lock <std::mutex> lock(mtx);
    return queue_.empty();
}

template<typename T>
void
BlockingQueue<T>::SetCapacity(const size_t capacity) {
    capacity_ = (capacity > 0 ? capacity : capacity_);
}

}
}
}

