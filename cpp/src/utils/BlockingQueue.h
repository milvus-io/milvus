/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <assert.h>
#include <condition_variable>
#include <iostream>
#include <queue>
#include <vector>

namespace zilliz {
namespace milvus {
namespace server {

template<typename T>
class BlockingQueue {
public:
    BlockingQueue() : mtx(), full_(), empty_() {}

    BlockingQueue(const BlockingQueue &rhs) = delete;

    BlockingQueue &operator=(const BlockingQueue &rhs) = delete;

    void Put(const T &task);

    T Take();

    T Front();

    T Back();

    size_t Size();

    bool Empty();

    void SetCapacity(const size_t capacity);

private:
    mutable std::mutex mtx;
    std::condition_variable full_;
    std::condition_variable empty_;
    std::queue<T> queue_;
    size_t capacity_ = 32;
};

}
}
}


#include "./BlockingQueue.inl"
