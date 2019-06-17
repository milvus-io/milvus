/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <easylogging++.h>
#include <assert.h>
#include <atomic>
#include "Env.h"

namespace zilliz {
namespace milvus {
namespace engine {

Env::Env()
    : bg_work_started_(false),
      shutting_down_(false) {
}

void Env::Schedule(void (*function)(void* arg), void* arg) {
    std::unique_lock<std::mutex> lock(bg_work_mutex_);
    if (shutting_down_) return;

    if (!bg_work_started_) {
        bg_work_started_ = true;
        std::thread bg_thread(Env::BackgroundThreadEntryPoint, this);
        bg_thread.detach();
    }

    if (bg_work_queue_.empty()) {
        bg_work_cv_.notify_one();
    }

    bg_work_queue_.emplace(function, arg);
}

void Env::BackgroundThreadMain() {
    while (!shutting_down_) {
        std::unique_lock<std::mutex> lock(bg_work_mutex_);
        while (bg_work_queue_.empty() && !shutting_down_) {
            bg_work_cv_.wait(lock);
        }

        if (shutting_down_) break;

        assert(!bg_work_queue_.empty());
        auto bg_function = bg_work_queue_.front().function_;
        void* bg_arg = bg_work_queue_.front().arg_;
        bg_work_queue_.pop();

        lock.unlock();
        bg_function(bg_arg);
    }

    std::unique_lock<std::mutex> lock(bg_work_mutex_);
    bg_work_started_ = false;
    bg_work_cv_.notify_all();
}

void Env::Stop() {
    {
        std::unique_lock<std::mutex> lock(bg_work_mutex_);
        if (shutting_down_ || !bg_work_started_) return;
    }
    shutting_down_ = true;
    {
        std::unique_lock<std::mutex> lock(bg_work_mutex_);
        if (bg_work_queue_.empty()) {
            bg_work_cv_.notify_one();
        }
        while (bg_work_started_) {
            bg_work_cv_.wait(lock);
        }
    }
    shutting_down_ = false;
}

Env::~Env() {}

Env* Env::Default() {
    static Env env;
    return &env;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
