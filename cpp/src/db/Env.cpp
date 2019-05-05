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
namespace vecwise {
namespace engine {

Env::Env()
    : _bg_work_started(false),
      _shutting_down(false) {
}

void Env::schedule(void (*function_)(void* arg_), void* arg_) {
    std::unique_lock<std::mutex> lock(_bg_work_mutex);
    if (_shutting_down) return;

    if (!_bg_work_started) {
        _bg_work_started = true;
        std::thread bg_thread(Env::BackgroundThreadEntryPoint, this);
        bg_thread.detach();
    }

    if (_bg_work_queue.empty()) {
        _bg_work_cv.notify_one();
    }

    _bg_work_queue.emplace(function_, arg_);
}

void Env::backgroud_thread_main() {
    while (!_shutting_down) {
        std::unique_lock<std::mutex> lock(_bg_work_mutex);
        while (_bg_work_queue.empty() && !_shutting_down) {
            _bg_work_cv.wait(lock);
        }

        if (_shutting_down) break;

        assert(!_bg_work_queue.empty());
        auto bg_function = _bg_work_queue.front()._function;
        void* bg_arg = _bg_work_queue.front()._arg;
        _bg_work_queue.pop();

        lock.unlock();
        bg_function(bg_arg);
    }

    std::unique_lock<std::mutex> lock(_bg_work_mutex);
    _bg_work_started = false;
    _bg_work_cv.notify_all();
}

void Env::Stop() {
    {
        std::unique_lock<std::mutex> lock(_bg_work_mutex);
        if (_shutting_down || !_bg_work_started) return;
    }
    _shutting_down = true;
    {
        std::unique_lock<std::mutex> lock(_bg_work_mutex);
        if (_bg_work_queue.empty()) {
            _bg_work_cv.notify_one();
        }
        while (_bg_work_started) {
            _bg_work_cv.wait(lock);
        }
    }
    _shutting_down = false;
}

Env::~Env() {}

Env* Env::Default() {
    static Env env;
    return &env;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
