#include "env.h"

namespace zilliz {
namespace vecwise {
namespace engine {

Env::Env()
    : _bg_work_cv(&_bg_work_mutex),
      _bg_work_started(false) {
}

void Env::schedule(void (*function_)(void* arg_), void* arg_) {
    std::lock_guard<std::mutex> lock;

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
    while (true) {
        std::lock_guard<std::mutex> lock;
        while (_bg_work_queue.empty()) {
            _bg_work_cv.wait();
        }

        assert(!_bg_work_queue.empty());
        auto bg_function = _bg_work_queue.front()._function;
        void* bg_arg = _bg_work_queue.front()._arg;
        _bg_work_queue.pop();

        lock.unlock();
        bg_function(bg_arg);
    }
}

Env::~Env() {}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
