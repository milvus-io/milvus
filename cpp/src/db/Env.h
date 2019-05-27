/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <condition_variable>
#include <thread>
#include <mutex>
#include <queue>
#include <atomic>

namespace zilliz {
namespace vecwise {
namespace engine {

class Env {
public:
    Env();

    Env(const Env&) = delete;
    Env& operator=(const Env&) = delete;

    void Schedule(void (*function)(void* arg), void* arg);

    virtual void Stop();

    virtual ~Env();

    static Env* Default();

protected:
    void BackgroundThreadMain();
    static void BackgroundThreadEntryPoint(Env* env) {
        env->BackgroundThreadMain();
    }

    struct BGWork {
      explicit BGWork(void (*function)(void*), void* arg)
          : function_(function), arg_(arg)  {}

      void (* const function_)(void*);
      void* const arg_;
    };

    std::mutex bg_work_mutex_;
    std::condition_variable bg_work_cv_;
    std::queue<BGWork> bg_work_queue_;
    bool bg_work_started_;
    std::atomic<bool> shutting_down_;
}; // Env

} // namespace engine
} // namespace vecwise
} // namespace zilliz
