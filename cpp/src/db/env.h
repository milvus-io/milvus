#ifndef STORAGE_VECENGINE_ENV_H_
#define STORAGE_VECENGINE_ENV_H_

#include <condition_variable>
#include <thread>
#include <mutex>
#include <queue>

namespace zilliz {
namespace vecwise {
namespace engine {

class Env {
public:
    Env();

    Env(const Env&) = delete;
    Env& operator=(const Env&) = delete;

    void schedule(void (*function_)(void* arg_), void* arg_);

    virtual ~Env();

    static Env* Default();

protected:
    void backgroud_thread_main();
    static void BackgroundThreadEntryPoint(Env* env) {
        env->backgroud_thread_main();
    }

    struct BGWork {
      explicit BGWork(void (*function_)(void*), void* arg_)
          : _function(function_), _arg(arg_)  {}

      void (* const _function)(void*);
      void* const _arg;
    };

    std::mutex _bg_work_mutex;
    std::condition_variable _bg_work_cv;
    std::queue<BGWork> _bg_work_queue;
    bool _bg_work_started;

}; // Env

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // STORAGE_VECENGINE_ENV_H_
