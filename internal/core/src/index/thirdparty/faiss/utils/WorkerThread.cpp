/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/utils/WorkerThread.h>
#include <faiss/impl/FaissAssert.h>
#include <exception>

namespace faiss {

namespace {

// Captures any exceptions thrown by the lambda and returns them via the promise
void runCallback(std::function<void()>& fn,
                 std::promise<bool>& promise) {
  try {
    fn();
    promise.set_value(true);
  } catch (...) {
    promise.set_exception(std::current_exception());
  }
}

} // namespace

WorkerThread::WorkerThread() :
    wantStop_(false) {
  startThread();

  // Make sure that the thread has started before continuing
  add([](){}).get();
}

WorkerThread::~WorkerThread() {
  stop();
  waitForThreadExit();
}

void
WorkerThread::startThread() {
  thread_ = std::thread([this](){ threadMain(); });
}

void
WorkerThread::stop() {
  std::lock_guard<std::mutex> guard(mutex_);

  wantStop_ = true;
  monitor_.notify_one();
}

std::future<bool>
WorkerThread::add(std::function<void()> f) {
  std::lock_guard<std::mutex> guard(mutex_);

  if (wantStop_) {
    // The timer thread has been stopped, or we want to stop; we can't
    // schedule anything else
    std::promise<bool> p;
    auto fut = p.get_future();

    // did not execute
    p.set_value(false);
    return fut;
  }

  auto pr = std::promise<bool>();
  auto fut = pr.get_future();

  queue_.emplace_back(std::make_pair(std::move(f), std::move(pr)));

  // Wake up our thread
  monitor_.notify_one();
  return fut;
}

void
WorkerThread::threadMain() {
  threadLoop();

  // Call all pending tasks
  FAISS_ASSERT(wantStop_);

  // flush all pending operations
  for (auto& f : queue_) {
    runCallback(f.first, f.second);
  }
}

void
WorkerThread::threadLoop() {
  while (true) {
    std::pair<std::function<void()>, std::promise<bool>> data;

    {
      std::unique_lock<std::mutex> lock(mutex_);

      while (!wantStop_ && queue_.empty()) {
        monitor_.wait(lock);
      }

      if (wantStop_) {
        return;
      }

      data = std::move(queue_.front());
      queue_.pop_front();
    }

    runCallback(data.first, data.second);
  }
}

void
WorkerThread::waitForThreadExit() {
  try {
    thread_.join();
  } catch (...) {
  }
}

} // namespace
