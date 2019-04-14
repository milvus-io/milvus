/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "WorkerThread.h"
#include "FaissAssert.h"

namespace faiss {

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

  for (auto& f : queue_) {
    f.first();
    f.second.set_value(true);
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

    data.first();
    data.second.set_value(true);
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
