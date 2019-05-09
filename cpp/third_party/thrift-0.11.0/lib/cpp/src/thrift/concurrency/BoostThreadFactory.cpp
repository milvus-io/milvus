/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <thrift/thrift-config.h>

#if USE_BOOST_THREAD

#include <thrift/concurrency/BoostThreadFactory.h>
#include <thrift/concurrency/Exception.h>
#include <thrift/stdcxx.h>
#include <cassert>

#include <boost/thread.hpp>

namespace apache {
namespace thrift {

using stdcxx::bind;
using stdcxx::scoped_ptr;
using stdcxx::shared_ptr;
using stdcxx::weak_ptr;

namespace concurrency {

/**
 * The boost thread class.
 *
 * @version $Id:$
 */
class BoostThread : public Thread {
public:
  enum STATE { uninitialized, starting, started, stopping, stopped };

  static void* threadMain(void* arg);

private:
  scoped_ptr<boost::thread> thread_;
  Monitor monitor_;
  STATE state_;
  weak_ptr<BoostThread> self_;
  bool detached_;

public:
  BoostThread(bool detached, shared_ptr<Runnable> runnable)
    : state_(uninitialized), detached_(detached) {
    this->Thread::runnable(runnable);
  }

  ~BoostThread() {
    if (!detached_ && thread_->joinable()) {
      try {
        join();
      } catch (...) {
        // We're really hosed.
      }
    }
  }

  STATE getState() const
  {
    Synchronized sync(monitor_);
    return state_;
  }

  void setState(STATE newState)
  {
    Synchronized sync(monitor_);
    state_ = newState;

    // unblock start() with the knowledge that the thread has actually
    // started running, which avoids a race in detached threads.
    if (newState == started) {
	  monitor_.notify();
    }
  }

  void start() {
    // Create reference
    shared_ptr<BoostThread>* selfRef = new shared_ptr<BoostThread>();
    *selfRef = self_.lock();

    setState(starting);

	Synchronized sync(monitor_);
	
    thread_.reset(new boost::thread(bind(threadMain, (void*)selfRef)));

    if (detached_)
      thread_->detach();
    
    // Wait for the thread to start and get far enough to grab everything
    // that it needs from the calling context, thus absolving the caller
    // from being required to hold on to runnable indefinitely.
    monitor_.wait();
  }

  void join() {
    if (!detached_ && getState() != uninitialized) {
      thread_->join();
    }
  }

  Thread::id_t getId() { return thread_.get() ? thread_->get_id() : boost::thread::id(); }

  shared_ptr<Runnable> runnable() const { return Thread::runnable(); }

  void runnable(shared_ptr<Runnable> value) { Thread::runnable(value); }

  void weakRef(shared_ptr<BoostThread> self) {
    assert(self.get() == this);
    self_ = weak_ptr<BoostThread>(self);
  }
};

void* BoostThread::threadMain(void* arg) {
  shared_ptr<BoostThread> thread = *(shared_ptr<BoostThread>*)arg;
  delete reinterpret_cast<shared_ptr<BoostThread>*>(arg);

  thread->setState(started);
  thread->runnable()->run();

  if (thread->getState() != stopping && thread->getState() != stopped) {
    thread->setState(stopping);
  }
  return (void*)0;
}

BoostThreadFactory::BoostThreadFactory(bool detached)
  : ThreadFactory(detached) {
}

shared_ptr<Thread> BoostThreadFactory::newThread(shared_ptr<Runnable> runnable) const {
  shared_ptr<BoostThread> result = shared_ptr<BoostThread>(new BoostThread(isDetached(), runnable));
  result->weakRef(result);
  runnable->thread(result);
  return result;
}

Thread::id_t BoostThreadFactory::getCurrentThreadId() const {
  return boost::this_thread::get_id();
}
}
}
} // apache::thrift::concurrency

#endif // USE_BOOST_THREAD
