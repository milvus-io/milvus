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

#include <thrift/concurrency/TimerManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/Util.h>

#include <assert.h>
#include <iostream>

namespace apache {
namespace thrift {
namespace concurrency {
namespace test {

using namespace apache::thrift::concurrency;

class TimerManagerTests {

public:
  class Task : public Runnable {
  public:
    Task(Monitor& monitor, int64_t timeout)
      : _timeout(timeout),
        _startTime(Util::currentTime()),
        _endTime(0),
        _monitor(monitor),
        _success(false),
        _done(false) {}

    ~Task() { std::cerr << this << std::endl; }

    void run() {

      _endTime = Util::currentTime();
      _success = (_endTime - _startTime) >= _timeout;

      {
        Synchronized s(_monitor);
        _done = true;
        _monitor.notifyAll();
      }
    }

    int64_t _timeout;
    int64_t _startTime;
    int64_t _endTime;
    Monitor& _monitor;
    bool _success;
    bool _done;
  };

  /**
   * This test creates two tasks and waits for the first to expire within 10%
   * of the expected expiration time. It then verifies that the timer manager
   * properly clean up itself and the remaining orphaned timeout task when the
   * manager goes out of scope and its destructor is called.
   */
  bool test00(int64_t timeout = 1000LL) {

    shared_ptr<TimerManagerTests::Task> orphanTask
        = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, 10 * timeout));

    {
      TimerManager timerManager;
      timerManager.threadFactory(shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory()));
      timerManager.start();
      if (timerManager.state() != TimerManager::STARTED) {
        std::cerr << "timerManager is not in the STARTED state, but should be" << std::endl;
        return false;
      }

      // Don't create task yet, because its constructor sets the expected completion time, and we
      // need to delay between inserting the two tasks into the run queue.
      shared_ptr<TimerManagerTests::Task> task;

      {
        Synchronized s(_monitor);
        timerManager.add(orphanTask, 10 * timeout);

        THRIFT_SLEEP_USEC(timeout * 1000);

        task.reset(new TimerManagerTests::Task(_monitor, timeout));
        timerManager.add(task, timeout);
        _monitor.wait();
      }

      if (!task->_done) {
        std::cerr << "task is not done, but it should have executed" << std::endl;
        return false;
      }

      std::cout << "\t\t\t" << (task->_success ? "Success" : "Failure") << "!" << std::endl;
    }

    if (orphanTask->_done) {
      std::cerr << "orphan task is done, but it should not have executed" << std::endl;
      return false;
    }

    return true;
  }

  /**
   * This test creates two tasks, removes the first one then waits for the second one. It then
   * verifies that the timer manager properly clean up itself and the remaining orphaned timeout
   * task when the manager goes out of scope and its destructor is called.
   */
  bool test01(int64_t timeout = 1000LL) {
    TimerManager timerManager;
    timerManager.threadFactory(shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory()));
    timerManager.start();
    assert(timerManager.state() == TimerManager::STARTED);

    Synchronized s(_monitor);

    // Setup the two tasks
    shared_ptr<TimerManagerTests::Task> taskToRemove
      = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, timeout / 2));
    timerManager.add(taskToRemove, taskToRemove->_timeout);

    shared_ptr<TimerManagerTests::Task> task
      = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, timeout));
    timerManager.add(task, task->_timeout);

    // Remove one task and wait until the other has completed
    timerManager.remove(taskToRemove);
    _monitor.wait(timeout * 2);

    assert(!taskToRemove->_done);
    assert(task->_done);

    return true;
  }

  /**
   * This test creates two tasks with the same callback and another one, then removes the two
   * duplicated then waits for the last one. It then verifies that the timer manager properly
   * clean up itself and the remaining orphaned timeout task when the manager goes out of scope
   * and its destructor is called.
   */
  bool test02(int64_t timeout = 1000LL) {
    TimerManager timerManager;
    timerManager.threadFactory(shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory()));
    timerManager.start();
    assert(timerManager.state() == TimerManager::STARTED);

    Synchronized s(_monitor);

    // Setup the one tasks and add it twice
    shared_ptr<TimerManagerTests::Task> taskToRemove
      = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, timeout / 3));
    timerManager.add(taskToRemove, taskToRemove->_timeout);
    timerManager.add(taskToRemove, taskToRemove->_timeout * 2);

    shared_ptr<TimerManagerTests::Task> task
      = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, timeout));
    timerManager.add(task, task->_timeout);

    // Remove the first task (e.g. two timers) and wait until the other has completed
    timerManager.remove(taskToRemove);
    _monitor.wait(timeout * 2);

    assert(!taskToRemove->_done);
    assert(task->_done);

    return true;
  }

  /**
   * This test creates two tasks, removes the first one then waits for the second one. It then
   * verifies that the timer manager properly clean up itself and the remaining orphaned timeout
   * task when the manager goes out of scope and its destructor is called.
   */
  bool test03(int64_t timeout = 1000LL) {
    TimerManager timerManager;
    timerManager.threadFactory(shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory()));
    timerManager.start();
    assert(timerManager.state() == TimerManager::STARTED);

    Synchronized s(_monitor);

    // Setup the two tasks
    shared_ptr<TimerManagerTests::Task> taskToRemove
        = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, timeout / 2));
    TimerManager::Timer timer = timerManager.add(taskToRemove, taskToRemove->_timeout);

    shared_ptr<TimerManagerTests::Task> task
      = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, timeout));
    timerManager.add(task, task->_timeout);

    // Remove one task and wait until the other has completed
    timerManager.remove(timer);
    _monitor.wait(timeout * 2);

    assert(!taskToRemove->_done);
    assert(task->_done);

    // Verify behavior when removing the removed task
    try {
      timerManager.remove(timer);
      assert(0 == "ERROR: This remove should send a NoSuchTaskException exception.");
    } catch (NoSuchTaskException&) {
    }

    return true;
  }

  /**
   * This test creates one tasks, and tries to remove it after it has expired.
   */
  bool test04(int64_t timeout = 1000LL) {
    TimerManager timerManager;
    timerManager.threadFactory(shared_ptr<PlatformThreadFactory>(new PlatformThreadFactory()));
    timerManager.start();
    assert(timerManager.state() == TimerManager::STARTED);

    Synchronized s(_monitor);

    // Setup the task
    shared_ptr<TimerManagerTests::Task> task
      = shared_ptr<TimerManagerTests::Task>(new TimerManagerTests::Task(_monitor, timeout / 10));
    TimerManager::Timer timer = timerManager.add(task, task->_timeout);

    // Wait until the task has completed
    _monitor.wait(timeout);

    // Verify behavior when removing the expired task
    try {
      timerManager.remove(timer);
      assert(0 == "ERROR: This remove should send a NoSuchTaskException exception.");
    } catch (NoSuchTaskException&) {
    }

    return true;
  }

  friend class TestTask;

  Monitor _monitor;
};

}
}
}
} // apache::thrift::concurrency
