//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include	"NGT/Common.h"

#include    <cstdio>
#include    <cstdlib>
#include    <sys/time.h>
#include    <unistd.h>

#include    <iostream>
#include    <deque>

namespace NGT {
void * evaluate_responce(void *);

class ThreadTerminationException : public Exception {
 public:
  ThreadTerminationException(const std::string &file, size_t line, std::stringstream &m) { set(file, line, m.str()); }
  ThreadTerminationException(const std::string &file, size_t line, const std::string &m) { set(file, line, m); }
};

class ThreadInfo;
class ThreadMutex;

class Thread
{
  public:
    Thread();

    virtual ~Thread();
    virtual int start();

    virtual int join();

    static ThreadMutex *constructThreadMutex();
    static void destructThreadMutex(ThreadMutex *t);

    static void mutexInit(ThreadMutex &m);

    static void lock(ThreadMutex &m);
    static void unlock(ThreadMutex &m);
    static void signal(ThreadMutex &m);
    static void wait(ThreadMutex &m);
    static void broadcast(ThreadMutex &m);

  protected:
    virtual int run() {
      return 0;
    }

  private:
    static void* startThread(void *thread) {
      if (thread == 0) {
        return 0;
      }
      Thread* p = (Thread*)thread;
      p->run();
      return thread;
    }

  public:
    int		threadNo;
    bool	isTerminate;

  protected:
    ThreadInfo	*threadInfo;
};

template <class JOB, class SHARED_DATA, class THREAD>
class ThreadPool {
  public:
  class JobQueue : public std::deque<JOB> {
      public:
        JobQueue() {
          threadMutex = Thread::constructThreadMutex();
          Thread::mutexInit(*threadMutex);
        }
        ~JobQueue() {
          Thread::destructThreadMutex(threadMutex);
        }
        bool isDeficient() { return std::deque<JOB>::size() <= requestSize; }
        bool isEmpty() { return std::deque<JOB>::size() == 0; }
        bool isFull() { return std::deque<JOB>::size() >= maxSize; }
        void setRequestSize(int s) { requestSize = s; }
        void setMaxSize(int s) { maxSize = s; }
        void lock() { Thread::lock(*threadMutex); }
        void unlock() { Thread::unlock(*threadMutex); }
        void signal() { Thread::signal(*threadMutex); }
        void wait() { Thread::wait(*threadMutex); }
        void wait(JobQueue &q) { wait(*q.threadMutex); }
        void broadcast() { Thread::broadcast(*threadMutex); }
        unsigned int	requestSize;
        unsigned int	maxSize;
        ThreadMutex	*threadMutex;
    };
    class InputJobQueue : public JobQueue {
      public:
        InputJobQueue() {
          isTerminate = false;
          underPushing = false;
          pushedSize = 0;
        }

        void popFront(JOB &d) {
          JobQueue::lock();
          while (JobQueue::isEmpty()) {
            if (isTerminate) {
              JobQueue::unlock();
	      NGTThrowSpecificException("Thread::termination", ThreadTerminationException);
            }
            JobQueue::wait();
          }
          d = std::deque<JOB>::front();
          std::deque<JOB>::pop_front();
          JobQueue::unlock();
          return;
        }

        void popFront(std::deque<JOB> &d, size_t s) {
          JobQueue::lock();
          while (JobQueue::isEmpty()) {
            if (isTerminate) {
              JobQueue::unlock();
	      NGTThrowSpecificException("Thread::termination", ThreadTerminationException);
            }
            JobQueue::wait();
          }
          for (size_t i = 0; i < s; i++) {
            d.push_back(std::deque<JOB>::front());
            std::deque<JOB>::pop_front();
            if (JobQueue::isEmpty()) {
              break;
            }
          }
          JobQueue::unlock();
          return;
        }

        void pushBack(JOB &data) {
          JobQueue::lock();
          if (!underPushing) {
            underPushing = true;
            pushedSize = 0;
          }
          pushedSize++;
          std::deque<JOB>::push_back(data);
          JobQueue::unlock();
          JobQueue::signal();
        }

        void pushBackEnd() {
          underPushing = false;
        }

        void terminate() {
          JobQueue::lock();
          if (underPushing || !JobQueue::isEmpty()) {
            JobQueue::unlock();
	    NGTThrowException("Thread::teminate:Under pushing!");
          }
          isTerminate = true;
          JobQueue::unlock();
          JobQueue::broadcast();
        }

        bool		isTerminate;
        bool		underPushing;
        size_t		pushedSize;

    };

    class OutputJobQueue : public JobQueue {
      public:
        void waitForFull() {
          JobQueue::wait();
          JobQueue::unlock();
        }

        void pushBack(JOB &data) {
          JobQueue::lock();
          std::deque<JOB>::push_back(data);
          if (!JobQueue::isFull()) {
            JobQueue::unlock();
            return;
          }
          JobQueue::unlock();
          JobQueue::signal();
        }

    };

    class SharedData {
      public:
        SharedData():isAvailable(false) {
          inputJobs.requestSize = 5;
          inputJobs.maxSize = 50;
        }
        SHARED_DATA	sharedData;
        InputJobQueue	inputJobs;
        OutputJobQueue	outputJobs;
	bool		isAvailable;
    };

    class Thread : public THREAD {
      public:
        SHARED_DATA &getSharedData() {
	  if (threadPool->sharedData.isAvailable) {
	    return threadPool->sharedData.sharedData;
	  } else {
	    NGTThrowException("Thread::getSharedData: Shared data is unavailable. No set yet.");
	  }
        }
        InputJobQueue &getInputJobQueue() {
          return threadPool->sharedData.inputJobs;
        }
        OutputJobQueue &getOutputJobQueue() {
          return threadPool->sharedData.outputJobs;
        }
        ThreadPool *threadPool;
    };

    ThreadPool(int s) {
      size = s;
      threads = new Thread[s];
    }

    ~ThreadPool() {
      delete[] threads;
    }

    void setSharedData(SHARED_DATA d) {
      sharedData.sharedData = d;
      sharedData.isAvailable = true;
    }

    void create() {
      for (unsigned int i = 0; i < size; i++) {
        threads[i].threadPool = this;
        threads[i].threadNo = i;
        threads[i].start();
      }
    }

    void pushInputQueue(JOB &data) {
      if (!sharedData.inputJobs.underPushing) {
        sharedData.outputJobs.lock();
      }
      sharedData.inputJobs.pushBack(data);
    }

    void waitForFinish() {
      sharedData.inputJobs.pushBackEnd();
      sharedData.outputJobs.setMaxSize(sharedData.inputJobs.pushedSize);
      sharedData.inputJobs.pushedSize = 0;
      sharedData.outputJobs.waitForFull();
    }

    void terminate() {
      sharedData.inputJobs.terminate();
      for (unsigned int i = 0; i < size; i++) {
        threads[i].join();
      }
    }

    InputJobQueue &getInputJobQueue() { return sharedData.inputJobs; }
    OutputJobQueue &getOutputJobQueue() { return sharedData.outputJobs; }

    SharedData		sharedData;	// shared data
    Thread		*threads;	// thread set
    unsigned int	size;		// thread size

};

}

