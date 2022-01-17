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

#include	<pthread.h>

#include	"Thread.h"

using namespace std;
using namespace NGT;

namespace NGT {
class ThreadInfo {
  public:
    pthread_t		threadid;
    pthread_attr_t	threadAttr;
};

class ThreadMutex {
  public:
    pthread_mutex_t	mutex;
    pthread_cond_t	condition;
};
}

Thread::Thread() {
  threadInfo = new ThreadInfo;
  threadInfo->threadid = 0;
  threadNo = -1;
  isTerminate = false;
}

Thread::~Thread() {
  if (threadInfo != 0) {
    delete threadInfo;
  }
}

ThreadMutex *
Thread::constructThreadMutex()
{
  return new ThreadMutex;
}

void
Thread::destructThreadMutex(ThreadMutex *t)
{
  if (t != 0) {
    pthread_mutex_destroy(&(t->mutex));
    pthread_cond_destroy(&(t->condition));
    delete t;
  }
}

int
Thread::start()
{
  pthread_attr_init(&(threadInfo->threadAttr));
  size_t stackSize = 0;
  pthread_attr_getstacksize(&(threadInfo->threadAttr), &stackSize);
  if (stackSize < 0xa00000) {	// 64bit stack size
    stackSize *= 4;
  }
  pthread_attr_setstacksize(&(threadInfo->threadAttr), stackSize);
  pthread_attr_getstacksize(&(threadInfo->threadAttr), &stackSize);
  return pthread_create(&(threadInfo->threadid), &(threadInfo->threadAttr), Thread::startThread, this);

}

int
Thread::join()
{
  return pthread_join(threadInfo->threadid, 0);
}

void
Thread::lock(ThreadMutex &m)
{
  pthread_mutex_lock(&m.mutex);
}
void
Thread::unlock(ThreadMutex &m)
{
  pthread_mutex_unlock(&m.mutex);
}
void
Thread::signal(ThreadMutex &m)
{
  pthread_cond_signal(&m.condition);
}

void
Thread::wait(ThreadMutex &m)
{
  if (pthread_cond_wait(&m.condition, &m.mutex) != 0) {
    cerr << "waitForSignalFromThread: internal error" << endl;
    NGTThrowException("waitForSignalFromThread: internal error");
  }
}

void
Thread::broadcast(ThreadMutex &m)
{
  pthread_cond_broadcast(&m.condition);
}

void
Thread::mutexInit(ThreadMutex &m)
{
  if (pthread_mutex_init(&m.mutex, NULL) != 0) {
    NGTThrowException("Thread::mutexInit: Cannot initialize mutex");
  }
  if (pthread_cond_init(&m.condition, NULL) != 0) {
    NGTThrowException("Thread::mutexInit: Cannot initialize condition");
  }
}
