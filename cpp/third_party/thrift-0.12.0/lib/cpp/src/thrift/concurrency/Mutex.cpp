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

// needed to test for pthread implementation capabilities:
#define __USE_GNU

#include <thrift/thrift-config.h>

#include <thrift/Thrift.h>
#include <thrift/concurrency/Exception.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Util.h>

#include <assert.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>

#include <boost/format.hpp>

namespace apache {
namespace thrift {
namespace concurrency {

// Enable this to turn on mutex contention profiling support
// #define THRIFT_PTHREAD_MUTEX_CONTENTION_PROFILING

#ifdef THRIFT_PTHREAD_MUTEX_CONTENTION_PROFILING

static int32_t mutexProfilingCounter = 0;
static int32_t mutexProfilingSampleRate = 0;
static MutexWaitCallback mutexProfilingCallback = 0;

void enableMutexProfiling(int32_t profilingSampleRate, MutexWaitCallback callback) {
  mutexProfilingSampleRate = profilingSampleRate;
  mutexProfilingCallback = callback;
}

#define PROFILE_MUTEX_START_LOCK() int64_t _lock_startTime = maybeGetProfilingStartTime();

#define PROFILE_MUTEX_NOT_LOCKED()                                                                 \
  do {                                                                                             \
    if (_lock_startTime > 0) {                                                                     \
      int64_t endTime = Util::currentTimeUsec();                                                   \
      (*mutexProfilingCallback)(this, endTime - _lock_startTime);                                  \
    }                                                                                              \
  } while (0)

#define PROFILE_MUTEX_LOCKED()                                                                     \
  do {                                                                                             \
    profileTime_ = _lock_startTime;                                                                \
    if (profileTime_ > 0) {                                                                        \
      profileTime_ = Util::currentTimeUsec() - profileTime_;                                       \
    }                                                                                              \
  } while (0)

#define PROFILE_MUTEX_START_UNLOCK()                                                               \
  int64_t _temp_profileTime = profileTime_;                                                        \
  profileTime_ = 0;

#define PROFILE_MUTEX_UNLOCKED()                                                                   \
  do {                                                                                             \
    if (_temp_profileTime > 0) {                                                                   \
      (*mutexProfilingCallback)(this, _temp_profileTime);                                          \
    }                                                                                              \
  } while (0)

static inline int64_t maybeGetProfilingStartTime() {
  if (mutexProfilingSampleRate && mutexProfilingCallback) {
    // This block is unsynchronized, but should produce a reasonable sampling
    // rate on most architectures.  The main race conditions are the gap
    // between the decrement and the test, the non-atomicity of decrement, and
    // potential caching of different values at different CPUs.
    //
    // - if two decrements race, the likeliest result is that the counter
    //      decrements slowly (perhaps much more slowly) than intended.
    //
    // - many threads could potentially decrement before resetting the counter
    //      to its large value, causing each additional incoming thread to
    //      profile every call.  This situation is unlikely to persist for long
    //      as the critical gap is quite short, but profiling could be bursty.
    sig_atomic_t localValue = --mutexProfilingCounter;
    if (localValue <= 0) {
      mutexProfilingCounter = mutexProfilingSampleRate;
      return Util::currentTimeUsec();
    }
  }

  return 0;
}

#else
#define PROFILE_MUTEX_START_LOCK()
#define PROFILE_MUTEX_NOT_LOCKED()
#define PROFILE_MUTEX_LOCKED()
#define PROFILE_MUTEX_START_UNLOCK()
#define PROFILE_MUTEX_UNLOCKED()
#endif // THRIFT_PTHREAD_MUTEX_CONTENTION_PROFILING

#define EINTR_LOOP(_CALL)          int ret; do { ret = _CALL; } while (ret == EINTR)
#define ABORT_ONFAIL(_CALL)      { EINTR_LOOP(_CALL); if (ret) { abort(); } }
#define THROW_SRE(_CALLSTR, RET) { throw SystemResourceException(boost::str(boost::format("%1% returned %2% (%3%)") % _CALLSTR % RET % ::strerror(RET))); }
#define THROW_SRE_ONFAIL(_CALL)  { EINTR_LOOP(_CALL); if (ret) { THROW_SRE(#_CALL, ret); } }
#define THROW_SRE_TRYFAIL(_CALL) { EINTR_LOOP(_CALL); if (ret == 0) { return true; } else if (ret == EBUSY) { return false; } THROW_SRE(#_CALL, ret); }

/**
 * Implementation of Mutex class using POSIX mutex
 *
 * Throws apache::thrift::concurrency::SystemResourceException on error.
 *
 * @version $Id:$
 */
class Mutex::impl {
public:
  impl(Initializer init) : initialized_(false) {
#ifdef THRIFT_PTHREAD_MUTEX_CONTENTION_PROFILING
    profileTime_ = 0;
#endif
    init(&pthread_mutex_);
    initialized_ = true;
  }

  ~impl() {
    if (initialized_) {
      initialized_ = false;
      ABORT_ONFAIL(pthread_mutex_destroy(&pthread_mutex_));
    }
  }

  void lock() const {
    PROFILE_MUTEX_START_LOCK();
    THROW_SRE_ONFAIL(pthread_mutex_lock(&pthread_mutex_));
    PROFILE_MUTEX_LOCKED();
  }

  bool trylock() const {
    THROW_SRE_TRYFAIL(pthread_mutex_trylock(&pthread_mutex_));
  }

  bool timedlock(int64_t milliseconds) const {
#if defined(_POSIX_TIMEOUTS) && _POSIX_TIMEOUTS >= 200112L
    PROFILE_MUTEX_START_LOCK();

    struct THRIFT_TIMESPEC ts;
    Util::toTimespec(ts, milliseconds + Util::currentTime());
    EINTR_LOOP(pthread_mutex_timedlock(&pthread_mutex_, &ts));
    if (ret == 0) {
      PROFILE_MUTEX_LOCKED();
      return true;
    } else if (ret == ETIMEDOUT) {
      PROFILE_MUTEX_NOT_LOCKED();
      return false;
    }

    THROW_SRE("pthread_mutex_timedlock(&pthread_mutex_, &ts)", ret);
#else
    /* Otherwise follow solution used by Mono for Android */
    struct THRIFT_TIMESPEC sleepytime, now, to;

    /* This is just to avoid a completely busy wait */
    sleepytime.tv_sec = 0;
    sleepytime.tv_nsec = 10000000L; /* 10ms */

    Util::toTimespec(to, milliseconds + Util::currentTime());

    while ((trylock()) == false) {
      Util::toTimespec(now, Util::currentTime());
      if (now.tv_sec >= to.tv_sec && now.tv_nsec >= to.tv_nsec) {
        return false;
      }
      nanosleep(&sleepytime, NULL);
    }

    return true;
#endif
  }

  void unlock() const {
    PROFILE_MUTEX_START_UNLOCK();
    THROW_SRE_ONFAIL(pthread_mutex_unlock(&pthread_mutex_));
    PROFILE_MUTEX_UNLOCKED();
  }

  void* getUnderlyingImpl() const { return (void*)&pthread_mutex_; }

private:
  mutable pthread_mutex_t pthread_mutex_;
  mutable bool initialized_;
#ifdef THRIFT_PTHREAD_MUTEX_CONTENTION_PROFILING
  mutable int64_t profileTime_;
#endif
};

Mutex::Mutex(Initializer init) : impl_(new Mutex::impl(init)) {
}

void* Mutex::getUnderlyingImpl() const {
  return impl_->getUnderlyingImpl();
}

void Mutex::lock() const {
  impl_->lock();
}

bool Mutex::trylock() const {
  return impl_->trylock();
}

bool Mutex::timedlock(int64_t ms) const {
  return impl_->timedlock(ms);
}

void Mutex::unlock() const {
  impl_->unlock();
}

void Mutex::DEFAULT_INITIALIZER(void* arg) {
  pthread_mutex_t* pthread_mutex = (pthread_mutex_t*)arg;
  THROW_SRE_ONFAIL(pthread_mutex_init(pthread_mutex, NULL));
}

#if defined(PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP) || defined(PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP) || defined(PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP)
static void init_with_kind(pthread_mutex_t* mutex, int kind) {
  pthread_mutexattr_t mutexattr;
  THROW_SRE_ONFAIL(pthread_mutexattr_init(&mutexattr));
  THROW_SRE_ONFAIL(pthread_mutexattr_settype(&mutexattr, kind));
  THROW_SRE_ONFAIL(pthread_mutex_init(mutex, &mutexattr));
  THROW_SRE_ONFAIL(pthread_mutexattr_destroy(&mutexattr));
}
#endif

#ifdef PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP
void Mutex::ADAPTIVE_INITIALIZER(void* arg) {
  // From mysql source: mysys/my_thr_init.c
  // Set mutex type to "fast" a.k.a "adaptive"
  //
  // In this case the thread may steal the mutex from some other thread
  // that is waiting for the same mutex. This will save us some
  // context switches but may cause a thread to 'starve forever' while
  // waiting for the mutex (not likely if the code within the mutex is
  // short).
  init_with_kind((pthread_mutex_t*)arg, PTHREAD_MUTEX_ADAPTIVE_NP);
}
#endif

#ifdef PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP
void Mutex::ERRORCHECK_INITIALIZER(void* arg) {
  init_with_kind((pthread_mutex_t*)arg, PTHREAD_MUTEX_ERRORCHECK);
}
#endif

#ifdef PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP
void Mutex::RECURSIVE_INITIALIZER(void* arg) {
  init_with_kind((pthread_mutex_t*)arg, PTHREAD_MUTEX_RECURSIVE_NP);
}
#endif

/**
 * Implementation of ReadWriteMutex class using POSIX rw lock
 *
 * @version $Id:$
 */
class ReadWriteMutex::impl {
public:
  impl() : initialized_(false) {
#ifdef THRIFT_PTHREAD_MUTEX_CONTENTION_PROFILING
    profileTime_ = 0;
#endif
    THROW_SRE_ONFAIL(pthread_rwlock_init(&rw_lock_, NULL));
    initialized_ = true;
  }

  ~impl() {
    if (initialized_) {
      initialized_ = false;
      ABORT_ONFAIL(pthread_rwlock_destroy(&rw_lock_));
    }
  }

  void acquireRead() const {
    PROFILE_MUTEX_START_LOCK();
    THROW_SRE_ONFAIL(pthread_rwlock_rdlock(&rw_lock_));
    PROFILE_MUTEX_NOT_LOCKED(); // not exclusive, so use not-locked path
  }

  void acquireWrite() const {
    PROFILE_MUTEX_START_LOCK();
    THROW_SRE_ONFAIL(pthread_rwlock_wrlock(&rw_lock_));
    PROFILE_MUTEX_LOCKED();
  }

  bool attemptRead() const { THROW_SRE_TRYFAIL(pthread_rwlock_tryrdlock(&rw_lock_)); }

  bool attemptWrite() const { THROW_SRE_TRYFAIL(pthread_rwlock_trywrlock(&rw_lock_)); }

  void release() const {
    PROFILE_MUTEX_START_UNLOCK();
    THROW_SRE_ONFAIL(pthread_rwlock_unlock(&rw_lock_));
    PROFILE_MUTEX_UNLOCKED();
  }

private:
  mutable pthread_rwlock_t rw_lock_;
  mutable bool initialized_;
#ifdef THRIFT_PTHREAD_MUTEX_CONTENTION_PROFILING
  mutable int64_t profileTime_;
#endif
};

ReadWriteMutex::ReadWriteMutex() : impl_(new ReadWriteMutex::impl()) {
}

void ReadWriteMutex::acquireRead() const {
  impl_->acquireRead();
}

void ReadWriteMutex::acquireWrite() const {
  impl_->acquireWrite();
}

bool ReadWriteMutex::attemptRead() const {
  return impl_->attemptRead();
}

bool ReadWriteMutex::attemptWrite() const {
  return impl_->attemptWrite();
}

void ReadWriteMutex::release() const {
  impl_->release();
}

NoStarveReadWriteMutex::NoStarveReadWriteMutex() : writerWaiting_(false) {
}

void NoStarveReadWriteMutex::acquireRead() const {
  if (writerWaiting_) {
    // writer is waiting, block on the writer's mutex until he's done with it
    mutex_.lock();
    mutex_.unlock();
  }

  ReadWriteMutex::acquireRead();
}

void NoStarveReadWriteMutex::acquireWrite() const {
  // if we can acquire the rwlock the easy way, we're done
  if (attemptWrite()) {
    return;
  }

  // failed to get the rwlock, do it the hard way:
  // locking the mutex and setting writerWaiting will cause all new readers to
  // block on the mutex rather than on the rwlock.
  mutex_.lock();
  writerWaiting_ = true;
  ReadWriteMutex::acquireWrite();
  writerWaiting_ = false;
  mutex_.unlock();
}
}
}
} // apache::thrift::concurrency
