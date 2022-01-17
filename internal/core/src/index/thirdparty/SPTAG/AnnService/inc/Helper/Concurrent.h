// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_CONCURRENT_H_
#define _SPTAG_HELPER_CONCURRENT_H_


#include <atomic>
#include <condition_variable>
#include <mutex>


namespace SPTAG
{
namespace Helper
{
namespace Concurrent
{

class SpinLock
{
public:
    SpinLock() = default;

    void Lock() noexcept 
    {
        while (m_lock.test_and_set(std::memory_order_acquire))
        {
        }
    }

    void Unlock() noexcept 
    {
        m_lock.clear(std::memory_order_release);
    }

    SpinLock(const SpinLock&) = delete;
    SpinLock& operator = (const SpinLock&) = delete;

private:
    std::atomic_flag m_lock = ATOMIC_FLAG_INIT;
};

template<typename Lock>
class LockGuard {
public:
    LockGuard(Lock& lock) noexcept 
        : m_lock(lock) {
        lock.Lock();
    }

    LockGuard(Lock& lock, std::adopt_lock_t) noexcept
        : m_lock(lock) {}

    ~LockGuard() {
        m_lock.Unlock();
    }

    LockGuard(const LockGuard&) = delete;
    LockGuard& operator=(const LockGuard&) = delete;

private:
    Lock& m_lock;
};


class WaitSignal
{
public:
    WaitSignal();

    WaitSignal(std::uint32_t p_unfinished);

    ~WaitSignal();

    void Reset(std::uint32_t p_unfinished);

    void Wait();

    void FinishOne();

private:
    std::atomic<std::uint32_t> m_unfinished;

    std::atomic_bool m_isWaiting;

    std::mutex m_mutex;

    std::condition_variable m_cv;
};


} // namespace Base64
} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_CONCURRENT_H_
