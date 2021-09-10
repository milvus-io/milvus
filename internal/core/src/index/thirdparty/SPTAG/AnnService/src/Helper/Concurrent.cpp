// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/Concurrent.h"

using namespace SPTAG;
using namespace SPTAG::Helper::Concurrent;

WaitSignal::WaitSignal()
    : m_isWaiting(false),
      m_unfinished(0)
{
}


WaitSignal::WaitSignal(std::uint32_t p_unfinished)
    : m_isWaiting(false),
      m_unfinished(p_unfinished)
{
}


WaitSignal::~WaitSignal()
{
    std::lock_guard<std::mutex> guard(m_mutex);
    if (m_isWaiting)
    {
        m_cv.notify_all();
    }
}


void
WaitSignal::Reset(std::uint32_t p_unfinished)
{
    std::lock_guard<std::mutex> guard(m_mutex);
    if (m_isWaiting)
    {
        m_cv.notify_all();
    }

    m_isWaiting = false;
    m_unfinished = p_unfinished;
}


void
WaitSignal::Wait()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_unfinished > 0)
    {
        m_isWaiting = true;
        m_cv.wait(lock);
    }
}


void
WaitSignal::FinishOne()
{
    if (1 == m_unfinished.fetch_sub(1))
    {
        std::lock_guard<std::mutex> guard(m_mutex);
        if (m_isWaiting)
        {
            m_isWaiting = false;
            m_cv.notify_all();
        }
    }
}
