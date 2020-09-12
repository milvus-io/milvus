// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/IndexBuilder/ThreadPool.h"

#include <boost/asio.hpp>

#include <memory>
#include <atomic>

using namespace SPTAG::IndexBuilder;

namespace Local
{
std::unique_ptr<boost::asio::thread_pool> g_threadPool;

std::atomic_bool g_initialized(false);

std::uint32_t g_threadNum = 1;
}


void
ThreadPool::Init(std::uint32_t p_threadNum)
{
    if (Local::g_initialized.exchange(true))
    {
        return;
    }

    Local::g_threadNum = std::max((std::uint32_t)1, p_threadNum);

    Local::g_threadPool.reset(new boost::asio::thread_pool(Local::g_threadNum));
}


bool
ThreadPool::Queue(std::function<void()> p_workItem)
{
    if (nullptr == Local::g_threadPool)
    {
        return false;
    }

    boost::asio::post(*Local::g_threadPool, std::move(p_workItem));
    return true;
}


std::uint32_t
ThreadPool::CurrentThreadNum()
{
    return Local::g_threadNum;
}
