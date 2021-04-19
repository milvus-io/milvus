// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SOCKET_RESOURCEMANAGER_H_
#define _SPTAG_SOCKET_RESOURCEMANAGER_H_

#include "Common.h"

#include <boost/asio/io_context.hpp>

#include <memory>
#include <chrono>
#include <functional>
#include <atomic>
#include <mutex>
#include <deque>
#include <unordered_map>
#include <thread>

namespace std
{
typedef atomic<uint32_t> atomic_uint32_t;
}

namespace SPTAG
{
namespace Socket
{

template<typename ResourceType>
class ResourceManager : public std::enable_shared_from_this<ResourceManager<ResourceType>>
{
public:
    typedef std::function<void(std::shared_ptr<ResourceType>)> TimeoutCallback;

    ResourceManager()
        : m_nextResourceID(1),
          m_isStopped(false),
          m_timeoutItemCount(0)
    {
        m_timeoutChecker = std::thread(&ResourceManager::StartCheckTimeout, this);
    }


    ~ResourceManager()
    {
        m_isStopped = true;
        m_timeoutChecker.join();
    }


    ResourceID Add(const std::shared_ptr<ResourceType>& p_resource,
                   std::uint32_t p_timeoutMilliseconds,
                   TimeoutCallback p_timeoutCallback)
    {
        ResourceID rid = m_nextResourceID.fetch_add(1);
        while (c_invalidResourceID == rid)
        {
            rid = m_nextResourceID.fetch_add(1);
        }

        {
            std::lock_guard<std::mutex> guard(m_resourcesMutex);
            m_resources.emplace(rid, p_resource);
        }

        if (p_timeoutMilliseconds > 0)
        {
            std::unique_ptr<ResourceItem> item(new ResourceItem);

            item->m_resourceID = rid;
            item->m_callback = std::move(p_timeoutCallback);
            item->m_expireTime = m_clock.now() + std::chrono::milliseconds(p_timeoutMilliseconds);

            {
                std::lock_guard<std::mutex> guard(m_timeoutListMutex);
                m_timeoutList.emplace_back(std::move(item));
            }

            ++m_timeoutItemCount;
        }

        return rid;
    }


    std::shared_ptr<ResourceType> GetAndRemove(ResourceID p_resourceID)
    {
        std::shared_ptr<ResourceType> ret;
        std::lock_guard<std::mutex> guard(m_resourcesMutex);
        auto iter = m_resources.find(p_resourceID);
        if (iter != m_resources.end())
        {
            ret = iter->second;
            m_resources.erase(iter);
        }

        return ret;
    }


    void Remove(ResourceID p_resourceID)
    {
        std::lock_guard<std::mutex> guard(m_resourcesMutex);
        auto iter = m_resources.find(p_resourceID);
        if (iter != m_resources.end())
        {
            m_resources.erase(iter);
        }
    }

private:
    void StartCheckTimeout()
    {
        std::vector<std::unique_ptr<ResourceItem>> timeouted;
        timeouted.reserve(1024);
        while (!m_isStopped)
        {
            if (m_timeoutItemCount > 0)
            {
                std::lock_guard<std::mutex> guard(m_timeoutListMutex);
                while (!m_timeoutList.empty()
                       && m_timeoutList.front()->m_expireTime <= m_clock.now())
                {
                    timeouted.emplace_back(std::move(m_timeoutList.front()));
                    m_timeoutList.pop_front();
                    --m_timeoutItemCount;
                }
            }

            if (timeouted.empty())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            else
            {
                for (auto& item : timeouted)
                {
                    auto resource = GetAndRemove(item->m_resourceID);
                    if (nullptr != resource)
                    {
                        item->m_callback(std::move(resource));
                    }
                }

                timeouted.clear();
            }
        }
    }


private:
    struct ResourceItem
    {
        ResourceItem()
            : m_resourceID(c_invalidResourceID)
        {
        }

        ResourceID m_resourceID;

        TimeoutCallback m_callback;

        std::chrono::time_point<std::chrono::high_resolution_clock> m_expireTime;
    };

    std::deque<std::unique_ptr<ResourceItem>> m_timeoutList;

    std::atomic<std::uint32_t> m_timeoutItemCount;

    std::mutex m_timeoutListMutex;

    std::unordered_map<ResourceID, std::shared_ptr<ResourceType>> m_resources;

    std::atomic<ResourceID> m_nextResourceID;

    std::mutex m_resourcesMutex;

    std::chrono::high_resolution_clock m_clock;

    std::thread m_timeoutChecker;

    bool m_isStopped;
};


} // namespace Socket
} // namespace SPTAG

#endif // _SPTAG_SOCKET_RESOURCEMANAGER_H_
