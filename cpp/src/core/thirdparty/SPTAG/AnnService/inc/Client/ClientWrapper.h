// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_CLIENT_CLIENTWRAPPER_H_
#define _SPTAG_CLIENT_CLIENTWRAPPER_H_

#include "inc/Socket/Client.h"
#include "inc/Socket/RemoteSearchQuery.h"
#include "inc/Socket/ResourceManager.h"
#include "Options.h"

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace SPTAG
{
namespace Client
{

class ClientWrapper
{
public:
    typedef std::function<void(Socket::RemoteSearchResult)> Callback;

    ClientWrapper(const ClientOptions& p_options);

    ~ClientWrapper();

    void SendQueryAsync(const Socket::RemoteQuery& p_query,
                        Callback p_callback,
                        const ClientOptions& p_options);

    void WaitAllFinished();

    bool IsAvailable() const;

private:
    typedef std::pair<Socket::ConnectionID, Socket::ConnectionID> ConnectionPair;

    Socket::PacketHandlerMapPtr GetHandlerMap();

    void DecreaseUnfnishedJobCount();

    const ConnectionPair& GetConnection();

    void SearchResponseHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet);

    void HandleDeadConnection(Socket::ConnectionID p_cid);

private:
    ClientOptions m_options;

    std::unique_ptr<Socket::Client> m_client;

    std::atomic<std::uint32_t> m_unfinishedJobCount;

    std::atomic_bool m_isWaitingFinish;

    std::condition_variable m_waitingQueue;

    std::mutex m_waitingMutex;

    std::vector<ConnectionPair> m_connections;

    std::atomic<std::uint32_t> m_spinCountOfConnection;

    Socket::ResourceManager<Callback> m_callbackManager;
};


} // namespace Socket
} // namespace SPTAG

#endif // _SPTAG_CLIENT_OPTIONS_H_
