// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_AGGREGATOR_AGGREGATORSERVICE_H_
#define _SPTAG_AGGREGATOR_AGGREGATORSERVICE_H_

#include "AggregatorContext.h"
#include "AggregatorExecutionContext.h"
#include "inc/Socket/Server.h"
#include "inc/Socket/Client.h"
#include "inc/Socket/ResourceManager.h"

#include <boost/asio.hpp>

#include <memory>
#include <vector>
#include <thread>
#include <condition_variable>

namespace SPTAG
{
namespace Aggregator
{

class AggregatorService
{
public:
    AggregatorService();

    ~AggregatorService();

    bool Initialize();

    void Run();

private:

    void StartClient();

    void StartListen();

    void WaitForShutdown();

    void ConnectToPendingServers();

    void AddToPendingServers(std::shared_ptr<RemoteMachine> p_remoteServer);

    void SearchRequestHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet);

    void SearchResponseHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet);

    void AggregateResults(std::shared_ptr<AggregatorExecutionContext> p_exectionContext);

    std::shared_ptr<AggregatorContext> GetContext();

private:
    typedef std::function<void(Socket::RemoteSearchResult)> AggregatorCallback;

    std::shared_ptr<AggregatorContext> m_aggregatorContext;

    std::shared_ptr<Socket::Server> m_socketServer;

    std::shared_ptr<Socket::Client> m_socketClient;

    bool m_initalized;

    std::unique_ptr<boost::asio::thread_pool> m_threadPool;

    boost::asio::io_context m_ioContext;

    boost::asio::signal_set m_shutdownSignals;

    std::vector<std::shared_ptr<RemoteMachine>> m_pendingConnectServers;

    std::mutex m_pendingConnectServersMutex;

    boost::asio::deadline_timer m_pendingConnectServersTimer;

    Socket::ResourceManager<AggregatorCallback> m_aggregatorCallbackManager;
};



} // namespace Aggregator
} // namespace AnnService


#endif // _SPTAG_AGGREGATOR_AGGREGATORSERVICE_H_
