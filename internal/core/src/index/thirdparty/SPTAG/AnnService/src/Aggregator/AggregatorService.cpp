// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Aggregator/AggregatorService.h"

using namespace SPTAG;
using namespace SPTAG::Aggregator;

AggregatorService::AggregatorService()
    : m_shutdownSignals(m_ioContext),
      m_pendingConnectServersTimer(m_ioContext)
{
}


AggregatorService::~AggregatorService()
{
}


bool
AggregatorService::Initialize()
{
    std::string configFilePath = "Aggregator.ini";
    m_aggregatorContext.reset(new AggregatorContext(configFilePath));

    m_initalized = m_aggregatorContext->IsInitialized();

    return m_initalized;
}


void
AggregatorService::Run()
{
    auto threadNum = max((SPTAG::SizeType)1, GetContext()->GetSettings()->m_threadNum);
    m_threadPool.reset(new boost::asio::thread_pool(threadNum));

    StartClient();
    StartListen();
    WaitForShutdown();
}


void
AggregatorService::StartClient()
{
    auto context = GetContext();
    Socket::PacketHandlerMapPtr handlerMap(new Socket::PacketHandlerMap);
    handlerMap->emplace(Socket::PacketType::SearchResponse,
                        [this](Socket::ConnectionID p_srcID, Socket::Packet p_packet)
                        {
                            boost::asio::post(*m_threadPool,
                                              std::bind(&AggregatorService::SearchResponseHanlder,
                                                        this,
                                                        p_srcID,
                                                        std::move(p_packet)));
                        });


    m_socketClient.reset(new Socket::Client(handlerMap,
                                            context->GetSettings()->m_socketThreadNum,
                                            30));

    m_socketClient->SetEventOnConnectionClose([this](Socket::ConnectionID p_cid)
                                              {
                                                  auto context = this->GetContext();
                                                  for (const auto& server : context->GetRemoteServers())
                                                  {
                                                      if (nullptr != server && p_cid == server->m_connectionID)
                                                      {
                                                          server->m_status = RemoteMachineStatus::Disconnected;
                                                          this->AddToPendingServers(server);
                                                      }
                                                  }
                                              });

    {
        std::lock_guard<std::mutex> guard(m_pendingConnectServersMutex);
        m_pendingConnectServers = context->GetRemoteServers();
    }

    ConnectToPendingServers();
}


void
AggregatorService::StartListen()
{
    auto context = GetContext();
    Socket::PacketHandlerMapPtr handlerMap(new Socket::PacketHandlerMap);
    handlerMap->emplace(Socket::PacketType::SearchRequest,
                        [this](Socket::ConnectionID p_srcID, Socket::Packet p_packet)
                        {
                            boost::asio::post(*m_threadPool,
                                              std::bind(&AggregatorService::SearchRequestHanlder,
                                                        this,
                                                        p_srcID,
                                                        std::move(p_packet)));
                        });

    m_socketServer.reset(new Socket::Server(context->GetSettings()->m_listenAddr,
                                            context->GetSettings()->m_listenPort,
                                            handlerMap,
                                            context->GetSettings()->m_socketThreadNum));

    fprintf(stderr,
            "Start to listen %s:%s ...\n",
            context->GetSettings()->m_listenAddr.c_str(),
            context->GetSettings()->m_listenPort.c_str());
}


void
AggregatorService::WaitForShutdown()
{
    m_shutdownSignals.add(SIGINT);
    m_shutdownSignals.add(SIGTERM);
#ifdef SIGQUIT
    m_shutdownSignals.add(SIGQUIT);
#endif

    m_shutdownSignals.async_wait([this](boost::system::error_code p_ec, int p_signal)
    {
        fprintf(stderr, "Received shutdown signals.\n");
        m_pendingConnectServersTimer.cancel();
    });

    m_ioContext.run();
    fprintf(stderr, "Start shutdown procedure.\n");

    m_socketServer.reset();
    m_threadPool->stop();
    m_threadPool->join();
}


void
AggregatorService::ConnectToPendingServers()
{
    auto context = GetContext();
    std::vector<std::shared_ptr<RemoteMachine>> pendingList;
    pendingList.reserve(context->GetRemoteServers().size());

    {
        std::lock_guard<std::mutex> guard(m_pendingConnectServersMutex);
        pendingList.swap(m_pendingConnectServers);
    }

    for (auto& pendingServer : pendingList)
    {
        if (pendingServer->m_status != RemoteMachineStatus::Disconnected)
        {
            continue;
        }

        pendingServer->m_status = RemoteMachineStatus::Connecting;
        std::shared_ptr<RemoteMachine> server = pendingServer;
        auto runner = [server, this]()
                      {
                          ErrorCode errCode;
                          auto cid = m_socketClient->ConnectToServer(server->m_address, server->m_port, errCode);
                          if (Socket::c_invalidConnectionID == cid)
                          {
                              if (ErrorCode::Socket_FailedResolveEndPoint == errCode)
                              {
                                  fprintf(stderr,
                                          "[Error] Failed to resolve %s %s.\n",
                                          server->m_address.c_str(),
                                          server->m_port.c_str());
                              }
                              else
                              {
                                  this->AddToPendingServers(std::move(server));
                              }
                          }
                          else
                          {
                              server->m_connectionID = cid;
                              server->m_status = RemoteMachineStatus::Connected;
                          }
                      };
        boost::asio::post(*m_threadPool, std::move(runner));
    }

    m_pendingConnectServersTimer.expires_from_now(boost::posix_time::seconds(30));
    m_pendingConnectServersTimer.async_wait([this](const boost::system::error_code& p_ec)
                                            {
                                                if (boost::asio::error::operation_aborted != p_ec)
                                                {
                                                    ConnectToPendingServers();
                                                }
                                            });
}


void
AggregatorService::AddToPendingServers(std::shared_ptr<RemoteMachine> p_remoteServer)
{
    std::lock_guard<std::mutex> guard(m_pendingConnectServersMutex);
    m_pendingConnectServers.emplace_back(std::move(p_remoteServer));
}


void
AggregatorService::SearchRequestHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet)
{
    auto context = GetContext();
    std::vector<Socket::ConnectionID> remoteServers;
    remoteServers.reserve(context->GetRemoteServers().size());

    for (const auto& server : context->GetRemoteServers())
    {
        if (RemoteMachineStatus::Connected != server->m_status)
        {
            continue;
        }

        remoteServers.push_back(server->m_connectionID);
    }

    Socket::PacketHeader requestHeader = p_packet.Header();
    if (Socket::c_invalidConnectionID == requestHeader.m_connectionID)
    {
        requestHeader.m_connectionID = p_localConnectionID;
    }

    std::shared_ptr<AggregatorExecutionContext> executionContext(
        new AggregatorExecutionContext(remoteServers.size(), requestHeader));

    for (std::uint32_t i = 0; i < remoteServers.size(); ++i)
    {
        AggregatorCallback callback = [this, executionContext, i](Socket::RemoteSearchResult p_result)
        {
            executionContext->GetResult(i).reset(new Socket::RemoteSearchResult(std::move(p_result)));
            if (executionContext->IsCompletedAfterFinsh(1))
            {
                this->AggregateResults(std::move(executionContext));
            }
        };

        auto timeoutCallback = [](std::shared_ptr<AggregatorCallback> p_callback)
        {
            if (nullptr != p_callback)
            {
                Socket::RemoteSearchResult result;
                result.m_status = Socket::RemoteSearchResult::ResultStatus::Timeout;

                (*p_callback)(std::move(result));
            }
        };

        auto connectCallback = [callback](bool p_connectSucc)
        {
            if (!p_connectSucc)
            {
                Socket::RemoteSearchResult result;
                result.m_status = Socket::RemoteSearchResult::ResultStatus::FailedNetwork;

                callback(std::move(result));
            }
        };

        Socket::Packet packet;
        packet.Header().m_packetType = Socket::PacketType::SearchRequest;
        packet.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
        packet.Header().m_bodyLength = p_packet.Header().m_bodyLength;
        packet.Header().m_connectionID = Socket::c_invalidConnectionID;
        packet.Header().m_resourceID = m_aggregatorCallbackManager.Add(std::make_shared<AggregatorCallback>(std::move(callback)),
                                                                       context->GetSettings()->m_searchTimeout,
                                                                       std::move(timeoutCallback));

        packet.AllocateBuffer(packet.Header().m_bodyLength);
        packet.Header().WriteBuffer(packet.HeaderBuffer());
        memcpy(packet.Body(), p_packet.Body(), packet.Header().m_bodyLength);

        m_socketClient->SendPacket(remoteServers[i], std::move(packet), connectCallback);
    }
}


void
AggregatorService::SearchResponseHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet)
{
    auto callback = m_aggregatorCallbackManager.GetAndRemove(p_packet.Header().m_resourceID);
    if (nullptr == callback)
    {
        return;
    }

    if (p_packet.Header().m_processStatus != Socket::PacketProcessStatus::Ok || 0 == p_packet.Header().m_bodyLength)
    {
        Socket::RemoteSearchResult result;
        result.m_status = Socket::RemoteSearchResult::ResultStatus::FailedExecute;

        (*callback)(std::move(result));
    }
    else
    {
        Socket::RemoteSearchResult result;
        result.Read(p_packet.Body());
        (*callback)(std::move(result));
    }
}


std::shared_ptr<AggregatorContext>
AggregatorService::GetContext()
{
    // Add mutex if necessary.
    return m_aggregatorContext;
}


void
AggregatorService::AggregateResults(std::shared_ptr<AggregatorExecutionContext> p_exectionContext)
{
    if (nullptr == p_exectionContext)
    {
        return;
    }

    Socket::Packet packet;
    packet.Header().m_packetType = Socket::PacketType::SearchResponse;
    packet.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
    packet.Header().m_resourceID = p_exectionContext->GetRequestHeader().m_resourceID;

    Socket::RemoteSearchResult remoteResult;
    remoteResult.m_status = Socket::RemoteSearchResult::ResultStatus::Success;

    std::size_t resultNum = 0;
    for (std::size_t i = 0; i < p_exectionContext->GetServerNumber(); ++i)
    {
        const auto& result = p_exectionContext->GetResult(i);
        if (nullptr == result)
        {
            continue;
        }

        resultNum += result->m_allIndexResults.size();
    }

    remoteResult.m_allIndexResults.reserve(resultNum);
    for (std::size_t i = 0; i < p_exectionContext->GetServerNumber(); ++i)
    {
        const auto& result = p_exectionContext->GetResult(i);
        if (nullptr == result)
        {
            continue;
        }

        for (auto& indexRes : result->m_allIndexResults)
        {
            remoteResult.m_allIndexResults.emplace_back(std::move(indexRes));
        }
    }

    std::uint32_t cap = static_cast<std::uint32_t>(remoteResult.EstimateBufferSize());
    packet.AllocateBuffer(cap);
    packet.Header().m_bodyLength = static_cast<std::uint32_t>(remoteResult.Write(packet.Body()) - packet.Body());
    packet.Header().WriteBuffer(packet.HeaderBuffer());

    m_socketServer->SendPacket(p_exectionContext->GetRequestHeader().m_connectionID,
                               std::move(packet),
                               nullptr);
}
