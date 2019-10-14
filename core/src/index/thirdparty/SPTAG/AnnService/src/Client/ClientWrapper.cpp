// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Client/ClientWrapper.h"

using namespace SPTAG;
using namespace SPTAG::Socket;
using namespace SPTAG::Client;

ClientWrapper::ClientWrapper(const ClientOptions& p_options)
    : m_options(p_options),
      m_unfinishedJobCount(0),
      m_isWaitingFinish(false)
{
    m_client.reset(new SPTAG::Socket::Client(GetHandlerMap(), p_options.m_socketThreadNum, 30));
    m_client->SetEventOnConnectionClose(std::bind(&ClientWrapper::HandleDeadConnection,
                                                  this,
                                                  std::placeholders::_1));

    m_connections.reserve(m_options.m_threadNum);
    for (std::uint32_t i = 0; i < m_options.m_threadNum; ++i)
    {
        SPTAG::ErrorCode errCode;
        ConnectionPair conn(c_invalidConnectionID, c_invalidConnectionID);
        conn.first = m_client->ConnectToServer(p_options.m_serverAddr, p_options.m_serverPort, errCode);
        if (SPTAG::ErrorCode::Socket_FailedResolveEndPoint == errCode)
        {
            fprintf(stderr, "Unable to resolve remote address.\n");
            return;
        }

        if (c_invalidConnectionID != conn.first)
        {
            m_connections.emplace_back(std::move(conn));
        }
    }
}


ClientWrapper::~ClientWrapper()
{
}


void
ClientWrapper::SendQueryAsync(const Socket::RemoteQuery& p_query,
                              Callback p_callback,
                              const ClientOptions& p_options)
{
    if (!bool(p_callback))
    {
        return;
    }

    auto conn = GetConnection();

    auto timeoutCallback = [this](std::shared_ptr<Callback> p_callback)
    {
        DecreaseUnfnishedJobCount();
        if (nullptr != p_callback)
        {
            Socket::RemoteSearchResult result;
            result.m_status = Socket::RemoteSearchResult::ResultStatus::Timeout;

            (*p_callback)(std::move(result));
        }
    };


    auto connectCallback = [p_callback, this](bool p_connectSucc)
    {
        if (!p_connectSucc)
        {
            Socket::RemoteSearchResult result;
            result.m_status = Socket::RemoteSearchResult::ResultStatus::FailedNetwork;

            p_callback(std::move(result));
            DecreaseUnfnishedJobCount();
        }
    };

    Socket::Packet packet;
    packet.Header().m_connectionID = c_invalidConnectionID;
    packet.Header().m_packetType = PacketType::SearchRequest;
    packet.Header().m_processStatus = PacketProcessStatus::Ok;
    packet.Header().m_resourceID = m_callbackManager.Add(std::make_shared<Callback>(std::move(p_callback)),
                                                         p_options.m_searchTimeout,
                                                         std::move(timeoutCallback));

    packet.Header().m_bodyLength = static_cast<std::uint32_t>(p_query.EstimateBufferSize());
    packet.AllocateBuffer(packet.Header().m_bodyLength);
    p_query.Write(packet.Body());
    packet.Header().WriteBuffer(packet.HeaderBuffer());

    ++m_unfinishedJobCount;
    m_client->SendPacket(conn.first, std::move(packet), connectCallback);
}


void
ClientWrapper::WaitAllFinished()
{
    if (m_unfinishedJobCount > 0)
    {
        std::unique_lock<std::mutex> lock(m_waitingMutex);
        if (m_unfinishedJobCount > 0)
        {
            m_isWaitingFinish = true;
            m_waitingQueue.wait(lock);
        }
    }
}


PacketHandlerMapPtr
ClientWrapper::GetHandlerMap()
{
    PacketHandlerMapPtr handlerMap(new PacketHandlerMap);
    handlerMap->emplace(PacketType::RegisterResponse,
                        [this](ConnectionID p_localConnectionID, Packet p_packet) -> void
    {
        for (auto& conn : m_connections)
        {
            if (conn.first == p_localConnectionID)
            {
                conn.second = p_packet.Header().m_connectionID;
                return;
            }
        }
    });

    handlerMap->emplace(PacketType::SearchResponse,
                        std::bind(&ClientWrapper::SearchResponseHanlder,
                                  this,
                                  std::placeholders::_1,
                                  std::placeholders::_2));

    return handlerMap;
}


void
ClientWrapper::DecreaseUnfnishedJobCount()
{
    --m_unfinishedJobCount;
    if (0 == m_unfinishedJobCount)
    {
        std::lock_guard<std::mutex> guard(m_waitingMutex);
        if (0 == m_unfinishedJobCount && m_isWaitingFinish)
        {
            m_waitingQueue.notify_all();
            m_isWaitingFinish = false;
        }
    }
}


const ClientWrapper::ConnectionPair&
ClientWrapper::GetConnection()
{
    if (m_connections.size() == 1)
    {
        return m_connections.front();
    }

    std::size_t triedCount = 0;
    std::uint32_t pos = m_spinCountOfConnection.fetch_add(1) % m_connections.size();
    while (c_invalidConnectionID == m_connections[pos].first && triedCount < m_connections.size())
    {
        pos = m_spinCountOfConnection.fetch_add(1) % m_connections.size();
        ++triedCount;
    }

    return m_connections[pos];
}


void
ClientWrapper::SearchResponseHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet)
{
    std::shared_ptr<Callback> callback = m_callbackManager.GetAndRemove(p_packet.Header().m_resourceID);
    if (nullptr == callback)
    {
        return;
    }

    if (p_packet.Header().m_processStatus != PacketProcessStatus::Ok || 0 == p_packet.Header().m_bodyLength)
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

    DecreaseUnfnishedJobCount();
}


void
ClientWrapper::HandleDeadConnection(Socket::ConnectionID p_cid)
{
    for (auto& conn : m_connections)
    {
        if (conn.first == p_cid)
        {
            conn.first = c_invalidConnectionID;
            conn.second = c_invalidConnectionID;

            SPTAG::ErrorCode errCode;
            while (c_invalidConnectionID == conn.first)
            {
                conn.first = m_client->ConnectToServer(m_options.m_serverAddr, m_options.m_serverPort, errCode);
                if (SPTAG::ErrorCode::Socket_FailedResolveEndPoint == errCode)
                {
                    break;
                }
            }

            return;
        }
    }
}


bool
ClientWrapper::IsAvailable() const
{
    return !m_connections.empty();
}
