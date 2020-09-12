// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Socket/Client.h"

#include <limits>

using namespace SPTAG::Socket;


Client::Client(const PacketHandlerMapPtr& p_handlerMap,
               std::size_t p_threadNum,
               std::uint32_t p_heartbeatIntervalSeconds)
    : c_requestHandlerMap(p_handlerMap),
      m_connectionManager(new ConnectionManager),
      m_deadlineTimer(m_ioContext),
      m_heartbeatIntervalSeconds(p_heartbeatIntervalSeconds),
      m_stopped(false)
{
    KeepIoContext();
    m_threadPool.reserve(p_threadNum);
    for (std::size_t i = 0; i < p_threadNum; ++i)
    {
        m_threadPool.emplace_back(std::move(std::thread([this]() { m_ioContext.run(); })));
    }
}


Client::~Client()
{
    m_stopped = true;

    m_deadlineTimer.cancel();
    m_connectionManager->StopAll();
    while (!m_ioContext.stopped())
    {
        m_ioContext.stop();
    }

    for (auto& t : m_threadPool)
    {
        t.join();
    }
}


ConnectionID
Client::ConnectToServer(const std::string& p_address,
                        const std::string& p_port,
                        SPTAG::ErrorCode& p_ec)
{
    boost::asio::ip::tcp::resolver resolver(m_ioContext);

    boost::system::error_code errCode;
    auto endPoints = resolver.resolve(p_address, p_port, errCode);
    if (errCode || endPoints.empty())
    {
        p_ec = ErrorCode::Socket_FailedResolveEndPoint;
        return c_invalidConnectionID;
    }

    boost::asio::ip::tcp::socket socket(m_ioContext);
    for (const auto ep : endPoints)
    {
        errCode.clear();
        socket.connect(ep, errCode);
        if (!errCode)
        {
            break;
        }

        socket.close(errCode);
    }

    if (socket.is_open())
    {
        p_ec = ErrorCode::Success;
        return m_connectionManager->AddConnection(std::move(socket),
                                                  c_requestHandlerMap,
                                                  m_heartbeatIntervalSeconds);
    }

    p_ec = ErrorCode::Socket_FailedConnectToEndPoint;
    return c_invalidConnectionID;
}


void 
Client::AsyncConnectToServer(const std::string& p_address,
                             const std::string& p_port,
                             ConnectCallback p_callback)
{
    boost::asio::post(m_ioContext,
                      [this, p_address, p_port, p_callback]()
    {
        SPTAG::ErrorCode errCode;
        auto connID = ConnectToServer(p_address, p_port, errCode);
        if (bool(p_callback))
        {
            p_callback(connID, errCode);
        }
    });
}


void
Client::SendPacket(ConnectionID p_connection, Packet p_packet, std::function<void(bool)> p_callback)
{
    auto connection = m_connectionManager->GetConnection(p_connection);
    if (nullptr != connection)
    {
        connection->AsyncSend(std::move(p_packet), std::move(p_callback));
    }
    else if (bool(p_callback))
    {
        p_callback(false);
    }
}


void
Client::SetEventOnConnectionClose(std::function<void(ConnectionID)> p_event)
{
    m_connectionManager->SetEventOnRemoving(std::move(p_event));
}


void
Client::KeepIoContext()
{
    if (m_stopped)
    {
        return;
    }

    m_deadlineTimer.expires_from_now(boost::posix_time::hours(24));
    m_deadlineTimer.async_wait([this](boost::system::error_code p_ec)
    {
        this->KeepIoContext();
    });
}
