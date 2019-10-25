// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Socket/Connection.h"
#include "inc/Socket/ConnectionManager.h"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/bind.hpp>

#include <chrono>

using namespace SPTAG::Socket;

Connection::Connection(ConnectionID p_connectionID,
                       boost::asio::ip::tcp::socket&& p_socket,
                       const PacketHandlerMapPtr& p_handlerMap,
                       std::weak_ptr<ConnectionManager> p_connectionManager)
    : c_connectionID(p_connectionID),
      c_handlerMap(p_handlerMap),
      c_connectionManager(std::move(p_connectionManager)),
      m_socket(std::move(p_socket)),
      m_strand(p_socket.get_executor().context()),
      m_heartbeatTimer(p_socket.get_executor().context()),
      m_remoteConnectionID(c_invalidConnectionID),
      m_stopped(true),
      m_heartbeatStarted(false)
{
}


void
Connection::Start()
{
#ifdef _DEBUG
    fprintf(stderr, "Connection Start, local: %u, remote: %s:%u\n",
            static_cast<uint32_t>(m_socket.local_endpoint().port()),
            m_socket.remote_endpoint().address().to_string().c_str(),
            static_cast<uint32_t>(m_socket.remote_endpoint().port()));
#endif

    if (!m_stopped.exchange(false))
    {
        return;
    }

    SendRegister();
    AsyncReadHeader();
}


void
Connection::Stop()
{
#ifdef _DEBUG
    fprintf(stderr, "Connection Stop, local: %u, remote: %s:%u\n",
            static_cast<uint32_t>(m_socket.local_endpoint().port()),
            m_socket.remote_endpoint().address().to_string().c_str(),
            static_cast<uint32_t>(m_socket.remote_endpoint().port()));
#endif

    if (m_stopped.exchange(true))
    {
        return;
    }

    boost::system::error_code errCode;
    if (m_heartbeatStarted.exchange(false))
    {
        m_heartbeatTimer.cancel(errCode);
    }
    
    m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, errCode);
    m_socket.close(errCode);
}


void
Connection::StartHeartbeat(std::size_t p_intervalSeconds)
{
    if (m_stopped || m_heartbeatStarted.exchange(true))
    {
        return;
    }

    SendHeartbeat(p_intervalSeconds);
}


ConnectionID
Connection::GetConnectionID() const
{
    return c_connectionID;
}


ConnectionID
Connection::GetRemoteConnectionID() const
{
    return m_remoteConnectionID;
}


void
Connection::AsyncSend(Packet p_packet, std::function<void(bool)> p_callback)
{
    if (m_stopped)
    {
        if (bool(p_callback))
        {
            p_callback(false);
        }

        return;
    }

    auto sharedThis = shared_from_this();
    boost::asio::post(m_strand,
                      [sharedThis, p_packet, p_callback]()
                      {
                          auto handler = [p_callback, p_packet, sharedThis](boost::system::error_code p_ec,
                                                                            std::size_t p_bytesTransferred)
                                         {
                                             if (p_ec && boost::asio::error::operation_aborted != p_ec)
                                             {
                                                 sharedThis->OnConnectionFail(p_ec);
                                             }

                                             if (bool(p_callback))
                                             {
                                                 p_callback(!p_ec);
                                             }
                                         };

                          boost::asio::async_write(sharedThis->m_socket,
                                                   boost::asio::buffer(p_packet.Buffer(),
                                                                       p_packet.BufferLength()),
                                                   std::move(handler));
                      });
}


void
Connection::AsyncReadHeader()
{
    if (m_stopped)
    {
        return;
    }

    auto sharedThis = shared_from_this();
    boost::asio::post(m_strand,
                      [sharedThis]()
                      {
                          auto handler = boost::bind(&Connection::HandleReadHeader,
                                                     sharedThis,
                                                     boost::asio::placeholders::error,
                                                     boost::asio::placeholders::bytes_transferred);
                  
                          boost::asio::async_read(sharedThis->m_socket,
                                                  boost::asio::buffer(sharedThis->m_packetHeaderReadBuffer),
                                                  std::move(handler));
                      });
}


void
Connection::AsyncReadBody()
{
    if (m_stopped)
    {
        return;
    }

    auto sharedThis = shared_from_this();
    boost::asio::post(m_strand,
                      [sharedThis]()
                      {
                          auto handler = boost::bind(&Connection::HandleReadBody,
                                                     sharedThis,
                                                     boost::asio::placeholders::error,
                                                     boost::asio::placeholders::bytes_transferred);

                          boost::asio::async_read(sharedThis->m_socket,
                                                  boost::asio::buffer(sharedThis->m_packetRead.Body(),
                                                                      sharedThis->m_packetRead.Header().m_bodyLength),
                                                  std::move(handler));
                      });
}


void
Connection::HandleReadHeader(boost::system::error_code p_ec, std::size_t p_bytesTransferred)
{
    if (!p_ec)
    {
        m_packetRead.Header().ReadBuffer(m_packetHeaderReadBuffer);
        if (m_packetRead.Header().m_bodyLength > 0)
        {
            m_packetRead.AllocateBuffer(m_packetRead.Header().m_bodyLength);
            AsyncReadBody();
        }
        else
        {
            HandleReadBody(p_ec, p_bytesTransferred);
        }

        return;
    }
    else if (boost::asio::error::operation_aborted != p_ec)
    {
        OnConnectionFail(p_ec);
        return;
    }

    AsyncReadHeader();
}


void
Connection::HandleReadBody(boost::system::error_code p_ec, std::size_t p_bytesTransferred)
{
    if (!p_ec)
    {
        bool foundHanlder = true;
        switch (m_packetRead.Header().m_packetType)
        {
        case PacketType::HeartbeatRequest:
            HandleHeartbeatRequest();
            break;

        case PacketType::HeartbeatResponse:
            break;

        case PacketType::RegisterRequest:
            HandleRegisterRequest();
            break;

        case PacketType::RegisterResponse:
            HandleRegisterResponse();
            break;

        default:
            foundHanlder = false;
            break;
        }

        if (nullptr != c_handlerMap)
        {
            auto iter = c_handlerMap->find(m_packetRead.Header().m_packetType);
            if (c_handlerMap->cend() != iter && bool(iter->second))
            {
                (iter->second)(c_connectionID, std::move(m_packetRead));
                foundHanlder = true;
            }
        }

        if (!foundHanlder)
        {
            HandleNoHandlerResponse();
        }
    }
    else if (boost::asio::error::operation_aborted != p_ec)
    {
        OnConnectionFail(p_ec);
        return;
    }

    AsyncReadHeader();
}


void
Connection::SendHeartbeat(std::size_t p_intervalSeconds)
{
    if (m_stopped)
    {
        return;
    }

    Packet msg;
    msg.Header().m_packetType = PacketType::HeartbeatRequest;
    msg.Header().m_processStatus = PacketProcessStatus::Ok;
    msg.Header().m_connectionID = 0;
    
    msg.AllocateBuffer(0);
    msg.Header().WriteBuffer(msg.HeaderBuffer());

    AsyncSend(std::move(msg), nullptr);

    m_heartbeatTimer.expires_from_now(boost::posix_time::seconds(p_intervalSeconds));
    m_heartbeatTimer.async_wait(boost::bind(&Connection::SendHeartbeat,
                                            shared_from_this(),
                                            p_intervalSeconds));
}


void
Connection::SendRegister()
{
    Packet msg;
    msg.Header().m_packetType = PacketType::RegisterRequest;
    msg.Header().m_processStatus = PacketProcessStatus::Ok;
    msg.Header().m_connectionID = 0;

    msg.AllocateBuffer(0);
    msg.Header().WriteBuffer(msg.HeaderBuffer());

    AsyncSend(std::move(msg), nullptr);
}


void
Connection::HandleHeartbeatRequest()
{
    Packet msg;
    msg.Header().m_packetType = PacketType::HeartbeatResponse;
    msg.Header().m_processStatus = PacketProcessStatus::Ok;

    msg.AllocateBuffer(0);

    if (0 == m_packetRead.Header().m_connectionID
        || c_connectionID == m_packetRead.Header().m_connectionID)
    {
        m_packetRead.Header().m_connectionID;
        msg.Header().WriteBuffer(msg.HeaderBuffer());

        AsyncSend(std::move(msg), nullptr);
    }
    else
    {
        msg.Header().m_connectionID = m_packetRead.Header().m_connectionID;
        msg.Header().WriteBuffer(msg.HeaderBuffer());

        auto mgr = c_connectionManager.lock();
        if (nullptr != mgr)
        {
            auto con = mgr->GetConnection(m_packetRead.Header().m_connectionID);
            if (nullptr != con)
            {
                con->AsyncSend(std::move(msg), nullptr);
            }
        }
    }
}


void
Connection::HandleRegisterRequest()
{
    Packet msg;
    msg.Header().m_packetType = PacketType::RegisterResponse;
    msg.Header().m_processStatus = PacketProcessStatus::Ok;
    msg.Header().m_connectionID = c_connectionID;
    msg.Header().m_resourceID = m_packetRead.Header().m_resourceID;

    msg.AllocateBuffer(0);
    msg.Header().WriteBuffer(msg.HeaderBuffer());

    AsyncSend(std::move(msg), nullptr);
}


void
Connection::HandleRegisterResponse()
{
    m_remoteConnectionID = m_packetRead.Header().m_connectionID;
}


void
Connection::HandleNoHandlerResponse()
{
    auto packetType = m_packetRead.Header().m_packetType;
    if (!PacketTypeHelper::IsRequestPacket(packetType))
    {
        return;
    }

    Packet msg;
    msg.Header().m_packetType = PacketTypeHelper::GetCrosspondingResponseType(packetType);
    msg.Header().m_processStatus = PacketProcessStatus::Dropped;
    msg.Header().m_connectionID = c_connectionID;
    msg.Header().m_resourceID = m_packetRead.Header().m_resourceID;

    msg.AllocateBuffer(0);
    msg.Header().WriteBuffer(msg.HeaderBuffer());

    AsyncSend(std::move(msg), nullptr);
}


void
Connection::OnConnectionFail(const boost::system::error_code& p_ec)
{
    auto mgr = c_connectionManager.lock();
    if (nullptr != mgr)
    {
        mgr->RemoveConnection(c_connectionID);
    }
}
