// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SOCKET_CONNECTION_H_
#define _SPTAG_SOCKET_CONNECTION_H_

#include "Packet.h"

#include <cstdint>
#include <memory>
#include <atomic>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/io_service_strand.hpp>
#include <boost/asio/deadline_timer.hpp>

namespace SPTAG
{
namespace Socket
{

class ConnectionManager;

class Connection : public std::enable_shared_from_this<Connection>
{
public:
    typedef std::shared_ptr<Connection> Ptr;

    Connection(ConnectionID p_connectionID,
               boost::asio::ip::tcp::socket&& p_socket,
               const PacketHandlerMapPtr& p_handlerMap,
               std::weak_ptr<ConnectionManager> p_connectionManager);

    void Start();

    void Stop();

    void StartHeartbeat(std::size_t p_intervalSeconds);

    void AsyncSend(Packet p_packet, std::function<void(bool)> p_callback);

    ConnectionID GetConnectionID() const;

    ConnectionID GetRemoteConnectionID() const;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

private:
    void AsyncReadHeader();

    void AsyncReadBody();

    void HandleReadHeader(boost::system::error_code p_ec, std::size_t p_bytesTransferred);

    void HandleReadBody(boost::system::error_code p_ec, std::size_t p_bytesTransferred);

    void SendHeartbeat(std::size_t p_intervalSeconds);

    void SendRegister();

    void HandleHeartbeatRequest();

    void HandleRegisterRequest();

    void HandleRegisterResponse();

    void HandleNoHandlerResponse();

    void OnConnectionFail(const boost::system::error_code& p_ec);

private:
    const ConnectionID c_connectionID;

    ConnectionID m_remoteConnectionID;

    const std::weak_ptr<ConnectionManager> c_connectionManager;

    const PacketHandlerMapPtr c_handlerMap;

    boost::asio::ip::tcp::socket m_socket;

    boost::asio::io_context::strand m_strand;

    boost::asio::deadline_timer m_heartbeatTimer;

    std::uint8_t m_packetHeaderReadBuffer[PacketHeader::c_bufferSize];

    Packet m_packetRead;

    std::atomic_bool m_stopped;

    std::atomic_bool m_heartbeatStarted;
};


} // namespace Socket
} // namespace SPTAG

#endif // _SPTAG_SOCKET_CONNECTION_H_
