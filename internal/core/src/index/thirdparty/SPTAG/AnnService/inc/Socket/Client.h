// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SOCKET_CLIENT_H_
#define _SPTAG_SOCKET_CLIENT_H_

#include "inc/Core/Common.h"
#include "Connection.h"
#include "ConnectionManager.h"
#include "Packet.h"

#include <string>
#include <memory>
#include <atomic>
#include <boost/asio.hpp>

namespace SPTAG
{
namespace Socket
{

class Client
{
public:
    typedef std::function<void(ConnectionID p_cid, SPTAG::ErrorCode)> ConnectCallback;

    Client(const PacketHandlerMapPtr& p_handlerMap,
           std::size_t p_threadNum,
           std::uint32_t p_heartbeatIntervalSeconds);

    ~Client();

    ConnectionID ConnectToServer(const std::string& p_address,
                                 const std::string& p_port,
                                 SPTAG::ErrorCode& p_ec);

    void AsyncConnectToServer(const std::string& p_address,
                              const std::string& p_port,
                              ConnectCallback p_callback);

    void SendPacket(ConnectionID p_connection, Packet p_packet, std::function<void(bool)> p_callback);

    void SetEventOnConnectionClose(std::function<void(ConnectionID)> p_event);

private:
    void KeepIoContext();

private:
    std::atomic_bool m_stopped;

    std::uint32_t m_heartbeatIntervalSeconds;

    boost::asio::io_context m_ioContext;

    boost::asio::deadline_timer m_deadlineTimer;

    std::shared_ptr<ConnectionManager> m_connectionManager;

    std::vector<std::thread> m_threadPool;

    const PacketHandlerMapPtr c_requestHandlerMap;
};


} // namespace Socket
} // namespace SPTAG

#endif // _SPTAG_SOCKET_CLIENT_H_
