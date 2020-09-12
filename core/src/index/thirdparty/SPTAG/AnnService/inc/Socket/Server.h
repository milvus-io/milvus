// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SOCKET_SERVER_H_
#define _SPTAG_SOCKET_SERVER_H_

#include "Connection.h"
#include "ConnectionManager.h"
#include "Packet.h"

#include <string>
#include <memory>
#include <boost/asio.hpp>

namespace SPTAG
{
namespace Socket
{

class Server
{
public:
    Server(const std::string& p_address,
           const std::string& p_port,
           const PacketHandlerMapPtr& p_handlerMap,
           std::size_t p_threadNum);

    ~Server();

    void StartListen();

    void SendPacket(ConnectionID p_connection, Packet p_packet, std::function<void(bool)> p_callback);

    void SetEventOnConnectionClose(std::function<void(ConnectionID)> p_event);

private:
    void StartAccept();

private:
    boost::asio::io_context m_ioContext;

    boost::asio::ip::tcp::acceptor m_acceptor;

    std::shared_ptr<ConnectionManager> m_connectionManager;

    std::vector<std::thread> m_threadPool;

    const PacketHandlerMapPtr m_requestHandlerMap;
};


} // namespace Socket
} // namespace SPTAG

#endif // _SPTAG_SOCKET_SERVER_H_
