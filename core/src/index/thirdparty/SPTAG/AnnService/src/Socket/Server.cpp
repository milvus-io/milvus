// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Socket/Server.h"

#include <exception>

using namespace SPTAG::Socket;

Server::Server(const std::string& p_address,
               const std::string& p_port,
               const PacketHandlerMapPtr& p_handlerMap,
               std::size_t p_threadNum)
    : m_requestHandlerMap(p_handlerMap),
      m_connectionManager(new ConnectionManager),
      m_acceptor(m_ioContext)
{
    boost::asio::ip::tcp::resolver resolver(m_ioContext);

    boost::system::error_code errCode;
    auto endPoints = resolver.resolve(p_address, p_port, errCode);
    if (errCode)
    {
        fprintf(stderr,
                "Failed to resolve %s %s, error: %s",
                p_address.c_str(),
                p_port.c_str(),
                errCode.message().c_str());

        throw std::runtime_error("Failed to resolve address.");
    }

    boost::asio::ip::tcp::endpoint endpoint = *(endPoints.begin());
    m_acceptor.open(endpoint.protocol());
    m_acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(false));

    m_acceptor.bind(endpoint, errCode);
    if (errCode)
    {
        fprintf(stderr,
                "Failed to bind %s %s, error: %s",
                p_address.c_str(),
                p_port.c_str(),
                errCode.message().c_str());

        throw std::runtime_error("Failed to bind port.");
    }

    m_acceptor.listen(boost::asio::socket_base::max_listen_connections, errCode);
    if (errCode)
    {
        fprintf(stderr,
                "Failed to listen %s %s, error: %s",
                p_address.c_str(),
                p_port.c_str(),
                errCode.message().c_str());

        throw std::runtime_error("Failed to listen port.");
    }

    StartAccept();
 
    m_threadPool.reserve(p_threadNum);
    for (std::size_t i = 0; i < p_threadNum; ++i)
    {
        m_threadPool.emplace_back(std::move(std::thread([this]() { StartListen(); })));
    }
}


Server::~Server()
{
    m_acceptor.close();
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


void
Server::SetEventOnConnectionClose(std::function<void(ConnectionID)> p_event)
{
    m_connectionManager->SetEventOnRemoving(std::move(p_event));
}


void
Server::StartAccept()
{
    m_acceptor.async_accept([this](boost::system::error_code p_ec,
                                   boost::asio::ip::tcp::socket p_socket)
                            {
                                if (!m_acceptor.is_open())
                                {
                                    return;
                                }

                                if (!p_ec)
                                {
                                    m_connectionManager->AddConnection(std::move(p_socket),
                                                                       m_requestHandlerMap,
                                                                       0);
                                }

                                StartAccept();
                            });
}


void
Server::StartListen()
{
    m_ioContext.run();
}


void
Server::SendPacket(ConnectionID p_connection, Packet p_packet, std::function<void(bool)> p_callback)
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
