// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SOCKET_CONNECTIONMANAGER_H_
#define _SPTAG_SOCKET_CONNECTIONMANAGER_H_

#include "Connection.h"
#include "inc/Helper/Concurrent.h"

#include <cstdint>
#include <memory>
#include <atomic>
#include <mutex>
#include <array>

#include <boost/asio/ip/tcp.hpp>

namespace SPTAG
{
namespace Socket
{

class ConnectionManager : public std::enable_shared_from_this<ConnectionManager>
{
public:
    ConnectionManager();

    ConnectionID AddConnection(boost::asio::ip::tcp::socket&& p_socket,
                               const PacketHandlerMapPtr& p_handlerMap,
                               std::uint32_t p_heartbeatIntervalSeconds);

    void RemoveConnection(ConnectionID p_connectionID);

    Connection::Ptr GetConnection(ConnectionID p_connectionID);

    void SetEventOnRemoving(std::function<void(ConnectionID)> p_event);

    void StopAll();

private:
    inline static std::uint32_t GetPosition(ConnectionID p_connectionID);

private:
    static constexpr std::uint32_t c_connectionPoolSize = 1 << 8;

    static constexpr std::uint32_t c_connectionPoolMask = c_connectionPoolSize - 1;

    struct ConnectionItem
    {
        ConnectionItem();

        std::atomic_bool m_isEmpty;

        Connection::Ptr m_connection;
    };

    // Start from 1. 0 means not assigned.
    std::atomic<ConnectionID> m_nextConnectionID;

    std::atomic<std::uint32_t> m_connectionCount;

    std::array<ConnectionItem, c_connectionPoolSize> m_connections;

    Helper::Concurrent::SpinLock m_spinLock;

    std::function<void(ConnectionID)> m_eventOnRemoving;
};


} // namespace Socket
} // namespace SPTAG

#endif // _SPTAG_SOCKET_CONNECTIONMANAGER_H_
