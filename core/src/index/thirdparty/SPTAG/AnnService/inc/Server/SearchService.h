// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SERVER_SERVICE_H_
#define _SPTAG_SERVER_SERVICE_H_

#include "ServiceContext.h"
#include "../Socket/Server.h"

#include <boost/asio.hpp>

#include <memory>
#include <vector>
#include <thread>
#include <condition_variable>

namespace SPTAG
{
namespace Service
{

class SearchExecutionContext;

class SearchService
{
public:
    SearchService();

    ~SearchService();

    bool Initialize(int p_argNum, char* p_args[]);

    void Run();

private:
    void RunSocketMode();

    void RunInteractiveMode();

    void SearchHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet);

    void SearchHanlderCallback(std::shared_ptr<SearchExecutionContext> p_exeContext,
                               Socket::Packet p_srcPacket);

private:
    enum class ServeMode : std::uint8_t
    {
        Interactive,

        Socket
    };

    std::shared_ptr<ServiceContext> m_serviceContext;

    std::shared_ptr<Socket::Server> m_socketServer;

    bool m_initialized;

    ServeMode m_serveMode;

    std::unique_ptr<boost::asio::thread_pool> m_threadPool;

    boost::asio::io_context m_ioContext;

    boost::asio::signal_set m_shutdownSignals;
};


} // namespace Server
} // namespace AnnService


#endif // _SPTAG_SERVER_SERVICE_H_
