// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Server/SearchService.h"
#include "inc/Server/SearchExecutor.h"
#include "inc/Socket/RemoteSearchQuery.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Helper/ArgumentsParser.h"

#include <iostream>

using namespace SPTAG;
using namespace SPTAG::Service;


namespace
{
namespace Local
{

class SerivceCmdOptions : public Helper::ArgumentsParser
{
public:
    SerivceCmdOptions()
        : m_serveMode("interactive"),
          m_configFile("AnnService.ini")
    {
        AddOptionalOption(m_serveMode, "-m", "--mode", "Service mode, interactive or socket.");
        AddOptionalOption(m_configFile, "-c", "--config", "Service config file path.");
    }

    virtual ~SerivceCmdOptions()
    {
    }

    std::string m_serveMode;

    std::string m_configFile;
};

}

} // namespace


SearchService::SearchService()
    : m_initialized(false),
      m_shutdownSignals(m_ioContext),
      m_serveMode(ServeMode::Interactive)
{
}


SearchService::~SearchService()
{
}


bool
SearchService::Initialize(int p_argNum, char* p_args[])
{
    Local::SerivceCmdOptions cmdOptions;
    if (!cmdOptions.Parse(p_argNum - 1, p_args + 1))
    {
        return false;
    }

    if (Helper::StrUtils::StrEqualIgnoreCase(cmdOptions.m_serveMode.c_str(), "interactive"))
    {
        m_serveMode = ServeMode::Interactive;
    }
    else if (Helper::StrUtils::StrEqualIgnoreCase(cmdOptions.m_serveMode.c_str(), "socket"))
    {
        m_serveMode = ServeMode::Socket;
    }
    else
    {
        fprintf(stderr, "Failed parse Serve Mode!\n");
        return false;
    }

    m_serviceContext.reset(new ServiceContext(cmdOptions.m_configFile));

    m_initialized = m_serviceContext->IsInitialized();

    return m_initialized;
}


void
SearchService::Run()
{
    if (!m_initialized)
    {
        return;
    }

    switch (m_serveMode)
    {
    case ServeMode::Interactive:
        RunInteractiveMode();
        break;

    case ServeMode::Socket:
        RunSocketMode();
        break;

    default:
        break;
    }
}


void
SearchService::RunSocketMode()
{
    auto threadNum = max((SizeType)1, m_serviceContext->GetServiceSettings()->m_threadNum);
    m_threadPool.reset(new boost::asio::thread_pool(threadNum));

    Socket::PacketHandlerMapPtr handlerMap(new Socket::PacketHandlerMap);
    handlerMap->emplace(Socket::PacketType::SearchRequest,
                        [this](Socket::ConnectionID p_srcID, Socket::Packet p_packet)
                        {
                            boost::asio::post(*m_threadPool, std::bind(&SearchService::SearchHanlder, this, p_srcID, std::move(p_packet)));
                        });

    m_socketServer.reset(new Socket::Server(m_serviceContext->GetServiceSettings()->m_listenAddr,
                                            m_serviceContext->GetServiceSettings()->m_listenPort,
                                            handlerMap,
                                            m_serviceContext->GetServiceSettings()->m_socketThreadNum));

    fprintf(stderr,
            "Start to listen %s:%s ...\n",
            m_serviceContext->GetServiceSettings()->m_listenAddr.c_str(),
            m_serviceContext->GetServiceSettings()->m_listenPort.c_str());

    m_shutdownSignals.add(SIGINT);
    m_shutdownSignals.add(SIGTERM);
#ifdef SIGQUIT
    m_shutdownSignals.add(SIGQUIT);
#endif

    m_shutdownSignals.async_wait([this](boost::system::error_code p_ec, int p_signal)
                                 {
                                     fprintf(stderr, "Received shutdown signals.\n");
                                 });

    m_ioContext.run();
    fprintf(stderr, "Start shutdown procedure.\n");

    m_socketServer.reset();
    m_threadPool->stop();
    m_threadPool->join();
}


void
SearchService::RunInteractiveMode()
{
    const std::size_t bufferSize = 1 << 16;
    std::unique_ptr<char[]> inputBuffer(new char[bufferSize]);
    while (true)
    {
        std::cout << "Query: ";
        if (!fgets(inputBuffer.get(), bufferSize, stdin))
        {
            break;
        }

        auto callback = [](std::shared_ptr<SearchExecutionContext> p_exeContext)
        {
            std::cout << "Result:" << std::endl;
            if (nullptr == p_exeContext)
            {
                std::cout << "Not Executed." << std::endl;
                return;
            }

            const auto& results = p_exeContext->GetResults();
            for (const auto& result : results)
            {
                std::cout << "Index: " << result.m_indexName << std::endl;
                int idx = 0;
                for (const auto& res : result.m_results)
                {
                    std::cout << "------------------" << std::endl;
                    std::cout << "DocIndex: " << res.VID << " Distance: " << res.Dist;
                    if (result.m_results.WithMeta())
                    {
                        const auto& metadata = result.m_results.GetMetadata(idx);
                        std::cout << " MetaData: " << std::string((char*)metadata.Data(), metadata.Length());
                    } 
                    std::cout << std::endl;
                    ++idx;
                }
            }
        };

        SearchExecutor executor(inputBuffer.get(), m_serviceContext, callback);
        executor.Execute();
    }
}


void
SearchService::SearchHanlder(Socket::ConnectionID p_localConnectionID, Socket::Packet p_packet)
{
    if (p_packet.Header().m_bodyLength == 0)
    {
        return;
    }

    if (Socket::c_invalidConnectionID == p_packet.Header().m_connectionID)
    {
        p_packet.Header().m_connectionID = p_localConnectionID;
    }

    Socket::RemoteQuery remoteQuery;
    remoteQuery.Read(p_packet.Body());

    auto callback = std::bind(&SearchService::SearchHanlderCallback,
                              this,
                              std::placeholders::_1,
                              std::move(p_packet));

    SearchExecutor executor(std::move(remoteQuery.m_queryString),
                            m_serviceContext,
                            callback);
    executor.Execute();
}


void
SearchService::SearchHanlderCallback(std::shared_ptr<SearchExecutionContext> p_exeContext,
                                     Socket::Packet p_srcPacket)
{
    Socket::Packet ret;
    ret.Header().m_packetType = Socket::PacketType::SearchResponse;
    ret.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
    ret.Header().m_connectionID = p_srcPacket.Header().m_connectionID;
    ret.Header().m_resourceID = p_srcPacket.Header().m_resourceID;

    if (nullptr == p_exeContext)
    {
        ret.Header().m_processStatus = Socket::PacketProcessStatus::Failed;
        ret.AllocateBuffer(0);
        ret.Header().WriteBuffer(ret.HeaderBuffer());
    }
    else
    {
        Socket::RemoteSearchResult remoteResult;
        remoteResult.m_status = Socket::RemoteSearchResult::ResultStatus::Success;
        remoteResult.m_allIndexResults.swap(p_exeContext->GetResults());
        ret.AllocateBuffer(static_cast<std::uint32_t>(remoteResult.EstimateBufferSize()));
        auto bodyEnd = remoteResult.Write(ret.Body());
        
        ret.Header().m_bodyLength = static_cast<std::uint32_t>(bodyEnd - ret.Body());
        ret.Header().WriteBuffer(ret.HeaderBuffer());
    }

    m_socketServer->SendPacket(p_srcPacket.Header().m_connectionID, std::move(ret), nullptr);
}
