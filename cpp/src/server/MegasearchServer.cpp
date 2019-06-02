/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "MegasearchServer.h"
#include "MegasearchHandler.h"
#include "megasearch_types.h"
#include "megasearch_constants.h"
#include "ServerConfig.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include <thread>

namespace zilliz {
namespace vecwise {
namespace server {

using namespace megasearch::thrift;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

static stdcxx::shared_ptr<TServer> s_server;

void
MegasearchServer::StartService() {
    if(s_server != nullptr){
        StopService();
    }

    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_SERVER);

    std::string address = server_config.GetValue(CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(CONFIG_SERVER_PORT, 33001);
    std::string protocol = server_config.GetValue(CONFIG_SERVER_PROTOCOL, "binary");
    std::string mode = server_config.GetValue(CONFIG_SERVER_MODE, "thread_pool");

    try {
        stdcxx::shared_ptr<MegasearchServiceHandler> handler(new MegasearchServiceHandler());
        stdcxx::shared_ptr<TProcessor> processor(new MegasearchServiceProcessor(handler));
        stdcxx::shared_ptr<TServerTransport> server_transport(new TServerSocket(address, port));
        stdcxx::shared_ptr<TTransportFactory> transport_factory(new TBufferedTransportFactory());

        stdcxx::shared_ptr<TProtocolFactory> protocol_factory;
        if (protocol == "binary") {
            protocol_factory.reset(new TBinaryProtocolFactory());
        } else if (protocol == "json") {
            protocol_factory.reset(new TJSONProtocolFactory());
        } else if (protocol == "compact") {
            protocol_factory.reset(new TCompactProtocolFactory());
        } else {
            //SERVER_LOG_INFO << "Service protocol: " << protocol << " is not supported currently";
            return;
        }

        std::string mode = "thread_pool";
        if (mode == "simple") {
            s_server.reset(new TSimpleServer(processor, server_transport, transport_factory, protocol_factory));
            s_server->serve();
        } else if (mode == "thread_pool") {
            stdcxx::shared_ptr<ThreadManager> threadManager(ThreadManager::newSimpleThreadManager());
            stdcxx::shared_ptr<PosixThreadFactory> threadFactory(new PosixThreadFactory());
            threadManager->threadFactory(threadFactory);
            threadManager->start();

            s_server.reset(new TThreadPoolServer(processor,
                                                 server_transport,
                                                 transport_factory,
                                                 protocol_factory,
                                                 threadManager));
            s_server->serve();
        } else {
            //SERVER_LOG_INFO << "Service mode: " << mode << " is not supported currently";
            return;
        }
    } catch (apache::thrift::TException& ex) {
        //SERVER_LOG_ERROR << "Server encounter exception: " << ex.what();
    }
}

void
MegasearchServer::StopService() {
    auto stop_server_worker = [&]{
        if(s_server != nullptr) {
            s_server->stop();
        }
    };

    std::shared_ptr<std::thread> stop_thread = std::make_shared<std::thread>(stop_server_worker);
    stop_thread->join();
}

}
}
}