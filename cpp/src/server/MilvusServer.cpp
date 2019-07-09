/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "MilvusServer.h"
#include "RequestHandler.h"
#include "ServerConfig.h"
#include "ThreadPoolServer.h"
#include "DBWrapper.h"

#include "milvus_types.h"
#include "milvus_constants.h"
#include "faiss/utils.h"

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
#include <iostream>

//extern int distance_compute_blas_threshold;

namespace zilliz {
namespace milvus {
namespace server {

using namespace ::milvus::thrift;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

static stdcxx::shared_ptr<TServer> s_server;

void
MilvusServer::StartService() {
    if(s_server != nullptr){
        StopService();
    }

    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_SERVER);
    ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
    std::string address = server_config.GetValue(CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(CONFIG_SERVER_PORT, 19530);
    std::string protocol = server_config.GetValue(CONFIG_SERVER_PROTOCOL, "binary");
    faiss::distance_compute_blas_threshold = engine_config.GetInt32Value(CONFIG_DCBT,20);
//    std::cout<<"distance_compute_blas_threshold = "<< faiss::distance_compute_blas_threshold << std::endl;
    try {
        DBWrapper::DB();//initialize db

        stdcxx::shared_ptr<RequestHandler> handler(new RequestHandler());
        stdcxx::shared_ptr<TProcessor> processor(new MilvusServiceProcessor(handler));
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
            // SERVER_LOG_INFO << "Service protocol: " << protocol << " is not supported currently";
            return;
        }

        stdcxx::shared_ptr<ThreadManager> threadManager(ThreadManager::newSimpleThreadManager());
        stdcxx::shared_ptr<PosixThreadFactory> threadFactory(new PosixThreadFactory());
        threadManager->threadFactory(threadFactory);
        threadManager->start();

        s_server.reset(new ThreadPoolServer(processor,
                                             server_transport,
                                             transport_factory,
                                             protocol_factory,
                                             threadManager));
        s_server->serve();

    } catch (apache::thrift::TException& ex) {
        std::cout << "ERROR! " << ex.what() << std::endl;
        kill(0, SIGUSR1);
    }
}

void
MilvusServer::StopService() {
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