/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ServiceWrapper.h"
#include "ServerConfig.h"

#include "utils/Log.h"

#include "thrift/gen-cpp/VecService.h"
#include "thrift/gen-cpp/VectorService_types.h"
#include "thrift/gen-cpp/VectorService_constants.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/server/TSimpleServer.h>
//#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include <thread>

namespace zilliz {
namespace vecwise {
namespace server {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

class VecServiceHandler : virtual public VecServiceIf {
public:
    VecServiceHandler() {
        // Your initialization goes here
    }

    void dummy() {
        // Your implementation goes here
        SERVER_LOG_INFO << "dummy() called";
    }

    /**
     * group interfaces
     *
     * @param group
     */
    void add_group(const VecGroup& group) {
        // Your implementation goes here
        SERVER_LOG_INFO << "add_group() called";
    }

    void get_group(VecGroup& _return, const std::string& group_id) {
        // Your implementation goes here
        SERVER_LOG_INFO << "get_group() called";
    }

    void del_group(const std::string& group_id) {
        // Your implementation goes here
        SERVER_LOG_INFO << "del_group() called";
    }

    /**
     * vector interfaces
     *
     *
     * @param group_id
     * @param tensor
     */
    int64_t add_vector(const std::string& group_id, const VecTensor& tensor) {
        // Your implementation goes here
        SERVER_LOG_INFO << "add_vector() called";
    }

    void add_vector_batch(VecTensorIdList& _return, const std::string& group_id, const VecTensorList& tensor_list) {
        // Your implementation goes here
        SERVER_LOG_INFO << "add_vector_batch() called";
    }

    /**
     * search interfaces
     * if time_range_list is empty, engine will search without time limit
     *
     * @param group_id
     * @param top_k
     * @param tensor
     * @param time_range_list
     */
    void search_vector(VecSearchResult& _return, const std::string& group_id, const int64_t top_k, const VecTensor& tensor, const VecTimeRangeList& time_range_list) {
        // Your implementation goes here
        SERVER_LOG_INFO << "search_vector() called";
    }

    void search_vector_batch(VecSearchResultList& _return, const std::string& group_id, const int64_t top_k, const VecTensorList& tensor_list, const VecTimeRangeList& time_range_list) {
        // Your implementation goes here
        SERVER_LOG_INFO << "search_vector_batch() called";
    }

};

static ::apache::thrift::stdcxx::shared_ptr<TServer> s_server;

void ServiceWrapper::StartService() {
    if(s_server != nullptr){
        StopService();
    }

    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_SERVER);

    std::string address = server_config.GetValue(CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(CONFIG_SERVER_PORT, 33001);
    std::string protocol = server_config.GetValue(CONFIG_SERVER_PROTOCOL, "binary");
    std::string mode = server_config.GetValue(CONFIG_SERVER_MODE, "thread_pool");

    ::apache::thrift::stdcxx::shared_ptr<VecServiceHandler> handler(new VecServiceHandler());
    ::apache::thrift::stdcxx::shared_ptr<TProcessor> processor(new VecServiceProcessor(handler));
    ::apache::thrift::stdcxx::shared_ptr<TServerTransport> server_transport(new TServerSocket(address, port));
    ::apache::thrift::stdcxx::shared_ptr<TTransportFactory> transport_factory(new TBufferedTransportFactory());

    ::apache::thrift::stdcxx::shared_ptr<TProtocolFactory> protocol_factory;
    if(protocol == "binary") {
        protocol_factory.reset(new TBinaryProtocolFactory());
    } else if(protocol == "json") {
        protocol_factory.reset(new TJSONProtocolFactory());
    } else if(protocol == "compact") {
        protocol_factory.reset(new TCompactProtocolFactory());
    } else if(protocol == "debug") {
        protocol_factory.reset(new TDebugProtocolFactory());
    } else {
        SERVER_LOG_INFO << "Service protocol: " << protocol << " is not supported currently";
        return;
    }

    if(mode == "simple") {
        s_server.reset(new TSimpleServer(processor, server_transport, transport_factory, protocol_factory));
        s_server->serve();
//    } else if(mode == "non_blocking") {
//        ::apache::thrift::stdcxx::shared_ptr<TNonblockingServerTransport> nb_server_transport(new TServerSocket(address, port));
//        ::apache::thrift::stdcxx::shared_ptr<ThreadManager> threadManager(ThreadManager::newSimpleThreadManager());
//        ::apache::thrift::stdcxx::shared_ptr<PosixThreadFactory> threadFactory(new PosixThreadFactory());
//        threadManager->threadFactory(threadFactory);
//        threadManager->start();
//
//        s_server.reset(new TNonblockingServer(processor,
//                                              protocol_factory,
//                                              nb_server_transport,
//                                              threadManager));
    } else if(mode == "thread_pool") {
        ::apache::thrift::stdcxx::shared_ptr<ThreadManager> threadManager(ThreadManager::newSimpleThreadManager());
        ::apache::thrift::stdcxx::shared_ptr<PosixThreadFactory> threadFactory(new PosixThreadFactory());
        threadManager->threadFactory(threadFactory);
        threadManager->start();

        s_server.reset(new TThreadPoolServer(processor,
                                             server_transport,
                                             transport_factory,
                                             protocol_factory,
                                             threadManager));
        s_server->serve();
    } else {
        SERVER_LOG_INFO << "Service mode: " << mode << " is not supported currently";
        return;
    }
}

void ServiceWrapper::StopService() {
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