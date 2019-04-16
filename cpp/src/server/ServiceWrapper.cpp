/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ServiceWrapper.h"
#include "ServerConfig.h"

#include "utils/CommonUtil.h"

#include "thrift/gen-cpp/VecService.h"
#include "thrift/gen-cpp/VectorService_types.h"
#include "thrift/gen-cpp/VectorService_constants.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
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

    /**
     * group interfaces
     *
     * @param group
     */
    void add_group(const VecGroup& group) {
        // Your implementation goes here
        printf("add_group\n");
    }

    void get_group(VecGroup& _return, const std::string& group_id) {
        // Your implementation goes here
        printf("get_group\n");
    }

    void del_group(const std::string& group_id) {
        // Your implementation goes here
        printf("del_group\n");
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
        printf("add_vector\n");
    }

    void add_vector_batch(VecTensorIdList& _return, const std::string& group_id, const VecTensorList& tensor_list) {
        // Your implementation goes here
        printf("add_vector_batch\n");
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
        printf("search_vector\n");
    }

    void search_vector_batch(VecSearchResultList& _return, const std::string& group_id, const int64_t top_k, const VecTensorList& tensor_list, const VecTimeRangeList& time_range_list) {
        // Your implementation goes here
        printf("search_vector_batch\n");
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

    if(mode == "simple") {
        ::apache::thrift::stdcxx::shared_ptr<TServerTransport> serverTransport(new TServerSocket(address, port));
        ::apache::thrift::stdcxx::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
        ::apache::thrift::stdcxx::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
        s_server.reset(new TSimpleServer(processor, serverTransport, transportFactory, protocolFactory));
        s_server->serve();
    } else if(mode == "thread_pool") {
        ::apache::thrift::stdcxx::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
        ::apache::thrift::stdcxx::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
        ::apache::thrift::stdcxx::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

        ::apache::thrift::stdcxx::shared_ptr<ThreadManager> threadManager(ThreadManager::newSimpleThreadManager(1));
        ::apache::thrift::stdcxx::shared_ptr<PosixThreadFactory> threadFactory(new PosixThreadFactory());
        threadManager->threadFactory(threadFactory);
        threadManager->start();

        s_server.reset(new TThreadPoolServer(processor, serverTransport, transportFactory, protocolFactory, threadManager));
        s_server->serve();
    } else {
        CommonUtil::PrintError("Server mode: " + mode + " is not supported currently");
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