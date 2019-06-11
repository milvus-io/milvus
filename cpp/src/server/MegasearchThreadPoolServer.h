/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#pragma once

#include <thrift/server/TThreadPoolServer.h>


namespace zilliz {
namespace vecwise {
namespace server {

class MegasearchThreadPoolServer : public apache::thrift::server::TThreadPoolServer {
 public:
    MegasearchThreadPoolServer(
        const std::shared_ptr<apache::thrift::TProcessor>& processor,
        const std::shared_ptr<apache::thrift::transport::TServerTransport>& serverTransport,
        const std::shared_ptr<apache::thrift::transport::TTransportFactory>& transportFactory,
        const std::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protocolFactory,
        const std::shared_ptr<apache::thrift::concurrency::ThreadManager>& threadManager
        = apache::thrift::concurrency::ThreadManager::newSimpleThreadManager());

 protected:
    void onClientConnected(const std::shared_ptr<apache::thrift::server::TConnectedClient>& pClient) override ;
    void onClientDisconnected(apache::thrift::server::TConnectedClient* pClient) override ;
};

}
}
}