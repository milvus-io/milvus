/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "metrics/Metrics.h"
#include "MegasearchThreadPoolServer.h"

namespace zilliz {
namespace milvus {
namespace server {

void
MegasearchThreadPoolServer::onClientConnected(const std::shared_ptr<apache::thrift::server::TConnectedClient> &pClient) {
    server::Metrics::GetInstance().ConnectionGaugeIncrement();
    TThreadPoolServer::onClientConnected(pClient);
}

void
MegasearchThreadPoolServer::onClientDisconnected(apache::thrift::server::TConnectedClient *pClient) {
    server::Metrics::GetInstance().ConnectionGaugeDecrement();
    TThreadPoolServer::onClientDisconnected(pClient);
}
zilliz::milvus::server::MegasearchThreadPoolServer::MegasearchThreadPoolServer(const std::shared_ptr<apache::thrift::TProcessor> &processor,
                                                                                const std::shared_ptr<apache::thrift::transport::TServerTransport> &serverTransport,
                                                                                const std::shared_ptr<apache::thrift::transport::TTransportFactory> &transportFactory,
                                                                                const std::shared_ptr<apache::thrift::protocol::TProtocolFactory> &protocolFactory,
                                                                                const std::shared_ptr<apache::thrift::concurrency::ThreadManager> &threadManager)
    : TThreadPoolServer(processor, serverTransport, transportFactory, protocolFactory, threadManager) {

}
}
}
}