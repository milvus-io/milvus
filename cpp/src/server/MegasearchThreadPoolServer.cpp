/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "metrics/Metrics.h"


#include "MegasearchThreadPoolServer.h"


void zilliz::vecwise::server::MegasearchThreadPoolServer::onClientConnected(const std::shared_ptr<apache::thrift::server::TConnectedClient> &pClient) {
    server::Metrics::GetInstance().ConnectionGaugeIncrement();
    TThreadPoolServer::onClientConnected(pClient);
}
void zilliz::vecwise::server::MegasearchThreadPoolServer::onClientDisconnected(apache::thrift::server::TConnectedClient *pClient) {
    server::Metrics::GetInstance().ConnectionGaugeDecrement();
    TThreadPoolServer::onClientDisconnected(pClient);
}
