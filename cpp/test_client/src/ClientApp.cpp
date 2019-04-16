/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientApp.h"
#include "server/ServerConfig.h"
#include "utils/CommonUtil.h"

#include <iostream>
#include <yaml-cpp/yaml.h>

#include "thrift/gen-cpp/VecService.h"
#include "thrift/gen-cpp/VectorService_types.h"
#include "thrift/gen-cpp/VectorService_constants.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/PosixThreadFactory.h>

namespace zilliz {
namespace vecwise {
namespace client {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::concurrency;

void ClientApp::Run(const std::string &config_file) {
    server::ServerConfig& config = server::ServerConfig::GetInstance();
    config.LoadConfigFile(config_file);

    server::ConfigNode server_config = config.GetConfig(server::CONFIG_SERVER);
    std::string address = server_config.GetValue(server::CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(server::CONFIG_SERVER_PORT, 33001);
    std::string protocol = server_config.GetValue(server::CONFIG_SERVER_PROTOCOL, "binary");
    std::string mode = server_config.GetValue(server::CONFIG_SERVER_MODE, "thread_pool");

    ::apache::thrift::stdcxx::shared_ptr<TProtocolFactory> protocolFactory;
    if(protocol == "binary") {
        protocolFactory.reset(new TBinaryProtocolFactory());
    } else if(protocol == "json") {
        protocolFactory.reset(new TJSONProtocolFactory());
    }

    if(mode == "simple") {

    } else if(mode == "thread_pool") {

    } else {
        server::CommonUtil::PrintError("Server mode: " + mode + " is not supported currently");
    }
}

}
}
}

