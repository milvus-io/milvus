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
#include <thrift/transport/TSocket.h>
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


    ::apache::thrift::stdcxx::shared_ptr<TSocket> socket_ptr(new ::apache::thrift::transport::TSocket(address, port));
    ::apache::thrift::stdcxx::shared_ptr<TTransport> transport_ptr(new TBufferedTransport(socket_ptr));
    ::apache::thrift::stdcxx::shared_ptr<TProtocol> protocol_ptr;
    if(protocol == "binary") {
        protocol_ptr.reset(new TBinaryProtocol(transport_ptr));
    } else if(protocol == "json") {
        protocol_ptr.reset(new TJSONProtocol(transport_ptr));
    } else {
        server::CommonUtil::PrintError("Service protocol: " + protocol + " is not supported currently");
        return;
    }

    transport_ptr->open();
    VecServiceClient client(protocol_ptr);
    try {
        client.dummy();

        VecGroup group;
        group.id = "test_group";
        group.dimension = 256;
        group.index_type = 0;
        client.add_group(group);
    } catch (apache::thrift::TException& ex) {
        printf("%s", ex.what());
    }

    transport_ptr->close();
}

}
}
}

