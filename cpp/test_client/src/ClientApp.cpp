/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientApp.h"
#include "server/ServerConfig.h"
#include "Log.h"

#include <iostream>
#include <yaml-cpp/yaml.h>

#include "thrift/gen-cpp/VecService.h"
#include "thrift/gen-cpp/VectorService_types.h"
#include "thrift/gen-cpp/VectorService_constants.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
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

    CLIENT_LOG_INFO << "Load config file:" << config_file;

    server::ConfigNode server_config = config.GetConfig(server::CONFIG_SERVER);
    std::string address = server_config.GetValue(server::CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(server::CONFIG_SERVER_PORT, 33001);
    std::string protocol = server_config.GetValue(server::CONFIG_SERVER_PROTOCOL, "binary");
    std::string mode = server_config.GetValue(server::CONFIG_SERVER_MODE, "thread_pool");

    CLIENT_LOG_INFO << "Connect to server: " << address << ":" << std::to_string(port);

    try {
        stdcxx::shared_ptr<TSocket> socket_ptr(new transport::TSocket(address, port));
        stdcxx::shared_ptr<TTransport> transport_ptr(new TBufferedTransport(socket_ptr));
        stdcxx::shared_ptr<TProtocol> protocol_ptr;
        if(protocol == "binary") {
            protocol_ptr.reset(new TBinaryProtocol(transport_ptr));
        } else if(protocol == "json") {
            protocol_ptr.reset(new TJSONProtocol(transport_ptr));
        } else if(protocol == "compact") {
            protocol_ptr.reset(new TCompactProtocol(transport_ptr));
        } else if(protocol == "debug") {
            protocol_ptr.reset(new TDebugProtocol(transport_ptr));
        } else {
            CLIENT_LOG_ERROR << "Service protocol: " << protocol << " is not supported currently";
            return;
        }

        transport_ptr->open();
        VecServiceClient client(protocol_ptr);
        try {
            const int32_t dim = 256;
            VecGroup group;
            group.id = "test_group";
            group.dimension = dim;
            group.index_type = 0;
            client.add_group(group);

            VecTensor tensor;
            for(int32_t i = 0; i < dim; i++) {
                tensor.tensor.push_back((double)i);
            }
            VecTensorIdList result;
            client.add_vector(result, group.id, tensor);

        } catch (apache::thrift::TException& ex) {
            printf("%s", ex.what());
        }

        transport_ptr->close();
    } catch (apache::thrift::TException& ex) {
        CLIENT_LOG_ERROR << "Server encounter exception: " << ex.what();
    }

    CLIENT_LOG_INFO << "Test finished";
}

}
}
}

