/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "milvus.grpc.pb.h"
#include "MilvusServer.h"
#include "ServerConfig.h"
#include "DBWrapper.h"
#include "utils/Log.h"
#include "faiss/utils.h"
#include "RequestHandler.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/grpcpp.h>

namespace zilliz {
namespace milvus {
namespace server {
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

static std::unique_ptr<Server> server;

void
MilvusServer::StartService() {
    if (server != nullptr){
        std::cout << "stopservice!\n";
        StopService();
    }

    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_SERVER);
    ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
    std::string address = server_config.GetValue(CONFIG_SERVER_ADDRESS, "127.0.0.1");
    int32_t port = server_config.GetInt32Value(CONFIG_SERVER_PORT, 19530);

    faiss::distance_compute_blas_threshold = engine_config.GetInt32Value(CONFIG_DCBT, 20);
    //TODO:add exception handle
    DBWrapper::DB();//initialize db

    std::string server_address("127.0.0.1:19530");

    ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(400 * 1024 * 1024); //default 4 * 1024 * 1024
    builder.SetMaxSendMessageSize(400 * 1024 * 1024);

    builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_STREAM_GZIP, true);

//    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_DEFLATE);
//    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_GZIP);
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_STREAM_GZIP);
//    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_ALGORITHMS_COUNT);
    builder.SetDefaultCompressionLevel(GRPC_COMPRESS_LEVEL_HIGH);

    RequestHandler service;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    builder.RegisterService(&service);

    builder.SetSyncServerOption(builder.MIN_POLLERS, 10);
    builder.SetSyncServerOption(builder.MAX_POLLERS, 10);

    server = builder.BuildAndStart();

    server->Wait();

}

void
MilvusServer::StopService() {
    auto stop_server_worker = [&]{
        if (server != nullptr) {
            server->Shutdown();
        }
    };

    std::shared_ptr<std::thread> stop_thread = std::make_shared<std::thread>(stop_server_worker);
    stop_thread->join();
}

}
}
}