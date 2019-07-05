/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ThriftClient.h"

#include "milvus_types.h"
#include "milvus_constants.h"

#include <exception>

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

namespace milvus {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::concurrency;

ThriftClient::ThriftClient() {

}

ThriftClient::~ThriftClient() {

}

ServiceClientPtr
ThriftClient::interface() {
    if(client_ == nullptr) {
        throw std::exception();
    }
    return client_;
}

Status
ThriftClient::Connect(const std::string& address, int32_t port, const std::string& protocol) {
    try {
        stdcxx::shared_ptr<TSocket> socket_ptr(new transport::TSocket(address, port));
        stdcxx::shared_ptr<TTransport> transport_ptr(new TBufferedTransport(socket_ptr));
        stdcxx::shared_ptr<TProtocol> protocol_ptr;
        if(protocol == THRIFT_PROTOCOL_BINARY) {
            protocol_ptr.reset(new TBinaryProtocol(transport_ptr));
        } else if(protocol == THRIFT_PROTOCOL_JSON) {
            protocol_ptr.reset(new TJSONProtocol(transport_ptr));
        } else if(protocol == THRIFT_PROTOCOL_COMPACT) {
            protocol_ptr.reset(new TCompactProtocol(transport_ptr));
        } else {
            //CLIENT_LOG_ERROR << "Service protocol: " << protocol << " is not supported currently";
            return Status(StatusCode::InvalidAgument, "unsupported protocol");
        }

        transport_ptr->open();
        client_ = std::make_shared<thrift::MilvusServiceClient>(protocol_ptr);
    } catch ( std::exception& ex) {
        //CLIENT_LOG_ERROR << "connect encounter exception: " << ex.what();
        return Status(StatusCode::NotConnected, "failed to connect server" + std::string(ex.what()));
    }

    return Status::OK();
}
Status
ThriftClient::Disconnect() {
    try {
        if(client_ != nullptr) {
            auto protocol = client_->getInputProtocol();
            if(protocol != nullptr) {
                auto transport = protocol->getTransport();
                if(transport != nullptr) {
                    transport->close();
                }
            }
        }
    } catch ( std::exception& ex) {
        //CLIENT_LOG_ERROR << "disconnect encounter exception: " << ex.what();
        return Status(StatusCode::UnknownError, "failed to disconnect: " + std::string(ex.what()));
    }

    return Status::OK();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
ThriftClientSession::ThriftClientSession(const std::string& address, int32_t port, const std::string& protocol) {
    Connect(address, port, protocol);
}

ThriftClientSession::~ThriftClientSession() {
    Disconnect();
}

}