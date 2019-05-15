/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientSession.h"
#include "Log.h"

#include "thrift/gen-cpp/megasearch_types.h"
#include "thrift/gen-cpp/megasearch_constants.h"

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

namespace zilliz {
namespace vecwise {
namespace client {

using namespace megasearch;

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::concurrency;

ClientSession::ClientSession(const std::string &address, int32_t port, const std::string &protocol)
: client_(nullptr) {
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
        client_ = std::make_shared<VecServiceClient>(protocol_ptr);
    } catch ( std::exception& ex) {
        CLIENT_LOG_ERROR << "connect encounter exception: " << ex.what();
    }

}

ClientSession::~ClientSession() {
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
        CLIENT_LOG_ERROR << "disconnect encounter exception: " << ex.what();
    }
}

VecServiceClientPtr ClientSession::interface() {
    if(client_ == nullptr) {
        throw std::exception();
    }
    return client_;
}

}
}
}