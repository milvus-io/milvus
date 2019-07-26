/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MilvusService.h"
#include "Status.h"

#include <memory>

namespace milvus {

using ServiceClientPtr = std::shared_ptr<::milvus::thrift::MilvusServiceClient>;

static const char* THRIFT_PROTOCOL_JSON = "json";
static const char* THRIFT_PROTOCOL_BINARY = "binary";
static const char* THRIFT_PROTOCOL_COMPACT = "compact";

class ThriftClient {
public:
    ThriftClient();
    virtual ~ThriftClient();

    ServiceClientPtr interface();

    Status Connect(const std::string& address, int32_t port, const std::string& protocol);
    Status Disconnect();

private:
    ServiceClientPtr client_;
};


class ThriftClientSession : public ThriftClient {
public:
    ThriftClientSession(const std::string& address, int32_t port, const std::string& protocol);
    ~ThriftClientSession();
};

}
