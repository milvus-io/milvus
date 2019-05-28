/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MegasearchService.h"
#include "Status.h"

#include <memory>

namespace megasearch {

using MegasearchServiceClientPtr = std::shared_ptr<megasearch::thrift::MegasearchServiceClient>;

class ThriftClient {
public:
    ThriftClient();
    virtual ~ThriftClient();

    MegasearchServiceClientPtr interface();

    Status Connect(const std::string& address, int32_t port, const std::string& protocol);
    Status Disconnect();

private:
    MegasearchServiceClientPtr client_;
};


class ThriftClientSession : public ThriftClient {
public:
    ThriftClientSession(const std::string& address, int32_t port, const std::string& protocol);
    ~ThriftClientSession();
};

}
