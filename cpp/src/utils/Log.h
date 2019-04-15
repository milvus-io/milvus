/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Error.h"
#include <easylogging++.h>

namespace zilliz {
namespace vecwise {
namespace server {

#define SERVER_DOMAIN_NAME "[SERVER] "
#define SERVER_ERROR_TEXT "SERVER Error:"

#define SERVER_LOG_TRACE LOG(TRACE) << SERVER_DOMAIN_NAME
#define SERVER_LOG_DEBUG LOG(DEBUG) << SERVER_DOMAIN_NAME
#define SERVER_LOG_INFO LOG(INFO) << SERVER_DOMAIN_NAME
#define SERVER_LOG_WARNING LOG(WARNING) << SERVER_DOMAIN_NAME
#define SERVER_LOG_ERROR LOG(ERROR) << SERVER_DOMAIN_NAME
#define SERVER_LOG_FATAL LOG(FATAL) << SERVER_DOMAIN_NAME

#define SERVER_ERROR(error)                             \
    ({                                                  \
        SERVER_LOG_ERROR << SERVER_ERROR_TEXT << error; \
        (error);                                        \
    })

#define SERVER_CHECK(func)                                  \
    {                                                       \
        zilliz::vecwise::server::ServerError error = func;  \
        if (error != zilliz::vecwise::server::SERVER_SUCCESS) { \
            return SERVER_ERROR(error);                     \
        }                                                   \
    }                                                       \

} // namespace sql
} // namespace zilliz
} // namespace server
