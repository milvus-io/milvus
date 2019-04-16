/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <easylogging++.h>

namespace zilliz {
namespace vecwise {
namespace client {

#define CLIENT_DOMAIN_NAME "[CLIENT] "
#define CLIENT_ERROR_TEXT "CLIENT Error:"

#define CLIENT_LOG_TRACE LOG(TRACE) << CLIENT_DOMAIN_NAME
#define CLIENT_LOG_DEBUG LOG(DEBUG) << CLIENT_DOMAIN_NAME
#define CLIENT_LOG_INFO LOG(INFO) << CLIENT_DOMAIN_NAME
#define CLIENT_LOG_WARNING LOG(WARNING) << CLIENT_DOMAIN_NAME
#define CLIENT_LOG_ERROR LOG(ERROR) << CLIENT_DOMAIN_NAME
#define CLIENT_LOG_FATAL LOG(FATAL) << CLIENT_DOMAIN_NAME

} // namespace sql
} // namespace zilliz
} // namespace server
