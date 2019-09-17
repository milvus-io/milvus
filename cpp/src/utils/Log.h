/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "easylogging++.h"

namespace zilliz {
namespace milvus {

/////////////////////////////////////////////////////////////////////////////////////////////////
#define SERVER_DOMAIN_NAME "[SERVER] "
#define SERVER_ERROR_TEXT "SERVER Error:"

#define SERVER_LOG_TRACE LOG(TRACE) << SERVER_DOMAIN_NAME
#define SERVER_LOG_DEBUG LOG(DEBUG) << SERVER_DOMAIN_NAME
#define SERVER_LOG_INFO LOG(INFO) << SERVER_DOMAIN_NAME
#define SERVER_LOG_WARNING LOG(WARNING) << SERVER_DOMAIN_NAME
#define SERVER_LOG_ERROR LOG(ERROR) << SERVER_DOMAIN_NAME
#define SERVER_LOG_FATAL LOG(FATAL) << SERVER_DOMAIN_NAME

/////////////////////////////////////////////////////////////////////////////////////////////////
#define ENGINE_DOMAIN_NAME "[ENGINE] "
#define ENGINE_ERROR_TEXT "ENGINE Error:"

#define ENGINE_LOG_TRACE LOG(TRACE) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_DEBUG LOG(DEBUG) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_INFO LOG(INFO) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_WARNING LOG(WARNING) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_ERROR LOG(ERROR) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_FATAL LOG(FATAL) << ENGINE_DOMAIN_NAME

/////////////////////////////////////////////////////////////////////////////////////////////////
#define WRAPPER_DOMAIN_NAME "[WRAPPER] "
#define WRAPPER_ERROR_TEXT "WRAPPER Error:"

#define WRAPPER_LOG_TRACE LOG(TRACE) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_DEBUG LOG(DEBUG) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_INFO LOG(INFO) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_WARNING LOG(WARNING) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_ERROR LOG(ERROR) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_FATAL LOG(FATAL) << WRAPPER_DOMAIN_NAME

} // namespace milvus
} // namespace zilliz
