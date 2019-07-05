/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <easylogging++.h>

namespace zilliz {
namespace milvus {
namespace engine {

#define ENGINE_DOMAIN_NAME "[ENGINE] "
#define ENGINE_ERROR_TEXT "ENGINE Error:"

#define ENGINE_LOG_TRACE LOG(TRACE) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_DEBUG LOG(DEBUG) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_INFO LOG(INFO) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_WARNING LOG(WARNING) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_ERROR LOG(ERROR) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_FATAL LOG(FATAL) << ENGINE_DOMAIN_NAME

} // namespace sql
} // namespace zilliz
} // namespace server
