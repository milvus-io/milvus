////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <easylogging++.h>

namespace zilliz {
namespace milvus {
namespace engine {

#define WRAPPER_DOMAIN_NAME "[WRAPPER] "
#define WRAPPER_ERROR_TEXT "WRAPPER Error:"

#define WRAPPER_LOG_TRACE LOG(TRACE) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_DEBUG LOG(DEBUG) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_INFO LOG(INFO) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_WARNING LOG(WARNING) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_ERROR LOG(ERROR) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_FATAL LOG(FATAL) << WRAPPER_DOMAIN_NAME

}
}
}

