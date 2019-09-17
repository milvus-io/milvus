// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#pragma once

#include "utils/easylogging++.h"

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

