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

#include <string>
#include <sstream>
#include "easylogging++.h"

namespace zilliz {
namespace milvus {
namespace server {
int32_t InitLog(const std::string& log_config_file);
inline std::string GetFileName(std::string filename) {
    int pos = filename.find_last_of('/');
    return filename.substr(pos + 1);
}
void RolloutHandler(const char *filename, std::size_t size, el::Level level);

#define SHOW_LOCATION
#ifdef SHOW_LOCATION
#define LOCATION_INFO "[" << zilliz::sql::server::GetFileName(__FILE__) << ":" << __LINE__ << "] "
#else
#define LOCATION_INFO ""
#endif

}
}
}
