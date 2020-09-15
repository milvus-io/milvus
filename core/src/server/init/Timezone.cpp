// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/init/Timezone.h"

#include <ctime>

namespace milvus::server {

Status
Timezone::SetTimezone(std::string time_zone) {
    try {
        if (time_zone.length() == 3) {
            time_zone = "CUT";
        } else {
            int time_bias = std::stoi(time_zone.substr(3, std::string::npos));
            if (time_bias == 0) {
                time_zone = "CUT";
            } else if (time_bias > 0) {
                time_zone = "CUT" + std::to_string(-time_bias);
            } else {
                time_zone = "CUT+" + std::to_string(-time_bias);
            }
        }

        if (setenv("TZ", time_zone.c_str(), 1) != 0) {
            std::string msg = "Cannot set timezone: " + time_zone + ", setenv failed.";
            return Status(SERVER_UNEXPECTED_ERROR, msg);
        }
        tzset();

        return Status::OK();
    } catch (...) {
        return Status(SERVER_UNEXPECTED_ERROR, "Cannot set timezone: " + time_zone);
    }
}
}  // namespace milvus::server
