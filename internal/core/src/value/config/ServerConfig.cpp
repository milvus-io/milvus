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

#include <fiu/fiu-local.h>

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "value/config/ServerConfig.h"

namespace milvus {

std::mutex config_mutex;

ServerConfig config;

std::vector<std::string>
ParsePreloadCollection(const std::string& str) {
    std::stringstream ss(str);
    std::vector<std::string> collections;
    std::string collection;

    while (std::getline(ss, collection, ',')) {
        collections.push_back(collection);
    }
    return collections;
}

std::vector<int64_t>
ParseGPUDevices(const std::string& str) {
    std::stringstream ss(str);
    std::vector<int64_t> devices;
    std::unordered_set<int64_t> device_set;
    std::string device;

    while (std::getline(ss, device, ',')) {
        fiu_do_on("ParseGPUDevices.invalid_format", device = "");
        if (device.length() < 4) {
            /* Invalid format string */
            return {};
        }
        device_set.insert(std::stoll(device.substr(3)));
    }

    devices.reserve(device_set.size());
    for (auto dev : device_set) {
        devices.push_back(dev);
    }
    return devices;
}

}  // namespace milvus
