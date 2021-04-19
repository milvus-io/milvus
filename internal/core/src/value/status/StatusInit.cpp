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

#include <string>
#include <unordered_map>

#include "value/status/ServerStatus.h"

namespace milvus {

#define Bool_(name, modifiable, default, is_valid) \
    { #name, CreateBoolValue(#name, modifiable, server_status.name, default, is_valid) }
#define String_(name, modifiable, default, is_valid) \
    { #name, CreateStringValue(#name, modifiable, server_status.name, default, is_valid) }
#define Enum_(name, modifiable, enumd, default, is_valid) \
    { #name, CreateEnumValue(#name, modifiable, enumd, server_status.name, default, is_valid) }
#define Integer_(name, modifiable, lower_bound, upper_bound, default, is_valid) \
    { #name, CreateIntegerValue(#name, modifiable, lower_bound, upper_bound, server_status.name, default, is_valid) }
#define Floating_(name, modifiable, lower_bound, upper_bound, default, is_valid) \
    { #name, CreateFloatingValue(#name, modifiable, lower_bound, upper_bound, server_status.name, default, is_valid) }
#define Size_(name, modifiable, lower_bound, upper_bound, default, is_valid) \
    { #name, CreateSizeValue(#name, modifiable, lower_bound, upper_bound, server_status.name, default, is_valid) }

#define Bool(name, default) Bool_(name, true, default, nullptr)
#define String(name, default) String_(name, true, default, nullptr)
#define Enum(name, enumd, default) Enum_(name, true, enumd, default, nullptr)
#define Integer(name, lower_bound, upper_bound, default) \
    Integer_(name, true, lower_bound, upper_bound, default, nullptr)
#define Floating(name, lower_bound, upper_bound, default) \
    Floating_(name, true, lower_bound, upper_bound, default, nullptr)
#define Size(name, lower_bound, upper_bound, default) Size_(name, true, lower_bound, upper_bound, default, nullptr)

std::unordered_map<std::string, BaseValuePtr>
InitStatus() {
    return std::unordered_map<std::string, BaseValuePtr>{
        Bool(indexing, false),
    };
}

}  // namespace milvus
