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

#pragma once
#include <string>
#include "ReferenceProxy.h"

namespace milvus {
namespace engine {
namespace snapshot {

template <typename... Fields>
class DBBaseResource : public ReferenceProxy, public Fields... {
 public:
    DBBaseResource(const Fields&... fields);

    virtual std::string
    ToString() const;

    virtual ~DBBaseResource() {
    }
};

template <typename... Fields>
DBBaseResource<Fields...>::DBBaseResource(const Fields&... fields) : Fields(fields)... {
    /* InstallField("id"); */
    /* InstallField("status"); */
    /* InstallField("created_on"); */
    /* std::vector<std::string> attrs = {Fields::ATTR...}; */
}

template <typename... Fields>
std::string
DBBaseResource<Fields...>::ToString() const {
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
