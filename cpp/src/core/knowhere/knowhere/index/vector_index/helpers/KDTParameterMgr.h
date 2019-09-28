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

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace zilliz {
namespace knowhere {

using KDTParameter = std::pair<std::string, std::string>;

class KDTParameterMgr {
 public:
    const std::vector<KDTParameter>&
    GetKDTParameters();

 public:
    static KDTParameterMgr&
    GetInstance() {
        static KDTParameterMgr instance;
        return instance;
    }

    KDTParameterMgr(const KDTParameterMgr&) = delete;
    KDTParameterMgr&
    operator=(const KDTParameterMgr&) = delete;

 private:
    KDTParameterMgr();

 private:
    std::vector<KDTParameter> kdt_parameters_;
};

}  // namespace knowhere
}  // namespace zilliz
