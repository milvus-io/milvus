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

#include "utils/Error.h"
#include "ConfigNode.h"

namespace zilliz {
namespace milvus {
namespace server {

// this class can parse nested config file and return config item
// config file example(yaml style)
//       AAA: 1
//       BBB:
//         CCC: hello
//         DDD: 23.5
//
// usage
//   const ConfigMgr* mgr = ConfigMgr::GetInstance();
//   const ConfigNode& node = mgr->GetRootNode();
//   std::string val = node.GetValue("AAA"); // return '1'
//   const ConfigNode& child = node.GetChild("BBB");
//   val = child.GetValue("CCC"); //return 'hello'

class ConfigMgr {
 public:
    static ConfigMgr* GetInstance();

    virtual ErrorCode LoadConfigFile(const std::string &filename) = 0;
    virtual void Print() const = 0;//will be deleted
    virtual std::string DumpString() const = 0;

    virtual const ConfigNode& GetRootNode() const = 0;
    virtual ConfigNode& GetRootNode() = 0;
};

}
}
}
