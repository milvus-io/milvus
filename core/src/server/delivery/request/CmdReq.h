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

#include "BaseReq.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

class CmdReq : public BaseReq {
 public:
    static BaseReqPtr
    Create(const ContextPtr& context, const std::string& cmd, std::string& result);

 protected:
    CmdReq(const ContextPtr& context, const std::string& cmd, std::string& result);

    Status
    OnExecute() override;

 private:
    static std::vector<std::string>
    split(const std::string& src, char delimiter);

    static std::string
    tolower(std::string s);

    static int64_t
    now();

    static int64_t
    uptime();

 private:
    const std::string origin_cmd_;
    const std::string cmd_;
    std::string& result_;

    static int64_t start_time;
};

}  // namespace server
}  // namespace milvus
